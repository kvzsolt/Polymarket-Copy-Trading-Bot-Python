"""Production adapters for the venue-neutral copy-task event processor.

These adapters translate detection rows and follower runtime snapshots. Venue
submission still flows exclusively through ``ExecutionCoordinator``; this
module never calls CCXT or a repository mutation method directly.
"""

from __future__ import annotations

from pmcopy.storage.postgres.risk_lot_repository import RiskLotCodec

import logging
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Protocol, cast

from pmcopy.domain.source_time import SourceTimestampParser, TransportReceipt
from pmcopy.services.metrics import LatencyPhase
from pmcopy.services.telemetry.latency import PipelineLatency
from pmcopy.domain.detection import (
    DetectionFinality,
    DetectionMode,
    LeaderTradeEvent,
    TradeSide,
)
from pmcopy.services.copy_tasks.domain import CopyTask
from pmcopy.services.copy_tasks.lots import RiskLotService
from pmcopy.services.copy_tasks.event_routing import (
    CopyTaskSourceEvent,
    FollowerTaskSnapshot,
    TaskExecutionCommand,
)
from pmcopy.services.copy_tasks.lifecycle import (
    TaskExecutionDisposition,
    TaskExecutionOutcome,
)
from pmcopy.services.execution.coordinator import (
    ExecutionCoordinatorResult,
    ExecutionIntent,
)
from pmcopy.services.onchain_detection.leader_sell_scope_guard import LeaderSellBuyScopeGuard

_ZERO = Decimal("0")

logger = logging.getLogger(__name__)


class Clock(Protocol):
    """Supply timezone-aware UTC time for deterministic normalization."""

    def now(self) -> datetime:
        """Return the current UTC instant."""

        ...


class SystemClock:
    """Production UTC clock."""

    def now(self) -> datetime:
        """Return the current timezone-aware UTC instant."""

        return datetime.now(UTC)


class FollowerBalanceReader(Protocol):
    """Read collateral for one follower wallet."""

    async def read(self, wallet: str) -> Decimal | None:
        """Return available pUSD collateral or ``None`` on failure."""

        ...


class ActiveReservationReader(Protocol):
    """Read exact active quote reservations for one follower."""

    async def sum_active_quote_for_user(self, user_id: str) -> Decimal:
        """Return active BUY collateral reservations in pUSD."""

        ...


class TaskSnapshotUnavailable(RuntimeError):
    """Raised when an authoritative task input cannot be obtained."""


class TaskExecutionUnavailable(RuntimeError):
    """Raised when a follower lacks the canonical execution coordinator."""


class _RuntimeContextRegistry:
    """Keep one canonical runtime context per follower user and wallet."""

    def __init__(self, contexts: Iterable[object]) -> None:
        self._by_identity: dict[str, object] = {}
        self.add(contexts)

    def add(self, contexts: Iterable[object]) -> None:
        """Register contexts while preserving the first account authority."""

        for context in contexts:
            identities = self._identities(context)
            if not identities or any(identity in self._by_identity for identity in identities):
                continue
            for identity in identities:
                self._by_identity[identity] = context

    def resolve(self, follower_account: str) -> object | None:
        """Return the canonical context for a user or wallet identity."""

        return self._by_identity.get(str(follower_account).strip().lower())

    @staticmethod
    def _identities(context: object) -> frozenset[str]:
        return frozenset(
            identity
            for identity in (
                str(getattr(context, "user_id", "") or "").strip().lower(),
                str(getattr(context, "wallet_address", "") or "").strip().lower(),
            )
            if identity
        )


class _TaskContextRegistry:
    """Resolve leader-owned follower context data without account ambiguity."""

    def __init__(self, contexts: Iterable[object]) -> None:
        self._contexts: list[object] = []
        self._keys: set[tuple[frozenset[str], str]] = set()
        self.add(contexts)

    def add(self, contexts: Iterable[object]) -> None:
        """Register each follower-and-leader binding once."""

        for context in contexts:
            identities = _RuntimeContextRegistry._identities(context)
            leader = self._leader(context)
            key = (identities, leader)
            if not identities or key in self._keys:
                continue
            self._keys.add(key)
            self._contexts.append(context)

    def resolve(self, follower_account: str, leader_wallet: str) -> object | None:
        """Return the exact leader binding or an unambiguous account fallback."""

        account = str(follower_account).strip().lower()
        leader = str(leader_wallet).strip().lower()
        matches = [
            context
            for context in self._contexts
            if account in _RuntimeContextRegistry._identities(context)
        ]
        exact = [context for context in matches if self._leader(context) == leader]
        if len(exact) == 1:
            return exact[0]
        if not exact and len(matches) == 1 and not self._leader(matches[0]):
            return matches[0]
        return None

    @staticmethod
    def _leader(context: object) -> str:
        for name in ("leader_wallet", "leader_id", "leader_address"):
            value = str(getattr(context, name, "") or "").strip().lower()
            if value:
                return value
        extra = getattr(context, "extra", {}) or {}
        if isinstance(extra, Mapping):
            return str(extra.get("leader_wallet") or "").strip().lower()
        return ""


class CopyTaskWorkerPort(Protocol):
    """Bounded per-leader queue surface used by detection ingress."""

    async def submit_batch(
        self,
        leader: str,
        events: Iterable[CopyTaskSourceEvent],
    ) -> None:
        """Atomically submit one normalized source batch."""

        ...


class CopyTaskEventHandler(Protocol):
    """Worker callback implemented by the task event processor."""

    async def __call__(self, leader: str, event: CopyTaskSourceEvent) -> None:
        """Process one normalized leader event."""

        ...


class OnchainSignal(Protocol):
    """Narrow on-chain signal shape consumed by the ingress."""

    leader_wallet: str
    provisional: bool

    def as_raw_signal_dict(self) -> dict[str, object]:
        """Return the shared detector dictionary representation."""

        ...


class DeferredCopyTaskEventHandler:
    """Bind the processor exactly once after the runtime core is constructed."""

    def __init__(self) -> None:
        self._handler: CopyTaskEventHandler | None = None

    def bind(self, handler: CopyTaskEventHandler) -> None:
        """Attach the sole processor before any worker is started."""

        if self._handler is not None:
            raise RuntimeError("copy-task event handler is already bound")
        self._handler = handler

    async def __call__(self, leader: str, event: CopyTaskSourceEvent) -> None:
        """Delegate one worker event or fail if composition is incomplete."""

        if self._handler is None:
            raise RuntimeError("copy-task event handler is not bound")
        await self._handler(leader, event)


class LeaderEventNormalizer:
    """Translate REST/on-chain dictionaries into immutable source events."""

    def __init__(
        self, *, clock: Clock | None = None, latency: PipelineLatency | None = None
    ) -> None:
        self._clock = clock or SystemClock()
        self._latency = latency

    def normalize_raw(
        self,
        leader: str,
        raw: Mapping[str, object],
        *,
        mode: DetectionMode,
    ) -> CopyTaskSourceEvent:
        """Normalize one detected fill without deriving missing market facts.

        Args:
            leader: Watched leader wallet.
            raw: Sanitized source row from polling or on-chain detection.
            mode: Detection source that produced the row.

        Returns:
            Immutable event accepted by the copy-task worker.

        Raises:
            ValueError: If identity, side, price, size, or notional is invalid.
        """

        now = self._aware(self._clock.now(), name="detected_at")
        side_text = self._text(raw, "side").upper()
        try:
            side = TradeSide(side_text)
        except ValueError as exc:
            raise ValueError("source side must be BUY or SELL") from exc
        token_id = self._first_text(raw, "tokenID", "token_id", "asset", "asset_id")
        condition_id = self._first_text(raw, "conditionId", "condition_id")
        if not token_id or not condition_id:
            raise ValueError("source token_id and condition_id are required")
        price = self._decimal(raw.get("price"), name="price", positive=True)
        shares = self._decimal(
            self._first_value(raw, "size", "size_tokens", "shares"),
            name="shares",
            positive=True,
        )
        notional_raw = self._first_value(raw, "usdcSize", "usdc_size", "notional")
        notional = (
            shares * price
            if notional_raw is None or not str(notional_raw).strip()
            else self._decimal(notional_raw, name="notional", positive=True)
        )
        provisional = bool(raw.get("provisional", False))
        finality = DetectionFinality.PROVISIONAL if provisional else DetectionFinality.CONFIRMED
        venue_timestamp = self._timestamp(
            self._first_value(raw, "event_ts", "timestamp"),
            unit=raw.get("timestamp_unit"),
        )
        log_index_raw = raw.get("log_index")
        log_index = None if log_index_raw is None else int(str(log_index_raw))
        receipt = raw.get("_source_receipt")
        if receipt is not None and not isinstance(receipt, TransportReceipt):
            raise ValueError("Source receipt must be validated transport evidence")
        trade = LeaderTradeEvent(
            leader_wallet=str(leader),
            token_id=token_id,
            condition_id=condition_id,
            side=side,
            price=price,
            shares=shares,
            notional=notional,
            mode=mode,
            finality=finality,
            detected_at=receipt.received_at if receipt is not None else now,
            source_receipt=receipt,
            source_action_id=self._first_text(raw, "source_action_id") or None,
            venue_timestamp=venue_timestamp,
            transaction_hash=(
                self._first_text(
                    raw,
                    "transactionHash",
                    "txHash",
                    "transaction_hash",
                    "tx_hash",
                )
                or None
            ),
            log_index=log_index,
            order_hash=self._first_text(raw, "order_hash", "orderHash") or None,
            source_identity=self._first_text(
                raw,
                "tradeId",
                "tradeID",
                "trade_id",
                "id",
            ),
        )
        if self._latency is not None:
            self._latency.observe(LatencyPhase.SOURCE_DETECTION_TO_NORMALIZED, receipt)
        return CopyTaskSourceEvent(
            trade=trade,
            normalized_receipt=self._latency.capture() if self._latency is not None else None,
            outcome_label=self._first_text(
                raw,
                "outcome",
                "outcomeName",
                "outcome_name",
                "selection",
            ),
            market_title=self._first_text(raw, "title", "market_title"),
            category=self._first_text(raw, "category"),
            days_until_close=self._optional_decimal(raw.get("days_until_close")),
            market_is_expired=bool(raw.get("market_is_expired", False)),
            market_is_resolved=bool(raw.get("market_is_resolved", False)),
            market_liquidity_usdc=self._optional_decimal(
                self._first_value(raw, "liquidity", "liquidity_usdc")
            ),
            market_volume_24h_usdc=self._optional_decimal(
                self._first_value(raw, "volume24hr", "volume_24h_usdc")
            ),
            token_symbol=self._first_text(raw, "token_symbol"),
            market_timeframe=self._first_text(raw, "market_timeframe", "timeframe"),
            sports_market_type=self._first_text(raw, "sports_market_type"),
            leader_pusd_balance=self._positive_optional_decimal(
                self._first_value(raw, "leader_pusd_balance", "leader_balance")
            ),
            leader_market_position_notional=self._non_negative_optional_decimal(
                raw.get("leader_market_position_notional")
            ),
            leader_total_portfolio_notional=self._non_negative_optional_decimal(
                raw.get("leader_total_portfolio_notional")
            ),
            leader_market_weight=self._non_negative_optional_decimal(
                raw.get("leader_market_weight")
            ),
            leader_sell_fraction=self._positive_optional_decimal(raw.get("_leader_sell_fraction")),
        )

    @classmethod
    def _first_text(cls, raw: Mapping[str, object], *names: str) -> str:
        for name in names:
            value = raw.get(name)
            if value is not None and str(value).strip():
                return str(value).strip()
        return ""

    @staticmethod
    def _first_value(raw: Mapping[str, object], *names: str) -> object | None:
        for name in names:
            value = raw.get(name)
            if value is not None:
                return value
        return None

    @staticmethod
    def _text(raw: Mapping[str, object], name: str) -> str:
        return str(raw.get(name) or "").strip()

    @staticmethod
    def _decimal(value: object, *, name: str, positive: bool = False) -> Decimal:
        try:
            result = Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise ValueError(f"{name} must be decimal-like") from exc
        if not result.is_finite() or result < 0 or (positive and result <= 0):
            qualifier = "positive" if positive else "non-negative"
            raise ValueError(f"{name} must be finite and {qualifier}")
        return result

    @classmethod
    def _optional_decimal(cls, value: object | None) -> Decimal:
        if value is None or not str(value).strip():
            return _ZERO
        return cls._decimal(value, name="optional market decimal")

    @classmethod
    def _positive_optional_decimal(cls, value: object | None) -> Decimal | None:
        if value is None or not str(value).strip():
            return None
        return cls._decimal(value, name="optional positive decimal", positive=True)

    @classmethod
    def _non_negative_optional_decimal(cls, value: object | None) -> Decimal | None:
        if value is None or not str(value).strip():
            return None
        return cls._decimal(value, name="optional non-negative decimal")

    @classmethod
    def _timestamp(cls, value: object | None, *, unit: object = None) -> datetime | None:
        return SourceTimestampParser.parse(value, unit=unit)

    @staticmethod
    def _aware(value: datetime, *, name: str) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(f"{name} must be timezone-aware")
        return value.astimezone(UTC)


class CopyTaskRuntimeIngress:
    """Normalize every detection source into the shared leader-worker queues."""

    def __init__(
        self,
        *,
        workers: CopyTaskWorkerPort,
        normalizer: LeaderEventNormalizer,
        buy_scope_guard: LeaderSellBuyScopeGuard | None = None,
    ) -> None:
        """Bind workers, normalization, and the shared BUY-safety guard.

        Args:
            workers: Bounded per-leader worker authority.
            normalizer: Detection-source event normalizer.
            buy_scope_guard: Durable emergency SELL scope guard.
        """

        self._workers = workers
        self._normalizer = normalizer
        self._buy_scope_guard = buy_scope_guard

    async def submit_raw_batch(
        self,
        leader: str,
        rows: Iterable[Mapping[str, object]],
    ) -> int:
        """Normalize and enqueue a polling batch, skipping malformed rows.

        Returns:
            Number of rows accepted by the bounded worker supervisor.
        """

        normalized: list[CopyTaskSourceEvent] = []
        for row in rows:
            try:
                event = self._normalizer.normalize_raw(
                    leader,
                    row,
                    mode=DetectionMode.ON_CHAIN_LOGS_ONLY,
                )
            except (TypeError, ValueError) as exc:
                logger.warning(
                    "copy_task_source_event_rejected",
                    extra={"error_type": type(exc).__name__, "source": "on_chain_logs_only"},
                )
                continue
            if await self._buy_is_paused(event):
                continue
            normalized.append(event)
        if normalized:
            await self._workers.submit_batch(leader, normalized)
        return len(normalized)

    async def submit_onchain(self, signal: OnchainSignal) -> None:
        """Normalize and enqueue one mined or provisional on-chain signal."""

        mode = (
            DetectionMode.ON_CHAIN_PENDING
            if signal.provisional
            else DetectionMode.ON_CHAIN_LOGS_ONLY
        )
        event = self._normalizer.normalize_raw(
            signal.leader_wallet,
            signal.as_raw_signal_dict(),
            mode=mode,
        )
        if await self._buy_is_paused(event):
            return
        await self._workers.submit_batch(signal.leader_wallet, (event,))

    async def _buy_is_paused(self, event: CopyTaskSourceEvent) -> bool:
        if event.trade.side is not TradeSide.BUY or self._buy_scope_guard is None:
            return False
        try:
            paused = await self._buy_scope_guard.is_buy_paused(
                event.trade.leader_wallet,
                event.trade.token_id,
            )
        except Exception as exc:
            logger.error(
                "leader_sell_scope_guard_unavailable",
                extra={
                    "leader": event.trade.leader_wallet,
                    "token_id": event.trade.token_id,
                    "error_type": type(exc).__name__,
                },
            )
            return True
        if paused:
            logger.warning(
                "copy_task_buy_scope_paused",
                extra={
                    "leader": event.trade.leader_wallet,
                    "token_id": event.trade.token_id,
                    "reason": "leader_sell_fraction_scope_degraded",
                },
            )
        return paused


class TradingContextTaskSnapshotProvider:
    """Build follower snapshots from existing context-owned repositories."""

    def __init__(
        self,
        *,
        contexts: Iterable[object],
        balance_reader: FollowerBalanceReader,
        risk_lots: RiskLotService | None = None,
    ) -> None:
        self._contexts = _TaskContextRegistry(contexts)
        self._balance_reader = balance_reader
        self._risk_lots = risk_lots

    def add_contexts(self, contexts: Iterable[object]) -> None:
        """Register newly discovered follower contexts without replacing state."""

        self._contexts.add(contexts)

    def has_task_binding(self, task: CopyTask) -> bool:
        """Return whether exact follower and leader evidence is registered."""

        return self._contexts.resolve(task.follower_account, task.leader_wallet) is not None

    async def snapshot_for(
        self,
        task: CopyTask,
        event: CopyTaskSourceEvent,
    ) -> FollowerTaskSnapshot:
        """Return authoritative follower balance, exposure, and sell size."""

        if task.reaction.counter_trade:
            raise TaskSnapshotUnavailable(
                "counter outcome token and price require an authoritative resolver"
            )
        context = self._context(task.follower_account, task.leader_wallet)
        user_id = str(getattr(context, "user_id", "") or "")
        wallet = str(getattr(context, "wallet_address", "") or "")
        balance = await self._balance_reader.read(wallet)
        if balance is None:
            raise TaskSnapshotUnavailable("follower collateral balance is unavailable")
        positions_repo = getattr(context, "positions_repo", None)
        if positions_repo is None:
            raise TaskSnapshotUnavailable("follower positions repository is unavailable")
        positions = await positions_repo.list_open_positions(user_id)
        total_positions = tuple(positions or ())
        market_positions = tuple(
            position
            for position in total_positions
            if str(getattr(position, "condition_id", "") or "") == str(event.trade.condition_id)
        )
        market_exposure = sum(
            (
                abs(Decimal(str(getattr(position, "size_tokens", 0))))
                * Decimal(str(getattr(position, "avg_entry_price", 0)))
                for position in market_positions
            ),
            _ZERO,
        )
        held = next(
            (
                Decimal(str(getattr(position, "size_tokens", 0)))
                for position in market_positions
                if str(getattr(position, "token_id", "") or "") == event.trade.token_id
            ),
            _ZERO,
        )
        if event.trade.side is TradeSide.SELL:
            if self._risk_lots is None:
                raise TaskSnapshotUnavailable("task inventory ownership is unavailable")
            held = sum(
                (
                    lot.lot.remaining_shares
                    for lot in await self._risk_lots.list_open(user_id)
                    if lot.lot.task_id == task.task_id
                    and lot.lot.market_id == event.trade.condition_id
                    and lot.lot.outcome_id == event.trade.token_id
                ),
                _ZERO,
            )
        sell_shares = (
            None
            if event.trade.side is TradeSide.SELL and event.leader_sell_fraction is None
            else (
                held * Decimal(str(event.leader_sell_fraction))
                if event.trade.side is TradeSide.SELL
                else None
            )
        )
        if sell_shares is not None and sell_shares <= 0:
            sell_shares = None
        reservations = self._reservations_repo(context)
        if reservations is None:
            raise TaskSnapshotUnavailable("follower reservation capacity is unavailable")
        reserved = Decimal(str(await reservations.sum_active_quote_for_user(user_id)))
        return FollowerTaskSnapshot(
            follower_pusd_balance=Decimal(str(balance)),
            follower_reserved_notional=reserved,
            follower_market_exposure=market_exposure,
            current_open_positions=len(total_positions),
            is_existing_market=bool(market_positions),
            sell_shares=sell_shares,
            execution_token_id=event.trade.token_id,
            execution_reference_price=event.trade.price,
            trust_multiplier=Decimal(str(getattr(context, "rank_size_multiplier", Decimal("1")))),
        )

    def _context(self, follower_account: str, leader_wallet: str) -> object:
        context = self._contexts.resolve(follower_account, leader_wallet)
        if context is None:
            raise TaskSnapshotUnavailable(
                "follower account must resolve to exactly one runtime context"
            )
        return context

    @staticmethod
    def _reservations_repo(context: object) -> ActiveReservationReader | None:
        extra = getattr(context, "extra", {}) or {}
        if isinstance(extra, Mapping):
            repository = extra.get("reservations_repo")
            if repository is not None:
                return cast(ActiveReservationReader, repository)
        coordinator = getattr(context, "execution_coordinator", None)
        repository = getattr(coordinator, "reservations_repo", None)
        return None if repository is None else cast(ActiveReservationReader, repository)


class TradingContextTaskExecutionAdapter:
    """Delegate task commands to each context's existing execution coordinator."""

    def __init__(self, *, contexts: Iterable[object]) -> None:
        self._contexts = _RuntimeContextRegistry(contexts)

    def add_contexts(self, contexts: Iterable[object]) -> None:
        """Register newly discovered follower contexts."""

        self._contexts.add(contexts)

    def has_task_binding(self, task: CopyTask) -> bool:
        """Return whether the follower has the canonical execution authority."""

        context = self._contexts.resolve(task.follower_account)
        return context is not None and getattr(context, "execution_coordinator", None) is not None

    def account_key(self, task: CopyTask) -> str | None:
        """Resolve wallet aliases to the canonical account queue identity."""
        context = self._contexts.resolve(task.follower_account)
        if context is None:
            return None
        account = str(getattr(context, "user_id", "") or "").strip().lower()
        return account or None

    async def submit(self, command: TaskExecutionCommand) -> TaskExecutionOutcome:
        """Submit one task command through risk, reservation, and reconciliation."""

        context = self._context(command.task.follower_account)
        coordinator = getattr(context, "execution_coordinator", None)
        if coordinator is None:
            raise TaskExecutionUnavailable(
                "copy-task execution requires the existing ExecutionCoordinator"
            )
        market_data = getattr(context, "market_data_service", None)
        if market_data is None:
            return TaskExecutionOutcome(
                TaskExecutionDisposition.PRE_POST_REJECTED,
                reason="market_data_unavailable",
            )
        try:
            metadata = await market_data.require_execution_metadata(
                command.token_id,
                condition_id=command.condition_id,
            )
        except Exception as exc:
            logger.warning(
                "copy_task_metadata_unavailable",
                extra={"task_id": command.task.task_id, "error_type": type(exc).__name__},
            )
            return TaskExecutionOutcome(
                TaskExecutionDisposition.PRE_POST_REJECTED,
                reason="market_metadata_unavailable",
            )
        execution_config = getattr(getattr(context, "config", None), "execution", None)
        execution_mode = getattr(execution_config, "trading_mode", "unknown")
        recovery_metadata: dict[str, object] = {
            "order_type": command.order_type,
            "price_limit": format(command.price_limit, "f"),
            "task_id": command.task.task_id,
            "task_version": command.task.version,
            "task_follower_account": command.task.follower_account,
            "protective_risk": RiskLotCodec().encode_policy(command.task.protective_risk),
            "source_promotion_id": command.source_promotion_id,
            "source_is_provisional": command.source_event.trade.is_provisional,
            "execution_mode": execution_mode if execution_mode in {"paper", "live"} else "unknown",
        }
        if command.source_event.trade.source_action_id is not None:
            recovery_metadata["source_action_id"] = command.source_event.trade.source_action_id
        if command.task_budget_reservation_id is not None:
            recovery_metadata["task_budget_reservation_id"] = command.task_budget_reservation_id
        leader_sell_fraction = command.source_event.leader_sell_fraction
        live_position_size = None
        if (
            command.side == "SELL"
            and leader_sell_fraction is not None
            and leader_sell_fraction > _ZERO
        ):
            live_position_size = command.amount / leader_sell_fraction
        intent = ExecutionIntent(
            user_id=str(getattr(context, "user_id", "")),
            condition_id=command.condition_id,
            token_id=command.token_id,
            side=command.side,
            size=command.amount,
            price=command.price_limit,
            notional_usdc=command.notional_usdc,
            idempotency_key=command.signal_id,
            source="copy_trade",
            signal_id=command.signal_id,
            leader_wallet=command.source_event.trade.leader_wallet,
            max_slippage_bps=int(command.task.price.max_slippage_percent * Decimal("100")),
            reason="copy_task",
            recovery_metadata=recovery_metadata,
            metadata={"task_id": command.task.task_id},
            task_id=command.task.task_id,
            tick_size=metadata.tick_size,
            min_order_size=metadata.min_order_size,
            neg_risk=metadata.neg_risk,
            leader_sell_fraction=(leader_sell_fraction if command.side == "SELL" else None),
            live_position_size=live_position_size,
        )
        if command.side == "SELL":
            extra = getattr(context, "extra", {}) or {}
            target_service = (
                extra.get("leader_sell_target_service") if isinstance(extra, Mapping) else None
            )
            if target_service is not None:
                try:
                    intent = await target_service.record_fresh_target(intent)
                except Exception as exc:
                    logger.error(
                        "leader_sell_target_record_failed",
                        extra={
                            "user_id": str(getattr(context, "user_id", "")),
                            "leader": command.source_event.trade.leader_wallet,
                            "token_id": command.token_id,
                            "error_type": type(exc).__name__,
                        },
                    )
        result = await coordinator.submit_copy_trade(intent)
        if not isinstance(result, ExecutionCoordinatorResult):
            raise TaskExecutionUnavailable("execution coordinator returned an invalid result")
        return self._outcome(result)

    def _context(self, follower_account: str) -> object:
        context = self._contexts.resolve(follower_account)
        if context is None:
            raise TaskExecutionUnavailable(
                "follower account must resolve to exactly one runtime context"
            )
        return context

    @staticmethod
    def _outcome(result: ExecutionCoordinatorResult) -> TaskExecutionOutcome:
        status = str(result.status or "").strip().lower()
        if status in {
            "duplicate_ignored",
            "reservation_failed",
            "position_drift_unresolved",
            "execution_coordinator_missing",
            "inflight_lock",
        }:
            disposition = TaskExecutionDisposition.PRE_POST_REJECTED
        elif status in {
            "submitted_unconfirmed",
            "ambiguous",
            "ambiguous_submission",
        }:
            disposition = TaskExecutionDisposition.SUBMITTED_UNCONFIRMED
        elif not result.accepted and status in {
            "rejected",
            "failed",
            "cancelled",
            "canceled",
            "expired",
            "order_not_fillable",
        }:
            disposition = TaskExecutionDisposition.TERMINAL_NO_FILL
        elif not result.accepted:
            # The production coordinator retains venue reservations only for
            # accepted/non-terminal or explicitly ambiguous submissions.  A
            # false accepted flag after the ambiguity cases above is therefore
            # authoritative no-POST/no-fill evidence for the task budget.
            disposition = TaskExecutionDisposition.PRE_POST_REJECTED
        else:
            # Immediate fill responses do not necessarily carry an
            # authoritative fill identity. Retain until reconciliation emits
            # durable fill/no-fill evidence instead of fabricating one here.
            disposition = TaskExecutionDisposition.ACCEPTED_PENDING
        return TaskExecutionOutcome(disposition=disposition, reason=result.skip_code or status)


__all__ = [
    "Clock",
    "CopyTaskEventHandler",
    "CopyTaskRuntimeIngress",
    "CopyTaskWorkerPort",
    "DeferredCopyTaskEventHandler",
    "FollowerBalanceReader",
    "LeaderEventNormalizer",
    "SystemClock",
    "TaskExecutionUnavailable",
    "TaskSnapshotUnavailable",
    "TradingContextTaskExecutionAdapter",
    "TradingContextTaskSnapshotProvider",
]
