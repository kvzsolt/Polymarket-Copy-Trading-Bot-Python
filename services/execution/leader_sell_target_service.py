"""Persist and safely re-drive the latest copied leader SELL target.

Re-drive is allowed only after authoritative terminal no-fill evidence releases
an *older* reservation. Each target/release pair is claimed durably once, and
an automatically created order can never trigger another automatic attempt.
"""

from __future__ import annotations

import logging
from dataclasses import replace
from decimal import Decimal, InvalidOperation
from typing import Mapping, Protocol, Sequence

from domain.execution import ExecutionOrderType
from domain.leader_sell_target import LeaderSellTarget
from services.execution.coordinator import ExecutionCoordinatorResult, ExecutionIntent
from services.execution.submission import sanitize_submission_value

_ZERO = Decimal("0")
_TERMINAL_NO_FILL_REASONS = frozenset(
    {
        "cancelled",
        "canceled",
        "expired",
        "rejected",
        "failed",
        "terminal_failed",
        "auto_released_ambiguous",
        "auto_release_after_ambiguous_timeout",
        # Operator-applied terminal no-fill resolutions (``bot resolve --apply``).
        # Exact matches only: ``manual_resolution:filled`` frees no shares and
        # must never become eligible.
        "manual_resolution:canceled",
        "manual_resolution:expired",
        "manual_resolution:confirmed_absent",
    }
)


class LeaderSellTargetRepository(Protocol):
    """Persist latest SELL targets and one-shot re-drive claims."""

    async def upsert_target(
        self,
        *,
        user_id: str,
        leader_wallet: str,
        condition_id: str,
        token_id: str,
        source_signal_id: str,
        intent_payload: dict[str, object],
        task_id: str | None = None,
    ) -> LeaderSellTarget:
        """Persist a fresh target and increment its scope generation."""

        ...

    async def get_target(
        self,
        *,
        user_id: str,
        leader_wallet: str,
        condition_id: str,
        token_id: str,
        task_id: str | None = None,
    ) -> LeaderSellTarget | None:
        """Return the latest target for one follower/leader/token scope."""

        ...

    async def claim_redrive(
        self,
        *,
        target: LeaderSellTarget,
        released_reservation_id: str,
    ) -> bool:
        """Claim one target/released-reservation pair exactly once."""

        ...


class FollowerPositionReader(Protocol):
    """Read exact current follower shares for one outcome token."""

    async def position_size(
        self,
        user_id: str,
        condition_id: str,
        token_id: str,
    ) -> Decimal | None:
        """Return held shares or ``None`` when the position is unavailable."""

        ...


class CopyTradeCoordinator(Protocol):
    """Submit a target through the canonical execution coordinator."""

    async def submit_copy_trade(self, intent: ExecutionIntent) -> ExecutionCoordinatorResult:
        """Submit one protected, reservation-backed copy SELL."""

        ...


class OpenPositionRepository(Protocol):
    """Expose persisted follower positions to the target service."""

    async def list_open_positions(self, user_id: str) -> Sequence[object]:
        """Return the follower's open position rows."""

        ...


class PersistedFollowerPositionReader:
    """Adapt the canonical positions repository to exact scoped share reads."""

    def __init__(self, positions_repo: OpenPositionRepository) -> None:
        """Bind the follower positions repository.

        Args:
            positions_repo: Canonical persisted position authority.
        """

        self._positions_repo = positions_repo

    async def position_size(
        self,
        user_id: str,
        condition_id: str,
        token_id: str,
    ) -> Decimal | None:
        """Return exact persisted shares for one scoped token.

        Args:
            user_id: Follower account identity.
            condition_id: Market condition identifier.
            token_id: Raw outcome-token identifier.

        Returns:
            Exact shares, zero when the scoped position is absent, or ``None``
            when persisted input is malformed.
        """

        rows = await self._positions_repo.list_open_positions(user_id)
        for row in rows:
            if str(getattr(row, "condition_id", "") or "") != condition_id:
                continue
            if str(getattr(row, "token_id", "") or "") != token_id:
                continue
            return self._decimal(getattr(row, "size_tokens", None))
        return _ZERO

    @staticmethod
    def _decimal(value: object) -> Decimal | None:
        try:
            parsed = Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError):
            return None
        return parsed if parsed.is_finite() and parsed >= _ZERO else None


class LeaderSellTargetService:
    """Own durable target generations and proof-triggered one-shot re-drives."""

    def __init__(
        self,
        *,
        repository: LeaderSellTargetRepository,
        position_reader: FollowerPositionReader,
        coordinator: CopyTradeCoordinator,
        logger: logging.Logger | None = None,
    ) -> None:
        """Bind persistence, current follower state, and execution authority.

        Args:
            repository: Durable target and claim repository.
            position_reader: Exact current follower share reader.
            coordinator: Canonical reservation/execution funnel.
            logger: Structured diagnostic logger.
        """

        self._repository = repository
        self._position_reader = position_reader
        self._coordinator = coordinator
        self._logger = logger if logger is not None else logging.getLogger(__name__)

    async def record_fresh_target(self, intent: ExecutionIntent) -> ExecutionIntent:
        """Persist a fresh leader SELL and return its generation-tagged intent.

        Args:
            intent: Fresh copy intent produced from a leader signal.

        Returns:
            The intent with a durable target generation in recovery metadata.
            Non-SELL intents and persistence failures are returned unchanged so
            an exit is never blocked by optional recovery bookkeeping.
        """

        if str(intent.side or "").upper() != "SELL":
            return intent
        source_signal_id = str(intent.signal_id or "").strip()
        leader_wallet = str(intent.leader_wallet or "").strip().lower()
        if not source_signal_id or not leader_wallet:
            return intent
        try:
            target = await self._repository.upsert_target(
                user_id=str(intent.user_id),
                leader_wallet=leader_wallet,
                condition_id=str(intent.condition_id),
                token_id=str(intent.token_id),
                source_signal_id=source_signal_id,
                intent_payload=self._serialize(intent),
                task_id=intent.task_id,
            )
        except Exception as exc:
            self._logger.error(
                "leader_sell_target_persist_failed | user=%s leader=%s token=%s error_type=%s",
                intent.user_id,
                leader_wallet,
                intent.token_id,
                type(exc).__name__,
            )
            return intent
        recovery = dict(intent.recovery_metadata or {})
        recovery["leader_sell_target_generation"] = target.generation
        return replace(intent, recovery_metadata=recovery)

    async def redrive_after_terminal_no_fill(
        self,
        record: Mapping[str, object],
        *,
        reason: str,
    ) -> bool:
        """Re-drive a newer target after an older order proves no fill.

        Args:
            record: Canonical reconciler order record whose reservation was
                just released.
            reason: Authoritative terminal release reason.

        Returns:
            ``True`` when one guarded coordinator attempt was made; ``False``
            when the record was ineligible, already claimed, or unsafe.
        """

        if str(reason or "").strip().lower() not in _TERMINAL_NO_FILL_REASONS:
            return False
        request_obj = record.get("request")
        request = request_obj if isinstance(request_obj, Mapping) else {}
        if str(request.get("side") or record.get("side") or "").upper() != "SELL":
            return False
        recovery_obj = request.get("recovery_metadata")
        recovery = recovery_obj if isinstance(recovery_obj, Mapping) else {}
        if bool(recovery.get("leader_sell_target_auto_redrive")):
            return False
        released_signal_id = str(request.get("signal_id") or "").strip()
        reservation_id = str(
            recovery.get("reservation_id") or record.get("reservation_id") or ""
        ).strip()
        user_id = str(record.get("user_id") or request.get("user_id") or "").strip()
        leader_wallet = (
            str(record.get("leader_id") or request.get("leader_id") or "").strip().lower()
        )
        condition_id = str(record.get("condition_id") or request.get("condition_id") or "").strip()
        token_id = str(
            record.get("asset") or request.get("token_id") or request.get("asset") or ""
        ).strip()
        if not all((reservation_id, user_id, leader_wallet, condition_id, token_id)):
            return False
        try:
            released_generation = int(recovery.get("leader_sell_target_generation") or 0)
        except (TypeError, ValueError):
            return False
        released_size = self._optional_decimal(recovery.get("reserved_sell_size"))
        if released_generation <= 0 or released_size is None or released_size <= _ZERO:
            return False
        target = await self._repository.get_target(
            user_id=user_id,
            leader_wallet=leader_wallet,
            condition_id=condition_id,
            token_id=token_id,
            task_id=str(recovery["task_id"]) if recovery.get("task_id") else None,
        )
        if (
            target is None
            or target.generation <= released_generation
            or target.source_signal_id == released_signal_id
        ):
            return False
        try:
            intent = self._deserialize(target.intent_payload)
        except (InvalidOperation, KeyError, TypeError, ValueError) as exc:
            self._logger.error(
                "leader_sell_target_invalid | user=%s leader=%s token=%s generation=%s "
                "error_type=%s",
                user_id,
                leader_wallet,
                token_id,
                target.generation,
                type(exc).__name__,
            )
            return False
        fraction = intent.leader_sell_fraction
        if intent.task_id != target.task_id:
            return False
        if fraction is None or not fraction.is_finite() or not (_ZERO < fraction <= Decimal("1")):
            return False
        held = await self._position_reader.position_size(user_id, condition_id, token_id)
        if held is None or not held.is_finite() or held <= _ZERO:
            return False
        correction_size = min(held, released_size * fraction)
        minimum = intent.min_order_size
        if minimum is not None and minimum > _ZERO and correction_size < minimum:
            if intent.sub_min_sell_policy != "round_up" or held < minimum:
                return False
            correction_size = minimum
        if not await self._repository.claim_redrive(
            target=target,
            released_reservation_id=reservation_id,
        ):
            return False
        auto_signal_id = f"leader-sell-target:{target.generation}:release:{reservation_id}"
        auto_recovery = dict(intent.recovery_metadata or {})
        auto_recovery.update(
            {
                "leader_sell_target_generation": target.generation,
                "leader_sell_target_auto_redrive": True,
                "leader_sell_target_release_id": reservation_id,
                "leader_sell_target_source_signal_id": target.source_signal_id,
                "leader_sell_target_fraction": str(fraction),
            }
        )
        redrive = replace(
            intent,
            size=correction_size,
            notional_usdc=correction_size * intent.price,
            signal_id=auto_signal_id,
            idempotency_key=auto_signal_id,
            leader_sell_fraction=None,
            live_position_size=held,
            recovery_metadata=auto_recovery,
        )
        try:
            result = await self._coordinator.submit_copy_trade(redrive)
        except Exception as exc:
            self._logger.error(
                "leader_sell_target_redrive_failed | user=%s leader=%s token=%s "
                "generation=%s error_type=%s",
                user_id,
                leader_wallet,
                token_id,
                target.generation,
                type(exc).__name__,
            )
            return True
        self._logger.info(
            "leader_sell_target_redrive | user=%s leader=%s token=%s generation=%s "
            "released_reservation=%s status=%s order_id=%s",
            user_id,
            leader_wallet,
            token_id,
            target.generation,
            reservation_id,
            result.status,
            result.order_id,
        )
        return True

    @staticmethod
    def _serialize(intent: ExecutionIntent) -> dict[str, object]:
        payload: dict[str, object] = {
            "user_id": str(intent.user_id),
            "condition_id": str(intent.condition_id),
            "token_id": str(intent.token_id),
            "side": str(intent.side),
            "size": str(intent.size),
            "price": str(intent.price),
            "notional_usdc": str(intent.notional_usdc),
            "source": str(intent.source),
            "signal_id": str(intent.signal_id or ""),
            "leader_wallet": str(intent.leader_wallet or ""),
            "max_slippage_bps": int(intent.max_slippage_bps),
            "reason": str(intent.reason or ""),
            "task_id": intent.task_id,
            "protective_lot_id": intent.protective_lot_id,
        }
        optional_decimals = {
            "price_limit": intent.price_limit,
            "tick_size": intent.tick_size,
            "min_order_size": intent.min_order_size,
            "leader_sell_fraction": intent.leader_sell_fraction,
            "live_position_size": intent.live_position_size,
        }
        for name, value in optional_decimals.items():
            if value is not None:
                payload[name] = str(value)
        if intent.order_type is not None:
            payload["order_type"] = str(intent.order_type.value)
        if intent.neg_risk is not None:
            payload["neg_risk"] = bool(intent.neg_risk)
        if intent.sub_min_sell_policy is not None:
            payload["sub_min_sell_policy"] = str(intent.sub_min_sell_policy)
        sanitized_recovery = sanitize_submission_value(dict(intent.recovery_metadata or {}))
        if isinstance(sanitized_recovery, dict):
            payload["recovery_metadata"] = sanitized_recovery
        return payload

    @classmethod
    def _deserialize(cls, payload: Mapping[str, object]) -> ExecutionIntent:
        order_type_raw = str(payload.get("order_type") or "").strip().upper()
        order_type = ExecutionOrderType(order_type_raw) if order_type_raw else None
        raw_recovery_metadata = payload.get("recovery_metadata")
        recovery_metadata = (
            dict(raw_recovery_metadata) if isinstance(raw_recovery_metadata, Mapping) else None
        )
        return ExecutionIntent(
            user_id=str(payload["user_id"]),
            condition_id=str(payload["condition_id"]),
            token_id=str(payload["token_id"]),
            side=str(payload["side"]),
            size=Decimal(str(payload["size"])),
            price=Decimal(str(payload["price"])),
            notional_usdc=Decimal(str(payload["notional_usdc"])),
            source=str(payload.get("source") or "copy_trade"),
            signal_id=str(payload.get("signal_id") or "") or None,
            idempotency_key=str(payload.get("signal_id") or "") or None,
            leader_wallet=str(payload.get("leader_wallet") or "") or None,
            max_slippage_bps=int(str(payload.get("max_slippage_bps") or 0)),
            reason=str(payload.get("reason") or "") or None,
            task_id=str(payload["task_id"]) if payload.get("task_id") else None,
            protective_lot_id=(
                str(payload["protective_lot_id"]) if payload.get("protective_lot_id") else None
            ),
            recovery_metadata=recovery_metadata,
            order_type=order_type,
            price_limit=cls._optional_decimal(payload.get("price_limit")),
            tick_size=cls._optional_decimal(payload.get("tick_size")),
            min_order_size=cls._optional_decimal(payload.get("min_order_size")),
            neg_risk=(bool(payload["neg_risk"]) if payload.get("neg_risk") is not None else None),
            leader_sell_fraction=cls._optional_decimal(payload.get("leader_sell_fraction")),
            live_position_size=cls._optional_decimal(payload.get("live_position_size")),
            sub_min_sell_policy=(
                str(payload["sub_min_sell_policy"])
                if payload.get("sub_min_sell_policy") is not None
                else None
            ),
        )

    @staticmethod
    def _optional_decimal(value: object) -> Decimal | None:
        if value is None or not str(value).strip():
            return None
        try:
            parsed = Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError):
            return None
        return parsed if parsed.is_finite() else None


__all__ = [
    "CopyTradeCoordinator",
    "FollowerPositionReader",
    "LeaderSellTargetRepository",
    "LeaderSellTargetService",
    "OpenPositionRepository",
    "PersistedFollowerPositionReader",
]
