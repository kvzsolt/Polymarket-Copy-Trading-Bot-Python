"""Route canonical leader events through copy-task policy and execution ports.

The service remains venue-neutral. It combines immutable source events with a
read-only follower snapshot, then delegates every decision to the existing
copy-task coordinator and every mutation to an injected execution port.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from hashlib import sha256
import time
from typing import Protocol

from pmcopy.domain.detection import LeaderTradeEvent
from pmcopy.domain.source_time import TransportReceipt
from pmcopy.services.observability.metrics import LatencyPhase
from pmcopy.services.telemetry.latency import LatencyMeasurement, PipelineLatency
from pmcopy.services.copy_sizing_v5.domain import CopySizingRequest
from pmcopy.services.copy_tasks.budget import (
    TaskBudgetKey,
    TaskBudgetReservationService,
    TaskBudgetSnapshot,
)
from pmcopy.services.copy_tasks.domain import (
    CopyTask,
    SourceTrade,
    TaskAdmissionContext,
)
from pmcopy.services.copy_tasks.lifecycle import TaskExecutionOutcome
from pmcopy.services.copy_tasks.reaction import CopyReactionEngine, ReactionAction
from pmcopy.services.copy_tasks.reconciliation import Clock
from pmcopy.services.copy_tasks.source_freshness import SourceFreshnessPolicy
from pmcopy.services.copy_tasks.runtime import (
    CopyTaskRuntimeCoordinator,
    RuntimeDecision,
    RuntimeExecutionEnvelope,
)
from pmcopy.utils.logger import get_logger

_ZERO = Decimal("0")
logger = get_logger("CopyTaskEventProcessor")


def _swallow_cancelled_budget_task(task: asyncio.Task[TaskBudgetSnapshot]) -> None:
    """Prevent unhandled-exception warnings when a speculative budget read is cancelled."""
    if task.cancelled():
        return
    task.exception()


def _decimal(value: object, *, name: str, positive: bool = False) -> Decimal:
    """Return one finite decimal validated at the event boundary."""

    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be decimal-like") from exc
    if not result.is_finite() or result < 0 or (positive and result <= 0):
        qualifier = "positive" if positive else "non-negative"
        raise ValueError(f"{name} must be finite and {qualifier}")
    return result


@dataclass(frozen=True, slots=True)
class CopyTaskSourceEvent:
    """Canonical leader trade plus sanitized market admission metadata."""

    trade: LeaderTradeEvent
    outcome_label: str
    market_title: str = ""
    category: str = ""
    days_until_close: Decimal = _ZERO
    market_is_expired: bool = False
    market_is_resolved: bool = False
    market_liquidity_usdc: Decimal = _ZERO
    market_volume_24h_usdc: Decimal = _ZERO
    token_symbol: str = ""
    market_timeframe: str = ""
    sports_market_type: str = ""
    leader_pusd_balance: Decimal | None = None
    leader_market_position_notional: Decimal | None = None
    leader_total_portfolio_notional: Decimal | None = None
    leader_market_weight: Decimal | None = None
    leader_sell_fraction: Decimal | None = None
    normalized_receipt: TransportReceipt | None = None
    leader_funder_wallet: str = ""

    def __post_init__(self) -> None:
        """Validate exact-decimal metadata without inventing missing values."""

        if not str(self.trade.condition_id or "").strip():
            raise ValueError("condition_id is required for copy-task routing")
        for name in (
            "days_until_close",
            "market_liquidity_usdc",
            "market_volume_24h_usdc",
        ):
            object.__setattr__(
                self,
                name,
                _decimal(getattr(self, name), name=name),
            )
        for name in (
            "leader_pusd_balance",
            "leader_market_position_notional",
            "leader_total_portfolio_notional",
            "leader_market_weight",
            "leader_sell_fraction",
        ):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(
                    self,
                    name,
                    _decimal(value, name=name, positive=name == "leader_pusd_balance"),
                )
        if self.leader_sell_fraction is not None and not (
            _ZERO < self.leader_sell_fraction <= Decimal("1")
        ):
            raise ValueError("leader_sell_fraction must be in (0, 1]")


@dataclass(frozen=True, slots=True)
class FollowerTaskSnapshot:
    """Read-only follower state required for one copy-task decision."""

    follower_pusd_balance: Decimal
    follower_reserved_notional: Decimal
    follower_market_exposure: Decimal
    current_open_positions: int
    is_existing_market: bool
    sell_shares: Decimal | None
    execution_token_id: str
    execution_reference_price: Decimal
    trust_multiplier: Decimal = Decimal("1")
    leader_pusd_balance: Decimal | None = None

    def __post_init__(self) -> None:
        """Validate money, share, and target-token inputs."""

        for name in (
            "follower_pusd_balance",
            "follower_reserved_notional",
            "follower_market_exposure",
        ):
            object.__setattr__(
                self,
                name,
                _decimal(
                    getattr(self, name),
                    name=name,
                    positive=False,
                ),
            )
        object.__setattr__(
            self,
            "execution_reference_price",
            _decimal(
                self.execution_reference_price,
                name="execution_reference_price",
                positive=True,
            ),
        )
        object.__setattr__(
            self,
            "trust_multiplier",
            _decimal(self.trust_multiplier, name="trust_multiplier", positive=True),
        )
        if not _ZERO < self.execution_reference_price < Decimal("1"):
            raise ValueError("execution_reference_price must be in (0, 1)")
        if self.current_open_positions < 0:
            raise ValueError("current_open_positions must be non-negative")
        if not str(self.execution_token_id).strip():
            raise ValueError("execution_token_id is required")
        if self.sell_shares is not None:
            object.__setattr__(
                self,
                "sell_shares",
                _decimal(self.sell_shares, name="sell_shares", positive=True),
            )
        if self.leader_pusd_balance is not None:
            object.__setattr__(
                self,
                "leader_pusd_balance",
                _decimal(self.leader_pusd_balance, name="leader_pusd_balance", positive=False),
            )


@dataclass(frozen=True, slots=True)
class TaskExecutionCommand:
    """Venue-neutral command for the existing risk/reservation funnel."""

    task: CopyTask
    source_event: CopyTaskSourceEvent
    side: str
    condition_id: str
    token_id: str
    amount: Decimal
    notional_usdc: Decimal
    price_limit: Decimal
    order_type: str
    signal_id: str
    task_budget_reservation_id: str | None
    source_promotion_id: str | None = None


@dataclass(frozen=True, slots=True)
class TaskRouteResult:
    """Stable per-task routing decision returned for telemetry and tests."""

    task_id: str
    allowed: bool
    reason: str
    runtime_decision: RuntimeDecision | None = None


class CopyTaskReader(Protocol):
    """Read the current non-deleted copy-task set."""

    async def list_active(self) -> tuple[CopyTask, ...]:
        """Return active and paused tasks from durable state."""

        ...


class CachedCopyTaskReader:
    """Short-TTL singleflight cache over durable ``CopyTaskReader`` for low latency.

    Eliminates a PostgreSQL ``SELECT * FROM copy_tasks`` + JSON decode on every
    incoming on-chain trade signal while staying bounded and explicitly
    invalidatable whenever task definitions change.
    """

    def __init__(
        self,
        reader: CopyTaskReader,
        *,
        ttl_seconds: float = 5.0,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if ttl_seconds < 0:
            raise ValueError("ttl_seconds must be non-negative")
        self._reader = reader
        self._ttl_seconds = float(ttl_seconds)
        self._monotonic = monotonic
        self._cached: tuple[CopyTask, ...] | None = None
        self._expires_at: float = 0.0
        self._inflight: asyncio.Future[tuple[CopyTask, ...]] | None = None

    async def list_active(self) -> tuple[CopyTask, ...]:
        """Return active copy tasks from memory when fresh, else refresh once."""
        if self._ttl_seconds > 0 and self._cached is not None and self._monotonic() < self._expires_at:
            return self._cached
        if self._inflight is not None:
            return await asyncio.shield(self._inflight)
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[tuple[CopyTask, ...]] = loop.create_future()
        self._inflight = fut
        try:
            tasks = tuple(await self._reader.list_active())
            if self._ttl_seconds > 0:
                self._cached = tasks
                self._expires_at = self._monotonic() + self._ttl_seconds
            if not fut.done():
                fut.set_result(tasks)
            return tasks
        except BaseException as exc:
            if not fut.done():
                fut.set_exception(exc)
            raise
        finally:
            self._inflight = None

    def invalidate(self) -> None:
        """Force the next read to fetch fresh task definitions from storage."""
        self._cached = None
        self._expires_at = 0.0


class FollowerTaskSnapshotProvider(Protocol):
    """Load a safe follower snapshot without mutating runtime state."""

    async def snapshot_for(
        self,
        task: CopyTask,
        event: CopyTaskSourceEvent,
    ) -> FollowerTaskSnapshot:
        """Return state needed for task admission, sizing, and execution."""

        ...


class TaskExecutionPort(Protocol):
    """Submit through the existing execution and reconciliation funnel."""

    async def submit(self, command: TaskExecutionCommand) -> TaskExecutionOutcome:
        """Return typed submission/fill evidence for task-budget lifecycle."""

        ...


class TaskDecisionObserver(Protocol):
    """Observe policy results synchronously through a bounded, non-raising sink."""

    def task_decision(
        self,
        task: CopyTask,
        event: CopyTaskSourceEvent,
        result: TaskRouteResult,
        evaluated_at: datetime,
        max_signal_age_seconds: float,
    ) -> bool:
        """Return whether delivery was admitted; false records a telemetry gap."""
        ...


class CopyTaskEventProcessor:
    """Process leader events serially across every matching active task."""

    def __init__(
        self,
        *,
        task_reader: CopyTaskReader,
        task_budgets: TaskBudgetReservationService,
        coordinator: CopyTaskRuntimeCoordinator,
        snapshot_provider: FollowerTaskSnapshotProvider,
        execution: TaskExecutionPort,
        clock: Clock | None = None,
        max_signal_age_seconds: float = 0,
        decision_observer: TaskDecisionObserver | None = None,
        latency: PipelineLatency | None = None,
    ) -> None:
        self._task_reader = task_reader
        self._task_budgets = task_budgets
        self._coordinator = coordinator
        self._snapshot_provider = snapshot_provider
        self._execution = execution
        self._clock = clock
        self._freshness = SourceFreshnessPolicy(max_signal_age_seconds)
        self._decision_observer = decision_observer
        self._latency = latency

    async def __call__(self, leader: str, event: CopyTaskSourceEvent) -> None:
        """Handle one worker event while keeping worker callback shape narrow."""

        await self.process(leader, event)

    async def process(
        self,
        leader: str,
        event: CopyTaskSourceEvent,
    ) -> tuple[TaskRouteResult, ...]:
        """Route one event through matching tasks in stable task-id order."""

        tasks = await self.matching_tasks(leader, event)
        return await self.process_batch(tasks, event)

    async def matching_tasks(self, leader: str, event: CopyTaskSourceEvent) -> tuple[CopyTask, ...]:
        """Read enabled task allocations for one validated leader observation."""
        normalized_leader = str(leader).strip().lower()
        if normalized_leader != event.trade.leader_wallet.lower():
            raise ValueError("worker leader does not match source event leader")
        return tuple(
            sorted(
                (
                    task
                    for task in await self._task_reader.list_active()
                    if task.enabled and task.leader_wallet.lower() == normalized_leader
                ),
                key=lambda task: task.task_id,
            )
        )

    async def process_batch(
        self, tasks: tuple[CopyTask, ...], event: CopyTaskSourceEvent
    ) -> tuple[TaskRouteResult, ...]:
        """Process an ordered allocation batch through the existing money path."""
        if self._latency is not None:
            self._latency.observe(LatencyPhase.NORMALIZED_TO_WORKER_START, event.normalized_receipt)
        results = []
        for task in sorted(tasks, key=lambda item: item.task_id):
            evaluated_at = self._clock.now() if self._clock is not None else event.trade.detected_at
            result = await self._process_task(task, event)
            results.append(result)
            if not result.allowed:
                logger.info(
                    "copy_task_skipped | task_id=%s leader=%s side=%s reason=%s "
                    "price=%s notional=%s tx=%s",
                    task.task_id,
                    event.trade.leader_wallet,
                    event.trade.side.value,
                    result.reason,
                    event.trade.price,
                    event.trade.notional,
                    event.trade.transaction_hash or "",
                )
            else:
                logger.info(
                    "copy_task_dispatched | task_id=%s leader=%s side=%s reason=%s "
                    "price=%s notional=%s tx=%s",
                    task.task_id,
                    event.trade.leader_wallet,
                    event.trade.side.value,
                    result.reason,
                    event.trade.price,
                    event.trade.notional,
                    event.trade.transaction_hash or "",
                )
            if self._decision_observer is not None:
                self._decision_observer.task_decision(
                    task, event, result, evaluated_at, self._freshness.max_age_seconds
                )
        return tuple(results)

    def reject_batch(
        self, tasks: tuple[CopyTask, ...], event: CopyTaskSourceEvent, reason: str
    ) -> None:
        """Publish explicit admission failures without taking financial ownership."""
        if self._decision_observer is None:
            return
        now = self._clock.now() if self._clock is not None else event.trade.detected_at
        for task in tasks:
            self._decision_observer.task_decision(
                task,
                event,
                TaskRouteResult(task.task_id, False, reason),
                now,
                self._freshness.max_age_seconds,
            )

    async def _process_task(
        self,
        task: CopyTask,
        event: CopyTaskSourceEvent,
    ) -> TaskRouteResult:
        with (
            self._latency.span(LatencyPhase.COPY_DECISION)
            if self._latency is not None
            else nullcontext()
        ) as decision_span:
            return await self._route_task(task, event, decision_span)

    async def _route_task(
        self, task: CopyTask, event: CopyTaskSourceEvent, decision_span: LatencyMeasurement | None
    ) -> TaskRouteResult:
        now = self._clock.now() if self._clock is not None else event.trade.detected_at
        source = self._source_trade(event)
        reaction = CopyReactionEngine().decide(task.reaction, source)
        follower_action = (
            reaction.action.value
            if reaction.action in (ReactionAction.BUY, ReactionAction.SELL)
            else source.action
        )
        if reaction.action is not ReactionAction.SKIP:
            reason = self._freshness.rejection(
                event.trade,
                now,
                follower_action=follower_action,
            )
            if reason is not None:
                return TaskRouteResult(task_id=task.task_id, allowed=False, reason=reason)
        budget_key = TaskBudgetKey(
            follower_account=task.follower_account,
            task_id=task.task_id,
            market_id=source.market_id,
            sports_market_type=event.sports_market_type,
        )
        budget_task: asyncio.Task[TaskBudgetSnapshot] = asyncio.create_task(
            self._task_budgets.snapshot(budget_key, now)
        )
        budget_task.add_done_callback(_swallow_cancelled_budget_task)
        try:
            follower = await self._snapshot_provider.snapshot_for(task, event)
        except Exception as exc:
            budget_task.cancel()
            logger.warning(
                "copy_task_snapshot_unavailable",
                extra={
                    "task_id": task.task_id,
                    "error_type": type(exc).__name__,
                },
            )
            return TaskRouteResult(
                task_id=task.task_id,
                allowed=False,
                reason="follower_snapshot_unavailable",
            )
        except BaseException:
            budget_task.cancel()
            raise
        if reaction.action is ReactionAction.SELL and follower.sell_shares is None:
            budget_task.cancel()
            return TaskRouteResult(
                task_id=task.task_id,
                allowed=False,
                reason="follower_sell_shares_unavailable",
            )
        budget = await budget_task
        admission = self._admission_context(event, source, budget, now=now)
        sizing = (
            self._sizing_request(event, follower) if reaction.action is ReactionAction.BUY else None
        )
        reservation_id = self._reservation_id(task, event)

        async def downstream(envelope: RuntimeExecutionEnvelope) -> TaskExecutionOutcome:
            command = self._execution_command(
                event=event,
                follower=follower,
                envelope=envelope,
            )
            if decision_span is not None:
                decision_span.finish()
            return await self._execution.submit(command)

        decision = await self._coordinator.process(
            task=task,
            source_trade=source,
            admission_context=admission,
            sizing_request=sizing,
            downstream=downstream,
            reservation_id=reservation_id,
            execution_reference_price=follower.execution_reference_price,
            prior_reservation_ids=(
                (self._execution_identity(task, event, promotion=True),)
                if event.trade.economic_key != event.trade.promotion_key
                else ()
            ),
        )
        return TaskRouteResult(
            task_id=task.task_id,
            allowed=decision.allowed,
            reason=decision.reason,
            runtime_decision=decision,
        )

    @staticmethod
    def _source_trade(event: CopyTaskSourceEvent) -> SourceTrade:
        trade = event.trade
        return SourceTrade(
            leader_wallet=trade.leader_wallet,
            market_id=str(trade.condition_id),
            outcome_label=event.outcome_label,
            action=trade.side.value,
            price=trade.price,
            notional=trade.notional,
            shares=trade.shares,
            finality=trade.finality,
        )

    @staticmethod
    def _admission_context(
        event: CopyTaskSourceEvent,
        source: SourceTrade,
        budget: TaskBudgetSnapshot,
        *,
        now: datetime,
    ) -> TaskAdmissionContext:
        return TaskAdmissionContext(
            now=now,
            action=source.action,
            outcome_label=source.outcome_label,
            leader_trade_notional=source.notional,
            leader_price=source.price,
            market_id=source.market_id,
            market_title=event.market_title,
            category=event.category,
            days_until_close=event.days_until_close,
            task_total_spend=budget.task_total,
            task_daily_spend=budget.task_daily,
            task_market_spend=budget.task_market,
            task_has_entered_market=budget.task_has_entry,
            global_market_has_entry=budget.global_market_has_entry,
            market_is_expired=event.market_is_expired,
            market_is_resolved=event.market_is_resolved,
            market_liquidity_usdc=event.market_liquidity_usdc,
            market_volume_24h_usdc=event.market_volume_24h_usdc,
            token_symbol=event.token_symbol,
            market_timeframe=event.market_timeframe,
            sports_market_type=event.sports_market_type,
            task_has_entered_sports_type=budget.task_has_sports_type_entry,
        )

    @staticmethod
    def _sizing_request(
        event: CopyTaskSourceEvent,
        follower: FollowerTaskSnapshot,
    ) -> CopySizingRequest:
        leader_balance = (
            event.leader_pusd_balance
            if event.leader_pusd_balance is not None
            else follower.leader_pusd_balance
        )
        return CopySizingRequest(
            leader_wallet=event.trade.leader_wallet,
            leader_trade_notional=event.trade.notional,
            follower_pusd_balance=follower.follower_pusd_balance,
            leader_pusd_balance=leader_balance,
            side="BUY",
            follower_market_exposure=follower.follower_market_exposure,
            follower_reserved_notional=follower.follower_reserved_notional,
            leader_market_position_notional=event.leader_market_position_notional,
            leader_total_portfolio_notional=event.leader_total_portfolio_notional,
            leader_market_weight=event.leader_market_weight,
            current_open_positions=follower.current_open_positions,
            is_existing_market=follower.is_existing_market,
            trust_multiplier=follower.trust_multiplier,
        )

    @staticmethod
    def _reservation_id(task: CopyTask, event: CopyTaskSourceEvent) -> str:
        return CopyTaskEventProcessor._execution_identity(task, event)

    @staticmethod
    def _execution_identity(
        task: CopyTask,
        event: CopyTaskSourceEvent,
        *,
        promotion: bool = False,
    ) -> str:
        """Return one task-scoped identity across source promotions."""

        payload = "|".join(
            (
                str(task.task_id),
                str(task.follower_account).strip().lower(),
                event.trade.promotion_key if promotion else event.trade.economic_key,
            )
        )
        return f"copy-task:{sha256(payload.encode('utf-8')).hexdigest()}"

    @staticmethod
    def _execution_command(
        *,
        event: CopyTaskSourceEvent,
        follower: FollowerTaskSnapshot,
        envelope: RuntimeExecutionEnvelope,
    ) -> TaskExecutionCommand:
        price_limit = envelope.entry_price_decision.price_limit
        if price_limit is None:
            raise RuntimeError("copy-task execution requires an explicit FAK price limit")
        side = envelope.reaction.action.value
        if side == "BUY":
            raw_notional = getattr(envelope.sizing_decision, "final_notional", None)
            amount = _decimal(raw_notional, name="BUY collateral amount", positive=True)
            notional = amount
        elif side == "SELL":
            if follower.sell_shares is None:
                raise RuntimeError("copy-task SELL requires authoritative follower shares")
            amount = follower.sell_shares
            notional = amount * follower.execution_reference_price
        else:
            raise RuntimeError("copy-task execution action must be BUY or SELL")
        return TaskExecutionCommand(
            task=envelope.task,
            source_event=event,
            side=side,
            condition_id=envelope.source_trade.market_id,
            token_id=follower.execution_token_id,
            amount=amount,
            notional_usdc=notional,
            price_limit=price_limit,
            order_type=envelope.entry_price_decision.order_type,
            signal_id=CopyTaskEventProcessor._execution_identity(envelope.task, event),
            task_budget_reservation_id=envelope.task_budget_reservation_id,
            source_promotion_id=CopyTaskEventProcessor._execution_identity(
                envelope.task,
                event,
                promotion=True,
            ),
        )


__all__ = [
    "CachedCopyTaskReader",
    "CopyTaskEventProcessor",
    "CopyTaskReader",
    "CopyTaskSourceEvent",
    "FollowerTaskSnapshot",
    "FollowerTaskSnapshotProvider",
    "TaskExecutionCommand",
    "TaskExecutionPort",
    "TaskRouteResult",
]
