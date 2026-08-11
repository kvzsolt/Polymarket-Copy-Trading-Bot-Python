"""Route canonical leader events through copy-task policy and execution ports.

The service remains venue-neutral. It combines immutable source events with a
read-only follower snapshot, then delegates every decision to the existing
copy-task coordinator and every mutation to an injected execution port.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from hashlib import sha256
from typing import Protocol

from domain.detection import LeaderTradeEvent
from services.copy_sizing_v5.domain import CopySizingRequest
from services.copy_tasks.budget import (
    TaskBudgetKey,
    TaskBudgetReservationService,
    TaskBudgetSnapshot,
)
from services.copy_tasks.domain import (
    CopyTask,
    SourceTrade,
    TaskAdmissionContext,
)
from services.copy_tasks.lifecycle import TaskExecutionOutcome
from services.copy_tasks.runtime import (
    CopyTaskRuntimeCoordinator,
    RuntimeDecision,
    RuntimeExecutionEnvelope,
)

_ZERO = Decimal("0")
logger = logging.getLogger(__name__)


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
                    positive=name == "follower_pusd_balance",
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
    ) -> None:
        self._task_reader = task_reader
        self._task_budgets = task_budgets
        self._coordinator = coordinator
        self._snapshot_provider = snapshot_provider
        self._execution = execution

    async def __call__(self, leader: str, event: CopyTaskSourceEvent) -> None:
        """Handle one worker event while keeping worker callback shape narrow."""

        await self.process(leader, event)

    async def process(
        self,
        leader: str,
        event: CopyTaskSourceEvent,
    ) -> tuple[TaskRouteResult, ...]:
        """Route one event through matching tasks in stable task-id order."""

        normalized_leader = str(leader).strip().lower()
        if normalized_leader != event.trade.leader_wallet.lower():
            raise ValueError("worker leader does not match source event leader")
        tasks = tuple(
            task
            for task in await self._task_reader.list_active()
            if task.enabled and task.leader_wallet.lower() == normalized_leader
        )
        return tuple(
            [
                await self._process_task(task, event)
                for task in sorted(tasks, key=lambda item: item.task_id)
            ]
        )

    async def _process_task(
        self,
        task: CopyTask,
        event: CopyTaskSourceEvent,
    ) -> TaskRouteResult:
        try:
            follower = await self._snapshot_provider.snapshot_for(task, event)
        except Exception as exc:
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
        source = self._source_trade(event)
        if source.action == "SELL" and follower.sell_shares is None:
            return TaskRouteResult(
                task_id=task.task_id,
                allowed=False,
                reason="follower_sell_shares_unavailable",
            )
        budget = await self._task_budgets.snapshot(
            TaskBudgetKey(
                follower_account=task.follower_account,
                task_id=task.task_id,
                market_id=source.market_id,
                sports_market_type=event.sports_market_type,
            ),
            event.trade.detected_at,
        )
        admission = self._admission_context(event, source, budget)
        sizing = self._sizing_request(event, follower) if source.action == "BUY" else None
        reservation_id = self._reservation_id(task, event)

        async def downstream(envelope: RuntimeExecutionEnvelope) -> TaskExecutionOutcome:
            command = self._execution_command(
                event=event,
                follower=follower,
                envelope=envelope,
            )
            return await self._execution.submit(command)

        decision = await self._coordinator.process(
            task=task,
            source_trade=source,
            admission_context=admission,
            sizing_request=sizing,
            downstream=downstream,
            reservation_id=reservation_id,
            execution_reference_price=follower.execution_reference_price,
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
    ) -> TaskAdmissionContext:
        return TaskAdmissionContext(
            now=event.trade.detected_at,
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
        return CopySizingRequest(
            leader_wallet=event.trade.leader_wallet,
            leader_trade_notional=event.trade.notional,
            follower_pusd_balance=follower.follower_pusd_balance,
            leader_pusd_balance=event.leader_pusd_balance,
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
    def _execution_identity(task: CopyTask, event: CopyTaskSourceEvent) -> str:
        """Return one task-scoped identity across source promotions."""

        payload = "|".join(
            (
                str(task.task_id),
                str(task.follower_account).strip().lower(),
                event.trade.economic_key,
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
        )


__all__ = [
    "CopyTaskEventProcessor",
    "CopyTaskReader",
    "CopyTaskSourceEvent",
    "FollowerTaskSnapshot",
    "FollowerTaskSnapshotProvider",
    "TaskExecutionCommand",
    "TaskExecutionPort",
    "TaskRouteResult",
]
