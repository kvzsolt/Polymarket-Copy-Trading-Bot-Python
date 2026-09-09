"""Lock-first copy-task orchestration seam.

This coordinator sequences pure task decisions and a sizing port. It does not
know about CCXT, the official SDK, PostgreSQL, or network clients. Existing
risk, reservation, and execution services remain downstream collaborators.
"""

from __future__ import annotations

import asyncio
import inspect
from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal, InvalidOperation
from typing import Awaitable, Callable, Protocol

from domain.detection import DetectionFinality
from services.copy_sizing_v5.domain import CopySizingDecision, CopySizingRequest
from services.copy_tasks.admission import CopyTaskAdmissionEngine
from services.copy_tasks.budget import (
    TaskBudgetKey,
    TaskBudgetReservationRequest,
    TaskBudgetReservationService,
)
from services.copy_tasks.domain import CopyTask, MonitoringMode, SourceTrade, TaskAdmissionContext
from services.copy_tasks.entry_price import TaskEntryPriceDecision
from services.copy_tasks.lifecycle import TaskExecutionDisposition, TaskExecutionOutcome
from services.copy_tasks.monitoring import MonitoringDecision
from services.copy_tasks.reaction import CopyReactionEngine, ReactionAction, ReactionDecision
from services.copy_tasks.reentry_lock import MarketReentryKey, MarketReentryLockService


class MonitoringPort(Protocol):
    """Small source-finality interface required by task orchestration."""

    def evaluate(
        self,
        *,
        mode: MonitoringMode,
        finality: DetectionFinality,
    ) -> MonitoringDecision:
        """Return whether one source event is actionable for the task."""

        ...


class EntryPricePort(Protocol):
    """Small explicit-price interface required by task orchestration."""

    def evaluate(
        self,
        *,
        task: CopyTask,
        follower_action: str,
        source_price: Decimal,
        execution_reference_price: Decimal | None = None,
        counter_trade: bool = False,
    ) -> TaskEntryPriceDecision:
        """Return one explicit FAK cap/floor decision."""

        ...


class SizingPort(Protocol):
    """Small sizing interface required by task orchestration."""

    def size(self, task: CopyTask, request: CopySizingRequest) -> CopySizingDecision:
        """Return a task-specific sizing decision for a BUY request."""

        ...


class TaskBudgetLifecyclePort(Protocol):
    """Small typed outcome port required after downstream execution."""

    async def apply_execution_outcome(
        self,
        *,
        reservation_id: str,
        outcome: TaskExecutionOutcome,
    ) -> object:
        """Settle or retain task budget from explicit execution evidence."""

        ...


@dataclass(frozen=True, slots=True)
class RuntimeExecutionEnvelope:
    """Normalized task decision passed to existing risk/execution orchestration.

    Attributes:
        task: Immutable task that produced the envelope.
        source_trade: Immutable source trade that triggered the decision.
        reaction: Follower reaction decision.
        entry_price_decision: Explicit FAK cap/floor decision.
        sizing_decision: Task-specific sizing decision (BUY only).
        task_budget_reservation_id: Task budget reservation identity when the
            task spend policy required a durable reservation.
    """

    task: CopyTask
    source_trade: SourceTrade
    reaction: ReactionDecision
    entry_price_decision: TaskEntryPriceDecision
    sizing_decision: object | None
    task_budget_reservation_id: str | None = None


@dataclass(frozen=True, slots=True)
class RuntimeDecision:
    """Auditable result of the v1 task orchestration boundary."""

    allowed: bool
    reason: str
    envelope: RuntimeExecutionEnvelope | None = None
    downstream_result: object | None = None


DownstreamHandler = Callable[[RuntimeExecutionEnvelope], Awaitable[object] | object]


class CopyTaskRuntimeCoordinator:
    """Reject re-entry before sizing/risk/reservation/execution side effects.

    The coordinator sequences: monitoring finality, reaction, re-entry lock,
    entry price, admission, sizing, task budget reservation, then downstream
    risk/execution. A rejected gate returns before any expensive or mutable
    operation.
    """

    def __init__(
        self,
        *,
        locks: MarketReentryLockService,
        monitoring: MonitoringPort,
        admission: CopyTaskAdmissionEngine,
        reactions: CopyReactionEngine,
        entry_prices: EntryPricePort,
        sizing: SizingPort,
        task_budgets: TaskBudgetReservationService | None = None,
        task_budget_lifecycle: TaskBudgetLifecyclePort | None = None,
        task_budget_ttl_seconds: int = 120,
        enabled: bool = True,
        reentry_lock_enabled: bool = True,
    ) -> None:
        self._locks = locks
        self._monitoring = monitoring
        self._admission = admission
        self._reactions = reactions
        self._entry_prices = entry_prices
        self._sizing = sizing
        self._task_budgets = task_budgets
        self._task_budget_lifecycle = task_budget_lifecycle
        self._enabled = bool(enabled)
        self._reentry_lock_enabled = bool(reentry_lock_enabled)
        if task_budget_ttl_seconds < 1:
            raise ValueError("task_budget_ttl_seconds must be positive")
        self._task_budget_ttl_seconds = int(task_budget_ttl_seconds)

    async def process(
        self,
        *,
        task: CopyTask,
        source_trade: SourceTrade,
        admission_context: TaskAdmissionContext,
        sizing_request: CopySizingRequest | None,
        downstream: DownstreamHandler,
        reservation_id: str | None = None,
        execution_reference_price: Decimal | None = None,
        prior_reservation_ids: tuple[str, ...] = (),
    ) -> RuntimeDecision:
        """Process one source event through task gates and existing downstream code.

        Args:
            task: Immutable task policy.
            source_trade: Vendor-neutral source trade.
            admission_context: Persisted task snapshot and market metadata.
            sizing_request: BUY sizing request (None for SELL).
            downstream: Callback that receives the envelope and invokes the
                existing risk/reservation/execution path.
            reservation_id: Optional task budget reservation identity.
            prior_reservation_ids: Provisional allocations that already own this action.
            execution_reference_price: Optional authoritative follower outcome
                price (required for counter trades).

        Returns:
            Auditable runtime decision with envelope and downstream result.
        """

        if not self._enabled:
            return RuntimeDecision(False, "copy_tasks_disabled")

        monitoring = self._monitoring.evaluate(
            mode=task.monitoring_mode,
            finality=source_trade.finality,
        )
        if not monitoring.allowed:
            return RuntimeDecision(False, monitoring.reason)

        reaction = self._reactions.decide(task.reaction, source_trade)
        if reaction.action is ReactionAction.SKIP:
            return RuntimeDecision(False, reaction.reason)

        follower_action = reaction.action.value
        normalized_context = TaskAdmissionContext(
            now=admission_context.now,
            action=follower_action,
            outcome_label=reaction.outcome_label,
            leader_trade_notional=admission_context.leader_trade_notional,
            leader_price=admission_context.leader_price,
            market_id=admission_context.market_id,
            market_title=admission_context.market_title,
            category=admission_context.category,
            days_until_close=admission_context.days_until_close,
            task_total_spend=admission_context.task_total_spend,
            task_daily_spend=admission_context.task_daily_spend,
            task_market_spend=admission_context.task_market_spend,
            task_has_entered_market=admission_context.task_has_entered_market,
            global_market_has_entry=admission_context.global_market_has_entry,
            proposed_notional=admission_context.proposed_notional,
            market_is_expired=admission_context.market_is_expired,
            market_is_resolved=admission_context.market_is_resolved,
            market_liquidity_usdc=admission_context.market_liquidity_usdc,
            market_volume_24h_usdc=admission_context.market_volume_24h_usdc,
            token_symbol=admission_context.token_symbol,
            market_timeframe=admission_context.market_timeframe,
            sports_market_type=admission_context.sports_market_type,
            task_has_entered_sports_type=admission_context.task_has_entered_sports_type,
        )

        if reaction.action is ReactionAction.BUY and self._reentry_lock_enabled:
            key = MarketReentryKey(
                follower_account=task.follower_account,
                task_id=task.task_id,
                leader_wallet=task.leader_wallet.lower(),
                market_id=source_trade.market_id,
            )
            if await self._locks.is_locked(key):
                return RuntimeDecision(False, "market_locked_after_protective_exit")

        entry_price = self._entry_prices.evaluate(
            task=task,
            follower_action=follower_action,
            source_price=source_trade.price,
            execution_reference_price=execution_reference_price,
            counter_trade=task.reaction.counter_trade,
        )
        if not entry_price.allowed:
            return RuntimeDecision(False, entry_price.reason)
        if entry_price.reference_price is None:
            return RuntimeDecision(False, "execution_reference_price_missing")
        normalized_context = _with_leader_price(normalized_context, entry_price.reference_price)

        admission = self._admission.evaluate(
            task,
            normalized_context,
            include_spend=reaction.action is not ReactionAction.BUY,
        )
        if not admission.allowed:
            return RuntimeDecision(False, admission.reason)

        sizing_decision: object | None = None
        task_budget_reservation_id: str | None = None
        if reaction.action is ReactionAction.BUY:
            if sizing_request is None:
                return RuntimeDecision(False, "buy_sizing_request_missing")
            sizing_decision = self._sizing.size(task, sizing_request)
            if getattr(sizing_decision, "allowed", True) is False:
                return RuntimeDecision(
                    False,
                    str(getattr(sizing_decision, "reason", "sizing_rejected")),
                )
            final_notional = self._final_notional(sizing_decision)
            sized_context = _with_proposed_notional(normalized_context, final_notional)
            sized_admission = self._admission.evaluate(
                task,
                sized_context,
                include_spend=True,
            )
            if not sized_admission.allowed:
                return RuntimeDecision(False, sized_admission.reason)
            if self._requires_task_budget(task):
                if self._task_budgets is None:
                    return RuntimeDecision(False, "task_budget_service_required")
                if not str(reservation_id or "").strip():
                    return RuntimeDecision(False, "task_budget_reservation_id_missing")
                budget = await self._task_budgets.reserve(
                    TaskBudgetReservationRequest(
                        reservation_id=str(reservation_id),
                        key=TaskBudgetKey(
                            follower_account=task.follower_account,
                            task_id=task.task_id,
                            market_id=source_trade.market_id,
                            sports_market_type=normalized_context.sports_market_type,
                        ),
                        amount=final_notional,
                        reserved_at=normalized_context.now,
                        expires_at=(
                            normalized_context.now
                            + timedelta(seconds=self._task_budget_ttl_seconds)
                        ),
                        prior_reservation_ids=prior_reservation_ids,
                    ),
                    task.spend,
                )
                if not budget.allowed:
                    return RuntimeDecision(False, budget.reason)
                task_budget_reservation_id = str(reservation_id)

        envelope = RuntimeExecutionEnvelope(
            task=task,
            source_trade=source_trade,
            reaction=reaction,
            entry_price_decision=entry_price,
            sizing_decision=sizing_decision,
            task_budget_reservation_id=task_budget_reservation_id,
        )
        if task_budget_reservation_id is not None and self._task_budgets is not None:
            await self._task_budgets.retain(task_budget_reservation_id)
        try:
            result = downstream(envelope)
            if inspect.isawaitable(result):
                result = await result
        except BaseException:
            if task_budget_reservation_id is not None and self._task_budgets is not None:
                await asyncio.shield(self._task_budgets.retain(task_budget_reservation_id))
            raise
        if task_budget_reservation_id is not None and self._task_budgets is not None:
            if isinstance(result, TaskExecutionOutcome):
                if self._task_budget_lifecycle is None:
                    await self._task_budgets.retain(task_budget_reservation_id)
                    raise RuntimeError(
                        "task budget lifecycle is required for typed execution outcomes"
                    )
                await self._task_budget_lifecycle.apply_execution_outcome(
                    reservation_id=task_budget_reservation_id,
                    outcome=result,
                )
            else:
                await self._task_budgets.retain(task_budget_reservation_id)
        if isinstance(result, TaskExecutionOutcome) and result.disposition in {
            TaskExecutionDisposition.PRE_POST_REJECTED,
            TaskExecutionDisposition.TERMINAL_NO_FILL,
            TaskExecutionDisposition.CONFIRMED_ABSENT,
        }:
            return RuntimeDecision(
                False,
                result.reason or result.disposition.value,
                envelope=envelope,
                downstream_result=result,
            )
        return RuntimeDecision(
            True,
            "admitted",
            envelope=envelope,
            downstream_result=result,
        )

    @staticmethod
    def _requires_task_budget(task: CopyTask) -> bool:
        policy = task.spend
        return policy.enabled and (
            policy.max_total > 0
            or policy.max_daily > 0
            or policy.max_per_market > 0
            or policy.one_entry_per_market
            or policy.global_one_entry_per_market
            or policy.one_entry_per_sports_type
        )

    @staticmethod
    def _final_notional(sizing_decision: object) -> Decimal:
        raw = getattr(sizing_decision, "final_notional", None)
        try:
            result = Decimal(str(raw))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise ValueError("sizing decision final_notional is required") from exc
        if not result.is_finite() or result <= 0:
            raise ValueError("sizing decision final_notional must be positive")
        return result


def _with_leader_price(
    context: TaskAdmissionContext,
    leader_price: Decimal,
) -> TaskAdmissionContext:
    """Return a copy of ``context`` with the explicit leader price replaced."""

    return TaskAdmissionContext(
        now=context.now,
        action=context.action,
        outcome_label=context.outcome_label,
        leader_trade_notional=context.leader_trade_notional,
        leader_price=leader_price,
        market_id=context.market_id,
        market_title=context.market_title,
        category=context.category,
        days_until_close=context.days_until_close,
        task_total_spend=context.task_total_spend,
        task_daily_spend=context.task_daily_spend,
        task_market_spend=context.task_market_spend,
        task_has_entered_market=context.task_has_entered_market,
        global_market_has_entry=context.global_market_has_entry,
        proposed_notional=context.proposed_notional,
        market_is_expired=context.market_is_expired,
        market_is_resolved=context.market_is_resolved,
        market_liquidity_usdc=context.market_liquidity_usdc,
        market_volume_24h_usdc=context.market_volume_24h_usdc,
        token_symbol=context.token_symbol,
        market_timeframe=context.market_timeframe,
        sports_market_type=context.sports_market_type,
        task_has_entered_sports_type=context.task_has_entered_sports_type,
    )


def _with_proposed_notional(
    context: TaskAdmissionContext,
    proposed_notional: Decimal,
) -> TaskAdmissionContext:
    """Return a copy of ``context`` with the sized proposed notional set."""

    return TaskAdmissionContext(
        now=context.now,
        action=context.action,
        outcome_label=context.outcome_label,
        leader_trade_notional=context.leader_trade_notional,
        leader_price=context.leader_price,
        market_id=context.market_id,
        market_title=context.market_title,
        category=context.category,
        days_until_close=context.days_until_close,
        task_total_spend=context.task_total_spend,
        task_daily_spend=context.task_daily_spend,
        task_market_spend=context.task_market_spend,
        task_has_entered_market=context.task_has_entered_market,
        global_market_has_entry=context.global_market_has_entry,
        proposed_notional=proposed_notional,
        market_is_expired=context.market_is_expired,
        market_is_resolved=context.market_is_resolved,
        market_liquidity_usdc=context.market_liquidity_usdc,
        market_volume_24h_usdc=context.market_volume_24h_usdc,
        token_symbol=context.token_symbol,
        market_timeframe=context.market_timeframe,
        sports_market_type=context.sports_market_type,
        task_has_entered_sports_type=context.task_has_entered_sports_type,
    )


__all__ = [
    "CopyTaskRuntimeCoordinator",
    "EntryPricePort",
    "MonitoringPort",
    "RuntimeDecision",
    "RuntimeExecutionEnvelope",
    "SizingPort",
]
