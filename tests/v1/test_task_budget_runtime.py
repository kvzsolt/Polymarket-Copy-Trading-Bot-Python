"""Runtime ordering tests for task budget reservations."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from services.copy_tasks.admission import CopyTaskAdmissionEngine
from services.copy_tasks.budget import (
    InMemoryTaskBudgetRepository,
    TaskBudgetReservationService,
)
from services.copy_tasks.domain import (
    CopyTask,
    SourceTrade,
    TaskAdmissionContext,
    TaskSpendPolicy,
)
from services.copy_tasks.entry_price import CopyTaskEntryPriceService
from services.copy_tasks.monitoring import CopyMonitoringModePolicy
from services.copy_tasks.reaction import CopyReactionEngine
from services.copy_tasks.reentry_lock import (
    InMemoryMarketReentryLockRepository,
    MarketReentryLockService,
)
from services.copy_tasks.runtime import CopyTaskRuntimeCoordinator


class SizingDecision:
    """Minimal successful sizing result exposing exact final notional."""

    allowed = True
    reason = "sized"
    final_notional = Decimal("7.5")


class Sizing:
    """Deterministic sizing fake."""

    def size(self, _task: object, _request: object) -> SizingDecision:
        return SizingDecision()


def context() -> TaskAdmissionContext:
    """Return one open admission snapshot."""

    return TaskAdmissionContext(
        now=datetime(2026, 8, 8, 12, tzinfo=UTC),
        action="BUY",
        outcome_label="YES",
        leader_trade_notional=Decimal("100"),
        leader_price=Decimal("0.5"),
        market_id="market-a",
        market_title="Market",
        category="Other",
        days_until_close=Decimal("2"),
        task_total_spend=Decimal("0"),
        task_daily_spend=Decimal("0"),
        task_market_spend=Decimal("0"),
        task_has_entered_market=False,
        global_market_has_entry=False,
    )


def trade() -> SourceTrade:
    """Return one source BUY."""

    return SourceTrade(
        leader_wallet="0x1111111111111111111111111111111111111111",
        market_id="market-a",
        outcome_label="YES",
        action="BUY",
        price=Decimal("0.5"),
        notional=Decimal("100"),
        shares=Decimal("200"),
    )


@pytest.mark.asyncio
async def test_runtime_reserves_final_sized_amount_before_downstream() -> None:
    budgets = TaskBudgetReservationService(InMemoryTaskBudgetRepository())
    task = CopyTask(
        task_id="task-a",
        name="Task",
        leader_wallet=trade().leader_wallet,
        follower_account="account-a",
        spend=TaskSpendPolicy(max_total=Decimal("8")),
    )
    observations: list[object] = []

    async def downstream(envelope: object) -> str:
        observations.append(envelope)
        return "submitted"

    coordinator = CopyTaskRuntimeCoordinator(
        locks=MarketReentryLockService(InMemoryMarketReentryLockRepository()),
        monitoring=CopyMonitoringModePolicy(),
        admission=CopyTaskAdmissionEngine(),
        reactions=CopyReactionEngine(),
        entry_prices=CopyTaskEntryPriceService(),
        sizing=Sizing(),
        task_budgets=budgets,
    )

    decision = await coordinator.process(
        task=task,
        source_trade=trade(),
        admission_context=context(),
        sizing_request=object(),
        reservation_id="intent-a",
        downstream=downstream,
    )

    assert decision.allowed is True
    assert decision.envelope is not None
    assert decision.envelope.task_budget_reservation_id == "intent-a"
    assert len(observations) == 1

    blocked = await coordinator.process(
        task=task,
        source_trade=trade(),
        admission_context=context(),
        sizing_request=object(),
        reservation_id="intent-b",
        downstream=downstream,
    )
    assert blocked.allowed is False
    assert blocked.reason == "task_max_total_spend_exceeded"
    assert len(observations) == 1


@pytest.mark.asyncio
async def test_downstream_exception_releases_task_budget_reservation() -> None:
    budgets = TaskBudgetReservationService(InMemoryTaskBudgetRepository())
    task = CopyTask(
        task_id="task-a",
        name="Task",
        leader_wallet=trade().leader_wallet,
        follower_account="account-a",
        spend=TaskSpendPolicy(max_total=Decimal("8")),
    )

    async def failing(_envelope: object) -> None:
        raise RuntimeError("boom")

    coordinator = CopyTaskRuntimeCoordinator(
        locks=MarketReentryLockService(InMemoryMarketReentryLockRepository()),
        monitoring=CopyMonitoringModePolicy(),
        admission=CopyTaskAdmissionEngine(),
        reactions=CopyReactionEngine(),
        entry_prices=CopyTaskEntryPriceService(),
        sizing=Sizing(),
        task_budgets=budgets,
    )

    with pytest.raises(RuntimeError, match="boom"):
        await coordinator.process(
            task=task,
            source_trade=trade(),
            admission_context=context(),
            sizing_request=object(),
            reservation_id="intent-a",
            downstream=failing,
        )

    admitted = await coordinator.process(
        task=task,
        source_trade=trade(),
        admission_context=context(),
        sizing_request=object(),
        reservation_id="intent-b",
        downstream=lambda _envelope: "ok",
    )
    assert admitted.allowed is True