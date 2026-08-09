"""Runtime ordering contracts for source finality monitoring modes."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from domain.detection import DetectionFinality
from services.copy_tasks.admission import CopyTaskAdmissionEngine
from services.copy_tasks.domain import (
    CopyTask,
    MonitoringMode,
    SourceTrade,
    TaskAdmissionContext,
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
    """Successful deterministic sizing result."""

    allowed = True
    reason = "sized"
    final_notional = Decimal("5")


class Sizing:
    """Sizing spy used to prove monitoring executes first."""

    def __init__(self) -> None:
        self.calls = 0

    def size(self, _task: CopyTask, _request: object) -> SizingDecision:
        self.calls += 1
        return SizingDecision()


def task(mode: MonitoringMode) -> CopyTask:
    """Return one task using the requested monitoring mode."""

    return CopyTask(
        task_id=f"task-{mode.value}",
        name="Task",
        leader_wallet="0x1111111111111111111111111111111111111111",
        follower_account="account-a",
        monitoring_mode=mode,
    )


def trade(finality: DetectionFinality) -> SourceTrade:
    """Return one source BUY with explicit detection finality."""

    return SourceTrade(
        leader_wallet="0x1111111111111111111111111111111111111111",
        market_id="market-a",
        outcome_label="YES",
        action="BUY",
        price=Decimal("0.5"),
        notional=Decimal("10"),
        shares=Decimal("20"),
        finality=finality,
    )


def context() -> TaskAdmissionContext:
    """Return one otherwise-valid task admission snapshot."""

    return TaskAdmissionContext(
        now=datetime(2026, 8, 8, tzinfo=UTC),
        action="BUY",
        outcome_label="YES",
        leader_trade_notional=Decimal("10"),
        leader_price=Decimal("0.5"),
        market_id="market-a",
        market_title="Market",
        category="Other",
        days_until_close=Decimal("1"),
        task_total_spend=Decimal("0"),
        task_daily_spend=Decimal("0"),
        task_market_spend=Decimal("0"),
        task_has_entered_market=False,
        global_market_has_entry=False,
    )


def coordinator(sizing: Sizing) -> CopyTaskRuntimeCoordinator:
    """Build one runtime with all pure policies injected."""

    return CopyTaskRuntimeCoordinator(
        locks=MarketReentryLockService(InMemoryMarketReentryLockRepository()),
        monitoring=CopyMonitoringModePolicy(),
        admission=CopyTaskAdmissionEngine(),
        reactions=CopyReactionEngine(),
        entry_prices=CopyTaskEntryPriceService(),
        sizing=sizing,
    )


@pytest.mark.asyncio
async def test_normal_task_skips_provisional_before_sizing_and_downstream() -> None:
    sizing = Sizing()
    downstream_calls = 0

    async def downstream(_envelope: object) -> None:
        nonlocal downstream_calls
        downstream_calls += 1

    decision = await coordinator(sizing).process(
        task=task(MonitoringMode.NORMAL),
        source_trade=trade(DetectionFinality.PROVISIONAL),
        admission_context=context(),
        sizing_request=object(),
        downstream=downstream,
    )

    assert decision.allowed is False
    assert decision.reason == "normal_mode_waiting_for_confirmation"
    assert sizing.calls == 0
    assert downstream_calls == 0


@pytest.mark.asyncio
async def test_priority_task_accepts_provisional_event() -> None:
    sizing = Sizing()

    decision = await coordinator(sizing).process(
        task=task(MonitoringMode.PRIORITY),
        source_trade=trade(DetectionFinality.PROVISIONAL),
        admission_context=context(),
        sizing_request=object(),
        downstream=lambda _envelope: "ok",
    )

    assert decision.allowed is True
    assert sizing.calls == 1