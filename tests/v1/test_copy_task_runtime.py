"""Contracts for lock-first task orchestration."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from services.copy_tasks.admission import CopyTaskAdmissionEngine
from services.copy_tasks.budget import (
    InMemoryTaskBudgetRepository,
    TaskBudgetReservationService,
)
from services.copy_tasks.domain import CopyTask, SourceTrade, TaskAdmissionContext
from services.copy_tasks.domain import TaskSpendPolicy
from services.copy_tasks.entry_price import CopyTaskEntryPriceService
from services.copy_tasks.monitoring import CopyMonitoringModePolicy
from services.copy_tasks.reaction import CopyReactionEngine
from services.copy_tasks.reentry_lock import (
    InMemoryMarketReentryLockRepository,
    MarketReentryKey,
    MarketReentryLock,
    MarketReentryLockService,
    ProtectiveExitReason,
)
from services.copy_tasks.runtime import CopyTaskRuntimeCoordinator, RuntimeDecision


_LEADER = "0x1111111111111111111111111111111111111111"
_NOW = datetime(2026, 8, 8, tzinfo=UTC)


def _admission_context(**changes: object) -> TaskAdmissionContext:
    base: dict[str, object] = {
        "now": _NOW,
        "action": "BUY",
        "outcome_label": "YES",
        "leader_trade_notional": Decimal("10"),
        "leader_price": Decimal("0.5"),
        "market_id": "market",
        "market_title": "Market",
        "category": "Other",
        "days_until_close": Decimal("1"),
        "task_total_spend": Decimal("0"),
        "task_daily_spend": Decimal("0"),
        "task_market_spend": Decimal("0"),
        "task_has_entered_market": False,
        "global_market_has_entry": False,
    }
    base.update(changes)
    return TaskAdmissionContext(**base)  # type: ignore[arg-type]


def _source_trade(**changes: object) -> SourceTrade:
    base: dict[str, object] = {
        "leader_wallet": _LEADER,
        "market_id": "market",
        "outcome_label": "YES",
        "action": "BUY",
        "price": Decimal("0.5"),
        "notional": Decimal("10"),
        "shares": Decimal("20"),
    }
    base.update(changes)
    return SourceTrade(**base)  # type: ignore[arg-type]


class _SizingStub:
    """Sizing port stub that returns an object with ``allowed`` and ``final_notional``."""

    def __init__(self, *, allowed: bool = True, final_notional: str = "10") -> None:
        self._allowed = allowed
        self._final_notional = final_notional

    def size(self, _task: object, _request: object) -> object:
        class _Decision:
            allowed = self._allowed
            final_notional = Decimal(self._final_notional)
            reason = "sized" if self._allowed else "sizing_rejected"

        return _Decision()


@pytest.mark.asyncio
async def test_locked_buy_returns_before_sizing_or_downstream_calls() -> None:
    repository = InMemoryMarketReentryLockRepository()
    lock_service = MarketReentryLockService(repository)
    key = MarketReentryKey("deposit", "task-1", _LEADER, "market")
    await repository.create_if_absent(
        MarketReentryLock(
            key=key,
            reason=ProtectiveExitReason.STOP_LOSS,
            fill_id="fill",
            locked_at=_NOW,
        )
    )
    calls = {"sizing": 0, "downstream": 0}

    class Sizing:
        def size(self, _task: object, _request: object) -> object:
            calls["sizing"] += 1
            return object()

    async def downstream(_decision: object) -> object:
        calls["downstream"] += 1
        return object()

    coordinator = CopyTaskRuntimeCoordinator(
        locks=lock_service,
        monitoring=CopyMonitoringModePolicy(),
        admission=CopyTaskAdmissionEngine(),
        reactions=CopyReactionEngine(),
        entry_prices=CopyTaskEntryPriceService(),
        sizing=Sizing(),
    )
    decision = await coordinator.process(
        task=CopyTask(
            task_id="task-1",
            name="leader",
            leader_wallet=key.leader_wallet,
            follower_account="deposit",
        ),
        source_trade=_source_trade(),
        admission_context=_admission_context(),
        sizing_request=object(),
        downstream=downstream,
    )
    assert isinstance(decision, RuntimeDecision)
    assert decision.allowed is False
    assert decision.reason == "market_locked_after_protective_exit"
    assert calls == {"sizing": 0, "downstream": 0}


@pytest.mark.asyncio
async def test_sell_is_never_blocked_by_reentry_lock() -> None:
    repository = InMemoryMarketReentryLockRepository()
    lock_service = MarketReentryLockService(repository)
    calls = {"sizing": 0, "downstream": 0}

    class Sizing:
        def size(self, _task: object, _request: object) -> object:
            calls["sizing"] += 1
            return object()

    async def downstream(_decision: object) -> str:
        calls["downstream"] += 1
        return "ok"

    task = CopyTask(
        task_id="task-1",
        name="leader",
        leader_wallet=_LEADER,
        follower_account="deposit",
    )
    coordinator = CopyTaskRuntimeCoordinator(
        locks=lock_service,
        monitoring=CopyMonitoringModePolicy(),
        admission=CopyTaskAdmissionEngine(),
        reactions=CopyReactionEngine(),
        entry_prices=CopyTaskEntryPriceService(),
        sizing=Sizing(),
    )
    decision = await coordinator.process(
        task=task,
        source_trade=_source_trade(action="SELL"),
        admission_context=_admission_context(action="SELL", task_has_entered_market=True),
        sizing_request=None,
        downstream=downstream,
    )
    assert decision.allowed is True
    assert calls == {"sizing": 0, "downstream": 1}


@pytest.mark.asyncio
async def test_disabled_runtime_returns_before_reaction_lock_sizing_and_downstream() -> None:
    calls: list[str] = []

    class LockSpy:
        async def is_locked(self, _key: object) -> bool:
            calls.append("lock")
            return False

    class ReactionSpy:
        def decide(self, _policy: object, _trade: object) -> object:
            calls.append("reaction")
            raise AssertionError("disabled runtime must not evaluate reaction")

    class SizingSpy:
        def size(self, _task: object, _request: object) -> object:
            calls.append("sizing")
            raise AssertionError("disabled runtime must not size")

    coordinator = CopyTaskRuntimeCoordinator(
        locks=LockSpy(),
        monitoring=CopyMonitoringModePolicy(),
        admission=CopyTaskAdmissionEngine(),
        reactions=ReactionSpy(),
        entry_prices=CopyTaskEntryPriceService(),
        sizing=SizingSpy(),
        enabled=False,
    )

    result = await coordinator.process(
        task=None,  # type: ignore[arg-type]
        source_trade=None,  # type: ignore[arg-type]
        admission_context=None,  # type: ignore[arg-type]
        sizing_request=object(),
        downstream=lambda _envelope: calls.append("downstream"),
    )

    assert result.allowed is False
    assert result.reason == "copy_tasks_disabled"
    assert calls == []


@pytest.mark.asyncio
async def test_buy_with_task_budget_reserves_before_downstream() -> None:
    budget_repo = InMemoryTaskBudgetRepository()
    budget_service = TaskBudgetReservationService(budget_repo)
    task = CopyTask(
        task_id="task-1",
        name="leader",
        leader_wallet=_LEADER,
        follower_account="deposit",
        spend=TaskSpendPolicy(max_total=Decimal("100")),
    )

    async def downstream(_envelope: object) -> str:
        return "ok"

    coordinator = CopyTaskRuntimeCoordinator(
        locks=MarketReentryLockService(InMemoryMarketReentryLockRepository()),
        monitoring=CopyMonitoringModePolicy(),
        admission=CopyTaskAdmissionEngine(),
        reactions=CopyReactionEngine(),
        entry_prices=CopyTaskEntryPriceService(),
        sizing=_SizingStub(final_notional="10"),
        task_budgets=budget_service,
    )
    decision = await coordinator.process(
        task=task,
        source_trade=_source_trade(),
        admission_context=_admission_context(),
        sizing_request=object(),
        downstream=downstream,
        reservation_id="res-1",
    )
    assert decision.allowed is True
    assert decision.envelope is not None
    assert decision.envelope.task_budget_reservation_id == "res-1"


@pytest.mark.asyncio
async def test_downstream_exception_releases_task_budget_reservation() -> None:
    budget_repo = InMemoryTaskBudgetRepository()
    budget_service = TaskBudgetReservationService(budget_repo)
    task = CopyTask(
        task_id="task-1",
        name="leader",
        leader_wallet=_LEADER,
        follower_account="deposit",
        spend=TaskSpendPolicy(max_total=Decimal("100")),
    )

    class DownstreamError(RuntimeError):
        pass

    async def downstream(_envelope: object) -> None:
        raise DownstreamError("venue failure")

    coordinator = CopyTaskRuntimeCoordinator(
        locks=MarketReentryLockService(InMemoryMarketReentryLockRepository()),
        monitoring=CopyMonitoringModePolicy(),
        admission=CopyTaskAdmissionEngine(),
        reactions=CopyReactionEngine(),
        entry_prices=CopyTaskEntryPriceService(),
        sizing=_SizingStub(final_notional="10"),
        task_budgets=budget_service,
    )
    with pytest.raises(DownstreamError):
        await coordinator.process(
            task=task,
            source_trade=_source_trade(),
            admission_context=_admission_context(),
            sizing_request=object(),
            downstream=downstream,
            reservation_id="res-err",
        )
    # After downstream failure the reservation must be released, so a new
    # reservation for the same market must be allowed again.
    from datetime import timedelta

    from services.copy_tasks.budget import TaskBudgetKey, TaskBudgetReservationRequest

    second = await budget_service.reserve(
        TaskBudgetReservationRequest(
            reservation_id="res-2",
            key=TaskBudgetKey(
                follower_account="deposit",
                task_id="task-1",
                market_id="market",
            ),
            amount=Decimal("10"),
            reserved_at=_NOW,
            expires_at=_NOW + timedelta(seconds=60),
        ),
        task.spend,
    )
    assert second.allowed is True


@pytest.mark.asyncio
async def test_buy_without_task_budget_service_when_required_fails_closed() -> None:
    task = CopyTask(
        task_id="task-1",
        name="leader",
        leader_wallet=_LEADER,
        follower_account="deposit",
        spend=TaskSpendPolicy(max_total=Decimal("100")),
    )

    async def downstream(_envelope: object) -> str:
        return "ok"

    coordinator = CopyTaskRuntimeCoordinator(
        locks=MarketReentryLockService(InMemoryMarketReentryLockRepository()),
        monitoring=CopyMonitoringModePolicy(),
        admission=CopyTaskAdmissionEngine(),
        reactions=CopyReactionEngine(),
        entry_prices=CopyTaskEntryPriceService(),
        sizing=_SizingStub(final_notional="10"),
        task_budgets=None,
    )
    decision = await coordinator.process(
        task=task,
        source_trade=_source_trade(),
        admission_context=_admission_context(),
        sizing_request=object(),
        downstream=downstream,
        reservation_id="res-1",
    )
    assert decision.allowed is False
    assert decision.reason == "task_budget_service_required"


@pytest.mark.asyncio
async def test_sizing_rejected_blocks_downstream() -> None:
    task = CopyTask(
        task_id="task-1",
        name="leader",
        leader_wallet=_LEADER,
        follower_account="deposit",
    )
    calls: list[str] = []

    async def downstream(_envelope: object) -> str:
        calls.append("downstream")
        return "ok"

    coordinator = CopyTaskRuntimeCoordinator(
        locks=MarketReentryLockService(InMemoryMarketReentryLockRepository()),
        monitoring=CopyMonitoringModePolicy(),
        admission=CopyTaskAdmissionEngine(),
        reactions=CopyReactionEngine(),
        entry_prices=CopyTaskEntryPriceService(),
        sizing=_SizingStub(allowed=False),
    )
    decision = await coordinator.process(
        task=task,
        source_trade=_source_trade(),
        admission_context=_admission_context(),
        sizing_request=object(),
        downstream=downstream,
    )
    assert decision.allowed is False
    assert decision.reason == "sizing_rejected"
    assert calls == []
