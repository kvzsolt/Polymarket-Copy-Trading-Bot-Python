"""Canonical leader-event routing into copy-task application services."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from pmcopy.domain.detection import (
    DetectionFinality,
    DetectionMode,
    LeaderTradeEvent,
    TradeSide,
)
from pmcopy.services.copy_sizing_v5.policy import CopySizingPolicy
from pmcopy.services.copy_tasks.admission import CopyTaskAdmissionEngine
from pmcopy.services.copy_tasks.budget import (
    InMemoryTaskBudgetRepository,
    TaskBudgetReservationService,
)
from pmcopy.services.copy_tasks.domain import (
    CopyTask,
    CopyTaskStatus,
    MonitoringMode,
    TaskSizingMode,
    TaskSizingPolicy,
    TaskSpendPolicy,
)
from pmcopy.services.copy_tasks.entry_price import CopyTaskEntryPriceService
from pmcopy.services.copy_tasks.event_routing import (
    CopyTaskEventProcessor,
    CopyTaskSourceEvent,
    FollowerTaskSnapshot,
    TaskExecutionCommand,
)
from pmcopy.services.copy_tasks.lifecycle import (
    CopyTaskFillLifecycleService,
    TaskExecutionDisposition,
    TaskExecutionOutcome,
)
from pmcopy.services.copy_tasks.monitoring import CopyMonitoringModePolicy
from pmcopy.services.copy_tasks.reaction import CopyReactionEngine
from pmcopy.services.copy_tasks.reentry_lock import (
    InMemoryMarketReentryLockRepository,
    MarketReentryLockService,
)
from pmcopy.services.copy_tasks.runtime import CopyTaskRuntimeCoordinator
from pmcopy.services.copy_tasks.service import InMemoryCopyTaskRepository
from pmcopy.services.copy_tasks.sizing import CopyTaskSizingService

_LEADER = "0x1111111111111111111111111111111111111111"
_OTHER = "0x2222222222222222222222222222222222222222"
_NOW = datetime(2026, 8, 11, 12, tzinfo=UTC)


class SnapshotProvider:
    """Return deterministic follower state without external I/O."""

    def __init__(self, snapshot: FollowerTaskSnapshot) -> None:
        self._snapshot = snapshot

    async def snapshot_for(
        self,
        task: CopyTask,
        event: CopyTaskSourceEvent,
    ) -> FollowerTaskSnapshot:
        """Return the configured snapshot."""

        del task, event
        return self._snapshot


class SelectiveSnapshotProvider:
    """Fail one task while keeping later task snapshots available."""

    async def snapshot_for(
        self,
        task: CopyTask,
        event: CopyTaskSourceEvent,
    ) -> FollowerTaskSnapshot:
        """Raise for task-a and return a snapshot for every other task."""

        del event
        if task.task_id == "task-a":
            raise RuntimeError("deterministic unavailable snapshot")
        return snapshot()


class ExecutionRecorder:
    """Capture task execution commands and return typed venue evidence."""

    def __init__(self, disposition: TaskExecutionDisposition) -> None:
        self.commands: list[TaskExecutionCommand] = []
        self._disposition = disposition

    async def submit(self, command: TaskExecutionCommand) -> TaskExecutionOutcome:
        """Record a command and return deterministic evidence."""

        self.commands.append(command)
        return TaskExecutionOutcome(disposition=self._disposition)


def source_event(
    *,
    leader: str = _LEADER,
    side: TradeSide = TradeSide.BUY,
    mode: DetectionMode = DetectionMode.ON_CHAIN_LOGS_ONLY,
    finality: DetectionFinality = DetectionFinality.CONFIRMED,
    log_index: int | None = 7,
) -> CopyTaskSourceEvent:
    """Build one exact-decimal canonical source event."""

    return CopyTaskSourceEvent(
        trade=LeaderTradeEvent(
            leader_wallet=leader,
            token_id="token-yes",
            condition_id="condition-a",
            side=side,
            price=Decimal("0.40"),
            shares=Decimal("25"),
            notional=Decimal("10"),
            mode=mode,
            finality=finality,
            detected_at=_NOW,
            venue_timestamp=_NOW,
            transaction_hash="0xabc",
            log_index=log_index,
        ),
        outcome_label="YES",
        market_title="Will the event happen?",
        category="politics",
        days_until_close=Decimal("3"),
    )


def task(
    *,
    task_id: str,
    leader: str = _LEADER,
    status: CopyTaskStatus = CopyTaskStatus.ACTIVE,
    monitoring_mode: MonitoringMode = MonitoringMode.NORMAL,
) -> CopyTask:
    """Build one fixed-size task that does not require leader-balance I/O."""

    return CopyTask(
        task_id=task_id,
        name=task_id,
        leader_wallet=leader,
        follower_account="follower-a",
        status=status,
        monitoring_mode=monitoring_mode,
        sizing=TaskSizingPolicy(
            mode=TaskSizingMode.FIXED,
            fixed_amount=Decimal("5"),
            buy_min_amount=Decimal("1"),
            buy_max_amount=Decimal("10"),
        ),
        sizing_mode="fixed",
        spend=TaskSpendPolicy(max_total=Decimal("20")),
    )


def snapshot(*, sell_shares: Decimal | None = Decimal("4")) -> FollowerTaskSnapshot:
    """Build one safe follower portfolio snapshot."""

    return FollowerTaskSnapshot(
        follower_pusd_balance=Decimal("100"),
        follower_reserved_notional=Decimal("0"),
        follower_market_exposure=Decimal("0"),
        current_open_positions=0,
        is_existing_market=False,
        sell_shares=sell_shares,
        execution_token_id="token-yes",
        execution_reference_price=Decimal("0.40"),
    )


def coordinator(
    budgets: TaskBudgetReservationService,
    locks: MarketReentryLockService,
) -> CopyTaskRuntimeCoordinator:
    """Build the real copy-task coordinator around in-memory ports."""

    return CopyTaskRuntimeCoordinator(
        locks=locks,
        monitoring=CopyMonitoringModePolicy(),
        admission=CopyTaskAdmissionEngine(),
        reactions=CopyReactionEngine(),
        entry_prices=CopyTaskEntryPriceService(),
        sizing=CopyTaskSizingService(
            base_policy=CopySizingPolicy(
                mode="fixed",
                fixed_amount=Decimal("5"),
                buy_min_amount=Decimal("1"),
                buy_max_amount=Decimal("10"),
                copyable_balance_percent=Decimal("100"),
                max_position_pct=Decimal("1"),
            )
        ),
        task_budgets=budgets,
        task_budget_lifecycle=CopyTaskFillLifecycleService(
            task_budgets=budgets,
            reentry_locks=locks,
        ),
    )


@pytest.mark.asyncio
async def test_routes_only_active_tasks_for_the_event_leader() -> None:
    """Keep paused and unrelated tasks out of the money path."""

    repository = InMemoryCopyTaskRepository()
    await repository.create(task(task_id="active"))
    await repository.create(task(task_id="paused", status=CopyTaskStatus.PAUSED))
    await repository.create(task(task_id="other", leader=_OTHER))
    budgets = TaskBudgetReservationService(InMemoryTaskBudgetRepository())
    locks = MarketReentryLockService(InMemoryMarketReentryLockRepository())
    execution = ExecutionRecorder(TaskExecutionDisposition.PRE_POST_REJECTED)
    processor = CopyTaskEventProcessor(
        task_reader=repository,
        task_budgets=budgets,
        coordinator=coordinator(budgets, locks),
        snapshot_provider=SnapshotProvider(snapshot()),
        execution=execution,
    )

    results = await processor.process(_LEADER.upper(), source_event())

    assert [(item.task_id, item.allowed) for item in results] == [("active", False)]
    assert results[0].reason == "pre_post_rejected"
    assert len(execution.commands) == 1


@pytest.mark.asyncio
async def test_buy_command_preserves_pusd_amount_and_explicit_fak_cap() -> None:
    """Keep BUY collateral units distinct from SELL share units."""

    repository = InMemoryCopyTaskRepository()
    await repository.create(task(task_id="task-a"))
    budgets = TaskBudgetReservationService(InMemoryTaskBudgetRepository())
    locks = MarketReentryLockService(InMemoryMarketReentryLockRepository())
    execution = ExecutionRecorder(TaskExecutionDisposition.SUBMITTED_UNCONFIRMED)
    processor = CopyTaskEventProcessor(
        task_reader=repository,
        task_budgets=budgets,
        coordinator=coordinator(budgets, locks),
        snapshot_provider=SnapshotProvider(snapshot()),
        execution=execution,
    )

    first = await processor.process(_LEADER, source_event())
    second = await processor.process(_LEADER, source_event())

    command = execution.commands[0]
    assert command.side == "BUY"
    assert command.amount == Decimal("5")
    assert command.notional_usdc == Decimal("5")
    assert command.price_limit == Decimal("0.4200")
    assert command.order_type == "FAK"
    assert command.task_budget_reservation_id is not None
    assert first[0].allowed is True
    assert second[0].allowed is False
    assert second[0].reason == "task_budget_submission_retained"
    assert len(execution.commands) == 1


@pytest.mark.asyncio
async def test_sell_command_uses_authoritative_follower_shares() -> None:
    """Never reinterpret source notional as follower SELL shares."""

    repository = InMemoryCopyTaskRepository()
    await repository.create(task(task_id="task-a"))
    budgets = TaskBudgetReservationService(InMemoryTaskBudgetRepository())
    locks = MarketReentryLockService(InMemoryMarketReentryLockRepository())
    execution = ExecutionRecorder(TaskExecutionDisposition.PRE_POST_REJECTED)
    processor = CopyTaskEventProcessor(
        task_reader=repository,
        task_budgets=budgets,
        coordinator=coordinator(budgets, locks),
        snapshot_provider=SnapshotProvider(snapshot(sell_shares=Decimal("4"))),
        execution=execution,
    )

    results = await processor.process(_LEADER, source_event(side=TradeSide.SELL))

    assert results[0].allowed is False
    assert results[0].reason == "pre_post_rejected"
    assert execution.commands[0].side == "SELL"
    assert execution.commands[0].amount == Decimal("4")
    assert execution.commands[0].notional_usdc == Decimal("1.6")
    assert execution.commands[0].price_limit == Decimal("0.3800")


@pytest.mark.asyncio
async def test_sell_without_authoritative_follower_shares_fails_closed() -> None:
    """Reject a SELL when proportional follower holdings are unknown."""

    repository = InMemoryCopyTaskRepository()
    await repository.create(task(task_id="task-a"))
    budgets = TaskBudgetReservationService(InMemoryTaskBudgetRepository())
    locks = MarketReentryLockService(InMemoryMarketReentryLockRepository())
    execution = ExecutionRecorder(TaskExecutionDisposition.PRE_POST_REJECTED)
    processor = CopyTaskEventProcessor(
        task_reader=repository,
        task_budgets=budgets,
        coordinator=coordinator(budgets, locks),
        snapshot_provider=SnapshotProvider(snapshot(sell_shares=None)),
        execution=execution,
    )

    results = await processor.process(_LEADER, source_event(side=TradeSide.SELL))

    assert results[0].allowed is False
    assert results[0].reason == "follower_sell_shares_unavailable"
    assert execution.commands == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("first", "second"),
    [
        (
            source_event(
                mode=DetectionMode.ON_CHAIN_PENDING,
                finality=DetectionFinality.PROVISIONAL,
                log_index=None,
            ),
            source_event(),
        ),
        (
            source_event(),
            source_event(mode=DetectionMode.CONFIRMED_MEMPOOL_RESERVED),
        ),
    ],
)
async def test_cross_source_promotion_uses_one_economic_execution_identity(
    first: CopyTaskSourceEvent,
    second: CopyTaskSourceEvent,
) -> None:
    """Prevent pending, mined, and polling evidence from posting twice."""

    repository = InMemoryCopyTaskRepository()
    await repository.create(task(task_id="task-a", monitoring_mode=MonitoringMode.PRIORITY))
    budgets = TaskBudgetReservationService(InMemoryTaskBudgetRepository())
    locks = MarketReentryLockService(InMemoryMarketReentryLockRepository())
    execution = ExecutionRecorder(TaskExecutionDisposition.SUBMITTED_UNCONFIRMED)
    processor = CopyTaskEventProcessor(
        task_reader=repository,
        task_budgets=budgets,
        coordinator=coordinator(budgets, locks),
        snapshot_provider=SnapshotProvider(snapshot()),
        execution=execution,
    )

    first_result = await processor.process(_LEADER, first)
    second_result = await processor.process(_LEADER, second)

    assert first.trade.exact_key != second.trade.exact_key
    assert first.trade.promotion_key == second.trade.promotion_key
    assert first_result[0].allowed is True
    assert second_result[0].allowed is False
    assert second_result[0].reason == "task_budget_submission_retained"
    assert len(execution.commands) == 1


@pytest.mark.asyncio
async def test_same_source_event_has_distinct_task_scoped_execution_identities() -> None:
    """Allow two intended tasks without colliding in ExecutionService."""

    repository = InMemoryCopyTaskRepository()
    await repository.create(task(task_id="task-a"))
    await repository.create(task(task_id="task-b"))
    budgets = TaskBudgetReservationService(InMemoryTaskBudgetRepository())
    locks = MarketReentryLockService(InMemoryMarketReentryLockRepository())
    execution = ExecutionRecorder(TaskExecutionDisposition.SUBMITTED_UNCONFIRMED)
    processor = CopyTaskEventProcessor(
        task_reader=repository,
        task_budgets=budgets,
        coordinator=coordinator(budgets, locks),
        snapshot_provider=SnapshotProvider(snapshot()),
        execution=execution,
    )

    results = await processor.process(_LEADER, source_event())

    assert [result.allowed for result in results] == [True, True]
    assert len(execution.commands) == 2
    assert execution.commands[0].signal_id != execution.commands[1].signal_id
    assert execution.commands[0].signal_id == execution.commands[0].task_budget_reservation_id
    assert execution.commands[1].signal_id == execution.commands[1].task_budget_reservation_id


@pytest.mark.asyncio
async def test_one_task_snapshot_failure_does_not_block_later_matching_tasks() -> None:
    """Isolate follower snapshot failures to the affected task."""

    repository = InMemoryCopyTaskRepository()
    await repository.create(task(task_id="task-a"))
    await repository.create(task(task_id="task-b"))
    budgets = TaskBudgetReservationService(InMemoryTaskBudgetRepository())
    locks = MarketReentryLockService(InMemoryMarketReentryLockRepository())
    execution = ExecutionRecorder(TaskExecutionDisposition.PRE_POST_REJECTED)
    processor = CopyTaskEventProcessor(
        task_reader=repository,
        task_budgets=budgets,
        coordinator=coordinator(budgets, locks),
        snapshot_provider=SelectiveSnapshotProvider(),
        execution=execution,
    )

    results = await processor.process(_LEADER, source_event())

    assert [(result.task_id, result.reason) for result in results] == [
        ("task-a", "follower_snapshot_unavailable"),
        ("task-b", "pre_post_rejected"),
    ]
    assert len(execution.commands) == 1
    assert execution.commands[0].task.task_id == "task-b"
