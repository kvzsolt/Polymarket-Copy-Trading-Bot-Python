"""Distinct canonical fills survive admission without duplicating source promotions."""

from dataclasses import replace

import pytest

from pmcopy.domain.detection import DetectionMode
from pmcopy.services.copy_tasks.budget import InMemoryTaskBudgetRepository, TaskBudgetReservationService
from pmcopy.services.copy_tasks.event_routing import CopyTaskEventProcessor
from pmcopy.services.copy_tasks.lifecycle import TaskExecutionDisposition
from pmcopy.services.copy_tasks.service import InMemoryCopyTaskRepository
from pmcopy.services.copy_tasks.reentry_lock import (
    InMemoryMarketReentryLockRepository,
    MarketReentryLockService,
)
from tests.v1.test_copy_task_event_routing import (
    ExecutionRecorder,
    SnapshotProvider,
    _LEADER,
    coordinator,
    snapshot,
    source_event,
    task,
)


@pytest.mark.asyncio
@pytest.mark.parametrize("order_hash", [None, "0xorder"])
async def test_equal_sized_fills_of_one_order_in_one_transaction_both_allocate(order_hash):
    repository = InMemoryCopyTaskRepository()
    await repository.create(task(task_id="task-a"))
    budgets = TaskBudgetReservationService(InMemoryTaskBudgetRepository())
    execution = ExecutionRecorder(TaskExecutionDisposition.SUBMITTED_UNCONFIRMED)
    processor = CopyTaskEventProcessor(
        task_reader=repository,
        task_budgets=budgets,
        coordinator=coordinator(
            budgets, MarketReentryLockService(InMemoryMarketReentryLockRepository())
        ),
        snapshot_provider=SnapshotProvider(snapshot()),
        execution=execution,
    )
    first = source_event(log_index=7)
    first = replace(first, trade=replace(first.trade, order_hash=order_hash))
    second = replace(first, trade=replace(first.trade, log_index=8))
    assert (await processor.process(_LEADER, first))[0].allowed
    assert (await processor.process(_LEADER, second))[0].allowed
    replay = replace(
        second,
        trade=replace(second.trade, mode=DetectionMode.CONFIRMED_MEMPOOL_RESERVED),
    )
    assert not (await processor.process(_LEADER, replay))[0].allowed
    assert len(execution.commands) == 2
    assert execution.commands[0].signal_id != execution.commands[1].signal_id
