"""Source time remains distinct from detection and processing time."""

from dataclasses import replace
from datetime import timedelta

import pytest

from pmcopy.domain.detection import DetectionMode, TradeSide
from pmcopy.orchestrators.v1_event_adapters import LeaderEventNormalizer
from pmcopy.services.copy_tasks.event_routing import CopyTaskEventProcessor
from tests.v1.test_copy_task_event_routing import (
    _LEADER,
    _NOW,
    ExecutionRecorder,
    SnapshotProvider,
    coordinator,
    snapshot,
    source_event,
    task,
    InMemoryCopyTaskRepository,
    InMemoryTaskBudgetRepository,
    TaskBudgetReservationService,
    MarketReentryLockService,
    InMemoryMarketReentryLockRepository,
    TaskExecutionDisposition,
)


class Clock:
    """Advance processing independently of source observation time."""

    def now(self):
        """Return the deterministic processing instant."""
        return _NOW + timedelta(seconds=60)


def test_missing_source_timestamp_is_not_replaced_with_fetch_time():
    event = LeaderEventNormalizer(clock=Clock()).normalize_raw(
        _LEADER,
        {"side": "BUY", "asset": "1", "conditionId": "a", "size": "10", "price": "0.5"},
        mode=DetectionMode.POLLING,
    )
    assert event.trade.venue_timestamp is None
    assert event.trade.detected_at == Clock().now()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("timestamp", "side", "expected"),
    [
        (None, TradeSide.BUY, "signal_age_unknown"),
        (_NOW, TradeSide.BUY, "signal_too_old"),
        (_NOW + timedelta(seconds=61), TradeSide.BUY, "signal_timestamp_future"),
        (_NOW, TradeSide.SELL, "pre_post_rejected"),
    ],
)
async def test_processing_age_gates_entries_and_preserves_reductions(timestamp, side, expected):
    repository = InMemoryCopyTaskRepository()
    await repository.create(task(task_id="task-a"))
    budgets = TaskBudgetReservationService(InMemoryTaskBudgetRepository())
    locks = MarketReentryLockService(InMemoryMarketReentryLockRepository())
    execution = ExecutionRecorder(TaskExecutionDisposition.PRE_POST_REJECTED)
    processor = CopyTaskEventProcessor(
        task_reader=repository,
        task_budgets=budgets,
        coordinator=coordinator(budgets, locks),
        snapshot_provider=SnapshotProvider(snapshot()),
        execution=execution,
        clock=Clock(),
        max_signal_age_seconds=30,
    )
    event = source_event(side=side)
    event = replace(event, trade=replace(event.trade, venue_timestamp=timestamp))
    (result,) = await processor.process(_LEADER, event)
    assert result.reason == expected
    assert len(execution.commands) == (1 if side is TradeSide.SELL else 0)
