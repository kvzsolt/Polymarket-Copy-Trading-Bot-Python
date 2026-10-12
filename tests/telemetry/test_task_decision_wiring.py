"""Real copy-task routing emits exact policy decisions through the bounded hub."""

from dataclasses import replace

import pytest

from pmcopy.services.copy_tasks.event_routing import CopyTaskEventProcessor
from pmcopy.services.telemetry import SkipEvent, TelemetryEmitters, TradeEvent, TradeOutcome
from tests.telemetry.test_emitters import _started_service
from tests.v1.test_copy_task_event_routing import (
    InMemoryCopyTaskRepository,
    InMemoryTaskBudgetRepository,
    TaskBudgetReservationService,
    MarketReentryLockService,
    InMemoryMarketReentryLockRepository,
    ExecutionRecorder,
    TaskExecutionDisposition,
    SnapshotProvider,
    snapshot,
    coordinator,
    source_event,
    task,
    _LEADER,
    _NOW,
)


class Clock:
    """Return a deterministic decision instant."""

    def now(self):
        return _NOW


@pytest.mark.asyncio
@pytest.mark.parametrize("missing_time", [True, False])
async def test_task_worker_emits_real_decision_and_preserves_submission_ownership(missing_time):
    service = await _started_service()
    try:
        repository = InMemoryCopyTaskRepository()
        configured = task(task_id="task-a")
        await repository.create(configured)
        budgets = TaskBudgetReservationService(InMemoryTaskBudgetRepository())
        locks = MarketReentryLockService(InMemoryMarketReentryLockRepository())
        execution = ExecutionRecorder(TaskExecutionDisposition.SUBMITTED_UNCONFIRMED)
        processor = CopyTaskEventProcessor(
            task_reader=repository,
            task_budgets=budgets,
            coordinator=coordinator(budgets, locks),
            snapshot_provider=SnapshotProvider(snapshot()),
            execution=execution,
            clock=Clock(),
            decision_observer=TelemetryEmitters.build(service).trade,
            max_signal_age_seconds=30,
        )
        event = source_event()
        if missing_time:
            event = replace(event, trade=replace(event.trade, venue_timestamp=None))
        await processor(_LEADER, event)
        [emitted] = service.history_snapshot()
        details = emitted.payload.decision
        assert details.task_version == str(configured.version)
        assert details.source_price == event.trade.price
        assert details.source_shares == event.trade.shares
        assert details.max_signal_age_seconds == 30
        assert details.evaluated_at == _NOW
        assert details.source_timestamp == event.trade.venue_timestamp
        if missing_time:
            assert isinstance(emitted.payload, SkipEvent)
            assert details.reason_code == "signal_age_unknown"
            assert execution.commands == []
        else:
            assert isinstance(emitted.payload, TradeEvent)
            assert emitted.payload.outcome is TradeOutcome.CANDIDATE_EVALUATED
            assert emitted.payload.order_outcome is None
            assert len(execution.commands) == 1
    finally:
        await service.close()
