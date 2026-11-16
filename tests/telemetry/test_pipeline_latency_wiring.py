"""Actual normalization, routing, venue calls and fill projections record elapsed time."""

from dataclasses import replace
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from pmcopy.adapters.clock import SystemReceiptClock
from pmcopy.domain.detection import DetectionMode
from pmcopy.integrations.polymarket.models import VenueSubmitResult
from pmcopy.interfaces.clob_gateway import ClobGateway
from pmcopy.orchestrators.v1_event_adapters import LeaderEventNormalizer
from pmcopy.services.executionService import ExecutionService
from pmcopy.services.metrics import LatencyPhase, MetricsRegistry, TELEMETRY_METRIC_SCHEMAS
from pmcopy.services.telemetry.emitters import ExecutionTelemetryEmitter
from pmcopy.services.telemetry.latency import PipelineLatency
from tests.config_helpers import load_test_config
from tests.telemetry.test_pipeline_latency import ReceiptClock, histogram
from tests.test_execution_fak_repost_ambiguity import _mock_mds, _order_repo, _req


def test_default_transport_clocks_share_one_process_boot() -> None:
    assert SystemReceiptClock().capture().boot_id == SystemReceiptClock().capture().boot_id


def test_normalizer_measures_receipt_to_normalization_and_carries_queue_start() -> None:
    clock = ReceiptClock()
    registry = MetricsRegistry(schemas=TELEMETRY_METRIC_SCHEMAS)
    timing = PipelineLatency(clock=clock, metrics=registry)
    receipt = clock.capture()
    clock.nanoseconds += 20_000_000
    event = LeaderEventNormalizer(latency=timing).normalize_raw(
        "0x" + "1" * 40,
        {
            "asset": "123",
            "conditionId": "condition",
            "side": "BUY",
            "price": ".5",
            "size": "10",
            "timestamp": "1800000000",
            "_source_receipt": receipt,
        },
        mode=DetectionMode.ON_CHAIN_LOGS_ONLY,
    )
    assert event.normalized_receipt == clock.capture()
    assert histogram(registry, LatencyPhase.SOURCE_DETECTION_TO_NORMALIZED).total == 0.02


@pytest.mark.asyncio
@pytest.mark.parametrize("ambiguous", [False, True])
async def test_gateway_ack_and_confirmed_fill_use_actual_boundaries(ambiguous: bool) -> None:
    clock = ReceiptClock()
    registry = MetricsRegistry(schemas=TELEMETRY_METRIC_SCHEMAS)
    timing = PipelineLatency(clock=clock, metrics=registry)
    gateway = MagicMock(spec=ClobGateway)

    async def submit(request):
        clock.nanoseconds += 30_000_000
        return VenueSubmitResult(
            success=not ambiguous,
            order_id=None if ambiguous else "venue-order",
            status="submitted_unconfirmed" if ambiguous else "matched",
            ambiguous_post_failure=ambiguous,
        )

    gateway.submit_marketable_order = AsyncMock(side_effect=submit)
    emitter = ExecutionTelemetryEmitter(None, latency=timing)
    service = ExecutionService(
        AsyncMock(),
        AsyncMock(get_fee_bps=AsyncMock(return_value=Decimal(0))),
        _order_repo(),
        config=load_test_config(
            execution={"trading_mode": "live"}, wallet={"private_key": "0x" + "1" * 64}
        ),
        clob_gateway=gateway,
        market_data_service=_mock_mds(),
        telemetry=emitter,
        latency=timing,
    )
    result = await service.execute(_req())
    gateway.submit_marketable_order.assert_awaited_once()
    assert histogram(registry, LatencyPhase.VENUE_SUBMISSION).total == 0.03
    if ambiguous:
        assert timing.pending_orders == 0
        assert len(registry.histograms) == 1
        return
    assert result.filled_amount == 0
    assert histogram(registry, LatencyPhase.SUBMISSION_TO_FIRST_ACK).total == 0.03
    assert timing.pending_orders == 1
    clock.nanoseconds += 70_000_000
    emitter.fill_observed("venue-order", Decimal(5))
    emitter.fill_observed("venue-order", Decimal(10))
    assert histogram(registry, LatencyPhase.SUBMISSION_TO_FIRST_FILL_EVIDENCE).count == 1
    assert histogram(registry, LatencyPhase.SUBMISSION_TO_FIRST_FILL_EVIDENCE).total == 0.1


@pytest.mark.asyncio
async def test_task_worker_observes_queue_wait_and_decision_before_downstream() -> None:
    from pmcopy.services.copy_tasks.event_routing import CopyTaskEventProcessor
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
    )

    clock = ReceiptClock()
    registry = MetricsRegistry(schemas=TELEMETRY_METRIC_SCHEMAS)
    timing = PipelineLatency(clock=clock, metrics=registry)
    normalized = timing.capture()
    clock.nanoseconds += 40_000_000
    repository = InMemoryCopyTaskRepository()
    await repository.create(task(task_id="task-a"))
    budgets = TaskBudgetReservationService(InMemoryTaskBudgetRepository())
    locks = MarketReentryLockService(InMemoryMarketReentryLockRepository())

    class TimedSnapshot(SnapshotProvider):
        async def snapshot_for(self, task, event):
            clock.nanoseconds += 15_000_000
            return await super().snapshot_for(task, event)

    execution = ExecutionRecorder(TaskExecutionDisposition.SUBMITTED_UNCONFIRMED)
    processor = CopyTaskEventProcessor(
        task_reader=repository,
        task_budgets=budgets,
        coordinator=coordinator(budgets, locks),
        snapshot_provider=TimedSnapshot(snapshot()),
        execution=execution,
        latency=timing,
    )
    await processor(_LEADER, replace(source_event(), normalized_receipt=normalized))
    assert len(execution.commands) == 1
    assert histogram(registry, LatencyPhase.NORMALIZED_TO_WORKER_START).total == 0.04
    assert histogram(registry, LatencyPhase.COPY_DECISION).total == 0.015


@pytest.mark.asyncio
async def test_canonical_coordinator_measures_atomic_reservation() -> None:
    from pmcopy.services.execution.coordinator import ExecutionCoordinator
    from tests.test_execution_coordinator_production import (
        _intent,
        _make_reservation,
        _make_exec_result,
    )

    clock = ReceiptClock()
    registry = MetricsRegistry(schemas=TELEMETRY_METRIC_SCHEMAS)
    timing = PipelineLatency(clock=clock, metrics=registry)

    async def reserve(intent):
        clock.nanoseconds += 12_000_000
        return _make_reservation()

    reservation_service = MagicMock()
    reservation_service.reserve_for_intent = AsyncMock(side_effect=reserve)
    execution = MagicMock()
    execution.execute = AsyncMock(return_value=_make_exec_result(status="submitted_unconfirmed"))
    coordinator = ExecutionCoordinator(
        reservation_service,
        MagicMock(),
        execution_service=execution,
        latency=timing,
    )
    result = await coordinator.submit_copy_trade(_intent())
    assert result.accepted
    assert histogram(registry, LatencyPhase.RESERVATION).total == 0.012
