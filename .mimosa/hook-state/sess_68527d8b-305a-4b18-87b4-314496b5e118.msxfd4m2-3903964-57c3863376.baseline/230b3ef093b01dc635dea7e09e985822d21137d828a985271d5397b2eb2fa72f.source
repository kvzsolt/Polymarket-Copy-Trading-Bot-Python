"""Characterization tests for the bounded v1 telemetry control plane."""

from __future__ import annotations

import asyncio
import inspect
import json
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime

import pytest

from services.metrics import MetricsRegistry
from services.telemetry import (
    AmbiguousEvent,
    AmbiguousOperation,
    DiscoveryEvent,
    DiscoveryOutcome,
    FailureComponent,
    FailureEvent,
    FillEvent,
    FillOutcome,
    HubPublishStatus,
    LeaderEvent,
    LeaderOutcome,
    LockEvent,
    LockOutcome,
    ProtectiveExitEvent,
    ProtectiveOutcome,
    ReconciliationEvent,
    ReconciliationOutcome,
    RiskEvent,
    RiskOutcome,
    RosterEvent,
    RosterOutcome,
    RuntimeEvent,
    RuntimeOutcome,
    SkipEvent,
    SkipReason,
    TaskEvent,
    TaskOutcome,
    TelemetryCategory,
    TelemetryEvent,
    TelemetryEventType,
    TelemetryHub,
    TelemetryProducer,
    TelemetryQueryService,
    TelemetryService,
    TelemetrySeverity,
    TradeEvent,
    TradeOutcome,
    TradeSide,
    WssEvent,
    WssOutcome,
)


class FixedClock:
    """Return a deterministic UTC instant."""

    def __init__(self) -> None:
        self.current = datetime(2026, 8, 11, 12, 0, tzinfo=UTC)

    def now(self) -> datetime:
        """Return the configured instant."""
        return self.current


class SequenceRandom:
    """Return deterministic opaque identifiers."""

    def __init__(self) -> None:
        self.value = 0

    def token_hex(self, nbytes: int) -> str:
        """Return a fixed-width hexadecimal token."""
        self.value += 1
        return f"{self.value:0{nbytes * 2}x}"


class MemorySink:
    """Capture sink writes without filesystem or network I/O."""

    def __init__(self) -> None:
        self.events: list[TelemetryEvent] = []
        self.resyncs: list[tuple[TelemetryEvent, ...]] = []
        self.closed = False

    async def write(self, event: TelemetryEvent) -> None:
        """Capture one event."""
        self.events.append(event)

    async def resync(self, events: tuple[TelemetryEvent, ...]) -> None:
        """Capture a bounded state replacement."""
        self.resyncs.append(events)

    async def close(self) -> None:
        """Record deterministic cleanup."""
        self.closed = True


class FailingCloseSink(MemorySink):
    """Raise during close so hub cleanup can be verified independently."""

    async def close(self) -> None:
        """Record closure and simulate an injected I/O cleanup failure."""
        await super().close()
        raise RuntimeError("sink close failed")


def _leader_received(ref: str = "leader-01") -> LeaderEvent:
    return LeaderEvent(
        outcome=LeaderOutcome.RECEIVED,
        leader_ref=ref,
        source="market_wss",
    )


def test_all_required_event_categories_are_typed_and_frozen() -> None:
    payloads = (
        _leader_received(),
        TaskEvent(outcome=TaskOutcome.ADMITTED, task_ref="task-01"),
        TradeEvent(outcome=TradeOutcome.EXECUTED, task_ref="task-01", side=TradeSide.BUY),
        FillEvent(outcome=FillOutcome.OBSERVED, order_ref="order-01", fill_ref="fill-01"),
        SkipEvent(reason=SkipReason.RISK_REJECTED, task_ref="task-01"),
        FailureEvent(component=FailureComponent.EXECUTION, code="venue_timeout"),
        AmbiguousEvent(operation=AmbiguousOperation.ORDER_SUBMISSION, order_ref="order-01"),
        ProtectiveExitEvent(outcome=ProtectiveOutcome.TRIGGERED, task_ref="task-01"),
        LockEvent(outcome=LockOutcome.CREATED, lock_ref="lock-01"),
        ReconciliationEvent(outcome=ReconciliationOutcome.RUN),
        DiscoveryEvent(outcome=DiscoveryOutcome.REQUEST),
        RosterEvent(outcome=RosterOutcome.UPDATED, active_count=3),
        WssEvent(outcome=WssOutcome.RECONNECTED, stream="market"),
        RuntimeEvent(outcome=RuntimeOutcome.STARTED),
        RiskEvent(outcome=RiskOutcome.APPROVED, task_ref="task-01"),
    )

    assert {payload.category for payload in payloads} == set(TelemetryCategory)
    with pytest.raises(FrozenInstanceError):
        payloads[0].leader_ref = "changed"  # type: ignore[misc]


def test_event_type_contract_is_stable_and_complete() -> None:
    assert {event_type.value for event_type in TelemetryEventType} == {
        "leader_signal",
        "task_decision",
        "trade_submitted",
        "trade_fill",
        "trade_skip",
        "trade_failure",
        "ambiguous_order",
        "protective_exit",
        "reentry_lock",
        "reconciliation",
        "discovery_cycle",
        "leader_promoted",
        "leader_demoted",
        "wss_state",
        "runtime_state",
        "risk_state",
    }


def test_failure_contract_rejects_unbounded_secret_bearing_text() -> None:
    with pytest.raises(ValueError, match="code"):
        FailureEvent(
            component=FailureComponent.EXECUTION,
            code="Authorization: Bearer super-secret-token",
        )
    with pytest.raises(ValueError, match="stream"):
        WssEvent(outcome=WssOutcome.RECONNECTED, stream="wallet-123")


@pytest.mark.asyncio
async def test_service_stamps_immutable_ordered_events_and_query_is_json_safe() -> None:
    registry = MetricsRegistry()
    hub = TelemetryHub(ingress_capacity=8, default_subscriber_capacity=8, metrics=registry)
    sink = MemorySink()
    clock = FixedClock()
    service = TelemetryService(
        hub=hub,
        clock=clock,
        rng=SequenceRandom(),
        sink=sink,
        metrics=registry,
        history_capacity=4,
    )
    await service.start()

    first = service.emit(
        producer=TelemetryProducer.DETECTION,
        payload=_leader_received(),
    )
    second = service.emit(
        producer=TelemetryProducer.RUNTIME,
        payload=RuntimeEvent(outcome=RuntimeOutcome.READY),
    )
    third = service.emit(
        producer=TelemetryProducer.DETECTION,
        payload=LeaderEvent(
            outcome=LeaderOutcome.PROMOTED,
            leader_ref="leader-01",
            source="market_wss",
        ),
        severity=TelemetrySeverity.NOTICE,
    )

    assert first.status is HubPublishStatus.ACCEPTED
    assert second.status is HubPublishStatus.ACCEPTED
    assert third.status is HubPublishStatus.ACCEPTED
    assert first.event.sequence == 1
    assert second.event.sequence == 1
    assert third.event.sequence == 2
    assert first.event.timestamp == clock.current
    assert first.event.event_type is TelemetryEventType.LEADER_SIGNAL
    assert first.event.event_id != second.event.event_id
    with pytest.raises(FrozenInstanceError):
        first.event.sequence = 9  # type: ignore[misc]

    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert sink.events == [first.event, second.event, third.event]

    query = TelemetryQueryService(service=service, metrics=registry, clock=clock)
    snapshot = query.snapshot(limit=2)
    encoded = json.dumps(snapshot, sort_keys=True)
    assert len(snapshot["events"]) == 2
    assert "generated_at" in snapshot
    assert "super-secret-token" not in encoded
    assert snapshot["events"][-1]["payload"]["category"] == "leader"

    await service.close()
    assert sink.closed is True
    assert hub.closed is True


@pytest.mark.asyncio
async def test_hub_ingress_is_bounded_and_publish_never_awaits() -> None:
    registry = MetricsRegistry()
    hub = TelemetryHub(ingress_capacity=1, default_subscriber_capacity=1, metrics=registry)
    await hub.start()
    assert inspect.iscoroutinefunction(hub.publish_nowait) is False

    event_one = TelemetryEvent(
        event_id="evt-01",
        event_type=TelemetryEventType.LEADER_SIGNAL,
        timestamp=datetime(2026, 8, 11, tzinfo=UTC),
        severity=TelemetrySeverity.INFO,
        correlation_id=None,
        payload=_leader_received(),
        producer=TelemetryProducer.DETECTION,
        sequence=1,
    )
    event_two = TelemetryEvent(
        event_id="evt-02",
        event_type=TelemetryEventType.LEADER_SIGNAL,
        timestamp=datetime(2026, 8, 11, tzinfo=UTC),
        severity=TelemetrySeverity.INFO,
        correlation_id=None,
        payload=_leader_received(),
        producer=TelemetryProducer.DETECTION,
        sequence=2,
    )

    assert hub.publish_nowait(event_one).status is HubPublishStatus.ACCEPTED
    assert hub.publish_nowait(event_two).status is HubPublishStatus.DROPPED_OVERFLOW
    counters, _ = registry.snapshot()
    assert counters['telemetry_queue_overflow_total{queue="ingress"}'] == 1

    await hub.close()
    assert hub.publish_nowait(event_two).status is HubPublishStatus.DROPPED_CLOSED


@pytest.mark.asyncio
async def test_slow_subscriber_gets_resync_signal_without_reordering_other_subscriber() -> None:
    registry = MetricsRegistry()
    hub = TelemetryHub(ingress_capacity=8, default_subscriber_capacity=2, metrics=registry)
    slow = hub.subscribe(capacity=1)
    ordered = hub.subscribe(capacity=4)
    await hub.start()

    events = [
        TelemetryEvent(
            event_id=f"evt-{sequence}",
            event_type=TelemetryEventType.LEADER_SIGNAL,
            timestamp=datetime(2026, 8, 11, tzinfo=UTC),
            severity=TelemetrySeverity.INFO,
            correlation_id=None,
            payload=_leader_received(),
            producer=TelemetryProducer.DETECTION,
            sequence=sequence,
        )
        for sequence in (1, 2, 3)
    ]
    for event in events:
        assert hub.publish_nowait(event).status is HubPublishStatus.ACCEPTED

    await asyncio.sleep(0)
    slow_delivery = await slow.get()
    assert slow_delivery.resync_required is True
    assert slow_delivery.event is None
    assert slow_delivery.dropped_count == 3

    ordered_deliveries = [await ordered.get() for _ in events]
    assert [delivery.event for delivery in ordered_deliveries] == events
    counters, _ = registry.snapshot()
    assert counters['telemetry_queue_overflow_total{queue="subscriber"}'] >= 1

    await hub.close()
    assert slow.closed is True
    assert ordered.closed is True
    assert hub.running is False


def test_hub_bounds_total_subscriber_count() -> None:
    hub = TelemetryHub(
        ingress_capacity=4,
        default_subscriber_capacity=2,
        max_subscribers=1,
        metrics=MetricsRegistry(),
    )
    hub.subscribe()
    with pytest.raises(RuntimeError, match="subscriber capacity"):
        hub.subscribe()


def test_hub_service_and_query_share_one_metrics_truth() -> None:
    registry = MetricsRegistry()
    hub = TelemetryHub(metrics=registry)
    service = TelemetryService(hub=hub)
    query = TelemetryQueryService(service=service)

    assert hub.metrics_registry is registry
    assert service.metrics_registry is registry
    assert query.metrics_registry is registry

    with pytest.raises(ValueError, match="same metrics registry"):
        TelemetryService(hub=hub, metrics=MetricsRegistry())
    with pytest.raises(ValueError, match="same metrics registry"):
        TelemetryQueryService(service=service, metrics=MetricsRegistry())


@pytest.mark.asyncio
async def test_service_closes_hub_even_when_injected_sink_cleanup_fails() -> None:
    registry = MetricsRegistry()
    hub = TelemetryHub(metrics=registry)
    sink = FailingCloseSink()
    service = TelemetryService(hub=hub, sink=sink, metrics=registry)
    await service.start()

    with pytest.raises(RuntimeError, match="sink close failed"):
        await service.close()

    assert sink.closed is True
    assert hub.closed is True
    assert service.started is False
    assert service.closed is True
