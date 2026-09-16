"""Only observed durations within one boot enter bounded latency histograms."""

from domain.source_time import TransportReceipt
from services.metrics import LatencyPhase, MetricsRegistry, TELEMETRY_METRIC_SCHEMAS
from services.telemetry.latency import PipelineLatency


class ReceiptClock:
    """Advance the exact monotonic clock independently from wall time."""

    def __init__(self) -> None:
        self.nanoseconds = 1_000_000_000
        self.boot_id = "test-boot"

    def capture(self) -> TransportReceipt:
        return TransportReceipt(1_800_000_000_000, self.nanoseconds, self.boot_id)


def histogram(registry: MetricsRegistry, phase: LatencyPhase):
    return registry.histograms[f'telemetry_latency_seconds{{phase="{phase.value}"}}']


def test_latency_uses_monotonic_duration_and_exposes_distribution() -> None:
    clock = ReceiptClock()
    registry = MetricsRegistry(schemas=TELEMETRY_METRIC_SCHEMAS)
    timing = PipelineLatency(clock=clock, metrics=registry)
    start = timing.capture()
    clock.nanoseconds += 25_000_000
    timing.observe(LatencyPhase.COPY_DECISION, start)
    recorded = histogram(registry, LatencyPhase.COPY_DECISION)
    assert recorded.count == 1
    assert recorded.total == 0.025


def test_missing_cross_boot_and_regressing_clocks_do_not_invent_latency() -> None:
    clock = ReceiptClock()
    registry = MetricsRegistry(schemas=TELEMETRY_METRIC_SCHEMAS)
    timing = PipelineLatency(clock=clock, metrics=registry)
    start = timing.capture()
    timing.observe(LatencyPhase.COPY_DECISION, None)
    clock.boot_id = "new-boot"
    timing.observe(LatencyPhase.COPY_DECISION, start)
    clock.boot_id = "test-boot"
    clock.nanoseconds -= 1
    timing.observe(LatencyPhase.COPY_DECISION, start)
    assert not registry.histograms
    assert sum(registry.counters.values()) == 3


def test_first_fill_latency_is_once_per_order_and_bounded() -> None:
    clock = ReceiptClock()
    registry = MetricsRegistry(schemas=TELEMETRY_METRIC_SCHEMAS)
    timing = PipelineLatency(clock=clock, metrics=registry, max_orders=2)
    started = timing.capture()
    for order in ("a", "b", "c"):
        timing.acknowledged(order, started)
    assert timing.pending_orders == 2
    clock.nanoseconds += 50_000_000
    timing.first_fill("c")
    timing.first_fill("c")
    timing.first_fill("a")
    recorded = histogram(registry, LatencyPhase.SUBMISSION_TO_FIRST_FILL_EVIDENCE)
    assert recorded.count == 1
    assert recorded.total == 0.05
    assert timing.pending_orders == 1


def test_observability_failure_never_changes_execution_control_flow(caplog) -> None:
    class FailedRegistry(MetricsRegistry):
        def observe(self, *args, **kwargs):
            raise RuntimeError("SYNTHETIC_METRICS_SECRET")

    clock = ReceiptClock()
    timing = PipelineLatency(clock=clock, metrics=FailedRegistry())
    timing.observe(LatencyPhase.COPY_DECISION, timing.capture())
    assert "SYNTHETIC_METRICS_SECRET" not in caplog.text


def test_decision_span_finishes_once_before_downstream_work() -> None:
    clock = ReceiptClock()
    registry = MetricsRegistry(schemas=TELEMETRY_METRIC_SCHEMAS)
    timing = PipelineLatency(clock=clock, metrics=registry)
    with timing.span(LatencyPhase.COPY_DECISION) as measurement:
        clock.nanoseconds += 5_000_000
        measurement.finish()
        clock.nanoseconds += 100_000_000
    recorded = histogram(registry, LatencyPhase.COPY_DECISION)
    assert recorded.count == 1
    assert recorded.total == 0.005
