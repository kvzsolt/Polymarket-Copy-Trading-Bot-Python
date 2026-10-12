"""Telemetry survives without the control plane (Task 11/12 decision guard).

Decision: the bounded in-process telemetry hub stays exactly as composed. Its
only external subscriber and query-service consumers were the control-plane
WebSocket stream and read routes, which Task 12 deleted. With the control
plane absent the hub simply runs subscriber-less: emits stay bounded,
non-blocking, and never break the trading path, while the metrics registry and
readiness check keep their observability value. No code change was needed;
these tests pin the invariants the decision depends on.
"""

from __future__ import annotations

from services.healthServer import ReadinessTracker
from services.metrics import MetricsRegistry
from services.telemetry import TelemetryEmitters
from orchestrators.telemetry_bootstrap import TelemetryRuntime


async def test_trading_emitters_never_break_with_a_subscriber_less_hub() -> None:
    """A started runtime with zero hub subscribers emits and closes cleanly."""

    runtime = TelemetryRuntime(
        readiness=ReadinessTracker(),
        metrics=MetricsRegistry(),
    )
    await runtime.start()
    emitters = TelemetryEmitters.build(runtime.service)

    try:
        accepted = emitters.detection.signal_received(
            "polling", "0x" + "ab" * 20, "0x" + "cd" * 32
        )
        assert accepted is True
        assert emitters.wss.connected("market") is True
        # One runtime-started event plus the two emits above.
        assert len(runtime.service.history_snapshot()) == 3
    finally:
        await runtime.close()

    assert runtime.service.closed is True


def test_disabled_bundle_is_a_total_null_object() -> None:
    """Hub-less composition yields a no-op bundle that never raises."""

    emitters = TelemetryEmitters.disabled()

    assert emitters.enabled is False
    assert emitters.detection.signal_received(
        "polling", "0x" + "ab" * 20, "0x" + "cd" * 32
    ) is False


async def test_telemetry_runtime_owns_the_readiness_check_without_subscribers() -> None:
    """The telemetry_ok readiness signal stays truthful with no CP attached."""

    readiness = ReadinessTracker()
    runtime = TelemetryRuntime(readiness=readiness, metrics=MetricsRegistry())

    await runtime.start()
    assert readiness.snapshot()["checks"]["telemetry_ok"]["ready"] is True

    await runtime.close()
    assert readiness.snapshot()["checks"]["telemetry_ok"]["ready"] is False
