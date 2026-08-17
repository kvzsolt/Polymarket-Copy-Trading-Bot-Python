"""Composition tests for the process-owned telemetry runtime."""

from __future__ import annotations

from services.healthServer import ReadinessTracker
from services.metrics import MetricsRegistry
from services.telemetry import RuntimeOutcome, TelemetryProducer


async def test_telemetry_runtime_uses_one_registry_and_optional_readiness_check() -> None:
    """Startup publishes runtime state without changing trading readiness."""

    from orchestrators.telemetry_bootstrap import TelemetryRuntime

    readiness = ReadinessTracker()
    registry = MetricsRegistry()
    runtime = TelemetryRuntime(readiness=readiness, metrics=registry)

    await runtime.start()

    assert runtime.service.metrics_registry is registry
    assert runtime.query.metrics_registry is registry
    assert runtime.hub.metrics_registry is registry
    check = readiness.snapshot()["checks"]["telemetry_ok"]
    assert check == {"ready": True, "required": False, "reason": None}
    events = runtime.service.history_snapshot()
    assert len(events) == 1
    assert events[0].producer is TelemetryProducer.RUNTIME
    assert events[0].payload.outcome is RuntimeOutcome.STARTED

    await runtime.close()

    assert runtime.service.closed is True
    assert readiness.snapshot()["checks"]["telemetry_ok"] == {
        "ready": False,
        "required": False,
        "reason": "stopped",
    }


async def test_telemetry_runtime_start_and_close_are_idempotent() -> None:
    """Repeated lifecycle calls do not duplicate events or tasks."""

    from orchestrators.telemetry_bootstrap import TelemetryRuntime

    runtime = TelemetryRuntime(
        readiness=ReadinessTracker(),
        metrics=MetricsRegistry(),
    )

    await runtime.start()
    await runtime.start()
    assert len(runtime.service.history_snapshot()) == 1

    await runtime.close()
    await runtime.close()
    assert runtime.hub.closed is True
