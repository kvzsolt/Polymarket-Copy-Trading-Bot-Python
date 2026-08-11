"""Composition-root regression tests for telemetry lifecycle wiring."""

from __future__ import annotations

from types import SimpleNamespace

from services.healthServer import ReadinessTracker
from services.metrics import MetricsRegistry


async def test_application_starts_and_registers_one_telemetry_owner() -> None:
    """The outer resource registry owns the same telemetry runtime it exposes."""

    from app import _start_telemetry_runtime

    resources: list[object] = []
    application = SimpleNamespace(readiness=ReadinessTracker())
    registry = MetricsRegistry()

    runtime = await _start_telemetry_runtime(
        application=application,
        client_registry=resources,
        metrics_registry=registry,
    )

    assert resources == [runtime]
    assert runtime.service.started is True
    assert runtime.service.metrics_registry is registry

    await runtime.close()
