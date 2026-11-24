"""Compose and own the process-wide telemetry application boundary.

The runtime binds the existing metrics registry, readiness tracker, bounded
event hub, telemetry service, and GUI-safe query service exactly once.  It has
no network or persistence dependency and is safe to start before trading
subsystems are ready.
"""

from __future__ import annotations

from pmcopy.services.observability.health_server import ReadinessTracker
from pmcopy.services.observability.metrics import MetricsRegistry
from pmcopy.services.telemetry import (
    RuntimeEvent,
    RuntimeOutcome,
    TelemetryHub,
    TelemetryProducer,
    TelemetryQueryService,
    TelemetryService,
)


class TelemetryRuntime:
    """Own one bounded telemetry service lifecycle for the application."""

    def __init__(
        self,
        *,
        readiness: ReadinessTracker,
        metrics: MetricsRegistry,
        ingress_capacity: int = 1024,
        subscriber_capacity: int = 256,
        max_subscribers: int = 32,
        history_capacity: int = 2048,
    ) -> None:
        """Construct telemetry collaborators without starting background work.

        Args:
            readiness: Existing application readiness truth source.
            metrics: Existing application metrics truth source.
            ingress_capacity: Maximum events awaiting hub fan-out.
            subscriber_capacity: Default per-subscriber pending delivery bound.
            max_subscribers: Maximum simultaneous event subscribers.
            history_capacity: Maximum events retained for snapshot resync.
        """

        self._readiness = readiness
        self._hub = TelemetryHub(
            ingress_capacity=ingress_capacity,
            default_subscriber_capacity=subscriber_capacity,
            max_subscribers=max_subscribers,
            metrics=metrics,
        )
        self._service = TelemetryService(
            hub=self._hub,
            metrics=metrics,
            history_capacity=history_capacity,
        )
        self._query = TelemetryQueryService(service=self._service, metrics=metrics)
        self._started = False
        self._closed = False
        self._readiness.register_check(
            "telemetry_ok",
            required=False,
            ready=False,
            reason="not_started",
        )

    @property
    def hub(self) -> TelemetryHub:
        """Return the bounded event hub used by WebSocket subscribers."""

        return self._hub

    @property
    def service(self) -> TelemetryService:
        """Return the non-blocking application telemetry service."""

        return self._service

    @property
    def query(self) -> TelemetryQueryService:
        """Return the GUI-safe in-memory telemetry query service."""

        return self._query

    async def start(self) -> None:
        """Start bounded fan-out and publish one runtime-started event."""

        if self._closed:
            raise RuntimeError("closed telemetry runtime cannot be restarted")
        if self._started:
            return
        await self._service.start()
        self._service.emit(
            producer=TelemetryProducer.RUNTIME,
            payload=RuntimeEvent(outcome=RuntimeOutcome.STARTED),
        )
        self._readiness.set_check("telemetry_ok", ready=True)
        self._started = True

    async def close(self) -> None:
        """Stop telemetry exactly once without changing trading readiness."""

        if self._closed:
            return
        self._closed = True
        if self._started:
            self._service.emit(
                producer=TelemetryProducer.RUNTIME,
                payload=RuntimeEvent(outcome=RuntimeOutcome.STOPPING),
            )
        await self._service.close()
        self._readiness.set_check(
            "telemetry_ok",
            ready=False,
            reason="stopped",
        )
        self._started = False


__all__ = ["TelemetryRuntime"]
