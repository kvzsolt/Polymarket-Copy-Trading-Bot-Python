"""Deterministic telemetry doubles shared by the emit-site test modules.

The emitter layer itself is covered by ``tests/telemetry/test_emitters.py``.
These doubles exist so an *emit site* can be tested without starting the
asyncio hub: they record every ``emit`` call verbatim and never touch a clock,
a queue, or a task.

Two services are provided:

* :class:`RecordingTelemetryService` accepts everything and stores the calls.
* :class:`RaisingTelemetryService` fails every ``emit``, which is how a
  producer proves that a degraded telemetry backend cannot change its own
  observable behaviour.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

from pmcopy.services.metrics import MetricsRegistry
from pmcopy.services.telemetry import TelemetryEmitters
from pmcopy.services.telemetry.service import TelemetryService


@dataclass(frozen=True, slots=True)
class RecordedEmit:
    """One captured ``TelemetryService.emit`` call.

    Attributes:
        producer: Producer identity the emitter published under.
        payload: The immutable telemetry payload that was built.
        severity: Bounded operator severity.
        correlation_id: Opaque correlation token, when one was supplied.
    """

    producer: Any
    payload: Any
    severity: Any
    correlation_id: str | None


class _AcceptedResult:
    """Minimal stand-in for ``HubPublishResult`` on the accepted path."""

    accepted = True


class RecordingTelemetryService:
    """Capture every emitted event without running the telemetry hub.

    Args:
        registry: Metrics registry surfaced to the emitter, mirroring
            ``TelemetryService.metrics_registry``.
    """

    def __init__(self, registry: MetricsRegistry | None = None) -> None:
        self.metrics_registry = registry if registry is not None else MetricsRegistry()
        self.calls: list[RecordedEmit] = []

    def emit(
        self,
        *,
        producer: Any,
        payload: Any,
        severity: Any = None,
        correlation_id: str | None = None,
    ) -> _AcceptedResult:
        """Record one event and report it as accepted.

        Args:
            producer: Producer identity supplied by the emitter.
            payload: Immutable telemetry payload.
            severity: Bounded operator severity.
            correlation_id: Optional opaque correlation token.

        Returns:
            An accepted publish result.
        """
        self.calls.append(
            RecordedEmit(
                producer=producer,
                payload=payload,
                severity=severity,
                correlation_id=correlation_id,
            )
        )
        return _AcceptedResult()

    def payloads(self, kind: type | None = None) -> list[Any]:
        """Return the recorded payloads, optionally filtered by payload type.

        Args:
            kind: Payload class to filter on, or ``None`` for every payload.

        Returns:
            The recorded payloads in emission order.
        """
        if kind is None:
            return [call.payload for call in self.calls]
        return [call.payload for call in self.calls if isinstance(call.payload, kind)]


class RaisingTelemetryService:
    """Fail every ``emit`` the way a broken telemetry backend would.

    Args:
        registry: Metrics registry surfaced to the emitter.
    """

    def __init__(self, registry: MetricsRegistry | None = None) -> None:
        self.metrics_registry = registry if registry is not None else MetricsRegistry()
        self.calls = 0

    def emit(self, **_kwargs: object) -> object:
        """Raise instead of publishing.

        Raises:
            RuntimeError: Always.
        """
        self.calls += 1
        raise RuntimeError("telemetry backend unavailable")


def recording_emitters(
    registry: MetricsRegistry | None = None,
) -> tuple[TelemetryEmitters, RecordingTelemetryService]:
    """Build an enabled emitter bundle backed by a recording service.

    Args:
        registry: Optional metrics registry shared with the service.

    Returns:
        The emitter bundle and the service recording its events.
    """
    service = RecordingTelemetryService(registry)
    # Structural stand-in: the emitters only ever call ``emit`` and read
    # ``metrics_registry``, so a cast keeps the doubles minimal.
    return TelemetryEmitters.build(cast(TelemetryService, service)), service


def raising_emitters(
    registry: MetricsRegistry | None = None,
) -> tuple[TelemetryEmitters, RaisingTelemetryService]:
    """Build an emitter bundle whose backend always fails.

    Args:
        registry: Optional metrics registry shared with the service.

    Returns:
        The emitter bundle and the failing service.
    """
    service = RaisingTelemetryService(registry)
    return TelemetryEmitters.build(cast(TelemetryService, service)), service


__all__ = [
    "RaisingTelemetryService",
    "RecordedEmit",
    "RecordingTelemetryService",
    "raising_emitters",
    "recording_emitters",
]
