"""Synchronous, exception-guarded telemetry emitter facade.

Individual domain emitters live in dedicated single-responsibility modules
(``emitter_base``, ``detection_emitter``, ``trade_emitter``,
``execution_emitter``, ``risk_wss_emitters``).
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING

from pmcopy.services.telemetry.detection_emitter import (
    DetectionCounters,
    DetectionCountersSnapshot,
    DetectionTelemetryEmitter,
)
from pmcopy.services.telemetry.emitter_base import (
    OrderRequestLike,
    OrderResultLike,
    PipelineOutcomeLike,
    SignalLike,
    TelemetryEmitter,
    bounded_code,
    exception_code,
    leader_ref,
    safe_ref,
    skip_reason_for_code,
)
from pmcopy.services.telemetry.execution_emitter import ExecutionTelemetryEmitter
from pmcopy.services.telemetry.latency import PipelineLatency
from pmcopy.services.telemetry.risk_wss_emitters import (
    RiskTelemetryEmitter,
    WssTelemetryEmitter,
)
from pmcopy.services.telemetry.trade_emitter import TradeTelemetryEmitter

if TYPE_CHECKING:
    from pmcopy.services.telemetry.service import TelemetryService


@dataclass(frozen=True, slots=True)
class TelemetryEmitters:
    """Immutable bundle of every producer-facing telemetry emitter.

    Composition passes this bundle down one path so no collaborator has to know
    whether telemetry is enabled: a disabled bundle is a total null object.
    """

    detection: DetectionTelemetryEmitter
    trade: TradeTelemetryEmitter
    execution: ExecutionTelemetryEmitter
    risk: RiskTelemetryEmitter
    wss: WssTelemetryEmitter
    latency: PipelineLatency | None = None

    @property
    def enabled(self) -> bool:
        """Return whether the bundle is bound to a telemetry service."""
        return self.detection.enabled

    @classmethod
    def build(
        cls,
        service: "TelemetryService | None",
        *,
        loop: asyncio.AbstractEventLoop | None = None,
        latency: PipelineLatency | None = None,
    ) -> "TelemetryEmitters":
        """Build one bundle sharing a single telemetry service.

        Args:
            service: Telemetry service, or ``None`` for a disabled bundle.
            loop: Optional owning event loop override.

        Returns:
            A frozen bundle with one emitter per producer identity.
        """
        return cls(
            detection=DetectionTelemetryEmitter(service, loop=loop),
            trade=TradeTelemetryEmitter(service, loop),
            execution=ExecutionTelemetryEmitter(service, loop, latency=latency),
            risk=RiskTelemetryEmitter(service, loop),
            wss=WssTelemetryEmitter(service, loop),
            latency=latency,
        )

    @classmethod
    def disabled(cls) -> "TelemetryEmitters":
        """Return a bundle whose every method is a no-op returning ``False``."""
        return cls.build(None)


__all__ = [
    "DetectionCounters",
    "DetectionCountersSnapshot",
    "DetectionTelemetryEmitter",
    "ExecutionTelemetryEmitter",
    "OrderRequestLike",
    "OrderResultLike",
    "PipelineOutcomeLike",
    "RiskTelemetryEmitter",
    "SignalLike",
    "TelemetryEmitter",
    "TelemetryEmitters",
    "TradeTelemetryEmitter",
    "WssTelemetryEmitter",
    "bounded_code",
    "exception_code",
    "leader_ref",
    "safe_ref",
    "skip_reason_for_code",
]
