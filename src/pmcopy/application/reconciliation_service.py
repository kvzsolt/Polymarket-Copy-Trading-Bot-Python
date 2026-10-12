"""Reconciliation-driven updates for execution safety snapshots."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Mapping, Protocol

from pmcopy.application.execution_safety import ReconciliationReadiness, RiskGate, RiskSnapshot


class ReconciliationClock(Protocol):
    """Provide deterministic monotonic reconciliation timestamps."""

    def monotonic(self) -> Decimal:
        """Return a non-negative monotonic time value."""

        ...


@dataclass(frozen=True, slots=True)
class ReconciliationObservation:
    """One normalized local-versus-exchange convergence observation."""

    exposure_known: bool
    total_notional: Decimal
    daily_pnl: Decimal
    positions: Mapping[str, Decimal]
    error_code: str | None = None


class ReconciliationPort(Protocol):
    """Fetch normalized state outside the order-critical path."""

    async def reconcile(self) -> ReconciliationObservation:
        """Return current normalized exposure evidence."""

        ...


class ReconciliationService:
    """Publish startup and periodic reconciliation into in-memory gates."""

    def __init__(
        self,
        *,
        provider: ReconciliationPort,
        risk_gate: RiskGate,
        readiness: ReconciliationReadiness,
        clock: ReconciliationClock,
    ) -> None:
        """Initialize the off-path reconciliation coordinator.

        Args:
            provider: External/local reconciliation adapter.
            risk_gate: Hot-path risk state owner.
            readiness: Hot-path execution readiness owner.
            clock: Deterministic monotonic timestamp provider.
        """

        self._provider = provider
        self._risk_gate = risk_gate
        self._readiness = readiness
        self._clock = clock

    async def run_startup(self) -> ReconciliationObservation:
        """Reconcile once before live execution can become ready.

        Returns:
            The normalized reconciliation observation.
        """

        self._readiness.mark_unhealthy(error_code="startup_reconciliation_required")
        return await self._run_once()

    async def run_periodic_once(self) -> ReconciliationObservation:
        """Refresh in-memory execution state during normal operation.

        Returns:
            The normalized reconciliation observation.
        """

        return await self._run_once()

    async def _run_once(self) -> ReconciliationObservation:
        try:
            observation = await self._provider.reconcile()
        except Exception:
            self._risk_gate.invalidate()
            self._readiness.mark_unhealthy(error_code="reconciliation_unavailable")
            raise
        if not observation.exposure_known:
            self._risk_gate.invalidate()
            self._readiness.mark_unhealthy(error_code=observation.error_code or "exposure_unknown")
            return observation
        observed_at = self._clock.monotonic()
        snapshot = RiskSnapshot(
            total_notional=observation.total_notional,
            daily_pnl=observation.daily_pnl,
            positions=observation.positions,
            observed_at=observed_at,
        )
        self._risk_gate.replace_snapshot(snapshot)
        self._readiness.mark_healthy(observed_at=observed_at)
        return observation


__all__ = [
    "ReconciliationClock",
    "ReconciliationObservation",
    "ReconciliationPort",
    "ReconciliationService",
]
