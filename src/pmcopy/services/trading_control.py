"""Apply durable runtime safety state to live venue resources.

PostgreSQL remains the submission authority. This application service performs
the only required live side effect after a durable full stop: enumerate and
cancel every resting order across injected follower gateways. It never submits
an order and never runs on the execution hot path.

The durable signal is the canonical ``trading_control`` singleton row: a full
stop is ``enabled = false`` with reason ``runtime.full_stop``, which is exactly
what :mod:`services.trading_permission` enforces before every submission.
"""

from __future__ import annotations

from typing import Protocol

from pmcopy.integrations.polymarket.models import VenueCancelResult, VenueOrderState
from pmcopy.services.telemetry import (
    RuntimeEvent,
    RuntimeOutcome,
    TelemetryProducer,
)


class OpenOrderControlGateway(Protocol):
    """Read and cancel resting venue orders for one follower account."""

    async def list_open_orders(
        self,
        *,
        order_id: str | None = None,
        market: str | None = None,
    ) -> list[VenueOrderState]:
        """Return the complete deduplicated open-order snapshot."""

        ...

    async def cancel_order(self, order_id: str) -> VenueCancelResult:
        """Cancel one order idempotently."""

        ...


class RuntimeTelemetry(Protocol):
    """Non-blocking typed telemetry emission boundary."""

    def emit(
        self,
        *,
        producer: TelemetryProducer,
        payload: RuntimeEvent,
    ) -> object:
        """Publish one bounded in-memory runtime event."""

        ...


class NullRuntimeTelemetry:
    """Total no-op telemetry for processes without a telemetry service.

    Operator CLI commands and tests apply the same safety state as the trading
    runtime without composing a bounded event hub.
    """

    def emit(
        self,
        *,
        producer: TelemetryProducer,
        payload: RuntimeEvent,
    ) -> object:
        """Absorb one event without recording it."""

        return None


class RuntimeSafetyApplyError(RuntimeError):
    """Raised after a full stop could not cancel every resting order."""


FULL_STOP_REASON = "runtime.full_stop"
"""Durable reason code that marks an enabled=false row as a full stop."""


class TradingControlRuntimeSignal:
    """Apply durable pause/full-stop state to bounded live collaborators."""

    _MAX_GATEWAYS = 64

    def __init__(
        self,
        *,
        gateways: tuple[OpenOrderControlGateway, ...],
        telemetry: RuntimeTelemetry,
    ) -> None:
        """Store follower gateways and the existing telemetry service.

        Args:
            gateways: One canonical venue gateway per follower account.
            telemetry: Non-blocking in-memory telemetry boundary.

        Raises:
            TypeError: If gateways are not supplied as an immutable tuple.
            ValueError: If the bounded account limit is exceeded.
        """

        if not isinstance(gateways, tuple):
            raise TypeError("runtime control gateways must be a tuple")
        if len(gateways) > self._MAX_GATEWAYS:
            raise ValueError("runtime control supports at most 64 follower gateways")
        self._gateways = gateways
        self._telemetry = telemetry

    async def apply(self, *, entries_paused: bool, full_stop: bool) -> None:
        """Apply one already-durable state without weakening submission gates.

        Args:
            entries_paused: Whether new entries are durably paused.
            full_stop: Whether the irreversible process full stop is active.

        Raises:
            RuntimeSafetyApplyError: After attempting every account when an
                open-order read or cancellation was unsuccessful.
        """

        outcome = RuntimeOutcome.DEGRADED if entries_paused else RuntimeOutcome.READY
        self._telemetry.emit(
            producer=TelemetryProducer.RUNTIME,
            payload=RuntimeEvent(outcome=outcome),
        )
        if not full_stop:
            return

        failures = 0
        for gateway in self._gateways:
            try:
                open_orders = await gateway.list_open_orders()
            except Exception:
                failures += 1
                continue
            seen: set[str] = set()
            for order in open_orders:
                order_id = str(order.order_id or "").strip()
                if not order_id or order_id in seen:
                    failures += int(not order_id)
                    continue
                seen.add(order_id)
                try:
                    result = await gateway.cancel_order(order_id)
                except Exception:
                    failures += 1
                    continue
                if not result.success:
                    failures += 1
        if failures:
            raise RuntimeSafetyApplyError(
                "one or more resting orders could not be canceled after full stop"
            )


__all__ = [
    "FULL_STOP_REASON",
    "NullRuntimeTelemetry",
    "OpenOrderControlGateway",
    "RuntimeSafetyApplyError",
    "RuntimeTelemetry",
    "TradingControlRuntimeSignal",
]
