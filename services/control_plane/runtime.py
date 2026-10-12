"""Temporary compatibility shim for the relocated trading-core safety apply.

The durable full-stop cancel-resting-orders apply now lives in the trading
core at :mod:`services.trading_control`. This module keeps the historical
control-plane import path working and adapts the core signal to the
entry-control contract port until the control plane is deleted; remove this
file together with the control plane.
"""

from __future__ import annotations

from services.control_plane.contracts import EntryControlState
from services.trading_control import (
    NullRuntimeTelemetry,
    OpenOrderControlGateway,
    RuntimeSafetyApplyError,
    RuntimeTelemetry,
    TradingControlRuntimeSignal,
)


class EntryControlRuntimeSignal:
    """Adapt the trading-core signal to the entry-control contract port."""

    def __init__(self, signal: TradingControlRuntimeSignal) -> None:
        """Store the relocated core signal."""

        self._signal = signal

    async def apply(self, state: EntryControlState) -> None:
        """Project one durable entry-control state onto the core signal."""

        await self._signal.apply(
            entries_paused=state.entries_paused,
            full_stop=state.full_stop,
        )


__all__ = [
    "EntryControlRuntimeSignal",
    "NullRuntimeTelemetry",
    "OpenOrderControlGateway",
    "RuntimeSafetyApplyError",
    "RuntimeTelemetry",
    "TradingControlRuntimeSignal",
]
