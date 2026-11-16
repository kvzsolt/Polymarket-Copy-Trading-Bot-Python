"""Fixed-cadence operator liveness heartbeat for the long-running supervisor.

The runtime is deliberately quiet: the native detection source logs only on
state *change*, and risk/reconciliation log only on action. A healthy idle bot
and a wedged one therefore produce identical (empty) logs, which is exactly the
ambiguity an operator cannot afford.

This loop restores a fixed-cadence "the process is alive, and here is what it
can currently see" line. It is defensive by design: a broken snapshot is
reported *inside* the heartbeat rather than killing the loop, because the
heartbeat's only job is to keep proving the process still schedules work.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Mapping
from typing import Any

DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 30.0
MINIMUM_HEARTBEAT_INTERVAL_SECONDS = 1.0

# Deterministic test seam: the loop awaits this instead of ``asyncio.sleep``
# directly so cadence can be asserted without real wall-clock waits.
_sleep = asyncio.sleep

_UNKNOWN = "unknown"


def _flag(value: Any) -> str:
    """Render one optional boolean readiness flag without inventing a green light."""
    if value is None:
        return _UNKNOWN
    return "ok" if value else "FAIL"


def _sanitize(value: Any) -> Any:
    """Reduce a snapshot value to a JSON-safe scalar the structured sink accepts."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, (list, tuple, set)):
        return [_sanitize(item) for item in value]
    return str(value)


# Structured-field names emitted alongside the human line. The shared logger
# formatter carries scalar extras only, so the snapshot is flattened and absent
# facts are omitted rather than rendered as ``null``.
_EXTRA_KEYS: tuple[tuple[str, str], ...] = (
    ("detectionConnected", "detection_connected"),
    ("activeLeaders", "active_leaders"),
    ("monitoredLeaders", "monitored_leaders"),
    ("ready", "ready"),
    ("wssUserOk", "wss_user_ok"),
    ("wssMarketOk", "wss_market_ok"),
    ("reconcilerOk", "reconciler_ok"),
    ("dbOk", "db_ok"),
    ("apiOk", "api_ok"),
    ("snapshotFailed", "snapshot_failed"),
)


def heartbeat_extras(state: Mapping[str, Any], *, interval_seconds: float) -> dict[str, Any]:
    """Flatten a heartbeat snapshot into scalar structured-log fields.

    Args:
        state: Snapshot produced by the heartbeat state provider.
        interval_seconds: Effective cadence reported with the tick.

    Returns:
        A flat mapping of JSON-safe scalars plus the ``type`` discriminator.
    """
    extras: dict[str, Any] = {
        "type": "bot_heartbeat",
        "intervalSeconds": interval_seconds,
    }
    for extra_key, state_key in _EXTRA_KEYS:
        value = _sanitize(state.get(state_key))
        if value is not None:
            extras[extra_key] = value
    return extras


def render_heartbeat(state: Mapping[str, Any]) -> str:
    """Render one operator-facing heartbeat line from a state snapshot.

    Args:
        state: Snapshot produced by the heartbeat state provider. Every key is
            optional; absent values are reported as ``unknown`` rather than
            being assumed healthy.

    Returns:
        A single human-readable line, including the snapshot failure when the
        provider could not produce one.
    """
    failed = state.get("snapshot_failed")
    if failed is not None:
        return f"🟢 bot_heartbeat | alive | snapshot_failed={failed}"
    connected = state.get("detection_connected")
    detection = _UNKNOWN if connected is None else ("connected" if connected else "DISCONNECTED")
    active = state.get("active_leaders")
    monitored = state.get("monitored_leaders")
    leaders = f"{active if active is not None else _UNKNOWN}/{monitored if monitored is not None else _UNKNOWN}"
    ready = state.get("ready")
    ready_text = _UNKNOWN if ready is None else ("yes" if ready else "NO")
    wss = f"user:{_flag(state.get('wss_user_ok'))},market:{_flag(state.get('wss_market_ok'))}"
    return (
        f"🟢 bot_heartbeat | alive | detection={detection} leaders={leaders} | "
        f"ready={ready_text} | wss={wss} | "
        f"recon={_flag(state.get('reconciler_ok'))} | "
        f"db={_flag(state.get('db_ok'))} api={_flag(state.get('api_ok'))}"
    )


async def bot_heartbeat_loop(
    state_provider: Callable[[], Mapping[str, Any]],
    *,
    logger: logging.Logger,
    interval_seconds: float = DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
) -> None:
    """Emit one liveness line immediately, then once per interval.

    Args:
        state_provider: Read-only snapshot callable. May raise; a raising
            provider degrades the line, never the loop.
        logger: Structured application logger.
        interval_seconds: Cadence, floored at one second so a misconfigured
            interval cannot spin the event loop.

    Returns:
        None. The loop runs until the supervisor cancels it.
    """
    interval = max(MINIMUM_HEARTBEAT_INTERVAL_SECONDS, float(interval_seconds))
    while True:
        try:
            state: dict[str, Any] = dict(state_provider())
        except Exception as exc:
            # The heartbeat must outlive a broken snapshot: proving the process
            # still schedules work is the entire point of this loop.
            state = {"snapshot_failed": type(exc).__name__}
        try:
            logger.info(
                render_heartbeat(state),
                extra=heartbeat_extras(state, interval_seconds=interval),
            )
        except Exception:
            # A logging failure must not kill the liveness signal either.
            pass
        await _sleep(interval)


__all__ = [
    "DEFAULT_HEARTBEAT_INTERVAL_SECONDS",
    "MINIMUM_HEARTBEAT_INTERVAL_SECONDS",
    "bot_heartbeat_loop",
    "heartbeat_extras",
    "render_heartbeat",
]
