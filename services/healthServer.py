from __future__ import annotations

import time
from copy import deepcopy
from typing import Any

from aiohttp import web

from config.env import get_metrics_bind_host
from services.metrics import metrics


# Subsystem keys whose truthiness is required for /health/ready (strict).
_STRICT_REQUIRED_KEYS: tuple[str, ...] = (
    "db_ok",
    "api_ok",
    "gateway_ok",
    "wss_market_ok",
    "wss_user_ok",
    "reconciler_ok",
    "market_data_ready"
)


class ReadinessTracker:
    """Injectable readiness state — one instance per Application.

    Replaces the old module-level ``_readiness_state`` dict. Services that
    need to update readiness accept a ``ReadinessTracker`` instance via
    their constructor; the composition root builds one at startup and
    hands it to every consumer.

    New subsystem fields default to False/0 so any unwired subsystem fails
    the strict readiness check (fail-closed). Live mode must wire them.
    """

    def __init__(self, initial: dict[str, Any] | None = None):
        self._state: dict[str, Any] = {
            "db_ok": False,
            "api_ok": False,
            "last_trade_poll_ts": 0,
            "last_risk_check_ts": 0,
            "last_successful_order_ts": 0,
            "monitored_leader_count": 0,
            "runtime_user_count": 0,
            "mode": "configured",
            # Subsystem readiness (issue #76)
            "gateway_ok": False,
            "wss_market_ok": False,
            "wss_user_ok": False,
            "market_data_ready": False,
            "reconciler_ok": False,
            "circuit_breaker_state": "unknown",
            "pending_intents_count": 0,
            "active_reservations_count": 0,
            "stale_locks_count": 0,
            "last_reconcile_ts": 0,
            "last_wss_message_ts": 0,
        }
        if initial:
            self._state.update(initial)

    def update(self, **kwargs: Any) -> None:
        self._state.update(kwargs)

    def snapshot(self) -> dict[str, Any]:
        return deepcopy(self._state)

    def get_all(self) -> dict[str, Any]:
        return self.snapshot()

    def is_ready(self, *, strict: bool = False) -> tuple[bool, list[str]]:
        """Return (ok, failed_keys).

        - strict=False: legacy semantics — db_ok AND api_ok must be true.
        - strict=True: every key in ``_STRICT_REQUIRED_KEYS`` must be truthy
          AND ``circuit_breaker_state == "closed"``. Used by /health/ready
          and the live-mode doctor gate.
        """
        snapshot = self._state
        if not strict:
            ok = bool(snapshot.get("db_ok", False)) and bool(snapshot.get("api_ok", False))
            failed_legacy = [k for k in ("db_ok", "api_ok") if not snapshot.get(k)]
            return ok, failed_legacy

        failed: list[str] = []
        for key in _STRICT_REQUIRED_KEYS:
            if not snapshot.get(key):
                failed.append(key)
        cb_state = snapshot.get("circuit_breaker_state", "unknown")
        if cb_state != "closed":
            failed.append("circuit_breaker_state")
        return (len(failed) == 0), failed


async def metrics_handler(_request: web.Request) -> web.Response:
    body = metrics.render_prometheus()
    return web.Response(text=body, content_type="text/plain; version=0.0.4")


def _readiness_payload(tracker: ReadinessTracker | None) -> dict[str, Any]:
    if tracker is None:
        return {"db_ok": False, "api_ok": False}
    return tracker.snapshot()


async def start_health_server(
    port: int = 9102, *, tracker: ReadinessTracker | None = None
) -> web.AppRunner:
    if tracker is not None:
        tracker.update(started_ts=int(time.time()))
    app = web.Application()

    async def health_handler(_request: web.Request) -> web.Response:
        # Legacy /health: db_ok AND api_ok must be true.
        readiness = _readiness_payload(tracker)
        if tracker is None:
            ok = False
        else:
            ok, _ = tracker.is_ready(strict=False)
        return web.json_response({"ok": ok, "readiness": readiness})

    async def ready_handler(_request: web.Request) -> web.Response:
        # Strict /health/ready — used by ops + the live-mode gate.
        readiness = _readiness_payload(tracker)
        if tracker is None:
            return web.json_response(
                {"ok": False, "failed": ["tracker_unset"], "readiness": readiness},
                status=503,
            )
        ok, failed = tracker.is_ready(strict=True)
        return web.json_response(
            {"ok": ok, "failed": failed, "readiness": readiness},
            status=200 if ok else 503,
        )

    app.router.add_get("/health", health_handler)
    app.router.add_get("/health/ready", ready_handler)
    app.router.add_get("/metrics", metrics_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    host = get_metrics_bind_host()
    site = web.TCPSite(runner, host=host, port=port)
    await site.start()
    return runner
