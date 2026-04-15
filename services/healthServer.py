from __future__ import annotations

import time
from copy import deepcopy
from typing import Any

from aiohttp import web

from config.env import get_metrics_bind_host
from services.metrics import metrics


class ReadinessTracker:
    """Injectable readiness state — one instance per Application.

    Replaces the old module-level ``_readiness_state`` dict. Services that
    need to update readiness accept a ``ReadinessTracker`` instance via
    their constructor; the composition root builds one at startup and
    hands it to every consumer.
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
        }
        if initial:
            self._state.update(initial)

    def update(self, **kwargs: Any) -> None:
        self._state.update(kwargs)

    def snapshot(self) -> dict[str, Any]:
        return deepcopy(self._state)

    def get_all(self) -> dict[str, Any]:
        return self.snapshot()


async def metrics_handler(_request: web.Request) -> web.Response:
    body = await metrics.render_prometheus()
    return web.Response(text=body, content_type="text/plain; version=0.0.4")


async def start_health_server(
    port: int = 9102, *, tracker: ReadinessTracker | None = None
) -> web.AppRunner:
    if tracker is not None:
        tracker.update(started_ts=int(time.time()))
    app = web.Application()

    async def health_handler(_request: web.Request) -> web.Response:
        readiness = tracker.snapshot() if tracker is not None else {}
        ok = bool(readiness.get("db_ok")) and bool(readiness.get("api_ok", True))
        return web.json_response({"ok": ok, "readiness": readiness})

    app.router.add_get("/health", health_handler)
    app.router.add_get("/metrics", metrics_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    host = get_metrics_bind_host()
    site = web.TCPSite(runner, host=host, port=port)
    await site.start()
    return runner
