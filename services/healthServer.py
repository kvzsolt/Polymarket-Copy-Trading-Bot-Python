from __future__ import annotations

import time
from copy import deepcopy

from aiohttp import web

from config.env import get_metrics_bind_host
from services.metrics import metrics


_readiness_state: dict[str, object] = {
    "db_ok": False,
    "api_ok": False,
    "last_trade_poll_ts": 0,
    "last_risk_check_ts": 0,
    "last_successful_order_ts": 0,
    "monitored_leader_count": 0,
    "runtime_user_count": 0,
    "mode": "configured",
}



def update_readiness(**kwargs) -> None:
    _readiness_state.update(kwargs)



def get_readiness() -> dict[str, object]:
    return deepcopy(_readiness_state)


async def health_handler(_request: web.Request) -> web.Response:
    readiness = get_readiness()
    ok = bool(readiness.get("db_ok")) and bool(readiness.get("api_ok", True))
    return web.json_response({"ok": ok, "readiness": readiness})


async def metrics_handler(_request: web.Request) -> web.Response:
    body = await metrics.render_prometheus()
    return web.Response(text=body, content_type="text/plain; version=0.0.4")


async def start_health_server(port: int = 9102) -> web.AppRunner:
    update_readiness(started_ts=int(time.time()))
    app = web.Application()
    app.router.add_get("/health", health_handler)
    app.router.add_get("/metrics", metrics_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    host = get_metrics_bind_host()
    site = web.TCPSite(runner, host=host, port=port)
    await site.start()
    return runner
