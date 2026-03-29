from __future__ import annotations

from aiohttp import web

from services.metrics import metrics


async def health_handler(_request: web.Request) -> web.Response:
    return web.json_response({"ok": True})


async def metrics_handler(_request: web.Request) -> web.Response:
    body = await metrics.render_prometheus()
    return web.Response(text=body, content_type="text/plain; version=0.0.4")


async def start_health_server(port: int = 9102) -> web.AppRunner:
    app = web.Application()
    app.router.add_get("/health", health_handler)
    app.router.add_get("/metrics", metrics_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host="0.0.0.0", port=port)
    await site.start()
    return runner
