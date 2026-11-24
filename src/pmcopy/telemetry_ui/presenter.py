"""Read-only HTTP presenter for the local telemetry and performance monitor.

Exposes zero-mutation observability endpoints (`GET /`, `GET /telemetry`,
`GET /assets/styles.css`, `GET /assets/app.js`, `GET /api/telemetry/snapshot`,
and `GET /api/telemetry/stream`) backed by injected read-only ports.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime
from importlib import resources
import json
import re
import time
from typing import Any, Protocol

from aiohttp import web

from pmcopy.services.observability.metrics import MetricsRegistry, metrics as default_metrics
from pmcopy.services.telemetry.hub import TelemetrySubscription
from pmcopy.services.telemetry.query import TelemetryQueryService
from pmcopy.utils.log_redaction import redact_object, redact_text

_CSP_HEADER = (
    "default-src 'self'; "
    "script-src 'self'; "
    "style-src 'self'; "
    "img-src 'self' data:; "
    "connect-src 'self'; "
    "object-src 'none'; "
    "base-uri 'self'; "
    "frame-ancestors 'none'"
)
_ALCHEMY_PATH_RE = re.compile(r"((?:wss?|https?)://[^/\s]+/v\d+/)[^\s\"']+")
_SSE_HEARTBEAT_SECONDS = 15.0


class ReadinessSnapshotPort(Protocol):
    """Read-only readiness port consumed by the telemetry presenter."""

    def snapshot(self) -> dict[str, Any]:
        """Return a point-in-time readiness state dictionary."""
        ...

    def is_ready(self, *, strict: bool = False) -> tuple[bool, list[str]]:
        """Return ``(ready, failed_check_keys)``."""
        ...


class TelemetryQueryPort(Protocol):
    """Read-only bounded telemetry query port."""

    def snapshot(self, *, limit: int = 100) -> dict[str, Any]:
        """Return JSON-safe events, counters, gauges, and histograms."""
        ...


class TelemetryStreamPort(Protocol):
    """Read-only bounded event subscription port."""

    def subscribe(self, *, capacity: int | None = None) -> TelemetrySubscription:
        """Register one bounded non-blocking subscriber."""
        ...

    def unsubscribe(self, subscription: TelemetrySubscription) -> None:
        """Close and remove one subscriber."""
        ...


DetectionSnapshotCallable = Callable[[], dict[str, Any] | None]


def _scrub_urls(text: str) -> str:
    """Mask any Alchemy `/v2/<key>` URL segments in serialized JSON text."""
    return _ALCHEMY_PATH_RE.sub(r"\1<redacted>", redact_text(text))


def _load_static_asset(filename: str) -> str:
    """Read a packaged static UI file from `pmcopy.telemetry_ui`."""
    return resources.files("pmcopy.telemetry_ui").joinpath(filename).read_text(encoding="utf-8")


class TelemetryWebPresenter:
    """Serve the read-only HTML/CSS/JS telemetry monitor and SSE stream.

    Follows the Single-Responsibility and Dependency-Inversion principles:
    consumes narrow read-only ports, performs zero state mutation on trading
    subsystems, and scrubs all outgoing payloads through `log_redaction`.
    """

    def __init__(
        self,
        *,
        tracker: ReadinessSnapshotPort | None = None,
        telemetry_query: TelemetryQueryPort | None = None,
        telemetry_hub: TelemetryStreamPort | None = None,
        detection_telemetry: DetectionSnapshotCallable | None = None,
        metrics_registry: MetricsRegistry | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        """Initialize the read-only telemetry web presenter.

        Args:
            tracker: Optional application readiness tracker.
            telemetry_query: Optional bounded telemetry query service.
            telemetry_hub: Optional bounded telemetry fan-out hub for SSE.
            detection_telemetry: Optional zero-arg callable returning live
                Alchemy WebSocket bandwidth/CU and reservation metrics.
            metrics_registry: Fallback metrics registry when `telemetry_query`
                is omitted.
            clock: Wall-clock epoch seconds provider for uptime computation.
        """
        self._tracker = tracker
        self._query = telemetry_query
        self._hub = telemetry_hub
        self._detection_telemetry = detection_telemetry
        self._metrics = metrics_registry if metrics_registry is not None else default_metrics
        self._clock = clock
        self._html = _load_static_asset("index.html")
        self._css = _load_static_asset("styles.css")
        self._js = _load_static_asset("app.js")

    def register_routes(self, app: web.Application) -> None:
        """Mount read-only `GET` routes onto an `aiohttp.web.Application`.

        Args:
            app: Target `aiohttp` application (only `GET`/`HEAD` are registered,
                so `POST`/`PUT`/`PATCH`/`DELETE` automatically return `405`).
        """
        app.router.add_get("/", self.handle_index)
        app.router.add_get("/telemetry", self.handle_index)
        app.router.add_get("/assets/styles.css", self.handle_styles)
        app.router.add_get("/assets/app.js", self.handle_script)
        app.router.add_get("/api/telemetry/snapshot", self.handle_snapshot)
        app.router.add_get("/api/telemetry/stream", self.handle_stream)

    async def handle_index(self, _request: web.Request) -> web.Response:
        """Serve the static HTML telemetry monitor shell."""
        return web.Response(
            text=self._html,
            content_type="text/html",
            charset="utf-8",
            headers={
                "Content-Security-Policy": _CSP_HEADER,
                "X-Content-Type-Options": "nosniff",
                "Cache-Control": "no-cache",
            },
        )

    async def handle_styles(self, _request: web.Request) -> web.Response:
        """Serve the static CSS stylesheet."""
        return web.Response(
            text=self._css,
            content_type="text/css",
            charset="utf-8",
            headers={
                "Content-Security-Policy": _CSP_HEADER,
                "X-Content-Type-Options": "nosniff",
                "Cache-Control": "no-cache",
            },
        )

    async def handle_script(self, _request: web.Request) -> web.Response:
        """Serve the static vanilla JavaScript telemetry client."""
        return web.Response(
            text=self._js,
            content_type="application/javascript",
            charset="utf-8",
            headers={
                "Content-Security-Policy": _CSP_HEADER,
                "X-Content-Type-Options": "nosniff",
                "Cache-Control": "no-cache",
            },
        )

    async def handle_snapshot(self, request: web.Request) -> web.Response:
        """Serve a unified, secret-scrubbed JSON telemetry snapshot."""
        raw_limit = request.query.get("limit", "100")
        try:
            limit = max(1, min(250, int(raw_limit)))
        except ValueError:
            limit = 100

        payload = self.build_snapshot(limit=limit)
        serialized = _scrub_urls(json.dumps(redact_object(payload), separators=(",", ":")))
        return web.Response(
            text=serialized,
            content_type="application/json",
            charset="utf-8",
            headers={
                "Cache-Control": "no-store",
                "Content-Security-Policy": _CSP_HEADER,
                "X-Content-Type-Options": "nosniff",
            },
        )

    async def handle_stream(self, request: web.Request) -> web.StreamResponse:
        """Stream live telemetry events over Server-Sent Events (SSE)."""
        response = web.StreamResponse(
            status=200,
            reason="OK",
            headers={
                "Content-Type": "text/event-stream; charset=utf-8",
                "Cache-Control": "no-store",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
                "Content-Security-Policy": _CSP_HEADER,
                "X-Content-Type-Options": "nosniff",
            },
        )
        await response.prepare(request)

        subscription: TelemetrySubscription | None = None
        if self._hub is not None:
            try:
                subscription = self._hub.subscribe(capacity=128)
            except RuntimeError:
                subscription = None

        try:
            hello = json.dumps(
                {
                    "type": "connected",
                    "streaming": subscription is not None,
                    "timestamp": datetime.now(UTC).isoformat(),
                },
                separators=(",", ":"),
            )
            await response.write(f"event: connected\ndata: {hello}\n\n".encode("utf-8"))

            if subscription is None:
                return response

            while not subscription.closed:
                try:
                    delivery = await asyncio.wait_for(
                        subscription.get(),
                        timeout=_SSE_HEARTBEAT_SECONDS,
                    )
                except TimeoutError:
                    await response.write(b": heartbeat\n\n")
                    continue

                if delivery.stream_closed:
                    break
                if delivery.resync_required:
                    resync_json = json.dumps(
                        {
                            "type": "resync_required",
                            "dropped_count": delivery.dropped_count,
                        },
                        separators=(",", ":"),
                    )
                    await response.write(f"event: resync\ndata: {resync_json}\n\n".encode("utf-8"))
                    continue
                if delivery.event is not None:
                    event_dict = TelemetryQueryService._event_to_json(delivery.event)
                    safe_json = _scrub_urls(
                        json.dumps(redact_object(event_dict), separators=(",", ":"))
                    )
                    await response.write(f"event: telemetry\ndata: {safe_json}\n\n".encode("utf-8"))
        except (ConnectionResetError, asyncio.CancelledError, RuntimeError):
            pass
        finally:
            if subscription is not None and self._hub is not None:
                self._hub.unsubscribe(subscription)
        return response

    def build_snapshot(self, *, limit: int = 100) -> dict[str, Any]:
        """Assemble readiness, Alchemy WSS cost/bandwidth, metrics, and events."""
        now_epoch = int(self._clock())
        readiness_data = self._build_readiness_section(now_epoch)
        detection_data = self._build_detection_section()
        query_data = self._build_telemetry_section(limit=limit)
        return {
            "generated_at": query_data["generated_at"],
            "readiness": readiness_data,
            "detection": detection_data,
            "metrics": query_data["metrics"],
            "events": query_data["events"],
        }

    def _build_readiness_section(self, now_epoch: int) -> dict[str, Any]:
        if self._tracker is None:
            return {
                "ok": False,
                "strict_ok": False,
                "failed": ["tracker_unset"],
                "uptime_seconds": 0,
                "state": {},
            }
        state = self._tracker.snapshot()
        ok, _ = self._tracker.is_ready(strict=False)
        strict_ok, failed = self._tracker.is_ready(strict=True)
        started_ts = int(state.get("started_ts") or 0)
        uptime = max(0, now_epoch - started_ts) if started_ts > 0 else 0
        return {
            "ok": ok,
            "strict_ok": strict_ok,
            "failed": failed,
            "uptime_seconds": uptime,
            "state": state,
        }

    def _build_detection_section(self) -> dict[str, Any]:
        if self._detection_telemetry is None:
            return {"available": False}
        try:
            snap = self._detection_telemetry()
        except Exception:
            return {"available": False}
        if not isinstance(snap, dict):
            return {"available": False}
        return {"available": True, **snap}

    def _build_telemetry_section(self, *, limit: int) -> dict[str, Any]:
        if self._query is not None:
            return self._query.snapshot(limit=limit)
        counters, gauges = self._metrics.snapshot()
        histograms = [
            {
                "name": h.name,
                "labels": dict(h.labels),
                "count": h.count,
                "sum": h.total,
                "p50": h.p50,
                "p95": h.p95,
                "p99": h.p99,
            }
            for h in self._metrics.histogram_snapshots()
        ]
        return {
            "generated_at": datetime.now(UTC).isoformat(),
            "events": [],
            "metrics": {
                "counters": dict(sorted(counters.items())),
                "gauges": dict(sorted(gauges.items())),
                "histograms": histograms,
            },
        }
