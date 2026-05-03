"""Lightweight HTTP health and metrics server for paper/live monitoring.

Exposes a minimal health endpoint — no framework, just stdlib —
so the bot can be monitored by external tooling (systemd, Docker, Uptime Kuma, etc.)
without pulling in a web framework dependency.
"""

from __future__ import annotations

import json
import logging
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class HealthState:
    """Mutable health state updated by the running bot."""

    mode: str = "unknown"  # "live" | "paper"
    uptime_seconds: float = 0.0
    last_trade_ts: str | None = None
    last_reconciliation_ts: str | None = None
    pending_intents: int = 0
    open_reservations: int = 0
    reconciliation_queue_depth: int = 0
    geoblock_ok: bool = True
    db_ok: bool = True
    rpc_ok: bool = True
    wallet_ok: bool = True
    kill_switch: bool = False
    errors: list[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        healthy = all(
            [
                self.db_ok,
                self.rpc_ok,
                self.wallet_ok,
                not self.kill_switch,
                self.geoblock_ok or self.mode == "paper",
            ]
        )
        return {
            "status": "ok" if healthy else "degraded",
            "mode": self.mode,
            "uptime_seconds": round(self.uptime_seconds, 1),
            "last_trade_ts": self.last_trade_ts,
            "last_reconciliation_ts": self.last_reconciliation_ts,
            "pending_intents": self.pending_intents,
            "open_reservations": self.open_reservations,
            "reconciliation_queue_depth": self.reconciliation_queue_depth,
            "checks": {
                "geoblock": "ok" if self.geoblock_ok else "failed",
                "db": "ok" if self.db_ok else "failed",
                "rpc": "ok" if self.rpc_ok else "failed",
                "wallet": "ok" if self.wallet_ok else "failed",
                "kill_switch": "active" if self.kill_switch else "inactive",
            },
        }


class HealthHandler(BaseHTTPRequestHandler):
    """Minimal handler that returns JSON health status."""

    # Set by the server at construction time
    state: HealthState

    def do_GET(self) -> None:
        if self.path == "/health":
            self._send_json(self.server.state.to_dict())  # type: ignore[attr-defined]
        elif self.path == "/health/live":
            data = self.server.state.to_dict()  # type: ignore[attr-defined]
            data["live"] = True
            self._send_json(data)
        elif self.path == "/health/ready":
            data = self.server.state.to_dict()  # type: ignore[attr-defined]
            healthy = data["status"] == "ok"
            self._send_json(data, status=200 if healthy else 503)
        else:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b'{"error":"not_found"}\n')

    def _send_json(self, data: dict, status: int = 200) -> None:
        body = json.dumps(data, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt: str, *args: object) -> None:
        """Suppress default stderr logging; route through our logger."""
        logger.debug("health_http %s", fmt % args)


class HealthServer:
    """Thread-safe health server wrapper."""

    def __init__(self, host: str = "0.0.0.0", port: int = 9090) -> None:
        self.host = host
        self.port = port
        self.state = HealthState()
        self._server: HTTPServer | None = None
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._server is not None:
            return

        handler = type(
            "BoundHealthHandler",
            (HealthHandler,),
            {"state": self.state},
        )

        self._server = HTTPServer((self.host, self.port), handler)
        self._server.state = self.state  # type: ignore[attr-defined]
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        logger.info(
            "health_server_started host=%s port=%d", self.host, self.port
        )

    def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server = None
        self._thread = None
        logger.info("health_server_stopped")
