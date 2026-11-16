"""Tests for the read-only HTML/CSS/JS telemetry and performance web monitor."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import aiohttp
import pytest

from pmcopy.services.healthServer import ReadinessTracker, start_health_server
from pmcopy.services.metrics import (
    METRIC_TELEMETRY_LATENCY,
    TELEMETRY_LATENCY_BUCKETS,
    TELEMETRY_METRIC_SCHEMAS,
    LatencyPhase,
    MetricsRegistry,
)
from pmcopy.services.telemetry.contracts import (
    RuntimeEvent,
    RuntimeOutcome,
    TelemetryProducer,
    TelemetrySeverity,
)
from pmcopy.services.telemetry.hub import TelemetryHub
from pmcopy.services.telemetry.query import TelemetryQueryService
from pmcopy.services.telemetry.service import TelemetryService
from pmcopy.utils.log_redaction import register_secret_values, reset_secret_registry


@pytest.mark.asyncio
async def test_telemetry_web_monitor_serves_assets_snapshot_sse_and_rejects_mutations() -> None:
    """Verify HTML/CSS/JS assets, snapshot JSON, live SSE stream, CSP, redaction, and 405 on writes."""
    reset_secret_registry()
    secret_token = "SuperSecretAlchemyApiKeyValue987654"
    register_secret_values([secret_token])

    reg = MetricsRegistry(schemas=TELEMETRY_METRIC_SCHEMAS)
    reg.inc("trades_executed_total", value=7.0, labels={"side": "BUY", "user": "acct"})
    reg.observe(
        METRIC_TELEMETRY_LATENCY,
        0.00145,
        buckets=TELEMETRY_LATENCY_BUCKETS,
        labels={"phase": LatencyPhase.SOURCE_DETECTION_TO_NORMALIZED.value},
    )
    reg.observe(
        METRIC_TELEMETRY_LATENCY,
        0.0182,
        buckets=TELEMETRY_LATENCY_BUCKETS,
        labels={"phase": LatencyPhase.VENUE_SUBMISSION.value},
    )
    hub = TelemetryHub(metrics=reg)
    svc = TelemetryService(metrics=reg, hub=hub)
    await svc.start()
    query = TelemetryQueryService(service=svc, metrics=reg)

    svc.emit(
        producer=TelemetryProducer.RUNTIME,
        severity=TelemetrySeverity.INFO,
        payload=RuntimeEvent(outcome=RuntimeOutcome.READY),
    )
    # Give hub fan-out loop one tick to process initial history
    await asyncio.sleep(0.02)

    tracker = ReadinessTracker(
        initial={
            "db_ok": True,
            "api_ok": True,
            "gateway_ok": True,
            "wss_market_ok": True,
            "wss_user_ok": True,
            "reconciler_ok": True,
            "market_data_ready": True,
            "drift_ok": True,
            "circuit_breaker_state": "closed",
            "mode": "paper",
            "monitored_leader_count": 3,
            "runtime_user_count": 1,
        }
    )

    def _detection_snapshot() -> dict[str, Any]:
        return {
            "configured_mode": "confirmed+mempool_reserved",
            "effective_mode": "confirmed+mempool_reserved",
            "connected": True,
            "breaker_tripped": False,
            "ws_frames_in": 420,
            "ws_bytes_in": 128_000,
            "rolling_hour_bytes": 128_000,
            "max_hourly_bytes": 262_144_000,
            "max_hourly_ws_mb": "250",
            "estimated_cu_total": "5120.00",
            "estimated_cu_min": "85.33",
            "estimated_cu_day_projection": "122875.20",
            "active_subscriptions": 3,
            "reconnect_count": 0,
            "matched_user_events": 12,
            "active_reservations": 1,
            "promoted_confirmed": 9,
            "direct_confirmed": 3,
            "expired_ghost_fills": 2,
            "endpoint_note": f"wss://polygon-mainnet.g.alchemy.com/v2/{secret_token}",
        }

    runner = await start_health_server(
        0,
        tracker=tracker,
        host="127.0.0.1",
        telemetry_query=query,
        telemetry_hub=hub,
        detection_telemetry=_detection_snapshot,
    )
    try:
        sites = list(runner.addresses)
        assert sites
        host, port = sites[0][0], sites[0][1]
        base_url = f"http://{host}:{port}"

        async with aiohttp.ClientSession() as session:
            # 1. HTML monitor page + strict CSP
            for route in ("/", "/telemetry"):
                async with session.get(f"{base_url}{route}") as resp:
                    assert resp.status == 200
                    assert "text/html" in resp.headers.get("Content-Type", "")
                    csp = resp.headers.get("Content-Security-Policy", "")
                    assert "default-src 'self'" in csp
                    html = await resp.text()
                    assert "/assets/styles.css" in html
                    assert "/assets/app.js" in html
                    assert "Alchemy WSS" in html

            # 2. Static CSS & JS assets (zero external HTTP/CDN dependencies)
            async with session.get(f"{base_url}/assets/styles.css") as css_resp:
                assert css_resp.status == 200
                assert "text/css" in css_resp.headers.get("Content-Type", "")
                css_text = await css_resp.text()
                assert "tabular-nums" in css_text
                assert "https://" not in css_text

            async with session.get(f"{base_url}/assets/app.js") as js_resp:
                assert js_resp.status == 200
                assert "javascript" in js_resp.headers.get("Content-Type", "")
                js_text = await js_resp.text()
                assert "/api/telemetry/snapshot" in js_text
                assert "/api/telemetry/stream" in js_text
                assert "https://" not in js_text

            # 3. Unified JSON snapshot + secret scrubbing
            async with session.get(f"{base_url}/api/telemetry/snapshot") as snap_resp:
                assert snap_resp.status == 200
                assert snap_resp.headers.get("Cache-Control") == "no-store"
                raw_text = await snap_resp.text()
                assert secret_token not in raw_text
                snap = json.loads(raw_text)
                assert snap["readiness"]["ok"] is True
                assert snap["readiness"]["strict_ok"] is True
                assert snap["detection"]["configured_mode"] == "confirmed+mempool_reserved"
                assert snap["detection"]["ws_bytes_in"] == 128_000
                assert snap["detection"]["promoted_confirmed"] == 9
                assert len(snap["metrics"]["histograms"]) >= 2
                assert len(snap["events"]) >= 1

            # 4. Live SSE stream receives published TelemetryHub events & cleans up on disconnect
            async with session.get(f"{base_url}/api/telemetry/stream") as sse_resp:
                assert sse_resp.status == 200
                assert "text/event-stream" in sse_resp.headers.get("Content-Type", "")

                # Read initial connected frame
                first_chunk = await asyncio.wait_for(sse_resp.content.readline(), timeout=1.0)
                assert first_chunk

                # Emit a fresh event and read it from the SSE stream
                svc.emit(
                    producer=TelemetryProducer.RECONCILIATION,
                    severity=TelemetrySeverity.INFO,
                    payload=RuntimeEvent(outcome=RuntimeOutcome.DEGRADED),
                )
                received_lines: list[str] = []
                deadline = asyncio.get_running_loop().time() + 1.5
                while asyncio.get_running_loop().time() < deadline:
                    line = await asyncio.wait_for(sse_resp.content.readline(), timeout=1.0)
                    decoded = line.decode("utf-8").strip()
                    received_lines.append(decoded)
                    if '"degraded"' in decoded:
                        break
                assert any('"degraded"' in line for line in received_lines)

            # 5. Mutation methods are strictly rejected (read-only surface)
            for method in ("POST", "PUT", "DELETE", "PATCH"):
                for target in (
                    "/",
                    "/telemetry",
                    "/api/telemetry/snapshot",
                    "/api/telemetry/stream",
                ):
                    async with session.request(method, f"{base_url}{target}") as mut_resp:
                        assert mut_resp.status == 405

            # 6. Legacy /health, /health/ready, /health/live, /metrics remain backward-compatible
            async with session.get(f"{base_url}/health") as h_resp:
                assert h_resp.status == 200
            async with session.get(f"{base_url}/health/ready") as hr_resp:
                assert hr_resp.status == 200
            async with session.get(f"{base_url}/health/live") as hl_resp:
                assert hl_resp.status == 200
            async with session.get(f"{base_url}/metrics") as m_resp:
                assert m_resp.status == 200
    finally:
        await runner.cleanup()
        await hub.close()
        reset_secret_registry()


def test_telemetry_ui_contains_no_prohibited_surface_tokens() -> None:
    """Static guard: telemetry_ui and healthServer remain 100% standalone and read-only."""
    repo_root = Path(__file__).resolve().parents[1]
    ui_dir = repo_root / "src" / "pmcopy" / "telemetry_ui"
    assert ui_dir.is_dir()
    forbidden = "control" + "_plane"
    for path in (*ui_dir.iterdir(), repo_root / "src" / "pmcopy" / "services" / "healthServer.py"):
        if path.is_file():
            assert forbidden not in path.read_text(encoding="utf-8")
