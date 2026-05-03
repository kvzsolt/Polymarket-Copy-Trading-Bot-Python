"""Tests for services/observability/health_server.py"""

from __future__ import annotations

import json
import time
import pytest

from services.observability.health_server import HealthServer, HealthState


class TestHealthState:
    def test_healthy_when_all_checks_ok(self):
        state = HealthState(mode="live", db_ok=True, rpc_ok=True, wallet_ok=True, kill_switch=False)
        data = state.to_dict()
        assert data["status"] == "ok"

    def test_degraded_when_db_fails(self):
        state = HealthState(mode="live", db_ok=False, rpc_ok=True, wallet_ok=True, kill_switch=False)
        data = state.to_dict()
        assert data["status"] == "degraded"

    def test_degraded_when_kill_switch_active(self):
        state = HealthState(mode="live", db_ok=True, rpc_ok=True, wallet_ok=True, kill_switch=True)
        data = state.to_dict()
        assert data["status"] == "degraded"

    def test_paper_ignores_geoblock(self):
        state = HealthState(mode="paper", geoblock_ok=False, db_ok=True, rpc_ok=True, wallet_ok=True)
        data = state.to_dict()
        assert data["status"] == "ok"

    def test_live_requires_geoblock(self):
        state = HealthState(mode="live", geoblock_ok=False, db_ok=True, rpc_ok=True, wallet_ok=True)
        data = state.to_dict()
        assert data["status"] == "degraded"

    def test_to_dict_includes_all_keys(self):
        state = HealthState()
        data = state.to_dict()
        expected_keys = {
            "status", "mode", "uptime_seconds", "last_trade_ts",
            "last_reconciliation_ts", "pending_intents", "open_reservations",
            "reconciliation_queue_depth", "checks",
        }
        assert expected_keys.issubset(data.keys())
        assert set(data["checks"].keys()) == {"geoblock", "db", "rpc", "wallet", "kill_switch"}


class TestHealthServerStartStop:
    def test_start_and_stop(self):
        srv = HealthServer(host="127.0.0.1", port=19990)
        srv.start()
        time.sleep(0.1)
        assert srv._server is not None
        srv.stop()
        assert srv._server is None

    def test_health_endpoint_returns_json(self):
        srv = HealthServer(host="127.0.0.1", port=19991)
        srv.state.mode = "paper"
        srv.start()
        time.sleep(0.1)

        import urllib.request
        url = f"http://{srv.host}:{srv.port}/health"
        resp = urllib.request.urlopen(url, timeout=2)
        data = json.loads(resp.read().decode())
        assert data["mode"] == "paper"
        srv.stop()

    def test_ready_endpoint_returns_200_when_healthy(self):
        srv = HealthServer(host="127.0.0.1", port=19992)
        srv.state.mode = "paper"
        srv.state.db_ok = True
        srv.state.rpc_ok = True
        srv.state.wallet_ok = True
        srv.start()
        time.sleep(0.1)

        import urllib.request
        url = f"http://{srv.host}:{srv.port}/health/ready"
        # 200 when healthy
        req = urllib.request.Request(url)
        resp = urllib.request.urlopen(req, timeout=2)
        assert resp.status == 200
        srv.stop()

    def test_ready_endpoint_returns_503_when_degraded(self):
        srv = HealthServer(host="127.0.0.1", port=19993)
        srv.state.mode = "live"
        srv.state.db_ok = False
        srv.start()
        time.sleep(0.1)

        import urllib.request
        url = f"http://{srv.host}:{srv.port}/health/ready"
        req = urllib.request.Request(url)
        try:
            urllib.request.urlopen(req, timeout=2)
            assert False, "expected HTTPError"
        except urllib.error.HTTPError as exc:
            assert exc.code == 503
        srv.stop()

    def test_unknown_path_returns_404(self):
        srv = HealthServer(host="127.0.0.1", port=19994)
        srv.start()
        time.sleep(0.1)

        import urllib.request
        url = f"http://{srv.host}:{srv.port}/nope"
        try:
            urllib.request.urlopen(url, timeout=2)
            assert False, "expected HTTPError"
        except urllib.error.HTTPError as exc:
            assert exc.code == 404
        srv.stop()
