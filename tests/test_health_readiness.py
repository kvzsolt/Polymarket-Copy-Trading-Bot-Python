from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from aiohttp.test_utils import TestClient, TestServer

from services.healthServer import (
    _DEFAULT_HEARTBEAT_STALENESS_SECONDS,
    _MIN_HEARTBEAT_STALENESS_SECONDS,
    ReadinessTracker,
    start_health_server,
)


def _all_subsystems_ready(tracker: ReadinessTracker) -> None:
    """Flip every strict subsystem flag + circuit breaker to ready.

    Leaves heartbeat timestamps untouched so the heartbeat-staleness path
    (P2-g) can be exercised in isolation.
    """
    tracker.update(
        db_ok=True,
        api_ok=True,
        gateway_ok=True,
        wss_market_ok=True,
        wss_user_ok=True,
        reconciler_ok=True,
        market_data_ready=True,
        drift_ok=True,
        circuit_breaker_state="closed",
    )


@pytest.mark.asyncio
async def test_readiness_tracker_logic():
    tracker = ReadinessTracker()
    tracker.update(db_ok=True, api_ok=True, runtime_user_count=2)
    
    snap = tracker.snapshot()
    assert snap["db_ok"] is True
    assert snap["api_ok"] is True
    assert snap["runtime_user_count"] == 2


@pytest.mark.asyncio
async def test_health_server_setup():
    tracker = ReadinessTracker()
    # Test that start_health_server correctly wires the tracker
    with patch("services.healthServer.web.TCPSite") as site_cls:
        site_instance = AsyncMock()
        site_cls.return_value = site_instance

        runner = await start_health_server(port=9102, tracker=tracker)
        
        # Verify started_ts was set
        assert tracker.snapshot().get("started_ts") is not None
        
        await runner.cleanup()


@pytest.mark.asyncio
async def test_metrics_server_uses_validated_bind_host_from_env():
    tracker = ReadinessTracker()
    with patch.dict("os.environ", {"METRICS_BIND_HOST": "127.0.0.1"}, clear=True):
        with patch("services.healthServer.web.TCPSite") as site_cls:
            site_instance = AsyncMock()
            site_cls.return_value = site_instance

            runner = await start_health_server(port=9102, tracker=tracker)

            _, kwargs = site_cls.call_args
            assert kwargs["host"] == "127.0.0.1"
            assert kwargs["port"] == 9102

            await runner.cleanup()


# --- P2-g: /health/ready must fail on a stale (silently-dead) loop heartbeat ---


def test_strict_ready_when_heartbeats_fresh():
    tracker = ReadinessTracker(heartbeat_staleness_seconds=90.0)
    _all_subsystems_ready(tracker)
    # Both loops ticked 10s ago — well within the 90s budget.
    with patch("services.healthServer.time.time", return_value=1_000_000):
        tracker.update(
            last_trade_poll_ts=1_000_000 - 10,
            last_risk_check_ts=1_000_000 - 10,
        )
        ok, failed = tracker.is_ready(strict=True)
    assert ok is True, f"expected ready, got failed={failed}"
    assert failed == []


def test_strict_not_ready_when_trade_poll_heartbeat_stale():
    tracker = ReadinessTracker(heartbeat_staleness_seconds=90.0)
    _all_subsystems_ready(tracker)
    with patch("services.healthServer.time.time", return_value=1_000_000):
        # Trade-poll loop ticked 5 minutes ago then went silent; risk loop fresh.
        tracker.update(
            last_trade_poll_ts=1_000_000 - 300,
            last_risk_check_ts=1_000_000 - 5,
        )
        ok, failed = tracker.is_ready(strict=True)
    assert ok is False
    assert "trade_poll_stale" in failed
    assert "risk_check_stale" not in failed


def test_strict_not_ready_when_risk_check_heartbeat_stale():
    tracker = ReadinessTracker(heartbeat_staleness_seconds=90.0)
    _all_subsystems_ready(tracker)
    with patch("services.healthServer.time.time", return_value=1_000_000):
        tracker.update(
            last_trade_poll_ts=1_000_000 - 5,
            last_risk_check_ts=1_000_000 - 300,
        )
        ok, failed = tracker.is_ready(strict=True)
    assert ok is False
    assert "risk_check_stale" in failed
    assert "trade_poll_stale" not in failed


def test_strict_not_penalized_before_first_tick_fail_closed_on_subsystems():
    """ts==0 (never ticked) must NOT be reported as stale.

    A never-ticked heartbeat is the legitimate boot state (metrics server
    starts before the monitor tasks) and the config-disabled-loop state (risk
    loop off). The endpoint still fails closed here — but on the *subsystem*
    flags, never on a heartbeat that hasn't started.
    """
    fresh = ReadinessTracker(heartbeat_staleness_seconds=90.0)
    _all_subsystems_ready(fresh)
    # default ts == 0 for both heartbeats
    with patch("services.healthServer.time.time", return_value=1_000_000):
        ok, failed = fresh.is_ready(strict=True)
    assert ok is True, f"never-ticked heartbeats must not block ready: {failed}"
    assert "trade_poll_stale" not in failed
    assert "risk_check_stale" not in failed


def test_strict_disabled_risk_loop_does_not_block_ready():
    """Risk loop disabled → last_risk_check_ts stays 0 forever → not stale."""
    tracker = ReadinessTracker(heartbeat_staleness_seconds=90.0)
    _all_subsystems_ready(tracker)
    with patch("services.healthServer.time.time", return_value=1_000_000):
        # Trade-poll alive; risk loop never ticks (disabled).
        tracker.update(last_trade_poll_ts=1_000_000 - 5)
        ok, failed = tracker.is_ready(strict=True)
    assert ok is True, f"disabled risk loop must not block ready: {failed}"


def test_non_numeric_heartbeat_is_treated_as_stale():
    tracker = ReadinessTracker(heartbeat_staleness_seconds=90.0)
    _all_subsystems_ready(tracker)
    with patch("services.healthServer.time.time", return_value=1_000_000):
        tracker.update(last_trade_poll_ts="corrupt", last_risk_check_ts=1_000_000 - 5)
        ok, failed = tracker.is_ready(strict=True)
    assert ok is False
    assert "trade_poll_stale" in failed


def test_configure_heartbeat_budget_floors_subsecond_interval():
    tracker = ReadinessTracker()
    tracker.configure_heartbeat_budget(staleness_seconds=0.75)  # 0.25s * 3
    assert tracker._heartbeat_staleness_seconds == _MIN_HEARTBEAT_STALENESS_SECONDS


def test_default_heartbeat_budget_used_when_unconfigured():
    tracker = ReadinessTracker()
    assert tracker._heartbeat_staleness_seconds == _DEFAULT_HEARTBEAT_STALENESS_SECONDS


@pytest.mark.asyncio
async def test_health_ready_endpoint_returns_503_on_stale_heartbeat():
    """End-to-end: GET /health/ready returns 503 when a loop heartbeat is stale."""
    tracker = ReadinessTracker(heartbeat_staleness_seconds=90.0)
    _all_subsystems_ready(tracker)

    with patch("services.healthServer.web.TCPSite") as site_cls:
        site_cls.return_value = AsyncMock()
        runner = await start_health_server(port=9102, tracker=tracker)

    try:
        # Drive a deterministic "now" so the stale check is reproducible.
        with patch("services.healthServer.time.time", return_value=1_000_000):
            tracker.update(
                last_trade_poll_ts=1_000_000 - 5,
                last_risk_check_ts=1_000_000 - 5,
            )
            async with TestClient(TestServer(runner.app)) as client:
                resp = await client.get("/health/ready")
                assert resp.status == 200
                body = await resp.json()
                assert body["ok"] is True

                # Now the trade-poll loop dies: its heartbeat stops advancing.
                tracker.update(last_trade_poll_ts=1_000_000 - 600)
                resp = await client.get("/health/ready")
                assert resp.status == 503
                body = await resp.json()
                assert body["ok"] is False
                assert "trade_poll_stale" in body["failed"]
    finally:
        await runner.cleanup()
