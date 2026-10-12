"""Characterization tests for keyed readiness and liveness compatibility."""

from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest
from aiohttp.test_utils import TestClient, TestServer

from pmcopy.services.healthServer import ReadinessTracker, start_health_server


def _legacy_ready(tracker: ReadinessTracker) -> None:
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


def test_required_keyed_check_joins_strict_readiness_truth() -> None:
    tracker = ReadinessTracker()
    _legacy_ready(tracker)

    registered = tracker.register_check(
        "telemetry_hub",
        required=True,
        ready=False,
        reason="not_started",
    )
    ok, failed = tracker.is_ready(strict=True)
    assert ok is False
    assert failed == ["telemetry_hub"]
    with pytest.raises(FrozenInstanceError):
        registered.ready = True  # type: ignore[misc]

    tracker.set_check("telemetry_hub", ready=True)
    ok, failed = tracker.is_ready(strict=True)
    assert ok is True
    assert failed == []


def test_optional_keyed_check_is_visible_but_does_not_block() -> None:
    tracker = ReadinessTracker()
    _legacy_ready(tracker)
    tracker.register_check(
        "telemetry_sink",
        required=False,
        ready=False,
        reason="degraded",
    )

    ok, failed = tracker.is_ready(strict=True)
    snapshot = tracker.snapshot()
    assert ok is True
    assert failed == []
    assert snapshot["checks"]["telemetry_sink"] == {
        "ready": False,
        "required": False,
        "reason": "degraded",
    }


def test_keyed_checks_reject_duplicate_or_unbounded_reason_text() -> None:
    tracker = ReadinessTracker()
    tracker.register_check("telemetry_hub", required=True, ready=False)
    with pytest.raises(ValueError, match="already registered"):
        tracker.register_check("telemetry_hub", required=True, ready=True)
    with pytest.raises(ValueError, match="reason"):
        tracker.set_check(
            "telemetry_hub",
            ready=False,
            reason="Authorization: Bearer secret-token",
        )


@pytest.mark.asyncio
async def test_liveness_is_independent_while_legacy_and_ready_contracts_are_preserved() -> None:
    tracker = ReadinessTracker()
    tracker.update(db_ok=True, api_ok=True)
    runner = await start_health_server(port=0, tracker=tracker)
    try:
        async with TestClient(TestServer(runner.app)) as client:
            live = await client.get("/health/live")
            health = await client.get("/health")
            ready = await client.get("/health/ready")

            assert live.status == 200
            assert await live.json() == {"ok": True, "status": "alive"}
            assert health.status == 200
            assert (await health.json())["ok"] is True
            assert ready.status == 503
            assert (await ready.json())["ok"] is False
    finally:
        await runner.cleanup()


@pytest.mark.asyncio
async def test_liveness_remains_true_without_a_readiness_tracker() -> None:
    runner = await start_health_server(port=0, tracker=None)
    try:
        async with TestClient(TestServer(runner.app)) as client:
            live = await client.get("/health/live")
            health = await client.get("/health")
            ready = await client.get("/health/ready")
            assert await live.json() == {"ok": True, "status": "alive"}
            assert (await health.json())["ok"] is False
            assert ready.status == 503
    finally:
        await runner.cleanup()
