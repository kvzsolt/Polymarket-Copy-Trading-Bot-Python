"""PR7: ReadinessTracker carries drift_ok and strict gating requires it."""

from __future__ import annotations

from pmcopy.services.observability.health_server import _STRICT_REQUIRED_KEYS, ReadinessTracker


def test_strict_required_keys_include_drift_ok():
    assert "drift_ok" in _STRICT_REQUIRED_KEYS


def test_default_strict_is_false_without_drift_ok():
    tracker = ReadinessTracker()
    ok, failed = tracker.is_ready(strict=True)
    assert ok is False
    assert "drift_ok" in failed


def test_strict_passes_when_all_keys_set():
    tracker = ReadinessTracker()
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
    ok, failed = tracker.is_ready(strict=True)
    assert ok is True, f"strict readiness failed: {failed}"


def test_drift_ok_false_blocks_strict_even_with_other_keys_set():
    tracker = ReadinessTracker()
    tracker.update(
        db_ok=True,
        api_ok=True,
        gateway_ok=True,
        wss_market_ok=True,
        wss_user_ok=True,
        reconciler_ok=True,
        market_data_ready=True,
        drift_ok=False,
        circuit_breaker_state="closed",
    )
    ok, failed = tracker.is_ready(strict=True)
    assert ok is False
    assert "drift_ok" in failed
