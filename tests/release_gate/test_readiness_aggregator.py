"""Release-gate: ReadinessTracker strict readiness contract (Issue #76).

The strict readiness check fails closed: every required subsystem flag must
be truthy AND the circuit breaker must be ``closed``. Toggling any single
flag false flips ``is_ready`` from True to False with the failing key
identifiable for operator drill-down.
"""
from __future__ import annotations

import pytest

from services.healthServer import _STRICT_REQUIRED_KEYS, ReadinessTracker


def _all_ready() -> ReadinessTracker:
    tracker = ReadinessTracker()
    tracker.update(
        db_ok=True,
        api_ok=True,
        gateway_ok=True,
        wss_market_ok=True,
        wss_user_ok=True,
        reconciler_ok=True,
        circuit_breaker_state="closed",
    )
    return tracker


def test_default_tracker_fails_strict() -> None:
    tracker = ReadinessTracker()
    ok, failed = tracker.is_ready(strict=True)
    assert ok is False
    assert set(failed) >= set(_STRICT_REQUIRED_KEYS)
    assert "circuit_breaker_state" in failed


def test_legacy_is_ready_only_needs_db_and_api() -> None:
    tracker = ReadinessTracker()
    tracker.update(db_ok=True, api_ok=True)
    ok, failed = tracker.is_ready(strict=False)
    assert ok is True
    assert failed == []


def test_strict_passes_when_all_subsystems_ok() -> None:
    ok, failed = _all_ready().is_ready(strict=True)
    assert ok is True
    assert failed == []


@pytest.mark.parametrize("key", list(_STRICT_REQUIRED_KEYS))
def test_strict_flips_when_any_required_key_false(key: str) -> None:
    tracker = _all_ready()
    tracker.update(**{key: False})
    ok, failed = tracker.is_ready(strict=True)
    assert ok is False
    assert key in failed


def test_open_circuit_fails_strict() -> None:
    tracker = _all_ready()
    tracker.update(circuit_breaker_state="open")
    ok, failed = tracker.is_ready(strict=True)
    assert ok is False
    assert "circuit_breaker_state" in failed
