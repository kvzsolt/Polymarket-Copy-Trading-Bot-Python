"""Tests for services.circuitBreaker – state transitions and API guard."""

import time
from unittest.mock import patch

import pytest

from services.circuitBreaker import CircuitBreaker, CircuitOpen, CircuitState


# ── helpers ───────────────────────────────────────────────────────────────

def _make(threshold: int = 3, cooldown: float = 1.0) -> CircuitBreaker:
    return CircuitBreaker("test", failure_threshold=threshold, cooldown_seconds=cooldown)


# ── basic state transitions ──────────────────────────────────────────────

class TestBasicTransitions:
    def test_starts_closed(self):
        cb = _make()
        assert cb.state == CircuitState.CLOSED
        assert cb.allow_request() is True

    def test_stays_closed_under_threshold(self):
        cb = _make(threshold=3)
        cb.record_failure()
        cb.record_failure()
        assert cb.state == CircuitState.CLOSED
        assert cb.allow_request() is True

    def test_trips_open_at_threshold(self):
        cb = _make(threshold=3)
        for _ in range(3):
            cb.record_failure()
        assert cb.state == CircuitState.OPEN
        assert cb.allow_request() is False

    def test_success_resets_failure_count(self):
        cb = _make(threshold=3)
        cb.record_failure()
        cb.record_failure()
        cb.record_success()
        # Counter should be reset, so 2 more failures shouldn't trip it
        cb.record_failure()
        cb.record_failure()
        assert cb.state == CircuitState.CLOSED

    def test_open_transitions_to_half_open_after_cooldown(self):
        cb = _make(threshold=2, cooldown=0.5)
        cb.record_failure()
        cb.record_failure()
        assert cb.state == CircuitState.OPEN

        # Simulate time passing
        cb._last_failure_time = time.monotonic() - 1.0
        assert cb.allow_request() is True
        assert cb.state == CircuitState.HALF_OPEN


class TestHalfOpen:
    def test_probe_success_closes_circuit(self):
        cb = _make(threshold=2, cooldown=0.1)
        cb.record_failure()
        cb.record_failure()
        cb._last_failure_time = time.monotonic() - 1.0
        cb.allow_request()  # triggers transition to HALF_OPEN
        assert cb.state == CircuitState.HALF_OPEN

        cb.record_success()
        assert cb.state == CircuitState.CLOSED
        assert cb.allow_request() is True

    def test_probe_failure_reopens_circuit(self):
        cb = _make(threshold=2, cooldown=0.1)
        cb.record_failure()
        cb.record_failure()
        cb._last_failure_time = time.monotonic() - 1.0
        cb.allow_request()  # transition to HALF_OPEN

        cb.record_failure()
        assert cb.state == CircuitState.OPEN
        assert cb.allow_request() is False

    def test_half_open_limits_probes(self):
        cb = _make(threshold=2, cooldown=0.1)
        cb.record_failure()
        cb.record_failure()
        cb._last_failure_time = time.monotonic() - 1.0

        assert cb.allow_request() is True  # first probe
        assert cb.allow_request() is False  # second probe blocked


class TestRemainingCooldown:
    def test_zero_when_closed(self):
        cb = _make()
        assert cb.remaining_cooldown() == 0.0

    def test_positive_when_open(self):
        cb = _make(threshold=1, cooldown=60.0)
        cb.record_failure()
        assert cb.state == CircuitState.OPEN
        remaining = cb.remaining_cooldown()
        assert 55.0 < remaining <= 60.0

    def test_zero_after_cooldown_elapsed(self):
        cb = _make(threshold=1, cooldown=1.0)
        cb.record_failure()
        cb._last_failure_time = time.monotonic() - 2.0
        assert cb.remaining_cooldown() == 0.0


class TestReset:
    def test_reset_clears_state(self):
        cb = _make(threshold=1)
        cb.record_failure()
        assert cb.state == CircuitState.OPEN
        cb.reset()
        assert cb.state == CircuitState.CLOSED
        assert cb.allow_request() is True


class TestCircuitOpenException:
    def test_exception_fields(self):
        exc = CircuitOpen("test_circuit", 42.5)
        assert exc.remaining_seconds == 42.5
        assert "test_circuit" in str(exc)
        assert "42" in str(exc)
