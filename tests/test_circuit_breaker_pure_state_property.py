"""Regression: CircuitBreaker.state is a pure read.

Pre-fix: reading ``cb.state`` after cooldown elapsed mutated the
internal _state from OPEN → HALF_OPEN AND reset _half_open_probes = 0.
Any log or monitoring site that touched ``.state`` twice after cooldown
would reset the probe counter, letting more probes through than
half_open_max_probes intended.

Post-fix: ``.state`` is a pure read. The OPEN → HALF_OPEN transition
moves to a private ``_advance_state()`` called only by
``allow_request()`` — a single explicit decision point.
"""

from __future__ import annotations

from unittest.mock import patch


from pmcopy.services.circuitBreaker import CircuitBreaker, CircuitState


def test_reading_state_does_not_consume_probe_budget():
    cb = CircuitBreaker(
        name="api",
        failure_threshold=2,
        cooldown_seconds=10.0,
        half_open_max_probes=3,
    )
    # Trip the breaker.
    for _ in range(2):
        cb.record_failure()
    assert cb.state == CircuitState.OPEN

    # Fast-forward past the cooldown.
    with patch("pmcopy.services.circuitBreaker.time.monotonic", return_value=cb._last_failure_time + 11.0):
        # Read .state 100 times — pre-fix this would have reset the probe
        # counter 100 times.
        for _ in range(100):
            _ = cb.state

        # No transition yet — .state must be pure read.
        assert cb._state == CircuitState.OPEN, (
            f"pure .state read must not transition; got {cb._state}"
        )

        # First explicit allow_request triggers the transition + consumes one probe.
        assert cb.allow_request() is True
        assert cb._state == CircuitState.HALF_OPEN
        assert cb._half_open_probes == 1

        # Two more probes allowed, then exhausted.
        assert cb.allow_request() is True
        assert cb.allow_request() is True
        assert cb.allow_request() is False, "fourth probe must be denied"


def test_default_failure_threshold_lowered():
    """Defensive default for trading code — 30 was too permissive."""
    cb = CircuitBreaker(name="api")
    assert cb.failure_threshold <= 10, (
        f"trading-grade default must be <= 10; got {cb.failure_threshold}"
    )


def test_allow_request_after_cooldown_transitions():
    cb = CircuitBreaker(
        name="api",
        failure_threshold=1,
        cooldown_seconds=10.0,
    )
    cb.record_failure()
    assert cb._state == CircuitState.OPEN
    assert cb.allow_request() is False

    with patch("pmcopy.services.circuitBreaker.time.monotonic", return_value=cb._last_failure_time + 11.0):
        # allow_request should drive the transition and let the first probe through.
        assert cb.allow_request() is True
        assert cb._state == CircuitState.HALF_OPEN
