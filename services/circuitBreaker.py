"""Simple circuit breaker for external API calls.

States:
  CLOSED   – normal operation, requests pass through.
  OPEN     – too many recent failures; requests are rejected immediately.
  HALF_OPEN – after cooldown, allow a single probe request.

Usage:
    cb = CircuitBreaker("clob_api", failure_threshold=5, cooldown_seconds=60)
    if not cb.allow_request():
        raise CircuitOpen(cb.name)
    try:
        result = call_external_api()
        cb.record_success()
    except SomeTransientError:
        cb.record_failure()
        raise
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum

from utils.logger import get_logger

logger = get_logger("CircuitBreaker")


class CircuitState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitOpen(Exception):
    """Raised when a circuit breaker is open and rejecting requests."""

    def __init__(self, name: str, remaining_seconds: float):
        self.name = name
        self.remaining_seconds = remaining_seconds
        super().__init__(f"Circuit '{name}' is OPEN — retry in {remaining_seconds:.0f}s")


@dataclass
class CircuitBreaker:
    name: str
    failure_threshold: int = 5
    cooldown_seconds: float = 60.0
    half_open_max_probes: int = 1

    _state: CircuitState = field(default=CircuitState.CLOSED, init=False)
    _failure_count: int = field(default=0, init=False)
    _last_failure_time: float = field(default=0.0, init=False)
    _half_open_probes: int = field(default=0, init=False)

    @property
    def state(self) -> CircuitState:
        if self._state == CircuitState.OPEN:
            elapsed = time.monotonic() - self._last_failure_time
            if elapsed >= self.cooldown_seconds:
                self._state = CircuitState.HALF_OPEN
                self._half_open_probes = 0
                logger.info(
                    "Circuit '%s' transitioning OPEN → HALF_OPEN after %.0fs cooldown",
                    self.name,
                    elapsed,
                )
        return self._state

    def allow_request(self) -> bool:
        s = self.state
        if s == CircuitState.CLOSED:
            return True
        if s == CircuitState.HALF_OPEN:
            if self._half_open_probes < self.half_open_max_probes:
                self._half_open_probes += 1
                return True
            return False
        return False

    def record_success(self) -> None:
        if self._state != CircuitState.CLOSED:
            logger.info("Circuit '%s' recovered → CLOSED", self.name)
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._half_open_probes = 0

    def record_failure(self) -> None:
        self._failure_count += 1
        self._last_failure_time = time.monotonic()

        if self._state == CircuitState.HALF_OPEN:
            self._state = CircuitState.OPEN
            logger.warning(
                "Circuit '%s' probe failed → back to OPEN (cooldown %.0fs)",
                self.name,
                self.cooldown_seconds,
            )
            return

        if self._failure_count >= self.failure_threshold:
            self._state = CircuitState.OPEN
            logger.warning(
                "Circuit '%s' tripped → OPEN after %d consecutive failures (cooldown %.0fs)",
                self.name,
                self._failure_count,
                self.cooldown_seconds,
            )

    def reset(self) -> None:
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._half_open_probes = 0
        self._last_failure_time = 0.0

    def remaining_cooldown(self) -> float:
        """Seconds remaining before next probe is allowed (0 if not OPEN)."""
        if self._state != CircuitState.OPEN:
            return 0.0
        elapsed = time.monotonic() - self._last_failure_time
        return max(0.0, self.cooldown_seconds - elapsed)
