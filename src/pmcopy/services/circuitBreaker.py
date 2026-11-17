"""Venue circuit breaker with explicit composition-root lifecycle ownership.

States:
  CLOSED    – normal operation, requests pass through.
  OPEN      – too many recent failures; requests are rejected immediately.
  HALF_OPEN – after cooldown, allow bounded probe requests.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum

from pmcopy.utils.logger import get_logger

logger = get_logger("CircuitBreaker")


class CircuitState(str, Enum):
    """Lifecycle states of a :class:`CircuitBreaker`."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitOpen(Exception):
    """Raised when a circuit breaker is open and rejecting requests."""

    def __init__(self, name: str, remaining_seconds: float) -> None:
        self.name = name
        self.remaining_seconds = remaining_seconds
        super().__init__(f"Circuit '{name}' is OPEN — retry in {remaining_seconds:.0f}s")


@dataclass
class CircuitBreaker:
    """In-memory failure-threshold circuit breaker with optional state observer."""

    name: str
    failure_threshold: int = 5
    cooldown_seconds: float = 60.0
    half_open_max_probes: int = 5
    on_state_change: Callable[[CircuitState], None] | None = field(
        default=None, repr=False, compare=False
    )

    _state: CircuitState = field(default=CircuitState.CLOSED, init=False)
    _failure_count: int = field(default=0, init=False)
    _last_failure_time: float = field(default=0.0, init=False)
    _half_open_probes: int = field(default=0, init=False)

    @classmethod
    def for_venue_config(
        cls,
        config: object,
        *,
        name: str = "clob_api",
        cooldown_seconds: float = 60.0,
        on_state_change: Callable[[CircuitState], None] | None = None,
    ) -> CircuitBreaker:
        """Construct a venue breaker honoring ``config.execution.circuit_failure_threshold``."""
        configured_threshold = getattr(
            getattr(config, "execution", None),
            "circuit_failure_threshold",
            None,
        )
        threshold = (
            configured_threshold
            if isinstance(configured_threshold, int)
            and not isinstance(configured_threshold, bool)
            and configured_threshold > 0
            else 5
        )
        return cls(
            name=name,
            failure_threshold=threshold,
            cooldown_seconds=cooldown_seconds,
            on_state_change=on_state_change,
        )

    @property
    def state(self) -> CircuitState:
        """Pure state read without mutating cooldown transitions."""
        return self._state

    def _set_state(self, new_state: CircuitState) -> None:
        previous = self._state
        self._state = new_state
        if previous is not new_state and self.on_state_change is not None:
            try:
                self.on_state_change(new_state)
            except Exception as exc:
                logger.debug("circuit_state_observer_failed | name=%s error=%r", self.name, exc)

    def _advance_state(self) -> None:
        """Transition ``OPEN -> HALF_OPEN`` once cooldown has elapsed."""
        if self._state == CircuitState.OPEN:
            elapsed = time.monotonic() - self._last_failure_time
            if elapsed >= self.cooldown_seconds:
                self._half_open_probes = 0
                self._set_state(CircuitState.HALF_OPEN)
                logger.info(
                    "Circuit '%s' transitioning OPEN → HALF_OPEN after %.0fs cooldown",
                    self.name,
                    elapsed,
                )

    def allow_request(self) -> bool:
        """Return True when a request is permitted to proceed."""
        self._advance_state()
        s = self._state
        if s == CircuitState.CLOSED:
            return True
        if s == CircuitState.HALF_OPEN:
            if self._half_open_probes < self.half_open_max_probes:
                self._half_open_probes += 1
                return True
            return False
        return False

    def record_success(self) -> None:
        """Reset failure counters and return the breaker to ``CLOSED``."""
        if self._state != CircuitState.CLOSED:
            logger.info("Circuit '%s' recovered → CLOSED", self.name)
        self._failure_count = 0
        self._half_open_probes = 0
        self._set_state(CircuitState.CLOSED)

    def record_failure(self) -> None:
        """Increment failure count and trip ``OPEN`` when threshold is reached."""
        self._failure_count += 1
        self._last_failure_time = time.monotonic()

        if self._state == CircuitState.HALF_OPEN:
            self._set_state(CircuitState.OPEN)
            logger.warning(
                "Circuit '%s' probe failed → back to OPEN (cooldown %.0fs)",
                self.name,
                self.cooldown_seconds,
            )
            return

        if self._failure_count >= self.failure_threshold:
            self._set_state(CircuitState.OPEN)
            logger.warning(
                "Circuit '%s' tripped → OPEN after %d consecutive failures (cooldown %.0fs)",
                self.name,
                self._failure_count,
                self.cooldown_seconds,
            )

    def reset(self) -> None:
        """Reset breaker counters and state back to ``CLOSED``."""
        self._failure_count = 0
        self._half_open_probes = 0
        self._last_failure_time = 0.0
        self._set_state(CircuitState.CLOSED)

    def remaining_cooldown(self) -> float:
        """Seconds remaining before next probe is allowed (0 if not OPEN)."""
        if self._state != CircuitState.OPEN:
            return 0.0
        elapsed = time.monotonic() - self._last_failure_time
        return max(0.0, self.cooldown_seconds - elapsed)
