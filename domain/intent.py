"""Intent lifecycle state machine and record for position coordination.

``IntentState`` encodes where a position-mutation intent sits in its
lifecycle.  ``IntentRecord`` is the mutable companion that tracks the
current state, timestamps, and an optional error message.

The state machine enforced by ``IntentRecord.transition`` is::

    IDLE ──► INTENT_PENDING ──► SUBMITTED ──► ACK_LIVE ──► PARTIAL ──► FILLED ──► CONFIRMING ──► DONE
                │                   │            │            │           │            │
                ▼                   ▼            ▼            ▼           ▼            ▼
             CANCELLED           ERROR        ERROR        ERROR       ERROR        ERROR

``IDLE`` is the terminal rest state after ``DONE`` or a fresh start.
"""

from __future__ import annotations

import enum
import time
from dataclasses import dataclass, field
from decimal import Decimal


class IntentState(enum.Enum):
    """Lifecycle states for a position-mutation intent."""

    IDLE = "IDLE"
    INTENT_PENDING = "INTENT_PENDING"
    SUBMITTED = "SUBMITTED"
    ACK_LIVE = "ACK_LIVE"
    PARTIAL = "PARTIAL"
    FILLED = "FILLED"
    CONFIRMING = "CONFIRMING"
    DONE = "DONE"
    ERROR = "ERROR"
    CANCELLED = "CANCELLED"


# Valid transitions: from_state -> set of allowed to_states
_TRANSITIONS: dict[IntentState, frozenset[IntentState]] = {
    IntentState.IDLE: frozenset({IntentState.INTENT_PENDING}),
    IntentState.INTENT_PENDING: frozenset({
        IntentState.SUBMITTED,
        IntentState.CANCELLED,
        IntentState.ERROR,
    }),
    IntentState.SUBMITTED: frozenset({
        IntentState.ACK_LIVE,
        IntentState.FILLED,  # immediate fill
        IntentState.ERROR,
        IntentState.CANCELLED,
    }),
    IntentState.ACK_LIVE: frozenset({
        IntentState.PARTIAL,
        IntentState.FILLED,
        IntentState.ERROR,
        IntentState.CANCELLED,
    }),
    IntentState.PARTIAL: frozenset({
        IntentState.PARTIAL,  # progressive partial fills
        IntentState.FILLED,
        IntentState.ERROR,
        IntentState.CANCELLED,
    }),
    IntentState.FILLED: frozenset({
        IntentState.CONFIRMING,
        IntentState.ERROR,
    }),
    IntentState.CONFIRMING: frozenset({
        IntentState.DONE,
        IntentState.ERROR,
    }),
    IntentState.DONE: frozenset({IntentState.IDLE}),
    IntentState.ERROR: frozenset({IntentState.IDLE}),
    IntentState.CANCELLED: frozenset({IntentState.IDLE}),
}


class InvalidTransition(ValueError):
    """Raised when an intent state transition is not allowed."""

    def __init__(self, from_state: IntentState, to_state: IntentState) -> None:
        super().__init__(
            f"Invalid intent transition: {from_state.value} -> {to_state.value}"
        )
        self.from_state = from_state
        self.to_state = to_state


@dataclass
class IntentRecord:
    """Mutable record tracking the current lifecycle state of an intent.

    Each :class:`~domain.position_key.PositionKey` gets at most one
    ``IntentRecord`` in the coordinator at any time.
    """

    state: IntentState = IntentState.IDLE
    order_id: str | None = None
    side: str | None = None
    size: Decimal = Decimal(0)
    price: Decimal = Decimal(0)
    filled_size: Decimal = Decimal(0)
    error: str | None = None
    created_at: float = field(default_factory=time.monotonic)
    updated_at: float = field(default_factory=time.monotonic)

    # ---- state machine helpers ----

    def transition(self, to: IntentState) -> None:
        """Move to *to* if the transition is valid, else raise.

        Raises
        ------
        InvalidTransition
            If the requested transition is not in the allowed graph.
        """
        allowed = _TRANSITIONS.get(self.state, frozenset())
        if to not in allowed:
            raise InvalidTransition(self.state, to)
        self.state = to
        self.updated_at = time.monotonic()

    def reset(self) -> None:
        """Return the record to ``IDLE`` and clear transient fields."""
        self.state = IntentState.IDLE
        self.order_id = None
        self.side = None
        self.size = Decimal(0)
        self.price = Decimal(0)
        self.filled_size = Decimal(0)
        self.error = None
        self.updated_at = time.monotonic()

    @property
    def is_terminal(self) -> bool:
        """True if the intent is in a terminal state (DONE, ERROR, CANCELLED)."""
        return self.state in (
            IntentState.DONE,
            IntentState.ERROR,
            IntentState.CANCELLED,
        )

    @property
    def is_active(self) -> bool:
        """True if the intent is in-flight (not IDLE, not terminal)."""
        return self.state not in (
            IntentState.IDLE,
            IntentState.DONE,
            IntentState.ERROR,
            IntentState.CANCELLED,
        )
