"""Intent + reservation registry (Phase 4).

Owns three things:

1. **Reservations** — quote (USDC) and token-share holds for in-flight intents,
   so two concurrent BUY/SELL intents on the same position cannot
   overspend / over-sell.
2. **Duplicate-intent blocking** — a new submitter is blocked while any
   unresolved intent owns the ``PositionScope``. Restart can restore several
   already-owned durable task allocations on that scope.
3. **Lifecycle ownership** — ``finalize_intent`` is the **only** path that
   moves an intent into a terminal state (``DONE`` / ``ERROR`` /
   ``CANCELLED``). Submitters may call ``transition`` for non-terminal
   moves; the reconciler is the sole writer of terminal transitions.

This module is process-local. Crashed runtimes start fresh — durable state
lives in the orders/positions repos.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
import copy
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Optional

from pmcopy.services.positionActionCoordinator import PositionScope


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Lifecycle states (from Phase 1 prompt)
# ---------------------------------------------------------------------------


class IntentState(str, Enum):
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


TERMINAL_STATES: frozenset[IntentState] = frozenset(
    {IntentState.DONE, IntentState.ERROR, IntentState.CANCELLED}
)

# Submitters may move an intent forward through these non-terminal states.
SUBMITTER_ALLOWED_TRANSITIONS: frozenset[IntentState] = frozenset(
    {
        IntentState.SUBMITTED,
        IntentState.ACK_LIVE,
        IntentState.PARTIAL,
        IntentState.FILLED,
        IntentState.CONFIRMING,
    }
)

# Per-state forward DAG. Submitter transitions may only move forward through
# the lifecycle: IDLE → INTENT_PENDING → SUBMITTED → ACK_LIVE → PARTIAL →
# FILLED → CONFIRMING → (DONE via reconciler). Forward leaps are permitted
# (e.g. SUBMITTED → FILLED on synchronous fill, INTENT_PENDING → CONFIRMING
# on crash recovery); going backward is not. PARTIAL may repeat itself for
# successive partial fills.
SUBMITTER_FORWARD_DAG: dict[IntentState, frozenset[IntentState]] = {
    IntentState.IDLE: frozenset(
        {
            IntentState.INTENT_PENDING,
            IntentState.SUBMITTED,
            IntentState.ACK_LIVE,
            IntentState.PARTIAL,
            IntentState.FILLED,
            IntentState.CONFIRMING,
        }
    ),
    IntentState.INTENT_PENDING: frozenset(
        {
            IntentState.SUBMITTED,
            IntentState.ACK_LIVE,
            IntentState.PARTIAL,
            IntentState.FILLED,
            IntentState.CONFIRMING,
        }
    ),
    IntentState.SUBMITTED: frozenset(
        {
            IntentState.ACK_LIVE,
            IntentState.PARTIAL,
            IntentState.FILLED,
            IntentState.CONFIRMING,
        }
    ),
    IntentState.ACK_LIVE: frozenset(
        {IntentState.PARTIAL, IntentState.FILLED, IntentState.CONFIRMING}
    ),
    IntentState.PARTIAL: frozenset(
        {IntentState.PARTIAL, IntentState.FILLED, IntentState.CONFIRMING}
    ),
    IntentState.FILLED: frozenset({IntentState.CONFIRMING}),
    IntentState.CONFIRMING: frozenset(),
}


class IntentSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


# ---------------------------------------------------------------------------
# Reservation
# ---------------------------------------------------------------------------


@dataclass
class Reservation:
    """USDC quote + token-share hold for one in-flight intent."""

    quote_usdc: Decimal = Decimal(0)
    token_shares: Decimal = Decimal(0)

    def is_zero(self) -> bool:
        return self.quote_usdc == 0 and self.token_shares == 0


# ---------------------------------------------------------------------------
# Intent record
# ---------------------------------------------------------------------------


@dataclass
class IntentRecord:
    intent_id: str
    scope: PositionScope
    side: IntentSide
    reservation: Reservation
    state: IntentState = IntentState.INTENT_PENDING
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    venue_order_id: Optional[str] = None

    def is_resolved(self) -> bool:
        return self.state in TERMINAL_STATES


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class DuplicateIntentError(Exception):
    """Raised when a second unresolved intent is opened on the same scope."""


class UnknownIntentError(Exception):
    """Raised when an intent_id is not in the registry."""


class IllegalTransitionError(Exception):
    """Raised on disallowed lifecycle transition."""


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


def _scope_key(scope: PositionScope) -> tuple[str, str, str]:
    return (scope.user_id, scope.condition_id, scope.token_id)


class IntentRegistry:
    """In-memory registry of open intents + reservations.

    All public methods are coroutine-safe via an internal asyncio.Lock;
    callers should still serialize per-scope mutation through the position
    action lock so reservation math stays consistent with the rest of the
    runtime.
    """

    def __init__(self) -> None:
        self._intents: dict[str, IntentRecord] = {}
        # scope -> one unresolved intent; restored scopes can have more than one
        self._scope_open: dict[tuple[str, str, str], str] = {}
        # scope -> aggregate reserved totals (always == sum of unresolved
        # reservations on that scope, including every restored allocation).
        self._reserved: dict[tuple[str, str, str], Reservation] = {}
        self._lock = asyncio.Lock()
        # Max retained resolved intents before pruning (memory leak guard)
        self._max_resolved = 10_000

    # --- introspection ------------------------------------------------------

    def open_intent_for(self, scope: PositionScope) -> Optional[IntentRecord]:
        """Return a **deep copy** of the open intent record (mutable escape protection)."""
        intent_id = self._scope_open.get(_scope_key(scope))
        if intent_id is None:
            return None
        record = self._intents.get(intent_id)
        return copy.deepcopy(record) if record else None

    def get(self, intent_id: str) -> Optional[IntentRecord]:
        """Return a **deep copy** of the intent record (mutable escape protection)."""
        record = self._intents.get(intent_id)
        return copy.deepcopy(record) if record else None

    def reserved_for(self, scope: PositionScope) -> Reservation:
        agg = self._reserved.get(_scope_key(scope))
        if agg is None:
            return Reservation()
        return Reservation(quote_usdc=agg.quote_usdc, token_shares=agg.token_shares)

    def open_intent_count(self) -> int:
        return len(self._scope_open)

    # --- lifecycle ----------------------------------------------------------

    async def restore_intent(self, record: IntentRecord) -> None:
        """Restore an already-owned durable allocation without admitting new risk.

        Several tasks can own distinct durable allocations on one scope.
        Normal ``begin_intent`` admission stays blocked until all are resolved.
        Replaying the same identity and financial evidence is idempotent.

        Args:
            record: Explicit durable identity, state, timestamps and reservation.

        Raises:
            ValueError: For invalid amounts, terminal state or conflicting evidence.
        """
        if not record.intent_id or record.is_resolved():
            raise ValueError("Recovery requires an unresolved durable intent")
        for value in (record.reservation.quote_usdc, record.reservation.token_shares):
            if not isinstance(value, Decimal) or not value.is_finite() or value < 0:
                raise ValueError("Recovery reservations must be finite non-negative decimals")
        async with self._lock:
            existing = self._intents.get(record.intent_id)
            if existing is not None:
                if (
                    existing.scope != record.scope
                    or existing.side != record.side
                    or existing.reservation != record.reservation
                    or existing.venue_order_id != record.venue_order_id
                    or existing.is_resolved()
                ):
                    raise ValueError("Recovered intent conflicts with existing ownership")
                return
            stored = copy.deepcopy(record)
            self._intents[record.intent_id] = stored
            key = _scope_key(record.scope)
            self._scope_open.setdefault(key, record.intent_id)
            aggregate = self._reserved.get(key, Reservation())
            self._reserved[key] = Reservation(
                quote_usdc=aggregate.quote_usdc + record.reservation.quote_usdc,
                token_shares=aggregate.token_shares + record.reservation.token_shares,
            )

    async def begin_intent(
        self,
        scope: PositionScope,
        side: IntentSide,
        reservation: Reservation,
        intent_id: Optional[str] = None,
    ) -> IntentRecord:
        """Register a new unresolved intent + reserve its quote/shares.

        Raises :class:`DuplicateIntentError` if an unresolved intent already
        exists for the same scope.
        """
        if reservation.quote_usdc < 0 or reservation.token_shares < 0:
            raise ValueError("Reservation amounts must be non-negative")

        async with self._lock:
            key = _scope_key(scope)
            existing_id = self._scope_open.get(key)
            if existing_id is not None:
                existing = self._intents.get(existing_id)
                if existing is not None and not existing.is_resolved():
                    raise DuplicateIntentError(
                        f"Unresolved intent {existing_id} already open on scope {key}"
                    )
                # Drop dangling pointer to a resolved intent.
                self._scope_open.pop(key, None)

            new_id = intent_id or uuid.uuid4().hex
            if new_id in self._intents:
                raise DuplicateIntentError(f"Intent id {new_id} already exists in registry")

            record = IntentRecord(
                intent_id=new_id,
                scope=scope,
                side=side,
                reservation=Reservation(
                    quote_usdc=reservation.quote_usdc,
                    token_shares=reservation.token_shares,
                ),
            )
            self._intents[new_id] = record
            self._scope_open[key] = new_id
            agg = self._reserved.get(key, Reservation())
            self._reserved[key] = Reservation(
                quote_usdc=agg.quote_usdc + reservation.quote_usdc,
                token_shares=agg.token_shares + reservation.token_shares,
            )
            logger.info(
                "intent.begin id=%s side=%s reserved_quote=%s reserved_shares=%s",
                new_id,
                side.value,
                reservation.quote_usdc,
                reservation.token_shares,
                extra=scope.log_fields(),
            )
            # Mutable-escape protection (mirror get() and open_intent_for()):
            # the live ``record`` is held inside ``self._intents`` for
            # subsequent state transitions, so returning the same object
            # to the caller would let it mutate state outside the lock.
            return copy.deepcopy(record)

    async def transition(
        self,
        intent_id: str,
        new_state: IntentState,
        venue_order_id: Optional[str] = None,
    ) -> IntentRecord:
        """Submitter-side non-terminal transition.

        Terminal transitions (DONE/ERROR/CANCELLED) must go through
        :meth:`finalize_intent` so the reconciler stays the sole final
        writer.
        """
        if new_state in TERMINAL_STATES:
            raise IllegalTransitionError(
                f"Terminal state {new_state.value} must be set via "
                f"finalize_intent (reconciler-owned)"
            )
        if new_state not in SUBMITTER_ALLOWED_TRANSITIONS:
            raise IllegalTransitionError(
                f"State {new_state.value} not a submitter-allowed transition"
            )

        async with self._lock:
            record = self._intents.get(intent_id)
            if record is None:
                raise UnknownIntentError(intent_id)
            if record.is_resolved():
                raise IllegalTransitionError(
                    f"Intent {intent_id} already in terminal state {record.state.value}"
                )
            allowed_next = SUBMITTER_FORWARD_DAG.get(record.state, frozenset())
            if new_state not in allowed_next:
                raise IllegalTransitionError(
                    f"Illegal forward transition {record.state.value} -> "
                    f"{new_state.value} (allowed: "
                    f"{sorted(s.value for s in allowed_next)})"
                )
            record.state = new_state
            record.updated_at = time.time()
            if venue_order_id is not None:
                record.venue_order_id = venue_order_id
            return copy.deepcopy(record)

    async def finalize_intent(
        self,
        intent_id: str,
        terminal_state: IntentState,
        *,
        by_reconciler: bool = False,
    ) -> IntentRecord:
        """Reconciler-only path to a terminal state.

        Releases the intent's reservation and clears the per-scope open
        pointer. ``by_reconciler`` must be True; the explicit flag is the
        ownership marker the Phase 4 spec requires.
        """
        if terminal_state not in TERMINAL_STATES:
            raise IllegalTransitionError(
                f"finalize_intent requires terminal state, got {terminal_state.value}"
            )
        if not by_reconciler:
            raise IllegalTransitionError(
                "Only the reconciler may finalize an intent (set by_reconciler=True)"
            )

        async with self._lock:
            return self._release_locked(intent_id, terminal_state)

    async def release_on_cancel_or_fail(
        self,
        intent_id: str,
        terminal_state: IntentState = IntentState.CANCELLED,
    ) -> IntentRecord:
        """Cancel/fail path used by submitter-side error handling.

        Releases the reservation immediately so retries are not starved by
        a stale hold. The reconciler may still observe the order downstream
        but will see a resolved record.
        """
        if terminal_state not in (IntentState.CANCELLED, IntentState.ERROR):
            raise IllegalTransitionError("release_on_cancel_or_fail only accepts CANCELLED/ERROR")
        async with self._lock:
            return self._release_locked(intent_id, terminal_state)

    # --- internal -----------------------------------------------------------

    def _release_locked(self, intent_id: str, terminal_state: IntentState) -> IntentRecord:
        record = self._intents.get(intent_id)
        if record is None:
            raise UnknownIntentError(intent_id)
        if record.is_resolved():
            return copy.deepcopy(record)

        record.state = terminal_state
        record.updated_at = time.time()

        key = _scope_key(record.scope)
        if self._scope_open.get(key) == intent_id:
            self._scope_open.pop(key, None)
            for other in self._intents.values():
                if not other.is_resolved() and _scope_key(other.scope) == key:
                    self._scope_open[key] = other.intent_id
                    break

        agg = self._reserved.get(key)
        if agg is not None:
            new_quote = agg.quote_usdc - record.reservation.quote_usdc
            new_shares = agg.token_shares - record.reservation.token_shares
            # Surface invariant violations — negative residuals indicate a bug
            if new_quote < 0 or new_shares < 0:
                logger.error(
                    "intent.reservation_negative_residual id=%s "
                    "new_quote=%s new_shares=%s (reservation leak?)",
                    intent_id,
                    new_quote,
                    new_shares,
                    extra=record.scope.log_fields(),
                )
            if new_quote == Decimal(0) and new_shares == Decimal(0):
                self._reserved.pop(key, None)
            else:
                self._reserved[key] = Reservation(
                    quote_usdc=max(Decimal(0), new_quote),
                    token_shares=max(Decimal(0), new_shares),
                )

        logger.info(
            "intent.release id=%s state=%s released_quote=%s released_shares=%s",
            intent_id,
            terminal_state.value,
            record.reservation.quote_usdc,
            record.reservation.token_shares,
            extra=record.scope.log_fields(),
        )

        # Prune oldest resolved intents to prevent unbounded memory growth
        self._maybe_prune_resolved()

        return copy.deepcopy(record)

    def _maybe_prune_resolved(self) -> None:
        """Remove oldest resolved intents when the registry exceeds _max_resolved."""
        resolved = [(iid, rec) for iid, rec in self._intents.items() if rec.is_resolved()]
        if len(resolved) <= self._max_resolved:
            return
        # Sort by updated_at ascending (oldest first) and remove excess
        resolved.sort(key=lambda r: r[1].updated_at)
        excess = len(resolved) - self._max_resolved
        for iid, _ in resolved[:excess]:
            del self._intents[iid]
        logger.debug("intent.prune removed=%d resolved_intents=%d", excess, self._max_resolved)
