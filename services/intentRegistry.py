"""Intent + reservation registry (Phase 4).

Owns three things:

1. **Reservations** — quote (USDC) and token-share holds for in-flight intents,
   so two concurrent BUY/SELL intents on the same position cannot
   overspend / over-sell.
2. **Duplicate-intent blocking** — at most one *unresolved* intent per
   ``PositionScope`` may be active. A second submitter on the same scope
   raises :class:`DuplicateIntentError`.
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

from services.positionActionCoordinator import PositionScope


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
        # scope -> intent_id of the currently unresolved intent (at most one)
        self._scope_open: dict[tuple[str, str, str], str] = {}
        # scope -> aggregate reserved totals (always == sum of unresolved
        # reservations on that scope; under this design that's at most one
        # in-flight intent's reservation).
        self._reserved: dict[tuple[str, str, str], Reservation] = {}
        self._lock = asyncio.Lock()
        # Max retained resolved intents before pruning (memory leak guard)
        self._max_resolved = 10_000

    # --- introspection ------------------------------------------------------

    def open_intent_for(self, scope: PositionScope) -> Optional[IntentRecord]:
        """Return a **copy** of the open intent record (mutable escape protection)."""
        intent_id = self._scope_open.get(_scope_key(scope))
        if intent_id is None:
            return None
        record = self._intents.get(intent_id)
        return copy.copy(record) if record else None

    def get(self, intent_id: str) -> Optional[IntentRecord]:
        """Return a **copy** of the intent record (mutable escape protection)."""
        record = self._intents.get(intent_id)
        return copy.copy(record) if record else None

    def reserved_for(self, scope: PositionScope) -> Reservation:
        agg = self._reserved.get(_scope_key(scope))
        if agg is None:
            return Reservation()
        return Reservation(
            quote_usdc=agg.quote_usdc, token_shares=agg.token_shares
        )

    def open_intent_count(self) -> int:
        return len(self._scope_open)

    # --- lifecycle ----------------------------------------------------------

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
                        f"Unresolved intent {existing_id} already open on "
                        f"scope {key}"
                    )
                # Drop dangling pointer to a resolved intent.
                self._scope_open.pop(key, None)

            new_id = intent_id or uuid.uuid4().hex
            if new_id in self._intents:
                raise DuplicateIntentError(
                    f"Intent id {new_id} already exists in registry"
                )

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
                "intent.begin id=%s scope=%s side=%s reserved_quote=%s "
                "reserved_shares=%s",
                new_id,
                key,
                side.value,
                reservation.quote_usdc,
                reservation.token_shares,
            )
            return record

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
                    f"Intent {intent_id} already in terminal "
                    f"state {record.state.value}"
                )
            record.state = new_state
            record.updated_at = time.time()
            if venue_order_id is not None:
                record.venue_order_id = venue_order_id
            return record

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
                f"finalize_intent requires terminal state, got "
                f"{terminal_state.value}"
            )
        if not by_reconciler:
            raise IllegalTransitionError(
                "Only the reconciler may finalize an intent "
                "(set by_reconciler=True)"
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
            raise IllegalTransitionError(
                "release_on_cancel_or_fail only accepts CANCELLED/ERROR"
            )
        async with self._lock:
            return self._release_locked(intent_id, terminal_state)

    # --- internal -----------------------------------------------------------

    def _release_locked(
        self, intent_id: str, terminal_state: IntentState
    ) -> IntentRecord:
        record = self._intents.get(intent_id)
        if record is None:
            raise UnknownIntentError(intent_id)
        if record.is_resolved():
            return record

        record.state = terminal_state
        record.updated_at = time.time()

        key = _scope_key(record.scope)
        if self._scope_open.get(key) == intent_id:
            self._scope_open.pop(key, None)

        agg = self._reserved.get(key)
        if agg is not None:
            new_quote = agg.quote_usdc - record.reservation.quote_usdc
            new_shares = agg.token_shares - record.reservation.token_shares
            # Surface invariant violations — negative residuals indicate a bug
            if new_quote < 0 or new_shares < 0:
                logger.error(
                    "intent.reservation_negative_residual id=%s scope=%s "
                    "new_quote=%s new_shares=%s (reservation leak?)",
                    intent_id, key, new_quote, new_shares,
                )
            if new_quote == Decimal(0) and new_shares == Decimal(0):
                self._reserved.pop(key, None)
            else:
                self._reserved[key] = Reservation(
                    quote_usdc=max(Decimal(0), new_quote),
                    token_shares=max(Decimal(0), new_shares),
                )

        logger.info(
            "intent.release id=%s scope=%s state=%s released_quote=%s "
            "released_shares=%s",
            intent_id,
            key,
            terminal_state.value,
            record.reservation.quote_usdc,
            record.reservation.token_shares,
        )

        # Prune oldest resolved intents to prevent unbounded memory growth
        self._maybe_prune_resolved()

        return record

    def _maybe_prune_resolved(self) -> None:
        """Remove oldest resolved intents when the registry exceeds _max_resolved."""
        resolved = [
            (iid, rec)
            for iid, rec in self._intents.items()
            if rec.is_resolved()
        ]
        if len(resolved) <= self._max_resolved:
            return
        # Sort by updated_at ascending (oldest first) and remove excess
        resolved.sort(key=lambda r: r[1].updated_at)
        excess = len(resolved) - self._max_resolved
        for iid, _ in resolved[:excess]:
            del self._intents[iid]
        logger.debug("intent.prune removed=%d resolved_intents=%d", excess, self._max_resolved)
