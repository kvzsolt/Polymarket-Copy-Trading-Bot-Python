"""Per-position coordinator with async locking and intent tracking.

``PositionCoordinator`` is the Phase-1 successor to
:class:`services.positionActionCoordinator.PositionActionCoordinator`.
It adds an **intent record** per position key so callers can track the
lifecycle state of the mutation they are serializing.

Migration note
--------------
The legacy ``PositionActionCoordinator`` and ``PositionScope`` remain
functional.  This module is additive.  The eventual Phase-2 work will
wire ``TradingContext`` to use ``PositionCoordinator`` and deprecate the
old one.

Usage
-----
::

    coord = PositionCoordinator()
    key = PositionKey(user="0xabc", condition_id="cid", token_id="tid")

    async with coord.acquire(key) as intent:
        intent.transition(IntentState.INTENT_PENDING)
        intent.side = "BUY"
        intent.size = Decimal("5.0")
        # ... submit order ...
        intent.transition(IntentState.SUBMITTED)
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict

from domain.intent import IntentRecord, IntentState
from domain.position_key import PositionKey

logger = logging.getLogger(__name__)


class PositionCoordinator:
    """Serialize position mutations and track per-position intent state.

    One instance per ``TradingContext`` (i.e. per user).

    Thread-safety
    ~~~~~~~~~~~~~
    This class is designed for ``asyncio`` cooperative concurrency within a
    single thread.  All mutable state is accessed under the per-key
    ``asyncio.Lock``, so concurrent tasks cannot corrupt the intent record
    for any given key.
    """

    def __init__(self) -> None:
        # defaultdict(asyncio.Lock) creates a lock on first access per key
        self._locks: dict[PositionKey, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._intents: dict[PositionKey, IntentRecord] = {}
        self._prune_threshold = 128

    @staticmethod
    def _has_waiters(lock: asyncio.Lock) -> bool:
        waiters = getattr(lock, "_waiters", None)
        if not waiters:
            return False
        return any(not waiter.cancelled() for waiter in waiters)

    # ---- public API ---------------------------------------------------------

    def acquire(self, key: PositionKey) -> _AcquireContext:
        """Return an async context manager that holds the per-key lock.

        Inside the ``async with`` block, the caller receives the
        ``IntentRecord`` for this key and may drive state transitions on it.
        """
        return _AcquireContext(self, key)

    def get_lock(self, key: PositionKey) -> asyncio.Lock:
        """Return (creating if needed) the per-key ``asyncio.Lock``.

        Prefer :meth:`acquire` for new code.  ``get_lock`` is exposed for
        compatibility with call sites that manage the lock manually.
        """
        lock = self._locks[key]
        self._maybe_prune()
        return lock

    def get_intent(self, key: PositionKey) -> IntentRecord:
        """Return (creating if needed) the per-key ``IntentRecord``."""
        intent = self._intents.get(key)
        if intent is None:
            intent = IntentRecord()
            self._intents[key] = intent
        return intent

    def snapshot_intents(self) -> dict[PositionKey, IntentRecord]:
        """Return a shallow copy of the current intent registry."""
        return dict(self._intents)

    # ---- introspection (tests / diagnostics) --------------------------------

    def scope_count(self) -> int:
        """Number of keys that have a lock in the registry."""
        return len(self._locks)

    def intent_count(self) -> int:
        """Number of keys that have an intent record."""
        return len(self._intents)

    def active_intents(self) -> list[tuple[PositionKey, IntentRecord]]:
        """Return all intents currently in an active (in-flight) state."""
        return [
            (key, rec) for key, rec in self._intents.items() if rec.is_active
        ]

    def is_locked(self, key: PositionKey) -> bool:
        """True if the per-key lock is currently held."""
        lock = self._locks.get(key)
        return bool(lock and lock.locked())

    def locks_held(self) -> int:
        """Number of keys whose lock is currently held."""
        return sum(1 for lock in self._locks.values() if lock.locked())

    # ---- cleanup ------------------------------------------------------------

    def release_key(self, key: PositionKey) -> bool:
        """Drop an idle key's lock and intent if it is safe to do so.

        Returns True if the key was successfully removed.
        """
        lock = self._locks.get(key)
        if lock is not None and (lock.locked() or self._has_waiters(lock)):
            return False
        intent = self._intents.get(key)
        if intent is not None and intent.is_active:
            return False
        self._locks.pop(key, None)
        if intent is not None:
            self._intents.pop(key, None)
        return True

    def prune(self) -> int:
        """Remove all idle keys.  Returns count removed."""
        removed = 0
        for key in list(self._locks.keys()):
            if self.release_key(key):
                removed += 1
        return removed

    def _maybe_prune(self) -> None:
        if len(self._locks) > self._prune_threshold:
            self.prune()


class _AcquireContext:
    """Async context manager for :meth:`PositionCoordinator.acquire`."""

    __slots__ = ("_coordinator", "_key")

    def __init__(self, coordinator: PositionCoordinator, key: PositionKey) -> None:
        self._coordinator = coordinator
        self._key = key

    async def __aenter__(self) -> IntentRecord:
        lock = self._coordinator.get_lock(self._key)
        await lock.acquire()
        intent = self._coordinator.get_intent(self._key)
        logger.debug(
            "PositionCoordinator: acquired key=%s state=%s",
            self._key,
            intent.state.value,
        )
        return intent

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:  # type: ignore[override]
        lock = self._coordinator._locks.get(self._key)
        if lock is not None and lock.locked():
            lock.release()
        self._coordinator.release_key(self._key)
        logger.debug(
            "PositionCoordinator: released key=%s",
            self._key,
        )
        return None
