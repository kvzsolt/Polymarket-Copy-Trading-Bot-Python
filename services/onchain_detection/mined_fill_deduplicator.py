"""Mined-fill deduplicator keyed on ``(tx_hash, log_index)`` (Phase 9).

A single Polymarket settlement transaction can emit multiple ``OrderFilled``
logs (one per matched maker order, plus the taker fill). ``tx_hash`` alone is
therefore an insufficient dedupe key: two distinct fills in the same tx would
collapse to one. The authoritative key is the composite ``(tx_hash, log_index)``.

Pending signals (``log_index is None``) are tracked separately so a later
mined signal with the same ``tx_hash`` can supersede them without being
treated as a duplicate of a *different* fill in the same tx. Pending signals
are keyed on ``(tx_hash, leader_wallet)`` — not ``tx_hash`` alone — because a
single pending ``matchOrders`` transaction can contain multiple watched-wallet
matches (e.g. taker = walletA, maker = walletB), and each match is a distinct
leader fill that must be copied independently (Greptile P1 re-review:
"Pending matches are dropped").

Both key sets are bounded FIFO caches. A long-lived process — especially in
``detection.mode=on_chain_pending``, where every watched mempool sighting
records a pending key — otherwise grows them without limit for the lifetime of
the run.
"""

from __future__ import annotations

import asyncio
from collections import OrderedDict
from collections.abc import Iterable
from typing import TypeVar

#: Either dedupe key shape: mined ``(tx_hash, log_index)`` or pending
#: ``(tx_hash, leader_wallet)``.
_KeyT = TypeVar("_KeyT", tuple[str, int], tuple[str, str])

#: Pending keys retained before the oldest is evicted. A pending key only has
#: to outlive the window between the mempool sighting and its mined fill
#: (seconds), so this is several orders of magnitude of headroom.
DEFAULT_MAX_PENDING = 10_000
#: Mined keys retained before the oldest is evicted. Sized above the pending
#: ceiling because one settlement tx contributes one key per ``OrderFilled``
#: log, and replay/backfill batches arrive in bursts.
DEFAULT_MAX_MINED = 50_000


class MinedFillDeduplicator:
    """Thread-safe (asyncio) dedupe of mined fills by ``(tx_hash, log_index)``.

    Also tracks provisional pending signals by ``(tx_hash, leader_wallet)``
    so the runtime can mark them superseded when the authoritative mined
    fill for the *same leader* arrives. A pending signal is never deduped
    against a mined fill (different key shape) — it is *superseded* by the
    mined fill that shares its ``(tx_hash, leader_wallet)``.

    Both sets are bounded: inserting past a ceiling evicts the oldest key by
    insertion order (FIFO, not LRU — a repeat hit does not extend a key's
    life). Eviction is silent and only reachable after tens of thousands of
    distinct keys, by which point the evicted transaction is long settled;
    the trade-off is that a replay older than the window is no longer
    recognised as a duplicate.
    """

    def __init__(
        self,
        *,
        max_pending: int = DEFAULT_MAX_PENDING,
        max_mined: int = DEFAULT_MAX_MINED,
    ) -> None:
        """Create an empty deduplicator with the given retention ceilings.

        Args:
            max_pending: Maximum retained ``(tx_hash, leader_wallet)`` keys.
            max_mined: Maximum retained ``(tx_hash, log_index)`` keys.

        Raises:
            ValueError: If either ceiling is not strictly positive. A zero
                ceiling would evict every key on insert, silently disabling
                deduplication instead of bounding it.
        """
        if max_pending <= 0:
            raise ValueError("max_pending must be > 0.")
        if max_mined <= 0:
            raise ValueError("max_mined must be > 0.")
        self._lock = asyncio.Lock()
        self._max_pending = int(max_pending)
        self._max_mined = int(max_mined)
        # ``OrderedDict`` used as an insertion-ordered set: O(1) membership
        # plus O(1) oldest-key eviction, which a plain ``set`` cannot give.
        self._mined_seen: OrderedDict[tuple[str, int], None] = OrderedDict()
        self._pending_seen: OrderedDict[tuple[str, str], None] = OrderedDict()

    @property
    def max_pending(self) -> int:
        """Return the retained-pending-key ceiling."""
        return self._max_pending

    @property
    def max_mined(self) -> int:
        """Return the retained-mined-key ceiling."""
        return self._max_mined

    async def is_duplicate_mined(self, tx_hash: str, log_index: int) -> bool:
        """Return True if this mined fill was already recorded."""
        if not tx_hash or log_index < 0:
            return False
        key = (tx_hash, log_index)
        async with self._lock:
            if key in self._mined_seen:
                return True
            self._record_mined_locked(key)
            return False

    async def record_pending(self, tx_hash: str, leader_wallet: str) -> bool:
        """Record a pending signal by ``(tx_hash, leader_wallet)``.

        Returns True if this pending signal is new (not a duplicate pending
        for the same leader in the same tx). Returns False if a pending
        signal for this ``(tx_hash, leader_wallet)`` was already recorded —
        the caller should not double-dispatch.
        """
        if not tx_hash or not leader_wallet:
            return False
        key = (tx_hash, leader_wallet.lower())
        async with self._lock:
            if key in self._pending_seen:
                return False
            self._pending_seen[key] = None
            self._evict_oldest(self._pending_seen, self._max_pending)
            return True

    async def supersede_pending(self, tx_hash: str, leader_wallet: str) -> bool:
        """Mark a pending signal superseded by a mined fill for the same leader.

        Returns True if a pending signal existed for this
        ``(tx_hash, leader_wallet)`` (caller should suppress the pending
        dispatch and let the mined signal be the authoritative one).
        Returns False if no pending signal was recorded for this
        ``(tx_hash, leader_wallet)`` (the mined signal is standalone, the
        pending was for a *different* leader in the same tx — which must not
        be suppressed — or the pending key has aged out of the window).
        """
        if not tx_hash or not leader_wallet:
            return False
        key = (tx_hash, leader_wallet.lower())
        async with self._lock:
            if key in self._pending_seen:
                del self._pending_seen[key]
                return True
            return False

    async def pending_known(self, tx_hash: str, leader_wallet: str) -> bool:
        """Check whether a pending signal for this leader is already recorded."""
        if not tx_hash or not leader_wallet:
            return False
        key = (tx_hash, leader_wallet.lower())
        async with self._lock:
            return key in self._pending_seen

    def mined_seen_snapshot(self) -> set[tuple[str, int]]:
        """Test-only: return a copy of the mined-seen set."""
        return set(self._mined_seen)

    def pending_seen_snapshot(self) -> set[tuple[str, str]]:
        """Test-only: return a copy of the pending-seen set."""
        return set(self._pending_seen)

    def reset(self) -> None:
        """Test-only: clear all state."""
        self._mined_seen.clear()
        self._pending_seen.clear()

    async def bulk_record_mined(self, keys: Iterable[tuple[str, int]]) -> list[bool]:
        """Record a batch of mined keys; return per-key dup/new flags."""
        results: list[bool] = []
        async with self._lock:
            for tx_hash, log_index in keys:
                key = (tx_hash, log_index)
                if not tx_hash or log_index < 0:
                    results.append(False)
                    continue
                if key in self._mined_seen:
                    results.append(True)
                else:
                    self._record_mined_locked(key)
                    results.append(False)
        return results

    def _record_mined_locked(self, key: tuple[str, int]) -> None:
        """Insert a new mined key and enforce the ceiling. Caller holds the lock."""
        self._mined_seen[key] = None
        self._evict_oldest(self._mined_seen, self._max_mined)

    @staticmethod
    def _evict_oldest(entries: OrderedDict[_KeyT, None], limit: int) -> None:
        """Drop oldest-first until *entries* fits *limit*. Caller holds the lock."""
        while len(entries) > limit:
            entries.popitem(last=False)
