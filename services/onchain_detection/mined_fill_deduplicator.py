"""Mined-fill deduplicator keyed on ``(tx_hash, log_index)`` (Phase 9).

A single Polymarket settlement transaction can emit multiple ``OrderFilled``
logs (one per matched maker order, plus the taker fill). ``tx_hash`` alone is
therefore an insufficient dedupe key: two distinct fills in the same tx would
collapse to one. The authoritative key is the composite ``(tx_hash, log_index)``.

Pending signals (``log_index is None``) are tracked separately so a later
mined signal with the same ``tx_hash`` can supersede them without being
treated as a duplicate of a *different* fill in the same tx.
"""
from __future__ import annotations

import asyncio
from collections.abc import Iterable


class MinedFillDeduplicator:
    """Thread-safe (asyncio) dedupe of mined fills by ``(tx_hash, log_index)``.

    Also tracks provisional pending signals by ``tx_hash`` so the runtime can
    mark them superseded when the authoritative mined fill arrives. A pending
    signal is never deduped against a mined fill (different key shape) — it
    is *superseded* by the mined fill that shares its ``tx_hash``.
    """

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._mined_seen: set[tuple[str, int]] = set()
        self._pending_seen: set[str] = set()

    async def is_duplicate_mined(self, tx_hash: str, log_index: int) -> bool:
        """Return True if this mined fill was already recorded."""
        if not tx_hash or log_index < 0:
            return False
        key = (tx_hash, log_index)
        async with self._lock:
            if key in self._mined_seen:
                return True
            self._mined_seen.add(key)
            return False

    async def record_pending(self, tx_hash: str) -> bool:
        """Record a pending signal by ``tx_hash``.

        Returns True if this pending signal is new (not a duplicate pending for
        the same tx). Returns False if a pending signal for this tx was already
        recorded — the caller should not double-dispatch.
        """
        if not tx_hash:
            return False
        async with self._lock:
            if tx_hash in self._pending_seen:
                return False
            self._pending_seen.add(tx_hash)
            return True

    async def supersede_pending(self, tx_hash: str) -> bool:
        """Mark a pending signal superseded by a mined fill with the same tx.

        Returns True if a pending signal existed for this tx (caller should
        suppress the pending dispatch and let the mined signal be the
        authoritative one). Returns False if no pending signal was recorded
        for this tx (the mined signal is standalone).
        """
        if not tx_hash:
            return False
        async with self._lock:
            if tx_hash in self._pending_seen:
                self._pending_seen.discard(tx_hash)
                return True
            return False

    async def pending_known(self, tx_hash: str) -> bool:
        """Check whether a pending signal for this tx is already recorded."""
        if not tx_hash:
            return False
        async with self._lock:
            return tx_hash in self._pending_seen

    def mined_seen_snapshot(self) -> set[tuple[str, int]]:
        """Test-only: return a copy of the mined-seen set."""
        return set(self._mined_seen)

    def pending_seen_snapshot(self) -> set[str]:
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
                    self._mined_seen.add(key)
                    results.append(False)
        return results