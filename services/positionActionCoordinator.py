"""Per-scoped-position action coordinator (issue #28).

Purpose
-------
Serialize contradictory runtime actions on a single scoped position so the
bot cannot race a protective SELL against a fresh BUY (or a reconciliation
state-apply) for the same ``(condition_id, token_id)``.

Canonical active-runtime position identity is ``(user_id, condition_id,
token_id)`` (see issue #26). This coordinator is instantiated **per
``TradingContext``** so ``user_id`` is implicit; the lock key is the
remaining ``(condition_id, token_id)`` pair.

Scope of serialization
----------------------
The coordinator guards the critical observe-then-mutate sections of:

* ``execute_trades`` in ``orchestrators/trade_pipeline.py`` — per-signal
  entry-permission check through durable ``apply_fill`` / processed-trade
  status write.
* ``run_risk_check_for_context`` in ``services/riskMonitor.py`` — per-open
  position protective-exit decision through durable SELL ``apply_fill`` and
  risk-block write.
* ``_reconcile_one`` in ``orchestrators/trade_pipeline.py`` — durable fill
  apply for submitted-unconfirmed orders that reach a terminal state.

Unrelated positions (different ``(condition_id, token_id)`` pairs) remain
fully concurrent — the coordinator only serializes within a single scope.

Non-goals
---------
* This coordinator does **not** gate venue submission (``executionService``
  already enforces submission-level idempotency through the orders repo).
* It does **not** persist ownership across process restarts. After a crash
  the next run starts with no locks held and relies on durable repo state
  (processed_trades, positions, risk_blocks) to decide next actions.
* It is **not re-entrant**. A single ``asyncio`` task that wraps a scope is
  expected to run its critical section end-to-end without trying to re-lock
  the same scope; this matches the current call sites.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import AsyncIterator


@dataclass(frozen=True)
class PositionScope:
    """Canonical scoped-position identity used for action coordination.

    ``user_id`` is carried for logging / cross-context safety; the per-context
    coordinator only keys locks by ``(condition_id, token_id)``.
    """

    user_id: str
    condition_id: str
    token_id: str


class PositionActionCoordinator:
    """Serializes contradictory actions on a single scoped position.

    One instance per ``TradingContext`` (i.e. per user). Internally uses a
    dict of ``asyncio.Lock`` keyed by ``(condition_id, token_id)``. Lock
    creation itself is guarded by a registry lock so two tasks that race
    to observe a brand-new scope cannot create two different locks for it.
    """

    def __init__(self) -> None:
        self._locks: dict[tuple[str, str], asyncio.Lock] = {}
        self._registry_lock = asyncio.Lock()
        self._prune_threshold = 128

    @staticmethod
    def _scope_key(scope: PositionScope) -> tuple[str, str]:
        return (scope.condition_id, scope.token_id)

    async def get_lock(self, scope: PositionScope) -> asyncio.Lock:
        """Return (creating if needed) the per-scope ``asyncio.Lock``.

        Prefer :meth:`acquire` for scoped critical sections. Use ``get_lock``
        only when you need a raw lock so you can wrap existing code with
        ``try``/``finally`` without reindenting the body.
        """
        key = self._scope_key(scope)
        existing = self._locks.get(key)
        if existing is not None:
            return existing
        async with self._registry_lock:
            existing = self._locks.get(key)
            if existing is None:
                existing = asyncio.Lock()
                self._locks[key] = existing
                self._maybe_prune()
            return existing

    # Backwards-compatible alias for the previous private name.
    _get_lock = get_lock

    @asynccontextmanager
    async def acquire(self, scope: PositionScope) -> AsyncIterator[None]:
        """Async context manager that holds the per-scope lock for its body."""
        lock = await self.get_lock(scope)
        async with lock:
            yield

    def release_scope(self, scope: PositionScope) -> bool:
        """Drop an unlocked scope lock so closed positions do not accumulate forever."""
        key = self._scope_key(scope)
        lock = self._locks.get(key)
        if lock is None or lock.locked():
            return False
        self._locks.pop(key, None)
        return True

    def prune(self) -> int:
        """Remove all currently-unlocked scope locks and return the count removed."""
        removed = 0
        for key, lock in list(self._locks.items()):
            if lock.locked():
                continue
            self._locks.pop(key, None)
            removed += 1
        return removed

    def _maybe_prune(self) -> None:
        if len(self._locks) > self._prune_threshold:
            self.prune()

    # --- Introspection (tests / diagnostics) ---------------------------------

    def scope_count(self) -> int:
        """Number of distinct scopes that have ever been locked."""
        return len(self._locks)

    def locks_held(self) -> int:
        """Number of scopes whose lock is currently held."""
        return sum(1 for lock in self._locks.values() if lock.locked())

    def is_locked(self, scope: PositionScope) -> bool:
        """Return True if the given scope's lock is currently held."""
        lock = self._locks.get(self._scope_key(scope))
        return bool(lock and lock.locked())
