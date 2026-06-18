"""P1-g: ``try_acquire_submission_lock`` self-heals a stale submission lock.

A task that died holding a ``pending_lock`` row (before it POSTed, so
``order_id IS NULL``) used to block that idempotency key until the next boot
(ON CONFLICT DO NOTHING, no runtime auto-release). The acquire now uses
ON CONFLICT DO UPDATE ... WHERE so a fresh acquirer takes over only a
never-submitted, direct-expirable lock older than the takeover TTL.

These unit tests pin the query shape + parameter wiring against a fake conn so
a regression back to DO NOTHING (or dropping the ``order_id IS NULL`` /
TTL guard) is caught without Postgres. Real multi-row takeover semantics are
exercised by the Postgres release-gate tier (test_stale_cleanup).
"""

from __future__ import annotations

import asyncio

from storage.postgres.orders_repo import (
    _DEFAULT_STALE_LOCK_TAKEOVER_SECONDS,
    _DIRECT_EXPIRABLE_STALE_LOCK_STATUSES,
    OrdersRepo,
)


class _FakeAcquire:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


class _FakeConn:
    def __init__(self, fetchval_return):
        self.calls: list = []
        self._ret = fetchval_return

    async def fetchval(self, sql, *args):
        self.calls.append((sql, args))
        return self._ret


class _FakePool:
    def __init__(self, conn):
        self._conn = conn

    def acquire(self):
        return _FakeAcquire(self._conn)


def test_acquire_uses_stale_takeover_conflict_clause():
    conn = _FakeConn(fetchval_return="k1")
    repo = OrdersRepo(_FakePool(conn))

    ok = asyncio.run(
        repo.try_acquire_submission_lock(
            "k1", "0xUser", None, {"a": 1}, stale_takeover_seconds=300
        )
    )
    assert ok is True

    sql, args = conn.calls[0]
    # Self-healing takeover, not the old DO NOTHING.
    assert "ON CONFLICT" in sql
    assert "DO UPDATE" in sql
    # Safety guard: only never-submitted, direct-expirable, stale rows.
    assert "order_id IS NULL" in sql
    # Params: $5 = direct-expirable statuses, $6 = takeover TTL.
    assert args[4] == list(_DIRECT_EXPIRABLE_STALE_LOCK_STATUSES)
    assert args[5] == 300


def test_acquire_default_takeover_ttl_is_generous():
    conn = _FakeConn(fetchval_return="k1")
    repo = OrdersRepo(_FakePool(conn))
    asyncio.run(repo.try_acquire_submission_lock("k1", "0xUser", None, {}))
    _, args = conn.calls[0]
    assert args[5] == _DEFAULT_STALE_LOCK_TAKEOVER_SECONDS
    assert _DEFAULT_STALE_LOCK_TAKEOVER_SECONDS >= 120  # well above submit duration


def test_acquire_returns_false_when_lock_held_fresh():
    # Conflict + fresh/submitted row -> WHERE false -> no RETURNING -> False.
    conn = _FakeConn(fetchval_return=None)
    repo = OrdersRepo(_FakePool(conn))
    ok = asyncio.run(repo.try_acquire_submission_lock("k1", "0xUser", None, {}))
    assert ok is False
