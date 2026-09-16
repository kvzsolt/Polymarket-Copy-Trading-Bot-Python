"""P0-5: startup order normalization must NOT delete a pending/delayed row that
carries a venue order_id.

A row that reached the venue (e.g. a 'delayed' ack with an order_id) is
recoverable by startup_reconcile_unconfirmed, which runs next. Marking it
terminal_failed and deleting it at boot would lose a venue-accepted order as an
untracked off-bot fill. Only rows with no venue handle (order_id IS NULL) are
normalized. This pins the SQL guard against a fake connection; the multi-row
behaviour is exercised by the Postgres release-gate tier.
"""

from __future__ import annotations

import asyncio

from orchestrators.startup_health import normalize_startup_order_statuses


class _FakeTx:
    async def __aenter__(self):
        return None

    async def __aexit__(self, *exc):
        return False


class _FakeConn:
    def __init__(self, row):
        self._row = row
        self.executed: list[str] = []
        self.queried: list[str] = []

    def transaction(self):
        return _FakeTx()

    async def fetchrow(self, sql, *args):
        self.queried.append(sql)
        return self._row

    async def execute(self, sql, *args):
        self.executed.append(sql)


class _FakeAcquire:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


class _FakePool:
    def __init__(self, conn):
        self._conn = conn

    def acquire(self):
        return _FakeAcquire(self._conn)


_ROW = {
    "pending_count": 1,
    "delayed_count": 1,
    "retry_pending_count": 0,
    "preserved_count": 1,
    "purge_count": 0,
}


def test_normalize_only_resets_pending_rows_without_order_id():
    conn = _FakeConn(_ROW)
    asyncio.run(normalize_startup_order_statuses(_FakePool(conn), user_id="0xUser"))

    update_sql = next(s for s in conn.executed if "UPDATE orders" in s)
    # Pending/delayed rows are normalized only when they carry no venue handle.
    assert "order_id IS NULL" in update_sql
    assert "status IN ('pending', 'pending_lock', 'delayed') AND order_id IS NULL" in update_sql


def test_normalize_counts_order_id_bearing_rows_as_preserved():
    conn = _FakeConn(_ROW)
    asyncio.run(normalize_startup_order_statuses(_FakePool(conn), user_id="0xUser"))
    # A SELECT that counts the preserved (order_id IS NOT NULL) rows is issued.
    assert any("order_id IS NOT NULL" in q for q in conn.queried)


def test_normalize_still_purges_failed_rows():
    conn = _FakeConn(_ROW)
    asyncio.run(normalize_startup_order_statuses(_FakePool(conn), user_id="0xUser"))
    assert any("DELETE FROM orders" in s for s in conn.executed)
