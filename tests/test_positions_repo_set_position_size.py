"""P1-b: ``set_position_size`` absolutely sets a position under the per-position
advisory lock + ``SELECT ... FOR UPDATE``, and skips the write when a concurrent
writer (reconciler ``apply_fill``) changed the row since the caller's snapshot.

This is the locked path manual-sync must use for its venue-size align so a
reconciler delta committing inside the read→write window is not clobbered.
These unit tests exercise the guard logic with a fake pool/conn; the real
multi-connection lock behaviour is covered by the Postgres release-gate tier.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal

from pmcopy.storage.postgres.positions_repo import PositionRecord, PositionsRepo

# A real Polymarket CLOB token id shape (passes is_valid_clob_token_id).
VALID_TOKEN = "52114319501245915516055106046884209969926127482827954674443846427813813222426"


class _FakeTx:
    async def __aenter__(self):
        return None

    async def __aexit__(self, *exc):
        return False


class _FakeAcquire:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


class _FakeConn:
    def __init__(self):
        self.executed: list = []

    async def execute(self, sql, *args):
        self.executed.append((sql, args))

    def transaction(self):
        return _FakeTx()


class _FakePool:
    def __init__(self, conn):
        self._conn = conn

    def acquire(self):
        return _FakeAcquire(self._conn)


def _repo_with_current(current_size: Decimal | None):
    conn = _FakeConn()
    repo = PositionsRepo(_FakePool(conn))
    captured = {"upserts": [], "deletes": []}

    async def _fake_get(conn_, user_id, condition_id, *, token_id=None, for_update=False):
        if current_size is None:
            return None
        return PositionRecord(
            user_id=user_id,
            condition_id=condition_id,
            token_id=token_id,
            leader_id=None,
            size_tokens=current_size,
            avg_entry_price=Decimal("0.5"),
        )

    async def _fake_upsert(conn_, record):
        captured["upserts"].append(record)

    async def _fake_delete(conn_, user_id, condition_id, *, token_id=None):
        captured["deletes"].append((user_id, condition_id, token_id))

    repo._get_position_in_conn = _fake_get
    repo._upsert_position_in_conn = _fake_upsert
    repo._delete_position_in_conn = _fake_delete
    return repo, conn, captured


def test_skips_write_when_locked_size_drifted_from_expected():
    # Caller read local=10; under the lock the row is now 15 (a reconciler
    # applied +5). The absolute set to 10 must be skipped, not clobber the 15.
    repo, conn, cap = _repo_with_current(Decimal("15"))
    result = asyncio.run(
        repo.set_position_size(
            user_id="0xUserChecksum",
            condition_id="0xcond",
            token_id=VALID_TOKEN,
            size_tokens=Decimal("10"),
            avg_entry_price=Decimal("0.5"),
            expected_size=Decimal("10"),
        )
    )
    assert result is False
    assert cap["upserts"] == []
    assert cap["deletes"] == []


def test_writes_absolute_size_when_consistent():
    repo, conn, cap = _repo_with_current(Decimal("10"))
    result = asyncio.run(
        repo.set_position_size(
            user_id="0xUserChecksum",
            condition_id="0xcond",
            token_id=VALID_TOKEN,
            size_tokens=Decimal("12"),
            avg_entry_price=Decimal("0.5"),
            expected_size=Decimal("10"),
        )
    )
    assert result is True
    assert len(cap["upserts"]) == 1
    assert cap["upserts"][0].size_tokens == Decimal("12")


def test_takes_position_advisory_lock_before_read():
    repo, conn, cap = _repo_with_current(Decimal("10"))
    asyncio.run(
        repo.set_position_size(
            user_id="0xUserChecksum",
            condition_id="0xcond",
            token_id=VALID_TOKEN,
            size_tokens=Decimal("12"),
            avg_entry_price=Decimal("0.5"),
            expected_size=Decimal("10"),
        )
    )
    assert any("pg_advisory_xact_lock" in sql for sql, _ in conn.executed)


def test_deletes_when_target_is_zero_and_consistent():
    repo, conn, cap = _repo_with_current(Decimal("10"))
    result = asyncio.run(
        repo.set_position_size(
            user_id="0xUserChecksum",
            condition_id="0xcond",
            token_id=VALID_TOKEN,
            size_tokens=Decimal("0"),
            avg_entry_price=Decimal("0"),
            expected_size=Decimal("10"),
        )
    )
    assert result is True
    assert len(cap["deletes"]) == 1
    assert cap["upserts"] == []
