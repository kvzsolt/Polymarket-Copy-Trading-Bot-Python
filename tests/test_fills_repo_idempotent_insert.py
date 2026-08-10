"""Regression: FillsRepo.append_fill is idempotent on duplicate inserts.

Pre-fix: bare ``INSERT INTO fills`` raised ``IntegrityError`` on the
second call with the same (user_id, order_id, condition_id, side,
fill_ts) tuple — the persist_execution_atomically path caught it but
direct ``FillsRepo`` callers (e.g. reconciliation_service) did not, so
a benign duplicate fill could crash the reconciliation cycle.

Post-fix: ``ON CONFLICT DO NOTHING`` (PG) / ``INSERT OR IGNORE``
(legacy SQLite) with rowcount check. First call returns lastrowid;
second call on the same fill returns 0.
"""

from __future__ import annotations

import contextlib
import time
from decimal import Decimal

import pytest

from storage.postgres.fills_repo import FillRecord, FillsRepo

pytestmark = pytest.mark.asyncio


@contextlib.asynccontextmanager
async def _pg_pool():
    """Spin a Postgres testcontainer + return a pool with the full
    schema applied. Auto-skips when Docker is unavailable."""
    try:
        from testcontainers.postgres import PostgresContainer
        import asyncpg
        from storage.postgres.migrations import apply_migrations as pg_apply
    except Exception as exc:
        pytest.skip(f"Docker / postgres image unavailable: {exc!r}")
    try:
        ctr = PostgresContainer("postgres:16-alpine")
        ctr.start()
    except Exception as exc:
        pytest.skip(f"Docker / postgres image unavailable: {exc!r}")
    try:
        dsn = ctr.get_connection_url(driver="asyncpg").replace(
            "postgresql+asyncpg://", "postgresql://"
        )
        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=3)
        async with pool.acquire() as conn:
            await pg_apply(conn)
        try:
            yield pool
        finally:
            await pool.close()
    finally:
        ctr.stop()


def _record() -> FillRecord:
    return FillRecord(
        user_id="u",
        leader_id="L",
        order_id="venue-1",
        condition_id="c",
        token_id="1" * 64,
        side="BUY",
        size_tokens=Decimal("5"),
        price=Decimal("0.42"),
        fee=Decimal("0.01"),
        fill_ts=int(time.time()),
    )


async def test_first_insert_returns_lastrowid():
    async with _pg_pool() as pool:
        repo = FillsRepo(pool)
        rid = await repo.append_fill(_record())
        assert rid > 0


async def test_duplicate_insert_returns_zero():
    async with _pg_pool() as pool:
        repo = FillsRepo(pool)
        rec = _record()
        first = await repo.append_fill(rec)
        second = await repo.append_fill(rec)
        assert first > 0
        assert second == 0, (
            f"duplicate insert must return 0 (not raise, not insert new row); got {second}"
        )


async def test_duplicate_insert_does_not_create_second_row():
    async with _pg_pool() as pool:
        repo = FillsRepo(pool)
        rec = _record()
        await repo.append_fill(rec)
        await repo.append_fill(rec)
        rows = await repo.list_fills_for_user("u")
        assert len(rows) == 1, f"expected 1 row, got {len(rows)}"
