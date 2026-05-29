"""Release-gate: ``bot drift check`` CLI (Issue #76).

Mocks live-holdings fetch and seeds the persisted ``positions`` table to
exercise the four drift classifications: should_close, shrink, phantom_grow,
and untracked_live.

Backed by a live ``postgres:16-alpine`` testcontainer per the PG-A5
sweep — the persisted-listing test owns its own pool via the shared
``_pg_pool()`` async context manager.
"""
from __future__ import annotations

import contextlib
from decimal import Decimal

import pytest

from bot_cli.commands.drift import (
    _classify_drift,
    _list_persisted_positions,
    _live_size_index,
)

pytestmark = pytest.mark.release_gate


@contextlib.asynccontextmanager
async def _pg_pool():
    """Spin a Postgres testcontainer + return a pool with the full
    schema applied. Auto-skips when Docker is unavailable."""
    try:
        from testcontainers.postgres import PostgresContainer
        import asyncpg
        from storage.postgres.migrations import apply_migrations as pg_apply

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


async def _seed(pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO positions
              (user_id, condition_id, token_id, leader_id,
               size_tokens, avg_entry_price)
            VALUES
              ('u-test','cond-x','tok-x','ldr-1','10','0.42'),
              ('u-test','cond-y','tok-y','ldr-1','5','0.50')
            """,
        )


def test_classify_drift_kinds() -> None:
    eps = Decimal("0.000001")
    assert _classify_drift(Decimal("10"), Decimal("0"), eps) == "should_close"
    assert _classify_drift(Decimal("10"), Decimal("5"), eps) == "shrink"
    assert _classify_drift(Decimal("5"), Decimal("10"), eps) == "phantom_grow"
    assert _classify_drift(Decimal("5"), Decimal("5"), eps) is None
    assert _classify_drift(Decimal("5"), Decimal("5.0000001"), eps) is None


@pytest.mark.asyncio
async def test_persisted_positions_listed() -> None:
    async with _pg_pool() as pool:
        await _seed(pool)
        rows = await _list_persisted_positions(pool, user_id="u-test")
    assert len(rows) == 2
    keys = {(r["condition_id"], r["token_id"]) for r in rows}
    assert keys == {("cond-x", "tok-x"), ("cond-y", "tok-y")}


def test_live_size_index_aggregates() -> None:
    live = [
        {"conditionId": "cond-x", "tokenID": "tok-x", "size": "3"},
        {"condition_id": "cond-x", "token_id": "tok-x", "size": "2"},
        {"conditionId": "cond-z", "asset": "tok-z", "size": "1"},
    ]
    idx = _live_size_index(live)
    assert idx[("cond-x", "tok-x")] == Decimal("5")
    assert idx[("cond-z", "tok-z")] == Decimal("1")
