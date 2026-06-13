"""P0-1: a reconciler settlement carries a STABLE dedup_key (the cumulative
matched total). Re-applying the same settlement — after a crash or a swallowed
watermark error left the reconciler recomputing the same delta — is deduped on
idx_fills_dedup_v2 and the position delta is applied exactly once.

Behavioural test against a real Postgres (release-gate tier; auto-skips without
Docker).
"""

from __future__ import annotations

import contextlib
from decimal import Decimal

import pytest

pytestmark = pytest.mark.asyncio

VALID_TOKEN = "52114319501245915516055106046884209969926127482827954674443846427813813222426"


@contextlib.asynccontextmanager
async def _pg_pool():
    try:
        from testcontainers.postgres import PostgresContainer
        import asyncpg
        from storage.postgres.migrations import apply_migrations as pg_apply
    except Exception as exc:  # pragma: no cover - env without docker
        pytest.skip(f"Docker / postgres image unavailable: {exc!r}")
    try:
        ctr = PostgresContainer("postgres:16-alpine")
        ctr.start()
    except Exception as exc:  # pragma: no cover
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


async def _position_size(pool, user_id, condition_id, token_id):
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT size_tokens FROM positions WHERE user_id=$1 AND condition_id=$2 AND token_id=$3",
            user_id, condition_id, token_id,
        )
    return Decimal(row["size_tokens"]) if row else None


async def _fill_count(pool, order_id):
    async with pool.acquire() as conn:
        return int(await conn.fetchval("SELECT COUNT(*) FROM fills WHERE order_id=$1", order_id))


async def test_reapply_same_cumulative_is_deduped_and_applied_once():
    from storage.postgres.fills_repo import FillRecord, FillsRepo
    from storage.postgres.positions_repo import PositionsRepo

    async with _pg_pool() as pool:
        fills = FillsRepo(pool)
        positions = PositionsRepo(pool)
        user, cond, tok, oid = "0xUserChecksum", "0xcond", VALID_TOKEN, "ord-1"

        def _rec(delta: str, dedup: str) -> FillRecord:
            return FillRecord(
                user_id=user, leader_id=None, order_id=oid, condition_id=cond,
                token_id=tok, side="BUY", size_tokens=Decimal(delta), price=Decimal("0.5"),
                dedup_key=dedup,
            )

        # First settlement: cumulative matched = 10, delta = 10.
        fid1, _ = await fills.append_fill_and_apply(_rec("10", "10"), positions)
        assert fid1 != 0
        assert await _position_size(pool, user, cond, tok) == Decimal("10")
        assert await _fill_count(pool, oid) == 1

        # Re-apply the SAME settlement (stale watermark recomputed the same
        # delta). dedup_key "10" already exists -> skipped, position unchanged.
        fid_dup, applied = await fills.append_fill_and_apply(_rec("10", "10"), positions)
        assert fid_dup == 0
        assert applied is None
        assert await _position_size(pool, user, cond, tok) == Decimal("10")  # not 20
        assert await _fill_count(pool, oid) == 1

        # A genuine next settlement: cumulative advances to 15, delta = 5.
        fid2, _ = await fills.append_fill_and_apply(_rec("5", "15"), positions)
        assert fid2 != 0
        assert await _position_size(pool, user, cond, tok) == Decimal("15")
        assert await _fill_count(pool, oid) == 2


async def test_no_dedup_key_falls_back_to_legacy_behaviour():
    # Non-reconciler callers pass no dedup_key; the partial index does not apply
    # to them, so their existing fill_ts dedup is unchanged.
    from storage.postgres.fills_repo import FillRecord, FillsRepo
    from storage.postgres.positions_repo import PositionsRepo

    async with _pg_pool() as pool:
        fills = FillsRepo(pool)
        positions = PositionsRepo(pool)
        user, cond, tok, oid = "0xUserChecksum", "0xcond2", VALID_TOKEN, "ord-2"

        rec = FillRecord(
            user_id=user, leader_id=None, order_id=oid, condition_id=cond,
            token_id=tok, side="BUY", size_tokens=Decimal("4"), price=Decimal("0.5"),
            fill_ts=1000,
        )
        fid, _ = await fills.append_fill_and_apply(rec, positions)
        assert fid != 0
        assert await _position_size(pool, user, cond, tok) == Decimal("4")
