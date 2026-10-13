"""Regression: positions_ledger must key on (user_id, condition_id, token_id).

Pre-fix bug: PRIMARY KEY (user_id, condition_id) silently overwrote the
YES outcome row when a NO row for the same condition was upserted (and
vice-versa). Loss of cost basis + size on binary markets.

This test pins the 3-column PK by inserting both outcomes for one
condition and verifying both rows persist after upsert.

PG-A6: ported to async PG. AccountingService writes via the singleton
asyncpg pool; tests patch ``storage.postgres.connection.get_pool`` to
return a per-test testcontainer pool.
"""

from __future__ import annotations

import contextlib
from decimal import Decimal

import pytest


@contextlib.asynccontextmanager
async def _pg_pool():
    try:
        from tests.integration_postgres import PostgresContainer
        import asyncpg
        from pmcopy.storage.postgres.migrations import apply_migrations as pg_apply
    except Exception as exc:
        pytest.skip(f"Docker / postgres image unavailable: {exc!r}")
    ctr = PostgresContainer("postgres:16-alpine")
    try:
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


@pytest.mark.asyncio
async def test_positions_ledger_pk_includes_token_id():
    """PG primary key on positions_ledger must include token_id."""
    async with _pg_pool() as pool:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT a.attname AS name
                FROM pg_index i
                JOIN pg_attribute a
                  ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = 'positions_ledger'::regclass
                  AND i.indisprimary
                ORDER BY a.attname
                """
            )
        pk_cols = [r["name"] for r in rows]
        assert pk_cols == ["condition_id", "token_id", "user_id"], (
            f"positions_ledger PK must be (user_id, condition_id, token_id); got {pk_cols}"
        )


@pytest.mark.asyncio
async def test_yes_and_no_outcomes_persist_independently(monkeypatch):
    """Both YES (token_a) and NO (token_b) rows survive concurrent upserts."""
    import pmcopy.services.accountingService as acct_mod
    from pmcopy.services.accountingService import AccountingService, PositionLot

    async with _pg_pool() as pool:

        async def _get_pool():
            return pool

        monkeypatch.setattr(acct_mod, "get_pool", _get_pool)

        svc = AccountingService()
        yes_lot = PositionLot(
            user_id="u1",
            condition_id="cond_x",
            token_id="token_yes",
            size_tokens=Decimal("10"),
            avg_entry_price=Decimal("0.30"),
            leader_id="leader1",
            category="copy",
        )
        no_lot = PositionLot(
            user_id="u1",
            condition_id="cond_x",
            token_id="token_no",
            size_tokens=Decimal("5"),
            avg_entry_price=Decimal("0.70"),
            leader_id="leader1",
            category="copy",
        )
        await svc.upsert_position(yes_lot)
        await svc.upsert_position(no_lot)

        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT token_id, size_tokens, avg_entry_price FROM positions_ledger "
                "WHERE user_id = $1 AND condition_id = $2 ORDER BY token_id",
                "u1",
                "cond_x",
            )
        assert len(rows) == 2, f"expected 2 rows (YES + NO), got {len(rows)}: {rows}"
        token_ids = sorted(r["token_id"] for r in rows)
        assert token_ids == ["token_no", "token_yes"]


@pytest.mark.asyncio
async def test_close_position_requires_token_id(monkeypatch):
    """close_position must accept token_id and only delete the matching token row."""
    import pmcopy.services.accountingService as acct_mod
    from pmcopy.services.accountingService import AccountingService, PositionLot

    async with _pg_pool() as pool:

        async def _get_pool():
            return pool

        monkeypatch.setattr(acct_mod, "get_pool", _get_pool)

        svc = AccountingService()
        for tok in ("token_yes", "token_no"):
            await svc.upsert_position(
                PositionLot(
                    user_id="u2",
                    condition_id="cond_y",
                    token_id=tok,
                    size_tokens=Decimal("1"),
                    avg_entry_price=Decimal("0.5"),
                    leader_id="L",
                    category="copy",
                )
            )
        await svc.close_position(user_id="u2", condition_id="cond_y", token_id="token_yes")
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT token_id FROM positions_ledger WHERE user_id = $1 AND condition_id = $2",
                "u2",
                "cond_y",
            )
        assert [r["token_id"] for r in rows] == ["token_no"], f"only token_no should remain: {rows}"
