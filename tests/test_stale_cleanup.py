"""Tests for stale-record purge in async PG OrdersRepo + utils.idempotency."""

from __future__ import annotations

import contextlib
import time

import pytest

pytestmark = pytest.mark.asyncio


@contextlib.asynccontextmanager
async def _pg_pool():
    """Spin a Postgres testcontainer + return a pool with the full
    schema applied. Auto-skips when Docker is unavailable."""
    try:
        from tests.integration_postgres import PostgresContainer
        import asyncpg
        from pmcopy.storage.postgres.migrations import apply_migrations as pg_apply
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

        from pmcopy.storage.postgres import connection as conn_mod

        conn_mod._reset_for_tests()
        prev_pool = conn_mod._pool
        prev_dsn = conn_mod._pool_dsn
        conn_mod._pool = pool
        conn_mod._pool_dsn = dsn
        try:
            yield pool
        finally:
            conn_mod._pool = prev_pool
            conn_mod._pool_dsn = prev_dsn
            await pool.close()
    finally:
        ctr.stop()


async def _insert_processed(pool, key: str, age_seconds: float):
    ts = time.time() - age_seconds
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO processed_trades
              (trade_key, user_id, leader_id, tx_hash, side, asset,
               condition_id, status, processed_at)
            VALUES ($1, '', '', '', '', '', '', 'processed', $2)
            """,
            key,
            ts,
        )


class TestPurgeProcessedTrades:
    async def test_purge_removes_old(self):
        async with _pg_pool() as pool:
            from pmcopy.storage.postgres.orders_repo import OrdersRepo

            repo = OrdersRepo(pool)
            await _insert_processed(pool, "old1", 40 * 86400)
            await _insert_processed(pool, "old2", 35 * 86400)
            await _insert_processed(pool, "fresh", 5 * 86400)
            deleted = await repo.purge_old_processed_trades(30 * 86400)
            assert deleted == 2
            assert await repo.has_processed_trade("fresh")
            assert not await repo.has_processed_trade("old1")

    async def test_purge_nothing_when_all_fresh(self):
        async with _pg_pool() as pool:
            from pmcopy.storage.postgres.orders_repo import OrdersRepo

            repo = OrdersRepo(pool)
            await _insert_processed(pool, "a", 1000)
            deleted = await repo.purge_old_processed_trades(30 * 86400)
            assert deleted == 0

    async def test_custom_age(self):
        async with _pg_pool() as pool:
            from pmcopy.storage.postgres.orders_repo import OrdersRepo

            repo = OrdersRepo(pool)
            await _insert_processed(pool, "k1", 7200)  # 2 hours old
            deleted = await repo.purge_old_processed_trades(3600)  # 1 hour cutoff
            assert deleted == 1


class TestIdempotencyPurge:
    async def test_purge_stale_records_convenience(self):
        """Test the convenience wrapper in utils/idempotency.py."""
        async with _pg_pool() as pool:
            await _insert_processed(pool, "old_trade", 40 * 86400)
            await _insert_processed(pool, "new_trade", 1 * 86400)

            import pmcopy.utils.idempotency as idem

            total = await idem.purge_stale_records(30 * 86400)
            assert total == 1  # only the old trade
