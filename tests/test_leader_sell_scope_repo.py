"""PostgreSQL contract for durable degraded SELL-fraction scope pauses."""

from __future__ import annotations

import contextlib

import pytest

from storage.postgres.leader_sell_scope_repo import LeaderSellSafetyScopeRepo


@contextlib.asynccontextmanager
async def _pg_pool():
    """Yield a migrated disposable PostgreSQL pool."""

    try:
        import asyncpg
        from testcontainers.postgres import PostgresContainer

        from storage.postgres.migrations import apply_migrations
    except Exception as exc:
        pytest.skip(f"Docker / postgres image unavailable: {exc!r}")
    container = PostgresContainer("postgres:16-alpine")
    try:
        container.start()
    except Exception as exc:
        pytest.skip(f"Docker / postgres image unavailable: {exc!r}")
    try:
        dsn = container.get_connection_url(driver="asyncpg").replace(
            "postgresql+asyncpg://",
            "postgresql://",
        )
        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=2)
        async with pool.acquire() as connection:
            await apply_migrations(connection)
        try:
            yield pool
        finally:
            await pool.close()
    finally:
        container.stop()


@pytest.mark.asyncio
async def test_scope_pause_round_trips_across_repository_instances() -> None:
    """A pause remains visible after rebuilding the repository adapter."""

    async with _pg_pool() as pool:
        first = LeaderSellSafetyScopeRepo(pool)
        await first.set_paused(
            "0xABC",
            "123",
            "authoritative_sources_unavailable",
        )

        restarted = LeaderSellSafetyScopeRepo(pool)
        assert await restarted.is_paused("0xabc", "123") is True

        await restarted.clear_paused("0xABC", "123")
        assert await first.is_paused("0xabc", "123") is False
