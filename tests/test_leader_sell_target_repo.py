"""PostgreSQL contract for durable leader SELL target generations."""

from __future__ import annotations

import contextlib

import pytest

from storage.postgres.leader_sell_target_repo import LeaderSellTargetRepo


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
async def test_target_generation_and_release_claim_are_restart_safe() -> None:
    """Latest target state and one-shot release claims persist durably."""

    async with _pg_pool() as pool:
        first = LeaderSellTargetRepo(pool)
        target_one = await first.upsert_target(
            user_id="user-1",
            leader_wallet="0xLeader",
            condition_id="condition-1",
            token_id="123",
            source_signal_id="signal-1",
            intent_payload={"price": "0.21"},
        )
        target_two = await first.upsert_target(
            user_id="user-1",
            leader_wallet="0xLeader",
            condition_id="condition-1",
            token_id="123",
            source_signal_id="signal-2",
            intent_payload={"price": "0.20"},
        )

        restarted = LeaderSellTargetRepo(pool)
        loaded = await restarted.get_target(
            user_id="user-1",
            leader_wallet="0xleader",
            condition_id="condition-1",
            token_id="123",
        )

        assert target_one.generation == 1
        assert target_two.generation == 2
        assert loaded == target_two
        assert (
            await restarted.claim_redrive(
                target=target_two,
                released_reservation_id="reservation-1",
            )
            is True
        )
        assert (
            await first.claim_redrive(
                target=target_two,
                released_reservation_id="reservation-1",
            )
            is False
        )
