"""PostgreSQL contract for durable leader SELL target generations."""

from __future__ import annotations

import contextlib

import pytest

from storage.postgres.leader_sell_target_repo import LeaderSellTargetRepo


@pytest.mark.asyncio
async def test_distinct_tasks_keep_independent_recovery_targets() -> None:
    """The latest SELL from another task cannot replace this task's recovery."""
    async with _pg_pool() as pool:
        repository = LeaderSellTargetRepo(pool)
        common = dict(user_id="user-1", leader_wallet="leader", condition_id="market", token_id="1")
        first = await repository.upsert_target(
            **common, task_id="task-a", source_signal_id="a", intent_payload={"task_id": "task-a"}
        )
        await repository.upsert_target(
            **common, task_id="task-b", source_signal_id="b", intent_payload={"task_id": "task-b"}
        )
        assert await repository.get_target(**common, task_id="task-a") == first
        assert await repository.get_target(**common) is None


@pytest.mark.asyncio
async def test_populated_legacy_targets_upgrade_without_invented_task_ownership() -> None:
    """An old unscoped target and its release claim survive the additive upgrade."""
    from storage.postgres.migrations import apply_migrations

    async with _pg_pool() as pool:
        async with pool.acquire() as connection:
            # Constant DDL per table (same statements and order as the previous
            # loop, without interpolation): downgrade both tables to their
            # pre-migration shape before re-applying the additive migration.
            await connection.execute(
                "ALTER TABLE leader_sell_targets DROP CONSTRAINT leader_sell_targets_pkey"
            )
            await connection.execute("ALTER TABLE leader_sell_targets DROP COLUMN task_id")
            await connection.execute(
                "ALTER TABLE leader_sell_target_redrives "
                "DROP CONSTRAINT leader_sell_target_redrives_pkey"
            )
            await connection.execute(
                "ALTER TABLE leader_sell_target_redrives DROP COLUMN task_id"
            )
            await connection.execute(
                "ALTER TABLE leader_sell_targets ADD PRIMARY KEY "
                "(user_id, leader_wallet, condition_id, token_id)"
            )
            await connection.execute(
                "ALTER TABLE leader_sell_target_redrives ADD PRIMARY KEY "
                "(user_id, leader_wallet, condition_id, token_id, generation, released_reservation_id)"
            )
            await connection.execute(
                "DELETE FROM app_schema_migrations WHERE migration_id = $1",
                "20260908_04_task_sell_recovery_scope",
            )
            await connection.execute(
                "INSERT INTO leader_sell_targets "
                "(user_id, leader_wallet, condition_id, token_id, generation, source_signal_id, intent_json) "
                "VALUES ('account', 'leader', 'market', '1', 7, 'old-signal', '{\"price\":\"0.45\"}')"
            )
            await connection.execute(
                "INSERT INTO leader_sell_target_redrives "
                "(user_id, leader_wallet, condition_id, token_id, generation, released_reservation_id) "
                "VALUES ('account', 'leader', 'market', '1', 7, 'old-reservation')"
            )
            await apply_migrations(connection)
            await apply_migrations(connection)
        repository = LeaderSellTargetRepo(pool)
        target = await repository.get_target(
            user_id="account",
            leader_wallet="leader",
            condition_id="market",
            token_id="1",
        )
        assert target is not None and target.task_id is None and target.generation == 7
        assert target.intent_payload == {"price": "0.45"}
        assert not await repository.claim_redrive(
            target=target, released_reservation_id="old-reservation"
        )


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
