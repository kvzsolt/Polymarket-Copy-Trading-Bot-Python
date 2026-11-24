"""Durable follower performance survives restart and excludes other scopes."""

from decimal import Decimal

import asyncpg
import pytest
from tests.integration_postgres import PostgresContainer

from pmcopy.services.trading.leader_performance import FollowerLeaderKey
from pmcopy.storage.postgres.fills_repo import FillRecord, FillsRepo
from pmcopy.storage.postgres.migrations import apply_migrations
from pmcopy.storage.postgres.leader_performance_repository import PostgresLeaderPerformanceReader


@pytest.mark.asyncio
async def test_reader_filters_account_leader_task_and_rebuilds_from_deduplicated_fills():
    with PostgresContainer("postgres:16-alpine") as container:
        dsn = container.get_connection_url().replace("postgresql+psycopg2://", "postgresql://")
        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=2)
        try:
            async with pool.acquire() as connection:
                await apply_migrations(connection)
            leader = "0x" + "a" * 40
            fills = FillsRepo(pool)
            for account, leader_id, task, sell_price in (
                ("account-a", leader, "task-a", ".6"),
                ("account-a", leader, "task-b", ".4"),
                ("account-b", leader, "task-a", ".9"),
                ("account-a", "0x" + "b" * 40, "task-a", ".9"),
            ):
                for side, price in (("BUY", ".5"), ("SELL", sell_price)):
                    order_id = f"{account}-{leader_id}-{task}-{side}"
                    async with pool.acquire() as connection:
                        await connection.execute(
                            "INSERT INTO orders (idempotency_key, user_id, order_id, request_json) "
                            "VALUES ($1, $2, $1, jsonb_build_object('recovery_metadata', "
                            "jsonb_build_object('task_id', $3::text)))",
                            order_id,
                            account,
                            task,
                        )
                    record = FillRecord(
                        account,
                        leader_id,
                        order_id,
                        "market",
                        "token",
                        side,
                        Decimal(10),
                        Decimal(price),
                        fee=Decimal("0"),
                        fill_ts=1,
                    )
                    assert await fills.append_fill(record) > 0
                    assert await fills.append_fill(record) == 0
            for _ in range(2):
                reader = PostgresLeaderPerformanceReader(pool)
                stats = await reader.get_closed_episode_stats(
                    FollowerLeaderKey("account-a", leader)
                )
                assert stats.closed_episodes == 2
                assert stats.win_rate_fraction == Decimal(".5")
                assert stats.realized_pnl == 0
                scoped = await reader.get_closed_episode_stats(
                    FollowerLeaderKey("account-a", leader, "task-a")
                )
                assert scoped.closed_episodes == 1
                assert scoped.realized_pnl == 1
        finally:
            await pool.close()
