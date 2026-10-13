"""Idempotent v1 migration runner contracts."""

from __future__ import annotations

import contextlib
import sys
from decimal import Decimal
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from pmcopy.services.copy_tasks.domain import (
    CopyTask,
    CopyTaskStatus,
    MonitoringMode,
    TaskSpendPolicy,
)
from pmcopy.services.copy_tasks.reentry_lock import (
    MarketReentryKey,
    MarketReentryLock,
    ProtectiveExitReason,
)
from pmcopy.storage.postgres.copy_task_repository import PostgresCopyTaskRepository
from pmcopy.storage.postgres.market_reentry_lock_repository import (
    PostgresMarketReentryLockRepository,
)
from pmcopy.storage.postgres.migrations import apply_migrations, apply_v1_migrations
from pmcopy.storage.postgres.task_budget_repository import PostgresTaskBudgetRepository


@contextlib.asynccontextmanager
async def _pg_pool():
    """Spin a Postgres testcontainer with the v1 schema applied.

    Auto-skips when Docker or the postgres image is unavailable so the
    deterministic unit suite remains green in environments without Docker.
    """
    try:
        import asyncpg
        from tests.integration_postgres import PostgresContainer
    except Exception as exc:  # pragma: no cover - environment-dependent
        pytest.skip(f"testcontainers/asyncpg unavailable: {exc!r}")
    try:
        ctr = PostgresContainer("postgres:16-alpine")
        ctr.start()
    except Exception as exc:  # pragma: no cover - environment-dependent
        pytest.skip(f"Docker / postgres image unavailable: {exc!r}")
    try:
        dsn = ctr.get_connection_url(driver="asyncpg").replace(
            "postgresql+asyncpg://", "postgresql://"
        )
        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=3)
        async with pool.acquire() as conn:
            await apply_migrations(conn)
        try:
            yield pool
        finally:
            await pool.close()
    finally:
        ctr.stop()


@pytest.mark.asyncio
async def test_v1_migration_is_idempotent_across_two_runs() -> None:
    async with _pg_pool() as pool:
        async with pool.acquire() as conn:
            await apply_migrations(conn)
            await apply_migrations(conn)

            tables = {
                row["table_name"]
                for row in await conn.fetch(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
                )
            }
            assert "copy_tasks" in tables
            assert "market_reentry_locks" in tables
            assert "copy_task_budget_reservations" in tables
            assert "copy_task_fill_ledger" in tables
            assert "copy_task_risk_lots" in tables
            assert "copy_task_protective_exit_fill_ledger" in tables

            migration_ids = {
                row["migration_id"]
                for row in await conn.fetch("SELECT migration_id FROM app_schema_migrations")
            }
            # Exact pin of the applied catalog (base trader-discovery migration
            # + the 13 packaged v1 migrations in filename order). Added entries:
            # 20260917_11 (evidence outbox, Task 6), 20261011_12 (order
            # submission identity, Task 9), 20261011_13 (account execution
            # leases, Task 21). Update this pin together with the catalog.
            assert migration_ids == {
                "20260808_01_copy_tasks_and_reentry_locks",
                "20260811_02_copy_task_execution_lifecycle",
                "20260811_03_control_plane",
                "20260908_04_task_sell_recovery_scope",
                "20260908_05_economic_intent_lookup",
                "20260908_06_partial_protective_layers",
                "20260908_07_fill_fee_evidence",
                "20260916_08_position_lifecycle",
                "20260916_09_redemption_attempts",
                "20260916_10_auto_trader",
                "20260917_11_evidence_transaction_outbox",
                "20261011_12_order_submission_identity",
                "20261011_13_account_execution_leases",
                "trader_discovery_single_generation_20260713",
            }


@pytest.mark.asyncio
async def test_v1_schema_supports_full_copy_task_round_trip() -> None:
    async with _pg_pool() as pool:
        async with pool.acquire() as conn:
            await apply_v1_migrations(conn)
        repo = PostgresCopyTaskRepository(pool)
        task = CopyTask(
            task_id="task-rt",
            name="Round trip",
            leader_wallet="0x1111111111111111111111111111111111111111",
            follower_account="account-a",
            status=CopyTaskStatus.ACTIVE,
            monitoring_mode=MonitoringMode.NORMAL,
            spend=TaskSpendPolicy(max_total=Decimal("100")),
        )
        await repo.create(task)
        fetched = await repo.get(task.task_id)
        assert fetched == task


@pytest.mark.asyncio
async def test_v1_schema_supports_reentry_lock_round_trip() -> None:
    from datetime import UTC, datetime

    async with _pg_pool() as pool:
        async with pool.acquire() as conn:
            await apply_v1_migrations(conn)
            await conn.execute(
                "INSERT INTO copy_tasks(task_id, name, leader_wallet, "
                "follower_account, status, monitoring_mode, sizing_mode, "
                "notifications_muted, version, configuration) "
                "VALUES ('task-l', 'L', '0x1111111111111111111111111111111111111111', "
                "'acct', 'active', 'normal', 'proportional', false, 1, '{}'::jsonb)"
            )
        repo = PostgresMarketReentryLockRepository(pool)
        key = MarketReentryKey(
            "acct",
            "task-l",
            "0x1111111111111111111111111111111111111111",
            "market-l",
        )
        when = datetime(2026, 8, 8, tzinfo=UTC)
        lock = MarketReentryLock(
            key=key,
            reason=ProtectiveExitReason.STOP_LOSS,
            fill_id="fill-l",
            locked_at=when,
        )
        assert await repo.create_if_absent(lock) is True
        assert await repo.create_if_absent(lock) is False
        fetched = await repo.get(key)
        assert fetched is not None
        assert fetched.reason is ProtectiveExitReason.STOP_LOSS


@pytest.mark.asyncio
async def test_v1_schema_supports_task_budget_reservation_round_trip() -> None:
    from datetime import UTC, datetime, timedelta

    from pmcopy.services.copy_tasks.budget import TaskBudgetKey, TaskBudgetReservationRequest

    async with _pg_pool() as pool:
        async with pool.acquire() as conn:
            await apply_v1_migrations(conn)
            await conn.execute(
                "INSERT INTO copy_tasks(task_id, name, leader_wallet, "
                "follower_account, status, monitoring_mode, sizing_mode, "
                "notifications_muted, version, configuration) "
                "VALUES ('task-b', 'B', '0x1111111111111111111111111111111111111111', "
                "'acct', 'active', 'normal', 'proportional', false, 1, '{}'::jsonb)"
            )
        repo = PostgresTaskBudgetRepository(pool)
        now = datetime(2026, 8, 8, 12, tzinfo=UTC)
        request = TaskBudgetReservationRequest(
            reservation_id="res-b",
            key=TaskBudgetKey(
                follower_account="acct",
                task_id="task-b",
                market_id="market-b",
            ),
            amount=Decimal("10"),
            reserved_at=now,
            expires_at=now + timedelta(seconds=120),
        )
        decision = await repo.reserve(request, TaskSpendPolicy(max_total=Decimal("100")))
        assert decision.allowed is True
        assert decision.created is True
        committed = await repo.commit(
            reservation_id="res-b",
            fill_id="fill-b",
            filled_notional=Decimal("8"),
            occurred_at=now,
        )
        assert committed is True
