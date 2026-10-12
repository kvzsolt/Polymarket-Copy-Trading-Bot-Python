"""The additive completion migrations preserve populated pre-completion state."""

from decimal import Decimal
from pathlib import Path

import asyncpg
import pytest
from testcontainers.postgres import PostgresContainer

from pmcopy.storage.postgres import migrations
from pmcopy.storage.postgres.v1_migration_runner import PostgresV1MigrationRunner, V1MigrationCatalog


@pytest.mark.asyncio
async def test_populated_pre_completion_schema_upgrades_without_inventing_attribution(
    monkeypatch, tmp_path: Path
) -> None:
    old_catalog = tmp_path / "old-migrations"
    old_catalog.mkdir()
    for migration in V1MigrationCatalog().migrations()[:3]:
        (old_catalog / migration.path.name).write_text(migration.path.read_text())
    old_runner = PostgresV1MigrationRunner(catalog=V1MigrationCatalog(old_catalog))
    with PostgresContainer("postgres:16-alpine") as container:
        dsn = container.get_connection_url().replace("postgresql+psycopg2://", "postgresql://")
        connection = await asyncpg.connect(dsn)
        try:
            with monkeypatch.context() as patch:
                patch.setattr(migrations, "apply_v1_migrations", old_runner.apply)
                await migrations.apply_migrations(connection)
            assert (
                await connection.fetchval(
                    "SELECT count(*) FROM app_schema_migrations WHERE migration_id LIKE '20260908%'"
                )
                == 0
            )
            await connection.execute(
                """INSERT INTO fills (user_id, order_id, condition_id, token_id, side,
                       size_tokens, price, fee, fill_json)
                   VALUES ('account-a', 'old-order', 'market', 'token', 'BUY',
                           '10.123456789123456789', '0.413', '0', '{}'::jsonb)"""
            )
            await connection.execute(
                """INSERT INTO leader_sell_targets
                       (user_id, leader_wallet, condition_id, token_id, generation,
                        source_signal_id, intent_json)
                   VALUES ('account-a', 'leader', 'market', 'token', 2,
                           'old-source', '{"size":"5"}'::jsonb)"""
            )
            await connection.execute(
                """INSERT INTO leader_sell_target_redrives
                       (user_id, leader_wallet, condition_id, token_id, generation,
                        released_reservation_id)
                   VALUES ('account-a', 'leader', 'market', 'token', 2, 'old-reservation')"""
            )
            await connection.execute(
                """INSERT INTO copy_tasks (task_id, name, leader_wallet, follower_account,
                       status, monitoring_mode, sizing_mode, notifications_muted,
                       version, configuration)
                   VALUES ('task-a', 'A', $1, 'account-a', 'paused', 'normal',
                           'proportional', false, 3, '{}'::jsonb)""",
                "0x" + "1" * 40,
            )
            await connection.execute(
                """INSERT INTO copy_task_risk_lots
                       (lot_id, entry_fill_id, follower_account, task_id, leader_wallet,
                        market_id, outcome_id, entry_price, original_shares, remaining_shares,
                        peak_price, policy_snapshot, triggered_levels, status, opened_at)
                   VALUES ('old-lot', 'old-fill', 'account-a', 'task-a', $1,
                           'market', 'token', .413, 10, 5, .6, '{}'::jsonb,
                           '["tp1"]'::jsonb, 'open', '2026-09-08T12:00:00Z')""",
                "0x" + "1" * 40,
            )
            await connection.execute(
                """INSERT INTO copy_task_protective_exit_fill_ledger
                       (fill_id, lot_id, shares, price, level_id, occurred_at)
                   VALUES ('old-exit', 'old-lot', 5, .55, 'tp1', '2026-09-08T12:01:00Z')"""
            )

            await migrations.apply_migrations(connection)
            await migrations.apply_migrations(connection)

            fill = await connection.fetchrow("SELECT * FROM fills WHERE order_id='old-order'")
            assert fill["size_tokens"] == "10.123456789123456789"
            assert fill["fee"] == "0"
            assert fill["fee_evidence_known"] is False
            target = await connection.fetchrow("SELECT * FROM leader_sell_targets")
            assert target["task_id"] == ""
            assert target["generation"] == 2
            assert target["intent_json"] == '{"size": "5"}'
            assert (
                await connection.fetchval("SELECT task_id FROM leader_sell_target_redrives") == ""
            )
            lot = await connection.fetchrow("SELECT * FROM copy_task_risk_lots")
            assert lot["remaining_shares"] == Decimal(5)
            assert lot["entry_price"] == Decimal(".413")
            assert lot["peak_price"] == Decimal(".6")
            assert lot["triggered_levels"] == '["tp1"]'
            assert lot["pending_exits"] == "[]"
            assert (
                await connection.fetchval(
                    "SELECT level_target_shares FROM copy_task_protective_exit_fill_ledger"
                )
                is None
            )
            assert (
                await connection.fetchval(
                    "SELECT count(*) FROM app_schema_migrations WHERE migration_id LIKE '20260908%'"
                )
                == 4
            )
            await connection.execute(
                """INSERT INTO leader_sell_targets
                       (user_id, leader_wallet, condition_id, token_id, task_id, generation,
                        source_signal_id, intent_json)
                   VALUES ('account-a', 'leader', 'market', 'token', 'task-a', 1,
                           'scoped-source', '{}'::jsonb)"""
            )
            assert await connection.fetchval("SELECT count(*) FROM leader_sell_targets") == 2
        finally:
            await connection.close()
