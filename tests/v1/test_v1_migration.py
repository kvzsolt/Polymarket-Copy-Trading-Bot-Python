"""Static contracts for the additive v1 PostgreSQL migration."""

from __future__ import annotations

from pathlib import Path


def test_v1_migration_contains_copy_tasks_and_durable_reentry_lock() -> None:
    migration = (
        Path(__file__).resolve().parents[2]
        / "storage"
        / "postgres"
        / "migrations"
        / "20260808_01_copy_tasks_and_reentry_locks.sql"
    ).read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS copy_tasks" in migration
    assert "configuration JSONB NOT NULL" in migration
    assert "version INTEGER NOT NULL" in migration
    assert "CREATE TABLE IF NOT EXISTS market_reentry_locks" in migration
    assert "PRIMARY KEY (follower_account, task_id, leader_wallet, market_id)" in migration
    assert "UNIQUE (fill_id)" in migration
    assert "CREATE INDEX IF NOT EXISTS idx_copy_tasks_leader_status" in migration
    assert "CREATE INDEX IF NOT EXISTS idx_reentry_locks_market" in migration


def test_v1_migration_contains_atomic_task_budget_ledger() -> None:
    migration = (
        Path(__file__).resolve().parents[2]
        / "storage"
        / "postgres"
        / "migrations"
        / "20260808_01_copy_tasks_and_reentry_locks.sql"
    ).read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS copy_task_budget_reservations" in migration
    assert "NUMERIC(38, 18)" in migration
    assert "status IN ('reserved', 'committed', 'released', 'expired')" in migration
    assert "CREATE TABLE IF NOT EXISTS copy_task_fill_ledger" in migration
    assert "reservation_id TEXT NOT NULL UNIQUE" in migration
    assert "idx_task_budget_reservations_account_market" in migration
