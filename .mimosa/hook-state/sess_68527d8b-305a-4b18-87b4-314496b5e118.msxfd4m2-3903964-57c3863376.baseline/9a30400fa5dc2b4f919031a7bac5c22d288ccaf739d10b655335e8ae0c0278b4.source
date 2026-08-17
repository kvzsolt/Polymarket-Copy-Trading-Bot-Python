"""Deterministic ordered v1 migration catalog contracts."""

from __future__ import annotations

from storage.postgres.v1_migration_runner import V1MigrationCatalog


def test_catalog_lists_every_sql_migration_in_immutable_order() -> None:
    """Apply additive migration files by filename-derived identity."""

    migrations = V1MigrationCatalog().migrations()

    assert tuple(migration.migration_id for migration in migrations) == (
        "20260808_01_copy_tasks_and_reentry_locks",
        "20260811_02_copy_task_execution_lifecycle",
        "20260811_03_control_plane",
    )
    assert all(migration.path.name == f"{migration.migration_id}.sql" for migration in migrations)
