"""Idempotent runner for the additive v1 copy-task PostgreSQL migration.

The runner reads the packaged SQL file and applies every statement
transactionally. The migration uses ``CREATE TABLE IF NOT EXISTS`` and
``ADD COLUMN IF NOT EXISTS`` so running it twice is a no-op. A schema-migrations
row records the migration id so callers can also skip the file read when the
migration has already been applied.
"""

from __future__ import annotations

from pathlib import Path

import asyncpg

_V1_MIGRATION_ID = "20260808_01_copy_tasks_and_reentry_locks"
_V1_MIGRATION_PATH = (
    Path(__file__).resolve().parent
    / "migrations"
    / "20260808_01_copy_tasks_and_reentry_locks.sql"
)


def _split_statements(sql: str) -> list[str]:
    """Split one SQL script into executable statements.

    The migration file contains only ``CREATE TABLE/INDEX`` and one
    ``ALTER TABLE ... ADD COLUMN IF NOT EXISTS`` per statement, each terminated
    by a semicolon. Comments and blank lines are stripped before splitting so
    asyncpg receives one statement at a time.
    """

    cleaned_lines: list[str] = []
    for raw_line in sql.splitlines():
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("--"):
            continue
        cleaned_lines.append(raw_line)
    joined = "\n".join(cleaned_lines)
    return [stmt.strip() for stmt in joined.split(";") if stmt.strip()]


async def apply_v1_migrations(conn: asyncpg.Connection) -> None:
    """Apply the v1 copy-task migration idempotently inside one transaction.

    Args:
        conn: An asyncpg connection. The caller retains ownership; this
            function opens and commits one transaction.
    """

    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS app_schema_migrations (
            migration_id text PRIMARY KEY,
            applied_at   timestamptz NOT NULL DEFAULT now()
        )
        """
    )
    already_applied = await conn.fetchval(
        "SELECT 1 FROM app_schema_migrations WHERE migration_id = $1",
        _V1_MIGRATION_ID,
    )
    if already_applied:
        return
    sql = _V1_MIGRATION_PATH.read_text(encoding="utf-8")
    statements = _split_statements(sql)
    async with conn.transaction():
        for statement in statements:
            await conn.execute(statement)
        await conn.execute(
            "INSERT INTO app_schema_migrations (migration_id) VALUES ($1)",
            _V1_MIGRATION_ID,
        )


__all__ = ["apply_v1_migrations"]