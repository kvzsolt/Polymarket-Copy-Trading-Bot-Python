"""Ordered idempotent runner for additive copy-task PostgreSQL migrations.

Migration identity comes from immutable, lexically ordered SQL filenames.
The runner serializes concurrent startup attempts with one transaction-scoped
advisory lock and records each successfully applied file.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, TypeAlias

from pmcopy.utils.packaged_resources import packaged_resource_dir

MigrationConnection: TypeAlias = Any


@dataclass(frozen=True, slots=True)
class V1Migration:
    """One immutable SQL migration descriptor."""

    migration_id: str
    path: Path


class V1MigrationCatalog:
    """Discover packaged v1 migrations in deterministic filename order."""

    def __init__(self, migration_directory: Path | None = None) -> None:
        # Packaged migrations are readonly inputs resolved through
        # importlib.resources so an installed wheel serves them from
        # (possibly read-only) site-packages; the explicit directory
        # override stays available for operators and tests.
        self._migration_directory = migration_directory or packaged_resource_dir(
            "pmcopy.storage.postgres", "migrations"
        )

    def migrations(self) -> tuple[V1Migration, ...]:
        """Return all packaged SQL files as immutable descriptors."""

        return tuple(
            V1Migration(migration_id=path.stem, path=path)
            for path in sorted(self._migration_directory.glob("*.sql"))
        )


class SqlStatementSplitter:
    """Split the repository's simple semicolon-terminated migration SQL."""

    def split(self, sql: str) -> tuple[str, ...]:
        """Return executable SQL statements with comments and blanks removed.

        Args:
            sql: Migration contents. These files intentionally contain no
                procedures, dollar-quoted blocks, or semicolons in literals.

        Returns:
            Non-empty SQL statements in source order.
        """

        cleaned_lines: list[str] = []
        for raw_line in sql.splitlines():
            stripped = raw_line.strip()
            if not stripped or stripped.startswith("--"):
                continue
            cleaned_lines.append(raw_line)
        joined = "\n".join(cleaned_lines)
        return tuple(statement.strip() for statement in joined.split(";") if statement.strip())


class PostgresV1MigrationRunner:
    """Apply every missing copy-task migration transactionally and once."""

    def __init__(
        self,
        *,
        catalog: V1MigrationCatalog | None = None,
        splitter: SqlStatementSplitter | None = None,
    ) -> None:
        self._catalog = catalog or V1MigrationCatalog()
        self._splitter = splitter or SqlStatementSplitter()

    async def apply(self, connection: MigrationConnection) -> None:
        """Apply all missing migrations on an owned asyncpg connection.

        Args:
            connection: Open asyncpg connection retained by the caller.
        """

        await connection.execute(
            """
            CREATE TABLE IF NOT EXISTS app_schema_migrations (
                migration_id text PRIMARY KEY,
                applied_at   timestamptz NOT NULL DEFAULT now()
            )
            """
        )
        async with connection.transaction():
            await connection.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                "pmcopy:v1-migrations",
            )
            for migration in self._catalog.migrations():
                already_applied = await connection.fetchval(
                    "SELECT 1 FROM app_schema_migrations WHERE migration_id = $1",
                    migration.migration_id,
                )
                if already_applied:
                    continue
                sql = migration.path.read_text(encoding="utf-8")
                for statement in self._splitter.split(sql):
                    await connection.execute(statement)
                await connection.execute(
                    "INSERT INTO app_schema_migrations (migration_id) VALUES ($1)",
                    migration.migration_id,
                )


async def apply_v1_migrations(conn: MigrationConnection) -> None:
    """Apply every packaged v1 migration through the canonical runner.

    Args:
        conn: Open asyncpg connection retained by the caller.
    """

    await PostgresV1MigrationRunner().apply(conn)


__all__ = [
    "MigrationConnection",
    "PostgresV1MigrationRunner",
    "SqlStatementSplitter",
    "V1Migration",
    "V1MigrationCatalog",
    "apply_v1_migrations",
]
