"""Wiring contract: core migrations must also apply the v1 migration.

Phase A found ``apply_v1_migrations`` imported and re-exported by
``storage.postgres.migrations`` but never invoked by ``apply_migrations``,
so a real deployment never created the copy-task/reentry-lock/task-budget
tables. This suite pins the startup runner to the composed behavior.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

import storage.postgres.migrations as migrations_module


class _FakeTransaction:
    """Async context manager recording enter/exit into a shared journal."""

    def __init__(self, journal: list[str]) -> None:
        self._journal = journal

    async def __aenter__(self) -> "_FakeTransaction":
        self._journal.append("core_tx_enter")
        return self

    async def __aexit__(self, *_exc_info: object) -> bool:
        self._journal.append("core_tx_exit")
        return False


class _FakeConnection:
    """Minimal asyncpg-connection stand-in for the migration runner."""

    def __init__(self) -> None:
        self.journal: list[str] = []
        self.executed: list[str] = []

    def transaction(self) -> _FakeTransaction:
        return _FakeTransaction(self.journal)

    async def execute(self, statement: str, *args: object) -> None:
        self.executed.append(statement)

    async def fetchval(self, _query: str, *args: object) -> object | None:
        return None


@pytest.mark.asyncio
async def test_apply_migrations_invokes_v1_runner_with_same_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _FakeConnection()

    async def _record_v1(passed_conn: object) -> None:
        assert passed_conn is conn
        conn.journal.append("v1_migrations")

    v1_mock = AsyncMock(side_effect=_record_v1)
    monkeypatch.setattr(migrations_module, "apply_v1_migrations", v1_mock)

    await migrations_module.apply_migrations(conn)  # type: ignore[arg-type]

    v1_mock.assert_awaited_once()


@pytest.mark.asyncio
async def test_v1_runner_runs_after_the_core_transaction_commits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The v1 runner owns its own transaction; it must not run nested inside
    the core DDL transaction (partial-failure isolation between the two)."""
    conn = _FakeConnection()

    async def _record_v1(_passed_conn: object) -> None:
        conn.journal.append("v1_migrations")

    monkeypatch.setattr(
        migrations_module, "apply_v1_migrations", AsyncMock(side_effect=_record_v1)
    )

    await migrations_module.apply_migrations(conn)  # type: ignore[arg-type]

    assert conn.journal.index("v1_migrations") > conn.journal.index("core_tx_exit")


@pytest.mark.asyncio
async def test_startup_apply_migrations_creates_v1_tables_from_empty_schema() -> None:
    """End-to-end: the startup runner alone must yield the full v1 schema.

    Uses a disposable Postgres container (auto-marked ``integration`` by
    conftest); skips cleanly where Docker is unavailable.
    """
    try:
        import asyncpg
        from testcontainers.postgres import PostgresContainer
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
        conn = await asyncpg.connect(dsn)
        try:
            await migrations_module.apply_migrations(conn)
            # Idempotency: a second full startup run is a no-op.
            await migrations_module.apply_migrations(conn)
            tables = {
                row["table_name"]
                for row in await conn.fetch(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = 'public'"
                )
            }
        finally:
            await conn.close()
        assert "copy_tasks" in tables
        assert "copy_task_budget_reservations" in tables
        assert "copy_task_fill_ledger" in tables
        assert "market_reentry_locks" in tables
    finally:
        ctr.stop()
