"""Regression: apply_migrations() must be atomic.

Pre-fix: ``_ensure_legacy_tables`` and ``_ensure_order_reservations_table``
called ``conn.executescript(...)`` which per the Python sqlite3 docs
"issues a COMMIT before executing", auto-committing any work that came
before it. If a later migration step crashed, the DB was left in a
half-applied state and subsequent boots could silently skip steps that
ran in the wrong order.

Test injects a failure into the middle of the migration pipeline and
asserts that NONE of the previously-created tables survive the
rollback — a classic all-or-nothing invariant for a startup-critical
schema script.
"""
from __future__ import annotations

import sqlite3

import pytest

import storage.sqlite.migrations as migrations
from storage.sqlite.connection import create_connection


def _tables_present(db_path: str) -> set[str]:
    with create_connection(db_path) as conn:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    return {str(r[0]) for r in rows}


def test_migrations_rollback_on_mid_pipeline_failure(tmp_path, monkeypatch):
    db_path = str(tmp_path / "atomic.db")

    boom_calls: list[bool] = []

    def boom(conn: sqlite3.Connection) -> None:  # noqa: ARG001
        boom_calls.append(True)
        raise RuntimeError("simulated mid-migration crash")

    # Patch a step that lives strictly between earlier and later steps so
    # that the rollback contract has something to actually undo.
    monkeypatch.setattr(migrations, "_ensure_risk_blocks_table", boom)

    with create_connection(db_path) as conn:
        with pytest.raises(RuntimeError, match="simulated mid-migration crash"):
            migrations.apply_migrations(conn)

    # Sanity — the patched step did fire (otherwise the test is testing
    # nothing).
    assert boom_calls, "patched migration step never ran"

    tables = _tables_present(db_path)
    # If apply_migrations is atomic, the rollback discards every table
    # that was created earlier in the pipeline.
    leftover = tables - {"sqlite_sequence"}  # SQLite internal bookkeeping
    assert not leftover, (
        "apply_migrations must roll back on failure; leftover tables: "
        f"{sorted(leftover)}"
    )


def test_migrations_idempotent_when_clean(tmp_path):
    """Calling apply_migrations twice on a clean DB must not raise."""
    db_path = str(tmp_path / "idempotent.db")
    with create_connection(db_path) as conn:
        migrations.apply_migrations(conn)
    with create_connection(db_path) as conn:
        migrations.apply_migrations(conn)  # second call must be a no-op
