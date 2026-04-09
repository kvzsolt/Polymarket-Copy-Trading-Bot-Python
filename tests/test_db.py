from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import config.db as db_mod
from storage.sqlite.fills_repo import FillRecord, FillsRepo
from storage.sqlite.migrations import apply_migrations


class TestSQLiteDatabase:
    @pytest.fixture(autouse=True)
    def _isolated_db(self, tmp_path):
        original_path = db_mod._DB_PATH
        original_initialized = db_mod._INITIALIZED
        db_mod._DB_PATH = tmp_path / "bot_state.db"
        db_mod._INITIALIZED = False
        yield
        db_mod._DB_PATH = original_path
        db_mod._INITIALIZED = original_initialized

    @pytest.mark.asyncio
    async def test_connect_db_creates_file_and_schema(self):
        await db_mod.connect_db()
        assert db_mod._DB_PATH.exists()

        with sqlite3.connect(db_mod._DB_PATH) as conn:
            rows = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        table_names = {row[0] for row in rows}
        assert "orders" in table_names
        assert "leader_cursors" in table_names
        assert "positions_ledger" in table_names

    def test_get_database_returns_sqlite_connection(self):
        conn = db_mod.get_database()
        try:
            assert isinstance(conn, sqlite3.Connection)
            row = conn.execute("SELECT 1 AS ok").fetchone()
            assert row["ok"] == 1
        finally:
            conn.close()

    def test_get_client_aliases_connection(self):
        conn = db_mod.get_client()
        try:
            assert isinstance(conn, sqlite3.Connection)
            conn.execute("SELECT 1")
        finally:
            conn.close()

    def test_schema_init_is_idempotent(self):
        db_mod._init_schema()
        db_mod._init_schema()
        with db_mod.get_database() as conn:
            conn.execute("SELECT 1")

    def test_apply_migrations_rebuilds_legacy_fills_schema_with_nullable_order_id(self, tmp_path):
        db_path = tmp_path / "legacy.db"
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                """
                CREATE TABLE fills (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_id TEXT NOT NULL,
                    fill_json TEXT NOT NULL,
                    created_at REAL NOT NULL
                )
                """
            )
            conn.execute(
                "INSERT INTO fills (order_id, fill_json, created_at) VALUES (?, ?, ?)",
                ("legacy-order", "{}", 123.0),
            )
            apply_migrations(conn)
            conn.commit()

            rows = conn.execute("PRAGMA table_info(fills)").fetchall()
            row_by_name = {row[1]: row for row in rows}
            assert row_by_name["order_id"][3] == 0
            assert row_by_name["fill_json"][3] == 0
            assert row_by_name["created_at"][3] == 0
            count = conn.execute("SELECT COUNT(*) FROM fills").fetchone()[0]
            assert count == 1

        repo = FillsRepo(str(db_path))
        row_id = repo.append_fill(
            FillRecord(
                user_id="user-1",
                leader_id="leader-1",
                order_id=None,
                condition_id="cond-1",
                token_id="123456789",
                side="BUY",
                size_tokens=1,
                price=0.5,
                fill_ts=123456,
            ),
            raw_payload={"source": "test"},
        )
        assert row_id > 0
