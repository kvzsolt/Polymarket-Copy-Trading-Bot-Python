from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import config.db as db_mod


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
