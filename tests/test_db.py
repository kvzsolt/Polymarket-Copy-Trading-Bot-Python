from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dotenv import load_dotenv

load_dotenv()

from config.db import _extract_database_name, get_database, get_client

MONGO_URI = os.getenv("MONGO_URI", "")


# ---------------------------------------------------------------------------
# _extract_database_name
# ---------------------------------------------------------------------------
class TestExtractDatabaseName:
    @pytest.mark.skipif(not MONGO_URI, reason="MONGO_URI not set")
    def test_production_uri(self):
        assert _extract_database_name(MONGO_URI) == "polymarket_copytrading"

    def test_srv_with_query_params(self):
        uri = "mongodb+srv://user:pass@host.mongodb.net/mydb?appName=myapp&retryWrites=true"
        assert _extract_database_name(uri) == "mydb"

    def test_standard_uri(self):
        assert _extract_database_name("mongodb://localhost:27017/mydb") == "mydb"

    def test_srv_uri(self):
        assert _extract_database_name("mongodb+srv://user:pass@host/testdb") == "testdb"

    def test_no_database_in_uri(self):
        assert _extract_database_name("mongodb://localhost:27017/") == "polymarket_copy_trading"

    def test_empty_uri(self):
        assert _extract_database_name("") == "polymarket_copy_trading"

    def test_malformed_uri(self):
        result = _extract_database_name("not-a-uri")
        assert isinstance(result, str)

    def test_uri_with_special_chars_in_password(self):
        uri = "mongodb+srv://user:p%40ss%23word@host.net/dbname"
        assert _extract_database_name(uri) == "dbname"

    def test_uri_with_options_no_db(self):
        uri = "mongodb+srv://user:pass@host.net/?retryWrites=true"
        assert _extract_database_name(uri) == "polymarket_copy_trading"

    def test_with_port_and_db(self):
        assert _extract_database_name("mongodb://localhost:27017/production_db") == "production_db"

    def test_replica_set_uri(self):
        uri = "mongodb://host1:27017,host2:27017,host3:27017/replicadb?replicaSet=rs0"
        assert _extract_database_name(uri) == "replicadb"

    def test_with_auth_source(self):
        uri = "mongodb://user:pass@localhost:27017/appdb?authSource=admin"
        assert _extract_database_name(uri) == "appdb"

    def test_whitespace_only_uri(self):
        assert _extract_database_name("   ") == "polymarket_copy_trading"

    def test_hyphenated_name(self):
        assert _extract_database_name("mongodb://localhost:27017/my-database-name") == "my-database-name"

    def test_underscore_name(self):
        assert _extract_database_name("mongodb://localhost:27017/my_database_name") == "my_database_name"

    def test_numeric_db_name(self):
        assert _extract_database_name("mongodb://localhost:27017/db123") == "db123"

    def test_only_scheme_and_host(self):
        assert _extract_database_name("mongodb://localhost") == "polymarket_copy_trading"

    def test_multiple_query_params(self):
        uri = "mongodb://localhost:27017/mydb?w=1&wtimeoutMS=5000&maxPoolSize=100"
        assert _extract_database_name(uri) == "mydb"


# ---------------------------------------------------------------------------
# connect_db
# ---------------------------------------------------------------------------
class TestConnectDb:
    @pytest.fixture(autouse=True)
    def _save_restore_db_state(self):
        """Save and restore module-level state around every test."""
        import config.db as db_mod
        orig_client = db_mod._client
        orig_db_name = db_mod._db_name
        orig_uri = db_mod.uri
        yield db_mod
        db_mod._client = orig_client
        db_mod._db_name = orig_db_name
        db_mod.uri = orig_uri

    @pytest.mark.skipif(not MONGO_URI, reason="MONGO_URI not set")
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_connect_and_ping(self, _save_restore_db_state):
        db_mod = _save_restore_db_state
        db_mod._client = None
        db_mod._db_name = None
        await db_mod.connect_db()
        assert db_mod._client is not None
        assert db_mod._db_name == "polymarket_copytrading"
        db_mod._client.admin.command("ping")

    @pytest.mark.asyncio
    async def test_empty_uri_does_nothing(self, _save_restore_db_state):
        db_mod = _save_restore_db_state
        db_mod.uri = ""
        db_mod._client = None
        await db_mod.connect_db()
        assert db_mod._client is None

    @pytest.mark.asyncio
    async def test_whitespace_only_uri_does_nothing(self, _save_restore_db_state):
        db_mod = _save_restore_db_state
        db_mod.uri = "   "
        db_mod._client = None
        await db_mod.connect_db()
        assert db_mod._client is None

    @pytest.mark.asyncio
    async def test_sets_global_db_name(self, _save_restore_db_state):
        db_mod = _save_restore_db_state
        db_mod.uri = "mongodb://localhost:27017/testdb"
        db_mod._client = None
        db_mod._db_name = None
        mock_client = MagicMock()
        mock_client.admin.command.return_value = {"ok": 1.0}
        with patch("config.db.MongoClient", return_value=mock_client):
            await db_mod.connect_db()
        assert db_mod._db_name == "testdb"

    @pytest.mark.asyncio
    async def test_invalid_uri_raises(self, _save_restore_db_state):
        db_mod = _save_restore_db_state
        db_mod.uri = "mongodb://invalid-host:27017/testdb"
        db_mod._client = None
        db_mod._db_name = None
        with patch("config.db.MongoClient", side_effect=Exception("Connection failed")):
            with pytest.raises(Exception, match="Connection failed"):
                await db_mod.connect_db()


# ---------------------------------------------------------------------------
# get_database / get_client
# ---------------------------------------------------------------------------
class TestGetDatabase:
    @pytest.fixture(autouse=True)
    def _save_restore_db_state(self):
        import config.db as db_mod
        orig_client = db_mod._client
        orig_db_name = db_mod._db_name
        orig_uri = db_mod.uri
        yield db_mod
        db_mod._client = orig_client
        db_mod._db_name = orig_db_name
        db_mod.uri = orig_uri

    def test_returns_none_when_no_client(self, _save_restore_db_state):
        _save_restore_db_state._client = None
        assert get_database() is None

    def test_returns_database_when_connected(self, _save_restore_db_state):
        db_mod = _save_restore_db_state
        mock_client = MagicMock()
        mock_client.__getitem__ = MagicMock(return_value="mock_db")
        db_mod._client = mock_client
        db_mod._db_name = "testdb"
        result = get_database()
        mock_client.__getitem__.assert_called_with("testdb")
        assert result == "mock_db"

    def test_get_client_returns_none_initially(self, _save_restore_db_state):
        _save_restore_db_state._client = None
        assert get_client() is None

    def test_returns_client_when_connected(self, _save_restore_db_state):
        mock_client = MagicMock()
        _save_restore_db_state._client = mock_client
        assert get_client() is mock_client

    def test_extracts_db_name_from_uri_when_db_name_is_none(self, _save_restore_db_state):
        db_mod = _save_restore_db_state
        mock_client = MagicMock()
        mock_client.__getitem__ = MagicMock(return_value="extracted_db")
        db_mod._client = mock_client
        db_mod._db_name = None
        db_mod.uri = "mongodb://localhost:27017/extracted_db"
        result = get_database()
        mock_client.__getitem__.assert_called_with("extracted_db")
        assert result == "extracted_db"

    def test_uses_default_db_name_when_uri_has_no_database(self, _save_restore_db_state):
        db_mod = _save_restore_db_state
        mock_client = MagicMock()
        mock_client.__getitem__ = MagicMock(return_value="default_db")
        db_mod._client = mock_client
        db_mod._db_name = None
        db_mod.uri = "mongodb://localhost:27017/"
        result = get_database()
        mock_client.__getitem__.assert_called_with("polymarket_copy_trading")
        assert result == "default_db"

    def test_get_database_idempotent(self, _save_restore_db_state):
        """Calling get_database twice returns the same result."""
        db_mod = _save_restore_db_state
        mock_client = MagicMock()
        mock_client.__getitem__ = MagicMock(return_value="same_db")
        db_mod._client = mock_client
        db_mod._db_name = "mydb"
        assert get_database() == get_database()


# ---------------------------------------------------------------------------
# Production connection (requires real MONGO_URI)
# ---------------------------------------------------------------------------
class TestProductionConnection:
    pytestmark = pytest.mark.integration

    @pytest.mark.skipif(not MONGO_URI, reason="MONGO_URI not set")
    def test_full_roundtrip(self):
        from pymongo import MongoClient
        client = MongoClient(MONGO_URI)
        try:
            result = client.admin.command("ping")
            assert result.get("ok") == 1.0
            db = client[_extract_database_name(MONGO_URI)]
            assert isinstance(db.list_collection_names(), list)
        finally:
            client.close()

    @pytest.mark.skipif(not MONGO_URI, reason="MONGO_URI not set")
    def test_database_name_matches_env(self):
        assert _extract_database_name(MONGO_URI) == "polymarket_copytrading"

    @pytest.mark.skipif(not MONGO_URI, reason="MONGO_URI not set")
    def test_trading_collection_accessible(self):
        from pymongo import MongoClient
        client = MongoClient(MONGO_URI)
        try:
            db = client[_extract_database_name(MONGO_URI)]
            assert db["trading"].name == "trading"
        finally:
            client.close()
