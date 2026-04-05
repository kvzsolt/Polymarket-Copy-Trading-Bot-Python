"""Tests for stale-record purge in idempotency + orders_repo."""
from __future__ import annotations

import os
import sqlite3
import tempfile
import time

import pytest

os.environ.setdefault("PRIVATE_KEY", "0x" + "ab" * 32)
os.environ.setdefault("RPC_URL", "https://polygon-rpc.com")

from storage.sqlite.migrations import apply_migrations
from storage.sqlite.orders_repo import OrdersRepo
from storage.sqlite.connection import create_connection


@pytest.fixture()
def repo(tmp_path):
    db = str(tmp_path / "test.db")
    with sqlite3.connect(db) as conn:
        apply_migrations(conn)
        conn.commit()
    return OrdersRepo(db)


def _insert_processed(repo: OrdersRepo, key: str, age_seconds: float):
    ts = time.time() - age_seconds
    with create_connection(repo.db_path) as conn:
        conn.execute(
            "INSERT INTO processed_trades (trade_key, user_id, leader_id, tx_hash, side, asset, condition_id, status, processed_at)"
            " VALUES (?, '', '', '', '', '', '', 'processed', ?)",
            (key, ts),
        )
        conn.commit()


class TestPurgeProcessedTrades:
    def test_purge_removes_old(self, repo):
        _insert_processed(repo, "old1", 40 * 86400)
        _insert_processed(repo, "old2", 35 * 86400)
        _insert_processed(repo, "fresh", 5 * 86400)
        deleted = repo.purge_old_processed_trades(30 * 86400)
        assert deleted == 2
        assert repo.has_processed_trade("fresh")
        assert not repo.has_processed_trade("old1")

    def test_purge_nothing_when_all_fresh(self, repo):
        _insert_processed(repo, "a", 1000)
        deleted = repo.purge_old_processed_trades(30 * 86400)
        assert deleted == 0

    def test_custom_age(self, repo):
        _insert_processed(repo, "k1", 7200)  # 2 hours old
        deleted = repo.purge_old_processed_trades(3600)  # 1 hour cutoff
        assert deleted == 1


class TestIdempotencyPurge:
    def test_purge_stale_records_convenience(self, repo, monkeypatch):
        """Test the convenience wrapper in utils/idempotency.py."""
        _insert_processed(repo, "old_trade", 40 * 86400)
        _insert_processed(repo, "new_trade", 1 * 86400)

        import utils.idempotency as idem
        monkeypatch.setattr(idem, "_repo", lambda: repo)
        total = idem.purge_stale_records(30 * 86400)
        assert total == 1  # only the old trade
