"""Issue #7 tests — redemption consolidation: retry-safe persistence, dedup, reconcile."""

import time
from decimal import Decimal
from pathlib import Path

import pytest

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from storage.sqlite.connection import create_connection
from storage.sqlite.migrations import apply_migrations
from storage.sqlite.redemptions_repo import RedemptionsRepo


def _prepare_db(path: Path) -> None:
    with create_connection(str(path)) as conn:
        apply_migrations(conn)
        conn.commit()


class TestRedemptionsRepo:
    def test_save_and_list_pending(self, tmp_path):
        db = tmp_path / "test_redemptions.db"
        _prepare_db(db)
        repo = RedemptionsRepo(str(db))

        repo.save_attempt({
            "attempt_id": "att-1",
            "user_id": "u1",
            "condition_id": "cond-1",
            "token_id": "tok-1",
            "size": "50",
            "status": "attempting",
        })

        pending = repo.list_pending_redemptions("u1")
        assert len(pending) == 1
        assert pending[0]["status"] == "attempting"
        assert pending[0]["attempt_id"] == "att-1"

    def test_confirmed_not_in_pending(self, tmp_path):
        db = tmp_path / "test_redemptions.db"
        _prepare_db(db)
        repo = RedemptionsRepo(str(db))

        repo.save_attempt({
            "attempt_id": "att-2",
            "user_id": "u1",
            "condition_id": "cond-2",
            "status": "confirmed",
        })

        pending = repo.list_pending_redemptions("u1")
        assert len(pending) == 0

    def test_mark_status(self, tmp_path):
        db = tmp_path / "test_redemptions.db"
        _prepare_db(db)
        repo = RedemptionsRepo(str(db))

        repo.save_attempt({
            "attempt_id": "att-3",
            "user_id": "u1",
            "condition_id": "cond-3",
            "status": "attempting",
        })

        ok = repo.mark_status(attempt_id="att-3", status="confirmed")
        assert ok is True

        pending = repo.list_pending_redemptions("u1")
        assert len(pending) == 0

    def test_is_attempted(self, tmp_path):
        db = tmp_path / "test_redemptions.db"
        _prepare_db(db)
        repo = RedemptionsRepo(str(db))

        assert repo.is_attempted(condition_id="cond-4", user_id="u1") is False

        repo.save_attempt({
            "attempt_id": "att-4",
            "user_id": "u1",
            "condition_id": "cond-4",
            "status": "confirmed",
        })

        assert repo.is_attempted(condition_id="cond-4", user_id="u1") is True

    def test_is_attempted_submitted_not_terminal(self, tmp_path):
        db = tmp_path / "test_redemptions.db"
        _prepare_db(db)
        repo = RedemptionsRepo(str(db))

        repo.save_attempt({
            "attempt_id": "att-5",
            "user_id": "u1",
            "condition_id": "cond-5",
            "status": "submitted",
        })

        # submitted is NOT terminal — should not block reconciliation
        assert repo.is_attempted(condition_id="cond-5", user_id="u1") is False

    def test_is_attempted_failed_does_not_count(self, tmp_path):
        db = tmp_path / "test_redemptions.db"
        _prepare_db(db)
        repo = RedemptionsRepo(str(db))

        repo.save_attempt({
            "attempt_id": "att-6",
            "user_id": "u1",
            "condition_id": "cond-6",
            "status": "failed",
        })

        assert repo.is_attempted(condition_id="cond-6", user_id="u1") is False

    def test_purge_old_attempts(self, tmp_path):
        db = tmp_path / "test_redemptions.db"
        _prepare_db(db)
        repo = RedemptionsRepo(str(db))

        # Insert old record
        repo.save_attempt({
            "attempt_id": "att-old",
            "user_id": "u1",
            "condition_id": "cond-old",
            "status": "failed",
            "created_at": time.time() - 100 * 86400,  # 100 days ago
        })

        # Insert recent record
        repo.save_attempt({
            "attempt_id": "att-new",
            "user_id": "u1",
            "condition_id": "cond-new",
            "status": "failed",
            "created_at": time.time(),
        })

        deleted = repo.purge_old_attempts(max_age_seconds=30 * 86400)
        assert deleted == 1

        # Old should be gone, new should remain
        assert repo.is_attempted(condition_id="cond-old", user_id="u1") is False
        assert repo.is_attempted(condition_id="cond-new", user_id="u1") is False  # failed, so false anyway

    def test_upsert_same_attempt_updates_status(self, tmp_path):
        db = tmp_path / "test_redemptions.db"
        _prepare_db(db)
        repo = RedemptionsRepo(str(db))

        repo.save_attempt({
            "attempt_id": "att-7",
            "user_id": "u1",
            "condition_id": "cond-7",
            "status": "attempting",
        })

        repo.save_attempt({
            "attempt_id": "att-7",
            "user_id": "u1",
            "condition_id": "cond-7",
            "status": "confirmed",
        })

        # Should only be 1 record, now confirmed
        assert repo.is_attempted(condition_id="cond-7", user_id="u1") is True
