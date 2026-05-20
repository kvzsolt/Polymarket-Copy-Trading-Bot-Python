"""Phase 3 — atomic idempotency / duplicate prevention tests.

Covers the new ``try_claim_processed_trade`` atomic claim path that replaces
the prior non-atomic ``has_processed_trade`` → ``mark_processed_trade``
check-then-set pattern.
"""
from __future__ import annotations

import sqlite3
import sys
import threading
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.orderRepository import OrderRepository
from storage.sqlite.connection import create_connection
from storage.sqlite.migrations import apply_migrations
from storage.sqlite.orders_repo import OrdersRepo


def _prepare_db(path: Path) -> None:
    with create_connection(str(path)) as conn:
        apply_migrations(conn)
        conn.commit()


def _make_repo(tmp_path: Path) -> OrderRepository:
    db_path = tmp_path / "phase03_state.db"
    _prepare_db(db_path)
    return OrderRepository(str(db_path))


def _claim_kwargs(trade_key: str) -> dict:
    return dict(
        trade_key=trade_key,
        user_id="u1",
        leader_id="l1",
        tx_hash="0xabc",
        side="BUY",
        asset="tok1",
        condition_id="cond1",
    )


class TestAtomicClaim:
    def test_first_claim_returns_true(self, tmp_path):
        repo = _make_repo(tmp_path)
        assert repo.try_claim_processed_trade(**_claim_kwargs("k-1")) is True

    def test_second_claim_returns_false_fast(self, tmp_path):
        repo = _make_repo(tmp_path)
        assert repo.try_claim_processed_trade(**_claim_kwargs("k-2")) is True
        # Second attempt sees existing 'in_progress' row younger than TTL,
        # the WHERE clause filters it out, RETURNING is empty → False.
        assert repo.try_claim_processed_trade(**_claim_kwargs("k-2")) is False

    def test_terminal_status_blocks_reclaim(self, tmp_path):
        repo = _make_repo(tmp_path)
        # First claim, then terminalize via mark_processed_trade.
        assert repo.try_claim_processed_trade(**_claim_kwargs("k-3")) is True
        # OrdersRepo direct call (the wrapper does not expose this kwarg).
        OrdersRepo(repo._repo.db_path).mark_processed_trade(
            **_claim_kwargs("k-3"), status="processed"
        )
        assert repo.try_claim_processed_trade(**_claim_kwargs("k-3")) is False

    def test_retryable_status_allows_reclaim(self, tmp_path):
        repo = _make_repo(tmp_path)
        kwargs = _claim_kwargs("k-4")
        assert repo.try_claim_processed_trade(**kwargs) is True
        # Simulate a retryable failure landing in the row.
        OrdersRepo(repo._repo.db_path).mark_processed_trade(
            **kwargs, status="retryable_failed"
        )
        # New attempt should re-claim because the existing row is retryable.
        assert repo.try_claim_processed_trade(**kwargs) is True
        # And immediate follow-up gets blocked again.
        assert repo.try_claim_processed_trade(**kwargs) is False

    def test_stale_in_progress_can_be_reclaimed(self, tmp_path):
        repo = _make_repo(tmp_path)
        kwargs = _claim_kwargs("k-5")
        assert repo.try_claim_processed_trade(**kwargs) is True
        # Pin the row's processed_at into the past beyond the TTL.
        with create_connection(repo._repo.db_path) as conn:
            conn.execute(
                "UPDATE processed_trades SET processed_at = ? WHERE trade_key = ?",
                (time.time() - 3600.0, "k-5"),
            )
            conn.commit()
        assert (
            repo.try_claim_processed_trade(
                **kwargs, stale_claim_ttl_seconds=60.0
            )
            is True
        )

    def test_empty_trade_key_returns_false(self, tmp_path):
        repo = _make_repo(tmp_path)
        assert repo.try_claim_processed_trade(**_claim_kwargs("")) is False


class TestConcurrentClaim:
    def test_concurrent_threads_exactly_one_winner(self, tmp_path):
        repo = _make_repo(tmp_path)
        kwargs = _claim_kwargs("race-key")

        results: list[bool] = []
        results_lock = threading.Lock()
        start_barrier = threading.Barrier(8)

        def attempt() -> None:
            start_barrier.wait()  # release all workers at once
            claimed = repo.try_claim_processed_trade(**kwargs)
            with results_lock:
                results.append(claimed)

        threads = [threading.Thread(target=attempt) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert results.count(True) == 1, (
            f"Expected exactly one winner under concurrency, got {results}"
        )
        assert results.count(False) == 7

    def test_concurrent_different_keys_all_win(self, tmp_path):
        repo = _make_repo(tmp_path)
        results: list[bool] = []
        results_lock = threading.Lock()
        start_barrier = threading.Barrier(6)

        def attempt(i: int) -> None:
            start_barrier.wait()
            claimed = repo.try_claim_processed_trade(
                **_claim_kwargs(f"distinct-key-{i}")
            )
            with results_lock:
                results.append(claimed)

        threads = [threading.Thread(target=attempt, args=(i,)) for i in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert all(results)
        assert len(results) == 6


class TestTransactionBoundaries:
    def test_failed_insert_does_not_create_partial_row(self, tmp_path, monkeypatch):
        """Force an IntegrityError mid-call and assert no row leaks through."""
        repo = _make_repo(tmp_path)
        underlying = OrdersRepo(repo._repo.db_path)

        original_connect = create_connection

        class _ConnFailing:
            def __init__(self, real):
                self._real = real

            def execute(self, sql, params=()):
                raise sqlite3.IntegrityError("simulated failure")

            def commit(self):
                self._real.commit()

            def rollback(self):
                self._real.rollback()

            def __enter__(self):
                return self

            def __exit__(self, *args):
                self._real.close()
                return False

        def _fake_connect(path: str):
            real = original_connect(path)
            return _ConnFailing(real)

        # Patch only the connection symbol the orders_repo module uses.
        monkeypatch.setattr(
            "storage.sqlite.orders_repo.create_connection", _fake_connect
        )

        ok = underlying.try_claim_processed_trade(**_claim_kwargs("rollback-key"))
        assert ok is False  # swallowed IntegrityError → "not claimed"

        # Confirm no row was persisted by the failed call.
        with original_connect(repo._repo.db_path) as conn:
            row = conn.execute(
                "SELECT trade_key FROM processed_trades WHERE trade_key = ?",
                ("rollback-key",),
            ).fetchone()
        assert row is None

    def test_claim_then_terminal_marks_row_processed(self, tmp_path):
        """After atomic claim, the standard mark_processed_trade still wins."""
        repo = _make_repo(tmp_path)
        kwargs = _claim_kwargs("term-key")
        assert repo.try_claim_processed_trade(**kwargs) is True

        OrdersRepo(repo._repo.db_path).mark_processed_trade(
            **kwargs, status="processed"
        )

        rec = repo.get_processed_trade("term-key")
        assert rec is not None
        assert rec["status"] == "processed"
        # And concurrent reclaim attempts fast-fail.
        assert repo.try_claim_processed_trade(**kwargs) is False


class TestPipelineIntegration:
    """Lightweight surface check that trade_pipeline imports use the new API."""

    def test_trade_pipeline_uses_try_claim(self):
        from orchestrators import trade_pipeline

        src = Path(trade_pipeline.__file__).read_text(encoding="utf-8")
        assert "try_claim_processed_trade" in src, (
            "trade_pipeline must call the atomic claim helper"
        )
        # The legacy non-atomic check must have been replaced at the
        # signal-loop gate. Allow other call sites in the file to remain
        # untouched (Phase 3 is scoped to the duplicate-prevention gate).
        gate_block = src.split("def execute_trade_pipeline", 1)
        # If the function name differs, still assert at least one removal.
        assert src.count("has_processed_trade(trade_key)") == 0, (
            "expected legacy check-then-set call to be removed from the gate"
        )


class TestWrapperForwarding:
    """Ensures OrderRepository wrapper forwards retry_attempt + order_id.

    Regression: prior wrapper hardcoded the kwargs it forwarded to the
    underlying OrdersRepo, silently dropping retry_attempt and order_id.
    The pipeline persisted ``retry_attempt=0`` on every failure, the
    retry counter never advanced, the budget never exhausted, and stale
    rows were re-claimable → duplicate live submissions.
    """

    def test_retry_attempt_persists_through_wrapper(self, tmp_path):
        repo = _make_repo(tmp_path)
        kwargs = _claim_kwargs("retry-fwd-key")
        # Initial claim creates the row.
        assert repo.try_claim_processed_trade(**kwargs) is True

        repo.mark_processed_trade(
            **kwargs,
            status="retryable_failed",
            retry_attempt=3,
            order_id="ord-xyz",
        )

        rec = repo.get_processed_trade("retry-fwd-key")
        assert rec is not None
        assert rec["retry_attempt"] == 3, (
            "wrapper must forward retry_attempt — silent drop causes infinite retry"
        )
        # order_id should also persist via the COALESCE path.
        with create_connection(repo._repo.db_path) as conn:
            row = conn.execute(
                "SELECT order_id FROM processed_trades WHERE trade_key = ?",
                ("retry-fwd-key",),
            ).fetchone()
        assert row["order_id"] == "ord-xyz"


class TestTerminalStateGuard:
    """mark_processed_trade must never downgrade a terminal row."""

    @pytest.mark.parametrize(
        "terminal_status",
        ["executed", "processed", "filled_reconciled", "duplicate_ignored", "terminal_failed"],
    )
    def test_terminal_row_not_overwritten_by_retryable(self, tmp_path, terminal_status):
        repo = _make_repo(tmp_path)
        kwargs = _claim_kwargs(f"term-{terminal_status}")
        assert repo.try_claim_processed_trade(**kwargs) is True

        # Land a terminal status.
        repo.mark_processed_trade(**kwargs, status=terminal_status, order_id="ord-1")

        # A late retryable_failed write must NOT overwrite the terminal row.
        repo.mark_processed_trade(
            **kwargs, status="retryable_failed", retry_attempt=7
        )

        rec = repo.get_processed_trade(f"term-{terminal_status}")
        assert rec is not None
        assert rec["status"] == terminal_status, (
            f"terminal status {terminal_status!r} was clobbered by a late retryable_failed"
        )

        # The guarded row must also reject reclaim attempts via try_claim.
        assert repo.try_claim_processed_trade(**kwargs) is False

    def test_non_terminal_row_is_still_overwritable(self, tmp_path):
        repo = _make_repo(tmp_path)
        kwargs = _claim_kwargs("non-term-key")
        assert repo.try_claim_processed_trade(**kwargs) is True
        # in_progress → submitted_unconfirmed should still flow through.
        repo.mark_processed_trade(**kwargs, status="submitted_unconfirmed", order_id="ord-2")
        rec = repo.get_processed_trade("non-term-key")
        assert rec is not None
        assert rec["status"] == "submitted_unconfirmed"
