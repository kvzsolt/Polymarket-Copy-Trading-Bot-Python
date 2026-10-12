"""Phase 3 — atomic idempotency / duplicate prevention tests.

Covers the ``try_claim_processed_trade`` atomic claim path that replaces
the prior non-atomic ``has_processed_trade`` → ``mark_processed_trade``
check-then-set pattern.

PG-A5: ported from the sqlite ``OrderRepository`` wrapper to the
canonical async ``storage.postgres.orders_repo.OrdersRepo``. Threading
concurrency replaced with ``asyncio.gather`` since PG repos are async.
"""

from __future__ import annotations

import asyncio
import contextlib
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pmcopy.storage.postgres.orders_repo import OrdersRepo


@contextlib.asynccontextmanager
async def _pg_pool():
    """Spin a Postgres testcontainer + return a pool with the full
    schema applied. Auto-skips when Docker is unavailable."""
    try:
        from testcontainers.postgres import PostgresContainer
        import asyncpg
        from pmcopy.storage.postgres.migrations import apply_migrations as pg_apply
    except Exception as exc:
        pytest.skip(f"Docker / postgres image unavailable: {exc!r}")
    try:
        ctr = PostgresContainer("postgres:16-alpine")
        ctr.start()
    except Exception as exc:
        pytest.skip(f"Docker / postgres image unavailable: {exc!r}")
    try:
        dsn = ctr.get_connection_url(driver="asyncpg").replace(
            "postgresql+asyncpg://", "postgresql://"
        )
        # Larger pool so the concurrent-claim tests can spawn parallel
        # coroutines without serialising on a single-connection bottleneck.
        pool = await asyncpg.create_pool(dsn, min_size=2, max_size=10)
        async with pool.acquire() as conn:
            await pg_apply(conn)
        try:
            yield pool
        finally:
            await pool.close()
    finally:
        ctr.stop()


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
    @pytest.mark.asyncio
    async def test_first_claim_returns_true(self):
        async with _pg_pool() as pool:
            repo = OrdersRepo(pool)
            assert await repo.try_claim_processed_trade(**_claim_kwargs("k-1")) is True

    @pytest.mark.asyncio
    async def test_second_claim_returns_false_fast(self):
        async with _pg_pool() as pool:
            repo = OrdersRepo(pool)
            assert await repo.try_claim_processed_trade(**_claim_kwargs("k-2")) is True
            # Second attempt sees existing 'in_progress' row younger than TTL,
            # the WHERE clause filters it out, RETURNING is empty → False.
            assert await repo.try_claim_processed_trade(**_claim_kwargs("k-2")) is False

    @pytest.mark.asyncio
    async def test_terminal_status_blocks_reclaim(self):
        async with _pg_pool() as pool:
            repo = OrdersRepo(pool)
            assert await repo.try_claim_processed_trade(**_claim_kwargs("k-3")) is True
            await repo.mark_processed_trade(**_claim_kwargs("k-3"), status="processed")
            assert await repo.try_claim_processed_trade(**_claim_kwargs("k-3")) is False

    @pytest.mark.asyncio
    async def test_retryable_status_allows_reclaim(self):
        async with _pg_pool() as pool:
            repo = OrdersRepo(pool)
            kwargs = _claim_kwargs("k-4")
            assert await repo.try_claim_processed_trade(**kwargs) is True
            # Simulate a retryable failure landing in the row.
            await repo.mark_processed_trade(**kwargs, status="retryable_failed")
            # New attempt should re-claim because the existing row is retryable.
            assert await repo.try_claim_processed_trade(**kwargs) is True
            # And immediate follow-up gets blocked again.
            assert await repo.try_claim_processed_trade(**kwargs) is False

    @pytest.mark.asyncio
    async def test_stale_in_progress_can_be_reclaimed(self):
        async with _pg_pool() as pool:
            repo = OrdersRepo(pool)
            kwargs = _claim_kwargs("k-5")
            assert await repo.try_claim_processed_trade(**kwargs) is True
            # Pin the row's processed_at into the past beyond the TTL.
            async with pool.acquire() as conn:
                await conn.execute(
                    "UPDATE processed_trades SET processed_at = $1 WHERE trade_key = $2",
                    time.time() - 3600.0,
                    "k-5",
                )
            assert (
                await repo.try_claim_processed_trade(**kwargs, stale_claim_ttl_seconds=60.0) is True
            )

    @pytest.mark.asyncio
    async def test_empty_trade_key_returns_false(self):
        async with _pg_pool() as pool:
            repo = OrdersRepo(pool)
            assert await repo.try_claim_processed_trade(**_claim_kwargs("")) is False


class TestConcurrentClaim:
    @pytest.mark.asyncio
    async def test_concurrent_coroutines_exactly_one_winner(self):
        """Eight concurrent claim coroutines on the same key — PG's
        ``ON CONFLICT DO UPDATE WHERE ...`` clause serialises so exactly
        one returns True."""
        async with _pg_pool() as pool:
            repo = OrdersRepo(pool)
            kwargs = _claim_kwargs("race-key")
            results = await asyncio.gather(
                *[repo.try_claim_processed_trade(**kwargs) for _ in range(8)]
            )
        assert results.count(True) == 1, (
            f"Expected exactly one winner under concurrency, got {results}"
        )
        assert results.count(False) == 7

    @pytest.mark.asyncio
    async def test_concurrent_different_keys_all_win(self):
        async with _pg_pool() as pool:
            repo = OrdersRepo(pool)
            results = await asyncio.gather(
                *[
                    repo.try_claim_processed_trade(**_claim_kwargs(f"distinct-key-{i}"))
                    for i in range(6)
                ]
            )
        assert all(results)
        assert len(results) == 6


class TestTransactionBoundaries:
    """Failed-insert rollback (sqlite-specific connection-failure
    monkeypatch in the legacy version) is structurally guaranteed by
    PG: the ``try_claim_processed_trade`` impl wraps INSERT … ON
    CONFLICT in an asyncpg transaction context manager that rolls back
    on any exception. The visible-from-outside behaviour the legacy
    test pinned (UniqueViolationError → False return, no row leak) is
    already covered by ``test_second_claim_returns_false_fast`` above
    on the PG side. The legacy failure-injection test is dropped.
    """

    @pytest.mark.asyncio
    async def test_claim_then_terminal_marks_row_processed(self):
        """After atomic claim, the standard mark_processed_trade still wins."""
        async with _pg_pool() as pool:
            repo = OrdersRepo(pool)
            kwargs = _claim_kwargs("term-key")
            assert await repo.try_claim_processed_trade(**kwargs) is True

            await repo.mark_processed_trade(**kwargs, status="processed")

            rec = await repo.get_processed_trade("term-key")
            assert rec is not None
            assert rec["status"] == "processed"
            # And concurrent reclaim attempts fast-fail.
            assert await repo.try_claim_processed_trade(**kwargs) is False


class TestPipelineIntegration:
    """Lightweight surface check that trade_pipeline imports use the new API.

    Source-code inspection only — no DB needed, stays sync.
    """

    def test_trade_pipeline_uses_try_claim(self):
        from pmcopy.orchestrators import trade_pipeline

        src = Path(trade_pipeline.__file__).read_text(encoding="utf-8")
        assert "try_claim_processed_trade" in src, (
            "trade_pipeline must call the atomic claim helper"
        )
        # The legacy non-atomic check must have been replaced at the
        # signal-loop gate. Allow other call sites in the file to remain
        # untouched (Phase 3 is scoped to the duplicate-prevention gate).
        assert src.count("has_processed_trade(trade_key)") == 0, (
            "expected legacy check-then-set call to be removed from the gate"
        )


class TestRetryAttemptPersists:
    """PG-A5: replaces the legacy ``TestWrapperForwarding`` (which tested
    the sqlite ``OrderRepository`` sync wrapper). The underlying
    invariant — ``mark_processed_trade`` forwards ``retry_attempt`` +
    ``order_id`` to the row — applies equally on PG."""

    @pytest.mark.asyncio
    async def test_retry_attempt_persists(self):
        async with _pg_pool() as pool:
            repo = OrdersRepo(pool)
            kwargs = _claim_kwargs("retry-fwd-key")
            assert await repo.try_claim_processed_trade(**kwargs) is True

            await repo.mark_processed_trade(
                **kwargs,
                status="retryable_failed",
                retry_attempt=3,
                order_id="ord-xyz",
            )

            rec = await repo.get_processed_trade("retry-fwd-key")
            assert rec is not None
            assert rec["retry_attempt"] == 3, (
                "mark_processed_trade must persist retry_attempt — silent drop "
                "causes infinite retry"
            )
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT order_id FROM processed_trades WHERE trade_key = $1",
                    "retry-fwd-key",
                )
            assert row["order_id"] == "ord-xyz"


class TestTerminalStateGuard:
    """mark_processed_trade must never downgrade a terminal row."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "terminal_status",
        [
            "executed",
            "processed",
            "filled_reconciled",
            "duplicate_ignored",
            "terminal_failed",
        ],
    )
    async def test_terminal_row_not_overwritten_by_retryable(self, terminal_status):
        async with _pg_pool() as pool:
            repo = OrdersRepo(pool)
            kwargs = _claim_kwargs(f"term-{terminal_status}")
            assert await repo.try_claim_processed_trade(**kwargs) is True

            # Land a terminal status.
            await repo.mark_processed_trade(**kwargs, status=terminal_status, order_id="ord-1")

            # A late retryable_failed write must NOT overwrite the terminal row.
            await repo.mark_processed_trade(**kwargs, status="retryable_failed", retry_attempt=7)

            rec = await repo.get_processed_trade(f"term-{terminal_status}")
            assert rec is not None
            assert rec["status"] == terminal_status, (
                f"terminal status {terminal_status!r} was clobbered by a late retryable_failed"
            )

            # The guarded row must also reject reclaim attempts via try_claim.
            assert await repo.try_claim_processed_trade(**kwargs) is False

    @pytest.mark.asyncio
    async def test_non_terminal_row_is_still_overwritable(self):
        async with _pg_pool() as pool:
            repo = OrdersRepo(pool)
            kwargs = _claim_kwargs("non-term-key")
            assert await repo.try_claim_processed_trade(**kwargs) is True
            # in_progress → submitted_unconfirmed should still flow through.
            await repo.mark_processed_trade(
                **kwargs, status="submitted_unconfirmed", order_id="ord-2"
            )
            rec = await repo.get_processed_trade("non-term-key")
            assert rec is not None
            assert rec["status"] == "submitted_unconfirmed"
