"""Atomic submission-lock semantics (PG-A6 port).

Repo-level invariants live on the canonical async PG ``OrdersRepo``. The
ExecutionService integration test runs against the same async repo so
the lock contract is exercised end-to-end on the production code path.
"""

from __future__ import annotations

import asyncio
import contextlib
import sys
import time as _time
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pmcopy.cache.market_state_store import BookSnapshot, QuoteSnapshot, TickSnapshot
from pmcopy.services.executionService import ExecutionRequest, ExecutionService
from pmcopy.services.market_data_service import MarketDataBundle, MarketDataSource
from pmcopy.storage.postgres.orders_repo import OrdersRepo as PgOrdersRepo
from pmcopy.utils.createClobClient import MarketMeta
from tests.conftest import make_clob_gateway
from pmcopy.integrations.polymarket.models import VenueSubmitResult


@contextlib.asynccontextmanager
async def _pg_pool():
    try:
        from testcontainers.postgres import PostgresContainer
        import asyncpg
        from pmcopy.storage.postgres.migrations import apply_migrations as pg_apply
    except Exception as exc:
        pytest.skip(f"Docker / postgres image unavailable: {exc!r}")
    ctr = PostgresContainer("postgres:16-alpine")
    try:
        ctr.start()
    except Exception as exc:
        pytest.skip(f"Docker / postgres image unavailable: {exc!r}")
    try:
        dsn = ctr.get_connection_url(driver="asyncpg").replace(
            "postgresql+asyncpg://", "postgresql://"
        )
        # Give the ExecutionService integration test genuine concurrency.
        pool = await asyncpg.create_pool(dsn, min_size=2, max_size=10)
        async with pool.acquire() as conn:
            await pg_apply(conn)
        try:
            yield pool
        finally:
            await pool.close()
    finally:
        ctr.stop()


def _make_exec_config() -> SimpleNamespace:
    """Minimal AppConfig-shaped object for ExecutionService tests."""
    return SimpleNamespace(
        execution=SimpleNamespace(
            min_share_price=0,
            max_share_price=0,
            pending_submission_ttl_seconds=120,
            circuit_failure_threshold=5,
            circuit_cooldown_seconds=60,
            execution_require_fresh_book=False,
            book_spread_mode="absolute_bps",
            max_book_spread_bps=500,
            max_book_spread_ticks=0,
            slippage_tolerance=2.0,
            max_retries=3,
            retry_backoff_base=1.0,
            non_terminal_failure_max_attempts=3,
            transaction_fee_bps=20,
            gas_cost_usdc=0.03,
            risk_buffer_pct=0.10,
            paper_trade_only=False,
            enable_copy_decision_gate=False,
            enable_near_resolution_gate=False,
            near_resolution_gate_price=0.95,
            near_resolution_max_age_seconds=15,
            meta_cache_ttl_good=300,
            meta_cache_ttl_thin=30,
        ),
        policy_version="test",
        raw={},
    )


class _FeeSvc:
    async def get_fee_bps(self, _token_id: str) -> int:
        return 0


class _MetaSvc:
    async def get(self, token_id: str, refresh: bool = False):
        return MarketMeta(
            token_id=token_id,
            tick_size=Decimal("0.01"),
            min_order_size=Decimal("0"),
            neg_risk=False,
            best_bid=Decimal("0.49"),
            best_ask=Decimal("0.51"),
        )


def _make_mds(token_id: str = "t1") -> AsyncMock:
    now = _time.monotonic()
    bid, ask = Decimal("0.49"), Decimal("0.51")
    bundle = MarketDataBundle(
        token_id=token_id,
        quote=QuoteSnapshot(
            token_id=token_id,
            best_bid=bid,
            best_ask=ask,
            last_trade_price=None,
            source="wss",
            monotonic_ts=now,
        ),
        book=BookSnapshot(
            token_id=token_id,
            bids={bid: Decimal("100")},
            asks={ask: Decimal("100")},
            best_bid=bid,
            best_ask=ask,
            source="wss",
            monotonic_ts=now,
        ),
        tick=TickSnapshot(
            token_id=token_id,
            tick_size=Decimal("0.01"),
            min_order_size=Decimal("1"),
            neg_risk=False,
            source="wss",
            monotonic_ts=now,
        ),
        source=MarketDataSource.WSS,
    )
    mds = AsyncMock()
    mds.require_fresh_data = AsyncMock(return_value=bundle)
    return mds


@pytest.mark.asyncio
async def test_order_repository_exposes_atomic_lock_methods():
    """Pure repo-level lock semantics on the canonical PG ``OrdersRepo``."""
    async with _pg_pool() as pool:
        repo = PgOrdersRepo(pool)
        key = "lock-key"
        payload = {"user_id": "u1", "token_id": "t1"}

        assert await repo.try_acquire_submission_lock(key, "u1", "l1", payload) is True
        assert await repo.is_submission_locked(key) is True
        # Idempotent re-lock fails deterministically.
        assert await repo.try_acquire_submission_lock(key, "u1", "l1", payload) is False


@pytest.mark.asyncio
async def test_atomic_lock_blocks_duplicate_concurrent_submission():
    """ExecutionService integration test on async PG ``OrdersRepo``.

    Two concurrent ``execute()`` calls with identical request shape: the
    first wins the submission lock and POSTs once; the second observes
    the lock and returns ``duplicate_ignored`` / ``inflight_lock``.
    """
    async with _pg_pool() as pool:
        repo = PgOrdersRepo(pool)
        started = asyncio.Event()
        proceed = asyncio.Event()

        async def submit(request):
            started.set()
            await proceed.wait()
            return VenueSubmitResult(success=True, order_id="ord-1", status="submitted")

        gateway = make_clob_gateway()
        gateway.submit_marketable_order = AsyncMock(side_effect=submit)
        svc = ExecutionService(
            None,
            _FeeSvc(),
            repo,
            config=_make_exec_config(),
            clob_gateway=gateway,
            market_data_service=_make_mds(),
        )
        req = ExecutionRequest(
            user_id="u1",
            leader_id="l1",
            token_id="t1",
            side="BUY",
            amount=Decimal("5"),
            max_slippage_bps=100,
            reason="copy_trade",
            signal_id="sig-lock",
        )

        first_task = asyncio.create_task(svc.execute(req))
        await started.wait()

        second_result = await svc.execute(req)
        proceed.set()
        first_result = await first_task

        assert second_result.status in {"duplicate_ignored", "inflight_lock"}
        assert first_result.accepted is True
        assert gateway.submit_marketable_order.await_count == 1
