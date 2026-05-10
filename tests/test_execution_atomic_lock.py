from __future__ import annotations

import asyncio
import sys
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.executionService import ExecutionRequest, ExecutionService
from services.orderRepository import OrderRepository
from storage.sqlite.connection import create_connection
from storage.sqlite.migrations import apply_migrations
from utils.createClobClient import MarketMeta


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
    async def get(self, token_id: str, refresh: bool = False):  # noqa: ARG002
        return MarketMeta(
            token_id=token_id,
            tick_size=Decimal("0.01"),
            min_order_size=Decimal("0"),
            neg_risk=False,
            best_bid=Decimal("0.49"),
            best_ask=Decimal("0.51"),
        )


class _ConcurrentClob:
    def __init__(self):
        self.get_price = AsyncMock(return_value={"price": "0.50"})
        self.post_signed_order = AsyncMock(
            return_value={"success": True, "orderID": "ord-1", "status": "submitted"}
        )
        self.client = SimpleNamespace(
            get_order=lambda _order_id: {
                "status": "filled",
                "size_matched": "5",
                "avg_price": "0.50",
            }
        )
        self.first_create_started = asyncio.Event()
        self.release_first_create = asyncio.Event()
        self._first_create_seen = False

    async def create_market_order(self, **kwargs):  # noqa: ARG002
        if not self._first_create_seen:
            self._first_create_seen = True
            self.first_create_started.set()
            await self.release_first_create.wait()
        return {"signed": True}


def _prepare_db(path: Path) -> None:
    with create_connection(str(path)) as conn:
        apply_migrations(conn)
        conn.commit()


def test_order_repository_exposes_atomic_lock_methods(tmp_path):
    db_path = tmp_path / "state.db"
    _prepare_db(db_path)

    repo = OrderRepository(str(db_path))
    key = "lock-key"
    payload = {"user_id": "u1", "token_id": "t1"}

    assert repo.try_acquire_submission_lock(key, "u1", "l1", payload) is True
    assert repo.is_submission_locked(key) is True
    assert repo.try_acquire_submission_lock(key, "u1", "l1", payload) is False


@pytest.mark.asyncio
async def test_atomic_lock_blocks_duplicate_concurrent_submission(tmp_path):
    db_path = tmp_path / "state.db"
    _prepare_db(db_path)

    repo = OrderRepository(str(db_path))
    clob = _ConcurrentClob()
    svc = ExecutionService(clob, _MetaSvc(), _FeeSvc(), repo, config=_make_exec_config())
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
    await clob.first_create_started.wait()

    second_result = await svc.execute(req)
    clob.release_first_create.set()
    first_result = await first_task

    assert second_result.status in {"duplicate_ignored", "inflight_lock"}
    assert first_result.accepted is True
    assert clob.post_signed_order.await_count == 1
