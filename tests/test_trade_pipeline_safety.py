"""Safety tests for the trade pipeline — duplicate prevention, failure handling, position updates."""

from __future__ import annotations

import time
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from config.env import ENV, rehydrate_env
from domain.trading_context import TradingContext
from orchestrators.trade_pipeline import execute_trades, build_trade_key
from services.executionService import ExecutionResult
from services.pnlTracker import PnLTracker
from services.positionManager import PositionManager
from services.riskEngine import RiskEngine
from services.accountingService import AccountingService
from storage.sqlite.connection import create_connection
from storage.sqlite.fills_repo import FillsRepo


@pytest.fixture(autouse=True)
def _lower_decision_gates_for_tests(monkeypatch):
    """Lower copy-decision thresholds so test signals pass the gate."""
    monkeypatch.setenv("COPY_DECISION_MIN_SCORE", "0.01")
    monkeypatch.setenv("COPY_MAX_SIGNAL_AGE_SECONDS", "999999")
    monkeypatch.setenv("COPY_MAX_PRICE_DRIFT_PCT", "999")
    monkeypatch.setenv("COPY_MAX_EDGE_DECAY_PCT", "999")
    monkeypatch.setenv("COPY_MIN_SHARES", "0.001")
    monkeypatch.setenv("COPY_MIN_NOTIONAL_USDC", "0.01")
    monkeypatch.setenv("BUY_MIN_AMOUNT", "0.01")
    monkeypatch.setenv("MIN_ORDER_SIZE", "0.00001")
    rehydrate_env()
    import orchestrators.trade_pipeline as _tp
    _tp._leader_scorer = None
    _tp._copy_decision = None
from storage.sqlite.migrations import apply_migrations
from storage.sqlite.orders_repo import OrdersRepo
from storage.sqlite.positions_repo import PositionsRepo


def _init_db(tmp_path):
    db_path = str(tmp_path / "test.db")
    with create_connection(db_path) as conn:
        apply_migrations(conn)
    return db_path


def _make_context(db_path, *, execution_service=None):
    return TradingContext(
        user_id="user1",
        wallet_address="0x" + "a" * 40,
        client=MagicMock(),
        positions_repo=PositionsRepo(db_path),
        fills_repo=FillsRepo(db_path),
        orders_repo=OrdersRepo(db_path),
        pnl_tracker=PnLTracker(log_path=str(db_path) + ".pnl"),
        risk_engine=RiskEngine(),
        accounting_service=AccountingService(),
        position_manager=PositionManager(),
        execution_service=execution_service,
        allocation_pct=1.0,
        buy_min_amount=0.01,
        buy_max_amount=1000.0,
        extra={"slippage_tolerance_bps": 200},
    )


def _signal(*, side="BUY", tx_hash="0xabc", asset="token1", condition_id="cond1", price=0.50, size=10.0):
    return {
        "side": side,
        "transactionHash": tx_hash,
        "asset": asset,
        "conditionId": condition_id,
        "price": price,
        "size": size,
        "usdcSize": size * price,
        "timestamp": int(time.time()),
        "title": "Test Market",
        "eventSlug": "test-slug",
    }


def _exec_result(*, accepted=True, filled=Decimal("10"), status="filled", order_id="ord1", error=None):
    return ExecutionResult(
        accepted=accepted,
        order_id=order_id,
        status=status,
        requested_amount=Decimal("5"),
        filled_amount=filled,
        avg_price=Decimal("0.50") if filled > 0 else None,
        fees_paid=Decimal("0.01"),
        error=error,
    )


@pytest.mark.asyncio
async def test_duplicate_trade_is_skipped(tmp_path):
    db_path = _init_db(tmp_path)
    exec_svc = AsyncMock()
    ctx = _make_context(db_path, execution_service=exec_svc)

    signal = _signal()
    trade_key = build_trade_key("user1", "leader1", signal)
    ctx.orders_repo.mark_processed_trade(
        trade_key=trade_key, user_id="user1", leader_id="leader1",
        tx_hash="0xabc", side="BUY", asset="token1",
        condition_id="cond1", status="executed",
    )

    results = await execute_trades(ctx, "leader1", [signal], time.time())
    assert len(results) == 1
    assert results[0].status == "skipped"
    assert results[0].reason == "already_processed"
    exec_svc.execute.assert_not_called()


@pytest.mark.asyncio
async def test_transient_failure_not_marked_processed(tmp_path):
    db_path = _init_db(tmp_path)
    exec_svc = AsyncMock()
    exec_svc.execute.return_value = _exec_result(
        accepted=False, filled=Decimal("0"), status="failed", order_id=None, error="connection timeout"
    )
    ctx = _make_context(db_path, execution_service=exec_svc)

    signal = _signal()
    results = await execute_trades(ctx, "leader1", [signal], time.time())
    assert len(results) == 1
    assert results[0].status == "failed"

    # The trade should NOT be in processed_trades so it can be retried
    trade_key = build_trade_key("user1", "leader1", signal)
    assert ctx.orders_repo.has_processed_trade(trade_key) is False


@pytest.mark.asyncio
async def test_transient_failure_marked_processed_after_retry_budget(tmp_path, monkeypatch):
    db_path = _init_db(tmp_path)
    exec_svc = AsyncMock()
    exec_svc.execute.return_value = _exec_result(
        accepted=False, filled=Decimal("0"), status="failed", order_id=None, error="connection timeout"
    )
    ctx = _make_context(db_path, execution_service=exec_svc)
    monkeypatch.setitem(ENV, "NON_TERMINAL_FAILURE_MAX_ATTEMPTS", "2")

    signal = _signal(tx_hash="0xretry")
    trade_key = build_trade_key("user1", "leader1", signal)

    first = await execute_trades(ctx, "leader1", [signal], time.time())
    assert first[0].status == "failed"
    assert "retry budget exhausted" not in str(first[0].reason or "")
    assert ctx.orders_repo.has_processed_trade(trade_key) is False

    second = await execute_trades(ctx, "leader1", [signal], time.time())
    assert second[0].status == "failed"
    assert "retry budget exhausted" in str(second[0].reason or "")
    assert ctx.orders_repo.has_processed_trade(trade_key) is True

    third = await execute_trades(ctx, "leader1", [signal], time.time())
    assert third[0].status == "skipped"
    assert third[0].reason == "already_processed"
    assert exec_svc.execute.await_count == 2


@pytest.mark.asyncio
async def test_hard_rejection_is_marked_processed(tmp_path):
    db_path = _init_db(tmp_path)
    exec_svc = AsyncMock()
    exec_svc.execute.return_value = _exec_result(
        accepted=False, filled=Decimal("0"), status="rejected", order_id=None, error="amount below min_order_size"
    )
    ctx = _make_context(db_path, execution_service=exec_svc)

    signal = _signal()
    results = await execute_trades(ctx, "leader1", [signal], time.time())
    assert len(results) == 1
    assert results[0].status == "failed"

    trade_key = build_trade_key("user1", "leader1", signal)
    assert ctx.orders_repo.has_processed_trade(trade_key) is True


@pytest.mark.asyncio
async def test_submitted_unconfirmed_is_marked_processed(tmp_path):
    db_path = _init_db(tmp_path)
    exec_svc = AsyncMock()
    exec_svc.execute.return_value = _exec_result(
        accepted=True, filled=Decimal("0"), status="submitted_unconfirmed", order_id="ord1"
    )
    ctx = _make_context(db_path, execution_service=exec_svc)

    signal = _signal()
    results = await execute_trades(ctx, "leader1", [signal], time.time())
    assert len(results) == 1
    assert results[0].status == "submitted_unconfirmed"

    trade_key = build_trade_key("user1", "leader1", signal)
    assert ctx.orders_repo.has_processed_trade(trade_key) is True


@pytest.mark.asyncio
async def test_sell_with_no_position_skipped(tmp_path):
    db_path = _init_db(tmp_path)
    exec_svc = AsyncMock()
    ctx = _make_context(db_path, execution_service=exec_svc)

    signal = _signal(side="SELL")
    results = await execute_trades(ctx, "leader1", [signal], time.time())
    assert len(results) == 1
    assert results[0].status == "skipped"
    assert results[0].reason == "no_position"

    trade_key = build_trade_key("user1", "leader1", signal)
    assert ctx.orders_repo.has_processed_trade(trade_key) is True
    exec_svc.execute.assert_not_called()


@pytest.mark.asyncio
async def test_buy_fill_updates_position_repo(tmp_path):
    db_path = _init_db(tmp_path)
    exec_svc = AsyncMock()
    exec_svc.execute.return_value = _exec_result(
        accepted=True, filled=Decimal("10"), status="filled", order_id="ord1"
    )
    ctx = _make_context(db_path, execution_service=exec_svc)

    signal = _signal(price=0.50, size=20.0)
    results = await execute_trades(ctx, "leader1", [signal], time.time())
    assert len(results) == 1
    assert results[0].status == "executed"

    pos = ctx.positions_repo.get_position("user1", "cond1")
    assert pos is not None
    assert pos.size_tokens == Decimal("10")
    assert pos.avg_entry_price == Decimal("0.50")


@pytest.mark.asyncio
async def test_sell_reduces_position(tmp_path):
    db_path = _init_db(tmp_path)
    exec_svc = AsyncMock()
    ctx = _make_context(db_path, execution_service=exec_svc)

    # Set up existing position
    from storage.sqlite.positions_repo import PositionRecord
    ctx.positions_repo.upsert_position(PositionRecord(
        user_id="user1", condition_id="cond1", token_id="token1",
        leader_id="leader1", size_tokens=Decimal("20"), avg_entry_price=Decimal("0.40"),
    ))

    exec_svc.execute.return_value = _exec_result(
        accepted=True, filled=Decimal("10"), status="filled", order_id="ord2"
    )

    signal = _signal(side="SELL", price=0.60, size=10.0)
    results = await execute_trades(ctx, "leader1", [signal], time.time())
    assert len(results) == 1
    assert results[0].status == "executed"

    pos = ctx.positions_repo.get_position("user1", "cond1")
    assert pos is not None
    assert pos.size_tokens == Decimal("10")
