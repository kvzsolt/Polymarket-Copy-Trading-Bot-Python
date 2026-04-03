from __future__ import annotations

import time
from decimal import Decimal

import pytest

from domain.trading_context import TradingContext
from orchestrators.trade_pipeline import execute_trades
from services.executionService import ExecutionResult
from services.positionManager import PositionManager
from storage.sqlite.connection import create_connection
from storage.sqlite.fills_repo import FillsRepo
from storage.sqlite.migrations import apply_migrations
from storage.sqlite.orders_repo import OrdersRepo
from storage.sqlite.positions_repo import PositionsRepo


class _FakeExecutionService:
    async def execute(self, req):
        return ExecutionResult(
            accepted=True,
            order_id=f"ord-{req.user_id}-{req.side}",
            status="filled",
            requested_amount=Decimal(str(req.amount)),
            filled_amount=Decimal(str(req.amount)),
            avg_price=Decimal("0.5"),
            fees_paid=Decimal("0"),
            error=None,
        )


class _FakePnlTracker:
    def __init__(self):
        self.buys: list[tuple[str, float]] = []
        self.sells: list[tuple[str, float]] = []

    def record_buy(self, *, condition_id: str, size_tokens: float, **_kwargs):
        self.buys.append((condition_id, size_tokens))

    def record_sell(self, *, condition_id: str, size_tokens: float, **_kwargs):
        self.sells.append((condition_id, size_tokens))


def _build_context(*, user_id: str, db_path: str) -> TradingContext:
    return TradingContext(
        user_id=user_id,
        wallet_address=f"wallet-{user_id}",
        client=object(),
        positions_repo=PositionsRepo(db_path),
        fills_repo=FillsRepo(db_path),
        orders_repo=OrdersRepo(db_path),
        pnl_tracker=_FakePnlTracker(),
        risk_engine=object(),
        accounting_service=object(),
        position_manager=PositionManager(),
        execution_service=_FakeExecutionService(),
        allocation_pct=1.0,
        buy_min_amount=1.0,
        buy_max_amount=1000.0,
        max_market_exposure_usdc=1000.0,
        extra={"slippage_tolerance_bps": 50},
    )


@pytest.mark.asyncio
async def test_multi_user_positions_and_pnl_are_isolated(tmp_path, monkeypatch):
    # Lower decision gates so test signals pass through
    monkeypatch.setenv("COPY_DECISION_MIN_SCORE", "0.01")
    monkeypatch.setenv("COPY_MAX_SIGNAL_AGE_SECONDS", "999999")
    monkeypatch.setenv("COPY_MAX_PRICE_DRIFT_PCT", "100")
    monkeypatch.setenv("COPY_MAX_EDGE_DECAY_PCT", "100")
    from config.env import rehydrate_env
    rehydrate_env()
    # Reset pipeline singletons so they pick up new ENV
    import orchestrators.trade_pipeline as _tp
    _tp._leader_scorer = None
    _tp._copy_decision = None

    db_path = tmp_path / "state.db"
    with create_connection(str(db_path)) as conn:
        apply_migrations(conn)

    leader_id = "0xleader"
    buy_signal = [
        {
            "transactionHash": "0xtx-buy",
            "timestamp": int(time.time()),
            "type": "TRADE",
            "side": "BUY",
            "asset": "token-1",
            "conditionId": "cond-1",
            "size": 10,
            "price": 0.5,
            "usdcSize": 5,
        }
    ]

    ctx_a = _build_context(user_id="user-a", db_path=str(db_path))
    ctx_b = _build_context(user_id="user-b", db_path=str(db_path))

    await execute_trades(ctx_a, leader_id, buy_signal, now_ts=time.time())
    await execute_trades(ctx_b, leader_id, buy_signal, now_ts=time.time())

    pos_a = ctx_a.positions_repo.get_position("user-a", "cond-1")
    pos_b = ctx_b.positions_repo.get_position("user-b", "cond-1")
    assert pos_a is not None
    assert pos_b is not None
    assert pos_a.size_tokens > 0
    assert pos_b.size_tokens > 0

    sell_signal = [
        {
            "transactionHash": "0xtx-sell-a",
            "timestamp": int(time.time()),
            "type": "TRADE",
            "side": "SELL",
            "asset": "token-1",
            "conditionId": "cond-1",
            "size": 0,
            "price": 0.5,
        }
    ]
    await execute_trades(ctx_a, leader_id, sell_signal, now_ts=time.time())

    pos_a_after = ctx_a.positions_repo.get_position("user-a", "cond-1")
    pos_b_after = ctx_b.positions_repo.get_position("user-b", "cond-1")
    assert pos_a_after is None
    assert pos_b_after is not None
    assert pos_b_after.size_tokens > 0
