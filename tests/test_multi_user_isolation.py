from __future__ import annotations

import time
from decimal import Decimal
from types import SimpleNamespace

import pytest

from config.app_config import load_config
from domain.trading_context import TradingContext
from orchestrators.trade_pipeline import execute_trades
from services.executionService import ExecutionResult
from services.positionManager import PositionManager
from storage.sqlite.connection import create_connection
from storage.sqlite.fills_repo import FillsRepo
from storage.sqlite.migrations import apply_migrations
from storage.sqlite.orders_repo import OrdersRepo
from storage.sqlite.positions_repo import PositionsRepo
from storage.sqlite.risk_blocks_repo import RiskBlocksRepo

FAKE_TOKEN_ID = "71321045679252212594626385532706912750332728571942532223535764789812338637396"


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
    import config.app_config as app_config_mod
    import time
    from decimal import Decimal as _D
    from unittest.mock import AsyncMock, patch
    from cache.market_state_store import BookSnapshot, QuoteSnapshot, TickSnapshot
    from services.market_data_service import MarketDataBundle, MarketDataSource

    with patch.object(app_config_mod, "_runtime_python_supported", return_value=True):
        cfg = load_config()

    now = time.monotonic()
    bundle = MarketDataBundle(
        token_id=FAKE_TOKEN_ID,
        quote=QuoteSnapshot(token_id=FAKE_TOKEN_ID, best_bid=_D("0.49"), best_ask=_D("0.51"), last_trade_price=None, source="wss", monotonic_ts=now),
        book=BookSnapshot(token_id=FAKE_TOKEN_ID, bids={_D("0.49"): _D("100")}, asks={_D("0.51"): _D("100")}, best_bid=_D("0.49"), best_ask=_D("0.51"), source="wss", monotonic_ts=now),
        tick=TickSnapshot(token_id=FAKE_TOKEN_ID, tick_size=_D("0.01"), min_order_size=_D("0.001"), neg_risk=False, source="wss", monotonic_ts=now),
        source=MarketDataSource.WSS,
    )
    mds = AsyncMock()
    mds.require_fresh_data = AsyncMock(return_value=bundle)

    ctx = TradingContext(
        user_id=user_id,
        wallet_address=("0x" + ("a" if user_id.endswith("a") else "b") * 40),
        client=object(),
        positions_repo=PositionsRepo(db_path),
        fills_repo=FillsRepo(db_path),
        orders_repo=OrdersRepo(db_path),
        risk_blocks_repo=RiskBlocksRepo(db_path),
        pnl_tracker=_FakePnlTracker(),
        risk_engine=object(),
        accounting_service=object(),
        position_manager=PositionManager(),
        execution_service=_FakeExecutionService(),
        config=cfg,
        allocation_pct=1.0,
        buy_min_amount=1.0,
        buy_max_amount=1000.0,
        max_market_exposure_usdc=1000.0,
        market_data_service=mds,
        extra={"slippage_tolerance_bps": 50},
    )
    ctx.market_data_runtime = AsyncMock()

    # PR6.1 — coordinator mandatory in live mode. Pass-through to the fake
    # ExecutionService used by these tests.
    fake_exec = ctx.execution_service

    class _PassThrough:
        async def submit_copy_trade(self, intent):
            from services.execution.coordinator import ExecutionCoordinatorResult
            from services.execution.types import ExecutionRequest

            req = ExecutionRequest(
                user_id=intent.user_id,
                leader_id=intent.leader_wallet or "",
                token_id=intent.token_id,
                side=intent.side,
                amount=intent.size,
                max_slippage_bps=intent.max_slippage_bps,
                reason=intent.reason or intent.source,
                signal_id=intent.signal_id,
                condition_id=intent.condition_id,
                recovery_metadata=dict(intent.recovery_metadata or {}),
            )
            result = await fake_exec.execute(req)
            return ExecutionCoordinatorResult(
                accepted=getattr(result, "accepted", False),
                status=getattr(result, "status", ""),
                order_id=getattr(result, "order_id", None),
                lifecycle_status=getattr(result, "status", ""),
                raw_result=result,
            )

    ctx.execution_coordinator = _PassThrough()
    return ctx


@pytest.mark.asyncio
async def test_multi_user_positions_and_pnl_are_isolated(tmp_path, monkeypatch):
    # Lower decision gates so test signals pass through
    monkeypatch.setenv("COPY_DECISION_MIN_SCORE", "0.01")
    monkeypatch.setenv("COPY_MAX_SIGNAL_AGE_SECONDS", "999999")
    monkeypatch.setenv("COPY_MAX_PRICE_DRIFT_PCT", "100")
    monkeypatch.setenv("COPY_MAX_EDGE_DECAY_PCT", "100")
    monkeypatch.setenv("PRIVATE_KEY", "0x" + "a" * 64)
    monkeypatch.setenv("FUNDER_ADDRESS", "0x" + "b" * 40)
    monkeypatch.setenv("RPC_URL", "https://polygon-rpc.example.com")
    monkeypatch.setenv("CHAIN_ID", "137")
    monkeypatch.setenv("LEADER_WALLETS", "0x" + "c" * 40)
    monkeypatch.setenv("SIGNATURE_TYPE", "0")
    from config.env import rehydrate_env
    rehydrate_env()
    import orchestrators.trade_pipeline as trade_pipeline

    async def _fake_balance(_wallet: str):
        return 1000.0

    monkeypatch.setattr(trade_pipeline, "get_my_balance", _fake_balance)

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
            "asset": FAKE_TOKEN_ID,
            "conditionId": "cond-1",
            "size": 10,
            "price": 0.5,
            "usdcSize": 10,
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
            "asset": FAKE_TOKEN_ID,
            "conditionId": "cond-1",
            "size": 0,
            "price": 0.5,
            "_leader_sell_fraction": 1.0,  # full close
        }
    ]
    await execute_trades(ctx_a, leader_id, sell_signal, now_ts=time.time())

    pos_a_after = ctx_a.positions_repo.get_position("user-a", "cond-1")
    pos_b_after = ctx_b.positions_repo.get_position("user-b", "cond-1")
    assert pos_a_after is None
    assert pos_b_after is not None
    assert pos_b_after.size_tokens > 0
