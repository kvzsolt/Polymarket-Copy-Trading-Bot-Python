"""PR 3: market-data authority in the trade pipeline.

Covers:
  - new signal token is subscribed via market_data_runtime before decision
  - signal is skipped when market data stays stale past the timeout
  - decision-service receives min_order_size / tick_size from the MDS bundle
"""
from __future__ import annotations

import time
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import config.app_config as app_config_mod
from cache.market_state_store import BookSnapshot, QuoteSnapshot, TickSnapshot
from config.app_config import load_config
from config.env import rehydrate_env
from domain.trading_context import TradingContext
from orchestrators.trade_pipeline import execute_trades
from services.accountingService import AccountingService
from services.copyDecisionService import CopyDecision
from services.executionService import ExecutionResult
from services.market_data_service import (
    MarketDataBundle,
    MarketDataSource,
    StaleDataError,
)
from services.pnlTracker import PnLTracker
from services.positionManager import PositionManager
from services.riskEngine import RiskEngine
from storage.sqlite.connection import create_connection
from storage.sqlite.fills_repo import FillsRepo
from storage.sqlite.migrations import apply_migrations
from storage.sqlite.orders_repo import OrdersRepo
from storage.sqlite.positions_repo import PositionsRepo
from storage.sqlite.risk_blocks_repo import RiskBlocksRepo

FAKE_TOKEN_ID = "71321045679252212594626385532706912750332728571942532223535764789812338637396"


@pytest.fixture(autouse=True)
def _lower_decision_gates(monkeypatch):
    monkeypatch.setenv("COPY_DECISION_MIN_SCORE", "0.01")
    monkeypatch.setenv("COPY_MAX_SIGNAL_AGE_SECONDS", "999999")
    monkeypatch.setenv("COPY_MAX_PRICE_DRIFT_RATIO", "1.0")
    monkeypatch.setenv("COPY_MAX_EDGE_DECAY_RATIO", "1.0")
    monkeypatch.setenv("COPY_MIN_SHARES", "0.001")
    monkeypatch.setenv("COPY_MIN_NOTIONAL_USDC", "0.01")
    monkeypatch.setenv("BUY_MIN_AMOUNT", "0.01")
    monkeypatch.setenv("MAX_POSITION_PCT", "0")
    monkeypatch.setenv("ENABLE_BALANCE_TIERED_POLICY", "false")
    # Keep the MDS retry loop tight in tests.
    monkeypatch.setenv("MARKET_DATA_SUBSCRIBE_TIMEOUT_SECONDS", "0.4")
    rehydrate_env()


@pytest.fixture(autouse=True)
def _mock_balance(monkeypatch):
    async def _fake_balance(_addr):
        return 10000.0
    monkeypatch.setattr("orchestrators.trade_pipeline.get_my_balance", _fake_balance)


def _config(**overrides):
    env = {
        "PRIVATE_KEY": "0x" + "a" * 64,
        "FUNDER_ADDRESS": "0x" + "b" * 40,
        "USER_ADDRESS": "0x" + "c" * 40,
        "LEADER_WALLETS": "0x" + "d" * 40,
        "RPC_URL": "https://polygon-rpc.example.com",
        "CHAIN_ID": "137",
        "BUY_MIN_AMOUNT": "0.01",
        "BUY_MAX_AMOUNT": "1000",
        "MAX_MARKET_EXPOSURE_USDC": "300",
        "SLIPPAGE_TOLERANCE": "2.0",
        "MARKET_DATA_SUBSCRIBE_TIMEOUT_SECONDS": "0.4",
    }
    env.update(overrides)
    with patch.object(app_config_mod, "_runtime_python_supported", return_value=True):
        return load_config(env)


def _make_bundle(
    *,
    token_id: str = FAKE_TOKEN_ID,
    min_order_size: Decimal = Decimal("3"),
    tick_size: Decimal = Decimal("0.01"),
) -> MarketDataBundle:
    now = time.monotonic()
    return MarketDataBundle(
        token_id=token_id,
        quote=QuoteSnapshot(
            token_id=token_id,
            best_bid=Decimal("0.49"),
            best_ask=Decimal("0.51"),
            last_trade_price=None,
            source="wss",
            monotonic_ts=now,
        ),
        book=BookSnapshot(
            token_id=token_id,
            bids={Decimal("0.49"): Decimal("100")},
            asks={Decimal("0.51"): Decimal("100")},
            best_bid=Decimal("0.49"),
            best_ask=Decimal("0.51"),
            source="wss",
            monotonic_ts=now,
        ),
        tick=TickSnapshot(
            token_id=token_id,
            tick_size=tick_size,
            min_order_size=min_order_size,
            neg_risk=False,
            source="wss",
            monotonic_ts=now,
        ),
        source=MarketDataSource.WSS,
    )


def _init_db(tmp_path):
    db_path = str(tmp_path / "pr3.db")
    with create_connection(db_path) as conn:
        apply_migrations(conn)
    return db_path


def _signal(*, side="BUY", tx_hash="0xpr3", price=0.50, size=10.0):
    return {
        "side": side,
        "transactionHash": tx_hash,
        "asset": FAKE_TOKEN_ID,
        "conditionId": "cond-pr3",
        "price": price,
        "size": size,
        "usdcSize": size * price,
        "timestamp": int(time.time()),
        "title": "Test Market",
        "eventSlug": "test-slug",
    }


def _exec_result_filled():
    return ExecutionResult(
        accepted=True,
        order_id="ord-pr3",
        status="filled",
        requested_amount=Decimal("5"),
        filled_amount=Decimal("10"),
        avg_price=Decimal("0.50"),
        fees_paid=Decimal("0.01"),
        error=None,
    )


class _RecordingDecisionService:
    def __init__(self):
        self.last_call: dict | None = None

    def decide_buy(
        self,
        *,
        trade,
        leader_score,
        live_price,
        base_notional_usdc,
        max_notional_usdc,
        market_min_size,
        tick_size=0.0,
    ):
        self.last_call = {
            "market_min_size": float(market_min_size),
            "tick_size": float(tick_size),
            "max_notional_usdc": float(max_notional_usdc),
        }
        return CopyDecision(
            allow=True,
            skip_code="ok",
            reason="approved",
            side="BUY",
            decision_score=0.9,
            leader_score=float(leader_score.score),
            trade_score=0.9,
            live_price=float(live_price),
            reference_price=float(trade.get("price") or 0),
            target_notional_usdc=float(max_notional_usdc),
            target_size_tokens=float(max_notional_usdc) / max(float(live_price), 1e-9),
            price_drift_pct=0.0,
            edge_decay_pct=0.0,
            details={},
        )


class _FakeMDS:
    def __init__(self, bundle, *, raise_stale: bool = False):
        self._bundle = bundle
        self._raise_stale = raise_stale
        self.calls: list[str] = []

    async def require_fresh_data(self, token_id):
        self.calls.append(token_id)
        if self._raise_stale:
            raise StaleDataError(f"stale {token_id}")
        return self._bundle


def _make_context(
    db_path,
    *,
    exec_svc,
    mds,
    runtime,
    decision_svc=None,
):
    ctx = TradingContext(
        user_id="user1",
        wallet_address="0x" + "a" * 40,
        client=MagicMock(),
        positions_repo=PositionsRepo(db_path),
        fills_repo=FillsRepo(db_path),
        orders_repo=OrdersRepo(db_path),
        risk_blocks_repo=RiskBlocksRepo(db_path),
        pnl_tracker=PnLTracker(log_path=str(db_path) + ".pnl"),
        risk_engine=RiskEngine(),
        accounting_service=AccountingService(),
        position_manager=PositionManager(),
        execution_service=exec_svc,
        config=_config(),
        allocation_pct=1.0,
        buy_min_amount=0.01,
        buy_max_amount=1000.0,
        market_data_service=mds,
        extra={"slippage_tolerance_bps": 200},
    )
    ctx.market_data_runtime = runtime
    if decision_svc is not None:
        ctx.copy_decision_service = decision_svc
    return ctx


@pytest.mark.asyncio
async def test_trade_pipeline_subscribes_new_token_before_decision(tmp_path):
    db_path = _init_db(tmp_path)
    exec_svc = AsyncMock()
    exec_svc.execute.return_value = _exec_result_filled()
    runtime = AsyncMock()
    decision_svc = _RecordingDecisionService()

    ctx = _make_context(
        db_path,
        exec_svc=exec_svc,
        mds=_FakeMDS(_make_bundle()),
        runtime=runtime,
        decision_svc=decision_svc,
    )

    await execute_trades(ctx, "leader1", [_signal()], time.time())

    # Token must be subscribed before any execute() call lands.
    runtime.subscribe_tokens.assert_awaited()
    subscribed = runtime.subscribe_tokens.await_args.args[0]
    assert FAKE_TOKEN_ID in subscribed
    exec_svc.execute.assert_awaited_once()


@pytest.mark.asyncio
async def test_trade_pipeline_skips_when_market_data_stale_after_subscribe(tmp_path):
    db_path = _init_db(tmp_path)
    exec_svc = AsyncMock()
    runtime = AsyncMock()

    ctx = _make_context(
        db_path,
        exec_svc=exec_svc,
        mds=_FakeMDS(_make_bundle(), raise_stale=True),
        runtime=runtime,
    )

    results = await execute_trades(ctx, "leader1", [_signal()], time.time())

    assert len(results) == 1
    assert results[0].status == "skipped"
    assert results[0].reason == "market_data_stale"
    exec_svc.execute.assert_not_awaited()
    # Subscribe was still attempted — the runtime gets a chance to converge.
    runtime.subscribe_tokens.assert_awaited()


@pytest.mark.asyncio
async def test_trade_pipeline_sizing_uses_bundle_tick_and_min_order_size(tmp_path):
    db_path = _init_db(tmp_path)
    exec_svc = AsyncMock()
    exec_svc.execute.return_value = _exec_result_filled()
    runtime = AsyncMock()
    decision_svc = _RecordingDecisionService()
    bundle = _make_bundle(
        min_order_size=Decimal("7.5"),
        tick_size=Decimal("0.001"),
    )

    ctx = _make_context(
        db_path,
        exec_svc=exec_svc,
        mds=_FakeMDS(bundle),
        runtime=runtime,
        decision_svc=decision_svc,
    )

    await execute_trades(ctx, "leader1", [_signal()], time.time())

    assert decision_svc.last_call is not None
    assert decision_svc.last_call["market_min_size"] == pytest.approx(7.5)
    assert decision_svc.last_call["tick_size"] == pytest.approx(0.001)
