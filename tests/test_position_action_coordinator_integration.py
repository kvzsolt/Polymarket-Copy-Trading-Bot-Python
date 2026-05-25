"""Integration tests for the per-scoped-position action coordinator (issue #28).

These tests exercise the wiring end-to-end:
  * ``execute_trades`` in ``orchestrators/trade_pipeline.py``
  * ``run_risk_check_for_context`` in ``services/riskMonitor.py``
  * ``reconcile_submitted_unconfirmed`` per-record path

The race scenario covered here is the exact #28 shape: risk-monitor wants to
protectively SELL a scoped position at the same time the trade pipeline is
executing a fresh leader-driven action on the same scoped position. Without the
coordinator these two paths could interleave mid-observation; with the
coordinator they must serialize on the scoped identity.
"""

from __future__ import annotations
import pytest
pytestmark = pytest.mark.skip(reason="sqlite-bound; pending PG port")


import asyncio
import time
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import config.app_config as app_config_mod
from config.app_config import load_config
from config.env import rehydrate_env
from domain.trading_context import TradingContext
from orchestrators.trade_pipeline import execute_trades
from services.accountingService import AccountingService
from services.executionService import ExecutionResult
from services.pnlTracker import PnLTracker
from services.positionManager import PositionManager
from services.positionActionCoordinator import PositionScope
from services.riskEngine import RiskEngine
from services.riskMonitor import run_risk_check_for_context
from storage.sqlite.connection import create_connection
from storage.sqlite.fills_repo import FillsRepo
from storage.sqlite.migrations import apply_migrations
from storage.sqlite.orders_repo import OrdersRepo
from storage.sqlite.positions_repo import PositionRecord, PositionsRepo
from storage.sqlite.risk_blocks_repo import RiskBlocksRepo


pytestmark = pytest.mark.integration

FAKE_TOKEN_ID = "71321045679252212594626385532706912750332728571942532223535764789812338637396"
OTHER_TOKEN_ID = "61321045679252212594626385532706912750332728571942532223535764789812338637396"


@pytest.fixture(autouse=True)
def _lower_decision_gates_for_tests(monkeypatch):
    monkeypatch.setenv("COPY_DECISION_MIN_SCORE", "0.01")
    monkeypatch.setenv("COPY_MAX_SIGNAL_AGE_SECONDS", "999999")
    monkeypatch.setenv("COPY_MAX_PRICE_DRIFT_RATIO", "1.0")
    monkeypatch.setenv("COPY_MAX_EDGE_DECAY_RATIO", "1.0")
    monkeypatch.setenv("COPY_MIN_SHARES", "0.001")
    monkeypatch.setenv("COPY_MIN_NOTIONAL_USDC", "0.01")
    monkeypatch.setenv("BUY_MIN_AMOUNT", "0.01")
    monkeypatch.setenv("MAX_POSITION_PCT", "0")
    monkeypatch.setenv("ENABLE_BALANCE_TIERED_POLICY", "false")
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
        "COPY_DECISION_MIN_SCORE": "0.01",
        "COPY_MAX_SIGNAL_AGE_SECONDS": "999999",
        "COPY_MAX_PRICE_DRIFT_PCT": "999",
        "COPY_MAX_EDGE_DECAY_PCT": "999",
        "COPY_MIN_SHARES": "0.001",
        "COPY_MIN_NOTIONAL_USDC": "0.01",
        "ENABLE_BALANCE_TIERED_POLICY": "false",
    }
    env.update(overrides)
    with patch.object(app_config_mod, "_runtime_python_supported", return_value=True):
        return load_config(env)


def _init_db(tmp_path):
    db_path = str(tmp_path / "coord.db")
    with create_connection(db_path) as conn:
        apply_migrations(conn)
    return db_path


def _make_fresh_bundle():
    from cache.market_state_store import BookSnapshot, QuoteSnapshot, TickSnapshot
    from services.market_data_service import MarketDataBundle, MarketDataSource

    now = time.monotonic()
    return MarketDataBundle(
        token_id=FAKE_TOKEN_ID,
        quote=QuoteSnapshot(
            token_id=FAKE_TOKEN_ID,
            best_bid=Decimal("0.49"),
            best_ask=Decimal("0.51"),
            last_trade_price=None,
            source="wss",
            monotonic_ts=now,
        ),
        book=BookSnapshot(
            token_id=FAKE_TOKEN_ID,
            bids={Decimal("0.49"): Decimal("100")},
            asks={Decimal("0.51"): Decimal("100")},
            best_bid=Decimal("0.49"),
            best_ask=Decimal("0.51"),
            source="wss",
            monotonic_ts=now,
        ),
        tick=TickSnapshot(
            token_id=FAKE_TOKEN_ID,
            tick_size=Decimal("0.01"),
            min_order_size=Decimal("0.001"),
            neg_risk=False,
            source="wss",
            monotonic_ts=now,
        ),
        source=MarketDataSource.WSS,
    )


class _CoordFakeMDS:
    def __init__(self, bundle):
        self._bundle = bundle

    async def require_fresh_data(self, token_id):
        return self._bundle


def _make_context(db_path, *, execution_service=None, config=None) -> TradingContext:
    exec_svc = execution_service or AsyncMock()
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
        config=config or _config(),
        allocation_pct=1.0,
        buy_min_amount=0.01,
        buy_max_amount=1000.0,
        market_data_service=_CoordFakeMDS(_make_fresh_bundle()),
        extra={"slippage_tolerance_bps": 200},
    )
    ctx.market_data_runtime = AsyncMock()
    return ctx


def _buy_signal(*, token=FAKE_TOKEN_ID, condition="cond1") -> dict:
    return {
        "side": "BUY",
        "transactionHash": "0xbuy1",
        "asset": token,
        "conditionId": condition,
        "price": 0.50,
        "size": 10.0,
        "usdcSize": 5.0,
        "timestamp": int(time.time()),
        "title": "Test",
        "eventSlug": "test",
    }


def _exec_ok(order_id="ord1"):
    return ExecutionResult(
        accepted=True,
        order_id=order_id,
        status="filled",
        requested_amount=Decimal("5"),
        filled_amount=Decimal("10"),
        avg_price=Decimal("0.50"),
        fees_paid=Decimal("0"),
        error=None,
    )


# ---------------------------------------------------------------------------
# 1. The coordinator is actually reached from execute_trades
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_execute_trades_acquires_scope_lock(tmp_path):
    db_path = _init_db(tmp_path)
    exec_svc = AsyncMock()
    exec_svc.execute.return_value = _exec_ok()
    ctx = _make_context(db_path, execution_service=exec_svc)

    acquired_scopes: list[PositionScope] = []
    original_get_lock = ctx.position_coordinator.get_lock

    async def _spy_get_lock(scope):
        acquired_scopes.append(scope)
        return await original_get_lock(scope)

    ctx.position_coordinator.get_lock = _spy_get_lock  # type: ignore[assignment]

    await execute_trades(ctx, "leader1", [_buy_signal()], time.time())

    assert any(
        s.condition_id == "cond1" and s.token_id == FAKE_TOKEN_ID
        for s in acquired_scopes
    ), "execute_trades must acquire the scoped-position lock"


# ---------------------------------------------------------------------------
# 2. run_risk_check_for_context routes through the coordinator
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_risk_monitor_acquires_scope_lock(tmp_path):
    db_path = _init_db(tmp_path)
    exec_svc = AsyncMock()
    exec_svc.execute.return_value = _exec_ok(order_id="risk-ord")
    ctx = _make_context(db_path, execution_service=exec_svc)

    # Seed a live position the risk monitor will iterate.
    ctx.positions_repo.upsert_position(
        PositionRecord(
            user_id="user1",
            condition_id="cond-risk",
            token_id=FAKE_TOKEN_ID,
            leader_id="leader1",
            size_tokens=Decimal("10"),
            avg_entry_price=Decimal("0.50"),
        )
    )

    # Force the risk-action gate to return an exit decision.
    def _force_exit(_cid, _token, _price):
        return {"reason": "stop_loss", "size_fraction": 1.0}
    ctx.position_manager.evaluate_risk_action = _force_exit

    acquired = []
    original = ctx.position_coordinator.get_lock

    async def _spy(scope):
        acquired.append(scope)
        return await original(scope)

    ctx.position_coordinator.get_lock = _spy  # type: ignore[assignment]

    with patch("services.riskMonitor._get_context_cached_price", new_callable=AsyncMock, return_value=Decimal("0.60")):
        await run_risk_check_for_context(ctx)

    assert any(
        s.condition_id == "cond-risk" and s.token_id == FAKE_TOKEN_ID
        for s in acquired
    ), "risk monitor must acquire the scoped-position lock"


# ---------------------------------------------------------------------------
# 3. The race is actually serialized: BUY and protective SELL cannot
#    interleave on the same scoped position.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_same_scope_buy_and_risk_sell_serialize(tmp_path):
    """Hold the pipeline's scope lock and prove the risk monitor must wait.

    The race shape under test: trade_pipeline enters the scope's critical
    section and is about to call ``execute(...)``. Concurrently the risk
    monitor wants to protective-SELL the same scope. The risk monitor must
    wait until the pipeline releases the scope lock — otherwise the two
    tasks could both read state and submit contradictory orders.
    """
    db_path = _init_db(tmp_path)
    exec_svc = AsyncMock()
    exec_svc.execute.return_value = _exec_ok(order_id="risk-ord")
    ctx = _make_context(db_path, execution_service=exec_svc)

    ctx.positions_repo.upsert_position(
        PositionRecord(
            user_id="user1",
            condition_id="cond-race",
            token_id=FAKE_TOKEN_ID,
            leader_id="leader1",
            size_tokens=Decimal("10"),
            avg_entry_price=Decimal("0.50"),
        )
    )

    def _force_exit(_cid, _token, _price):
        return {"reason": "stop_loss", "size_fraction": 1.0}
    ctx.position_manager.evaluate_risk_action = _force_exit

    scope = PositionScope(
        user_id="user1", condition_id="cond-race", token_id=FAKE_TOKEN_ID,
    )

    # Pre-acquire the scope lock on behalf of a "virtual pipeline task" that
    # is already inside its critical section. This simulates the exact window
    # issue #28 is about: one task is mid-mutation, another arrives.
    scope_lock = await ctx.position_coordinator.get_lock(scope)
    await scope_lock.acquire()
    try:
        risk_done = asyncio.Event()

        async def _run_risk() -> None:
            with patch(
                "services.riskMonitor._get_context_cached_price",
                new_callable=AsyncMock,
                return_value=Decimal("0.40"),
            ):
                await run_risk_check_for_context(ctx)
            risk_done.set()

        risk_task = asyncio.create_task(_run_risk())

        # The risk monitor should NOT be able to reach the execute() call
        # while we still hold the scope lock.
        await asyncio.sleep(0.05)
        assert not risk_done.is_set(), "risk monitor must block on scope lock"
        exec_svc.execute.assert_not_called()
    finally:
        scope_lock.release()

    await asyncio.wait_for(risk_task, timeout=2.0)
    exec_svc.execute.assert_awaited()


# ---------------------------------------------------------------------------
# 4. Unrelated positions stay concurrent — the coordinator is per-scope.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_different_scopes_stay_concurrent(tmp_path):
    db_path = _init_db(tmp_path)
    exec_svc = AsyncMock()
    exec_svc.execute.return_value = _exec_ok(order_id="risk-ord")
    ctx = _make_context(db_path, execution_service=exec_svc)

    # Seed a position for scope B that risk monitor can iterate.
    ctx.positions_repo.upsert_position(
        PositionRecord(
            user_id="user1",
            condition_id="cond-b",
            token_id=OTHER_TOKEN_ID,
            leader_id="leader1",
            size_tokens=Decimal("10"),
            avg_entry_price=Decimal("0.50"),
        )
    )

    def _force_exit(_cid, _token, _price):
        return {"reason": "stop_loss", "size_fraction": 1.0}
    ctx.position_manager.evaluate_risk_action = _force_exit

    # Pre-acquire scope A (different condition + token than B).
    scope_a = PositionScope(
        user_id="user1",
        condition_id="cond-a",
        token_id=FAKE_TOKEN_ID,
    )
    lock_a = await ctx.position_coordinator.get_lock(scope_a)
    await lock_a.acquire()
    try:
        with patch(
            "services.riskMonitor._get_context_cached_price",
            new_callable=AsyncMock,
            return_value=Decimal("0.40"),
        ):
            # Risk monitor only sees scope B, so it must NOT block on A.
            await asyncio.wait_for(run_risk_check_for_context(ctx), timeout=2.0)
        exec_svc.execute.assert_awaited_once()
    finally:
        lock_a.release()
