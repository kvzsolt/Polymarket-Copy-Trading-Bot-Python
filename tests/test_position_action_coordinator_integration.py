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

import asyncio
import contextlib
import time
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import config.app_config as app_config_mod
from tests.config_helpers import load_test_config
from application.trading_context import TradingContext
from domain.execution import ExecutionMetadata
from orchestrators.trade_pipeline import execute_trades
from services.accountingService import AccountingService
from services.executionService import ExecutionResult
from services.pnlTracker import PnLTracker
from services.positionManager import PositionManager
from services.positionActionCoordinator import PositionScope
from services.risk.risk_engine import RiskEngine
from services.risk.risk_monitor import run_risk_check_for_context
from storage.postgres.fills_repo import FillsRepo
from storage.postgres.orders_repo import OrdersRepo
from storage.postgres.positions_repo import PositionRecord, PositionsRepo
from storage.postgres.risk_blocks_repo import RiskBlocksRepo


@contextlib.asynccontextmanager
async def _pg_pool():
    try:
        from testcontainers.postgres import PostgresContainer
        import asyncpg
        from storage.postgres.migrations import apply_migrations as pg_apply
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
        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=5)
        async with pool.acquire() as conn:
            await pg_apply(conn)
        try:
            yield pool
        finally:
            await pool.close()
    finally:
        ctr.stop()


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
        return load_test_config(env)


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

    async def require_fresh_data(self, token_id, **_kwargs):
        return self._bundle

    async def require_execution_metadata(self, token_id, **_kwargs):
        return ExecutionMetadata(
            token_id=token_id,
            tick_size=self._bundle.tick.tick_size,
            min_order_size=self._bundle.tick.min_order_size,
            neg_risk=self._bundle.tick.neg_risk,
        )


def _make_context(pool, *, execution_service=None, config=None, tmp_path=None) -> TradingContext:
    exec_svc = execution_service or AsyncMock()
    pnl_log = str((tmp_path or "/tmp") / "pnl.jsonl") if tmp_path else "/tmp/pnl.jsonl"
    ctx = TradingContext(
        user_id="user1",
        wallet_address="0x" + "a" * 40,
        client=MagicMock(),
        positions_repo=PositionsRepo(pool),
        fills_repo=FillsRepo(pool),
        orders_repo=OrdersRepo(pool),
        risk_blocks_repo=RiskBlocksRepo(pool),
        pnl_tracker=PnLTracker(log_path=pnl_log),
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
    async with _pg_pool() as pool:
        exec_svc = AsyncMock()
        exec_svc.execute.return_value = _exec_ok()
        ctx = _make_context(pool, execution_service=exec_svc, tmp_path=tmp_path)

        acquired_scopes: list[PositionScope] = []
        original_get_lock = ctx.position_coordinator.action_coordinator.get_lock

        async def _spy_get_lock(scope):
            acquired_scopes.append(scope)
            return await original_get_lock(scope)

        ctx.position_coordinator.action_coordinator.get_lock = _spy_get_lock  # type: ignore[assignment]

        await execute_trades(ctx, "leader1", [_buy_signal()], time.time())

        assert any(
            s.condition_id == "cond1" and s.token_id == FAKE_TOKEN_ID for s in acquired_scopes
        ), "execute_trades must acquire the scoped-position lock"


# ---------------------------------------------------------------------------
# 2. run_risk_check_for_context routes through the coordinator
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_risk_monitor_acquires_scope_lock(tmp_path):
    async with _pg_pool() as pool:
        exec_svc = AsyncMock()
        exec_svc.execute.return_value = _exec_ok(order_id="risk-ord")
        ctx = _make_context(pool, execution_service=exec_svc, tmp_path=tmp_path)

        # Seed a live position the risk monitor will iterate.
        await ctx.positions_repo.upsert_position(
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
        original = ctx.position_coordinator.action_coordinator.get_lock

        async def _spy(scope):
            acquired.append(scope)
            return await original(scope)

        ctx.position_coordinator.action_coordinator.get_lock = _spy  # type: ignore[assignment]

        with patch(
            "services.risk.risk_monitor._get_context_cached_price",
            new_callable=AsyncMock,
            return_value=Decimal("0.60"),
        ):
            await run_risk_check_for_context(ctx)

        assert any(
            s.condition_id == "cond-risk" and s.token_id == FAKE_TOKEN_ID for s in acquired
        ), "risk monitor must acquire the scoped-position lock"


# ---------------------------------------------------------------------------
# 3. The race is actually serialized: BUY and protective SELL cannot
#    interleave on the same scoped position.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_busy_scope_defers_risk_exit_until_next_tick(tmp_path):
    """A busy scope defers protection until a later unlocked tick without overselling."""
    async with _pg_pool() as pool:
        exec_svc = AsyncMock()
        exec_svc.execute.return_value = _exec_ok(order_id="risk-ord")
        ctx = _make_context(pool, execution_service=exec_svc, tmp_path=tmp_path)

        await ctx.positions_repo.upsert_position(
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
            user_id="user1",
            condition_id="cond-race",
            token_id=FAKE_TOKEN_ID,
        )

        async with ctx.position_coordinator.acquire(scope):
            with patch(
                "services.risk.risk_monitor._get_context_cached_price",
                new_callable=AsyncMock,
                return_value=Decimal("0.40"),
            ):
                await run_risk_check_for_context(ctx)
            exec_svc.execute.assert_not_awaited()

        with patch(
            "services.risk.risk_monitor._get_context_cached_price",
            new_callable=AsyncMock,
            return_value=Decimal("0.40"),
        ):
            await run_risk_check_for_context(ctx)
        exec_svc.execute.assert_awaited_once()


# ---------------------------------------------------------------------------
# 4. Unrelated positions stay concurrent — the coordinator is per-scope.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_different_scopes_stay_concurrent(tmp_path):
    async with _pg_pool() as pool:
        exec_svc = AsyncMock()
        exec_svc.execute.return_value = _exec_ok(order_id="risk-ord")
        ctx = _make_context(pool, execution_service=exec_svc, tmp_path=tmp_path)

        # Seed a position for scope B that risk monitor can iterate.
        await ctx.positions_repo.upsert_position(
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
        lock_a = await ctx.position_coordinator.action_coordinator.get_lock(scope_a)
        await lock_a.acquire()
        try:
            with patch(
                "services.risk.risk_monitor._get_context_cached_price",
                new_callable=AsyncMock,
                return_value=Decimal("0.40"),
            ):
                # Risk monitor only sees scope B, so it must NOT block on A.
                await asyncio.wait_for(run_risk_check_for_context(ctx), timeout=2.0)
            exec_svc.execute.assert_awaited_once()
        finally:
            lock_a.release()
