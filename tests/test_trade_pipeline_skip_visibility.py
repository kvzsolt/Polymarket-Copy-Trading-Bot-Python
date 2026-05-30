"""Skip-reason visibility in the trade pipeline.

Every decision path that declines to copy a detected trade must surface a
``⚠️ COPY NOT EXECUTED`` panel (``print_skip_reason``) so the operator is not
left with a silent "NEW TRADE DETECTED" followed by nothing. These tests patch
``orchestrators.trade_pipeline.print_skip_reason`` and assert it fires on the
paths that were previously JSONL-only.

The pipeline requires a Postgres-backed repo set, so each test uses the same
testcontainers harness as ``test_trade_pipeline_market_data_authority`` and
skips when Docker is unavailable.
"""
from __future__ import annotations

import contextlib
import time
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import config.app_config as app_config_mod
from cache.market_state_store import BookSnapshot, QuoteSnapshot, TickSnapshot
from config.app_config import load_config
from config.env import rehydrate_env
from domain.trading_context import TradingContext
from orchestrators.trade_pipeline import execute_trades
from services.accountingService import AccountingService
from services.executionService import ExecutionResult
from services.market_data_service import (
    MarketDataBundle,
    MarketDataSource,
    StaleDataError,
)
from services.pnlTracker import PnLTracker
from services.positionManager import PositionManager
from services.risk.risk_engine import RiskEngine
from storage.postgres.fills_repo import FillsRepo
from storage.postgres.orders_repo import OrdersRepo
from storage.postgres.positions_repo import PositionsRepo
from storage.postgres.risk_blocks_repo import RiskBlocksRepo

FAKE_TOKEN_ID = "71321045679252212594626385532706912750332728571942532223535764789812338637396"


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
        "MARKET_DATA_PRIMARY_SOURCE": "wss",
    }
    env.update(overrides)
    with patch.object(app_config_mod, "_runtime_python_supported", return_value=True):
        return load_config(env)


def _make_bundle(*, token_id: str = FAKE_TOKEN_ID) -> MarketDataBundle:
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
            tick_size=Decimal("0.01"),
            min_order_size=Decimal("3"),
            neg_risk=False,
            source="wss",
            monotonic_ts=now,
        ),
        source=MarketDataSource.WSS,
    )


def _signal(*, side="BUY", tx_hash="0xvis", price=0.50, size=10.0):
    return {
        "side": side,
        "transactionHash": tx_hash,
        "asset": FAKE_TOKEN_ID,
        "conditionId": "cond-vis",
        "price": price,
        "size": size,
        "usdcSize": size * price,
        "timestamp": int(time.time()),
        "title": "Test Market",
        "eventSlug": "test-slug",
    }


class _FakeMDS:
    """Serves both the WSS (`require_fresh_data`) and REST-snapshot
    (`require_execution_snapshot`) freshness entry points. When
    ``raise_stale`` is set, the matching call raises ``StaleDataError`` with
    ``skip_code='market_data_stale'`` so the pipeline fails closed."""

    def __init__(self, bundle, *, raise_stale: bool = False):
        self._bundle = bundle
        self._raise_stale = raise_stale

    async def require_fresh_data(self, token_id, **kwargs):
        if self._raise_stale:
            raise StaleDataError(f"stale {token_id}")
        return self._bundle

    async def require_execution_snapshot(self, token_id, *, side, condition_id=None):
        if self._raise_stale:
            raise StaleDataError(f"stale {token_id}", skip_code="market_data_stale")
        return self._bundle


def _passthrough_coordinator(exec_svc):
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
            result = await exec_svc.execute(req)
            return ExecutionCoordinatorResult(
                accepted=getattr(result, "accepted", False),
                status=getattr(result, "status", ""),
                order_id=getattr(result, "order_id", None),
                reason=getattr(result, "error", None),
                lifecycle_status=getattr(result, "status", ""),
                raw_result=result,
            )

    return _PassThrough()


def _make_context(pool, tmp_path, *, exec_svc, mds, runtime, primary="wss"):
    ctx = TradingContext(
        user_id="user1",
        wallet_address="0x" + "a" * 40,
        client=MagicMock(),
        positions_repo=PositionsRepo(pool),
        fills_repo=FillsRepo(pool),
        orders_repo=OrdersRepo(pool),
        risk_blocks_repo=RiskBlocksRepo(pool),
        pnl_tracker=PnLTracker(log_path=str(tmp_path / "vis.pnl")),
        risk_engine=RiskEngine(),
        accounting_service=AccountingService(),
        position_manager=PositionManager(),
        execution_service=exec_svc,
        config=_config(MARKET_DATA_PRIMARY_SOURCE=primary),
        allocation_pct=1.0,
        buy_min_amount=0.01,
        buy_max_amount=1000.0,
        market_data_service=mds,
        extra={"slippage_tolerance_bps": 200},
    )
    ctx.market_data_runtime = runtime
    ctx.execution_coordinator = _passthrough_coordinator(exec_svc)
    return ctx


@pytest.mark.asyncio
async def test_market_data_stale_prints_panel(tmp_path):
    async with _pg_pool() as pool:
        exec_svc = AsyncMock()
        runtime = AsyncMock()
        runtime.ensure_token_ready_or_bootstrapped = AsyncMock(return_value=True)
        runtime.subscribe_tokens = AsyncMock()
        # rest_snapshot is the production default; its pre-loop snapshot gate
        # is where the market_data_stale panel fires.
        ctx = _make_context(
            pool, tmp_path,
            exec_svc=exec_svc,
            mds=_FakeMDS(_make_bundle(), raise_stale=True),
            runtime=runtime,
            primary="rest_snapshot",
        )
        with patch("orchestrators.trade_pipeline.print_skip_reason") as p:
            results = await execute_trades(ctx, "leader1", [_signal()], time.time())
        assert results[0].status == "skipped"
        assert results[0].reason == "market_data_stale"
        exec_svc.execute.assert_not_awaited()
        p.assert_called()
        assert "stale" in p.call_args.args[0].lower()


@pytest.mark.asyncio
async def test_sell_never_held_prints_panel(tmp_path):
    async with _pg_pool() as pool:
        exec_svc = AsyncMock()
        runtime = AsyncMock()
        runtime.ensure_token_ready_or_bootstrapped = AsyncMock(return_value=True)
        ctx = _make_context(
            pool, tmp_path,
            exec_svc=exec_svc,
            mds=_FakeMDS(_make_bundle()),
            runtime=runtime,
        )
        with patch("orchestrators.trade_pipeline.print_skip_reason") as p:
            results = await execute_trades(
                ctx, "leader1", [_signal(side="SELL")], time.time()
            )
        assert results[0].status == "skipped"
        assert results[0].reason == "never_held"
        exec_svc.execute.assert_not_awaited()
        p.assert_called_once()
        assert "never held" in p.call_args.args[1].lower()


@pytest.mark.asyncio
async def test_not_accepted_reservation_conflict_prints_panel(tmp_path):
    async with _pg_pool() as pool:
        exec_svc = AsyncMock()
        exec_svc.execute.return_value = ExecutionResult(
            accepted=False,
            order_id=None,
            status="reservation_conflict",
            requested_amount=Decimal("5"),
            filled_amount=Decimal("0"),
            avg_price=Decimal("0"),
            fees_paid=Decimal("0"),
            error="active reservation exists for intent",
        )
        runtime = AsyncMock()
        runtime.ensure_token_ready_or_bootstrapped = AsyncMock(return_value=True)
        ctx = _make_context(
            pool, tmp_path,
            exec_svc=exec_svc,
            mds=_FakeMDS(_make_bundle()),
            runtime=runtime,
        )
        with patch("orchestrators.trade_pipeline.print_skip_reason") as p:
            await execute_trades(ctx, "leader1", [_signal()], time.time())
        p.assert_called()
        titles = " ".join(str(c.args[0]) for c in p.call_args_list)
        assert "reservation_conflict" in titles


@pytest.mark.asyncio
async def test_fak_accepted_no_fill_prints_transient_panel(tmp_path):
    async with _pg_pool() as pool:
        exec_svc = AsyncMock()
        exec_svc.execute.return_value = ExecutionResult(
            accepted=True,
            order_id="ord-vis",
            status="delayed",
            requested_amount=Decimal("5"),
            filled_amount=Decimal("0"),
            avg_price=Decimal("0"),
            fees_paid=Decimal("0"),
            error=None,
        )
        runtime = AsyncMock()
        runtime.ensure_token_ready_or_bootstrapped = AsyncMock(return_value=True)
        ctx = _make_context(
            pool, tmp_path,
            exec_svc=exec_svc,
            mds=_FakeMDS(_make_bundle()),
            runtime=runtime,
        )
        with patch("orchestrators.trade_pipeline.print_skip_reason") as p:
            results = await execute_trades(ctx, "leader1", [_signal()], time.time())
        assert results[0].status == "submitted_unconfirmed"
        p.assert_called()
        detail = " ".join(str(c.args[1]) for c in p.call_args_list if len(c.args) > 1)
        assert "did not fill" in (" ".join(str(c.args[0]) for c in p.call_args_list)).lower() \
            or "fill" in detail.lower()
