"""Skip and failure visibility in the trade pipeline.

Every decision path that declines to copy a detected trade must surface one
``⚠️ COPY NOT EXECUTED`` panel so the operator is not left with a silent
"NEW TRADE DETECTED" followed by nothing. Pre-submit skips use
``print_skip_reason``; post-execution failures use the richer
``print_trade_result`` panel exactly once.

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
from tests.config_helpers import load_test_config
from application.trading_context import TradingContext
from domain.execution import ExecutionMetadata
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
        return load_test_config(env)


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
    """Serve WSS bundles and metadata-only copy execution lookups."""

    def __init__(self, bundle, *, raise_stale: bool = False):
        self._bundle = bundle
        self._raise_stale = raise_stale

    async def require_fresh_data(self, token_id, **kwargs):
        if self._raise_stale:
            raise StaleDataError(f"stale {token_id}")
        return self._bundle

    async def require_execution_metadata(self, token_id, *, condition_id=None):
        if self._raise_stale:
            raise StaleDataError(
                f"metadata unavailable {token_id}",
                skip_code="market_metadata_unavailable",
            )
        return ExecutionMetadata(
            token_id=token_id,
            tick_size=self._bundle.tick.tick_size,
            min_order_size=self._bundle.tick.min_order_size,
            neg_risk=self._bundle.tick.neg_risk,
        )


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
                amount=(intent.notional_usdc if intent.side.upper() == "BUY" else intent.size),
                max_slippage_bps=intent.max_slippage_bps,
                reason=intent.reason or intent.source,
                signal_id=intent.signal_id,
                condition_id=intent.condition_id,
                recovery_metadata=dict(intent.recovery_metadata or {}),
                order_type=intent.order_type,
                price_limit=intent.price_limit,
                tick_size=intent.tick_size,
                min_order_size=intent.min_order_size,
                neg_risk=intent.neg_risk,
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
        # The legacy rest_snapshot mode now performs a metadata-only pre-gate.
        ctx = _make_context(
            pool,
            tmp_path,
            exec_svc=exec_svc,
            mds=_FakeMDS(_make_bundle(), raise_stale=True),
            runtime=runtime,
            primary="rest_snapshot",
        )
        with patch("orchestrators.trade_pipeline.print_skip_reason") as p:
            results = await execute_trades(ctx, "leader1", [_signal()], time.time())
        assert results[0].status == "skipped"
        assert results[0].reason == "market_metadata_unavailable"
        exec_svc.execute.assert_not_awaited()
        p.assert_called()
        assert "unavailable" in p.call_args.args[0].lower()


@pytest.mark.asyncio
async def test_sell_never_held_prints_panel(tmp_path):
    async with _pg_pool() as pool:
        exec_svc = AsyncMock()
        runtime = AsyncMock()
        runtime.ensure_token_ready_or_bootstrapped = AsyncMock(return_value=True)
        ctx = _make_context(
            pool,
            tmp_path,
            exec_svc=exec_svc,
            mds=_FakeMDS(_make_bundle()),
            runtime=runtime,
        )
        with patch("orchestrators.trade_pipeline.print_skip_reason") as p:
            results = await execute_trades(ctx, "leader1", [_signal(side="SELL")], time.time())
        assert results[0].status == "skipped"
        assert results[0].reason == "never_held"
        exec_svc.execute.assert_not_awaited()
        p.assert_called_once()
        assert "never held" in p.call_args.args[1].lower()


@pytest.mark.asyncio
async def test_terminal_copy_failure_has_one_panel_and_follower_amounts(tmp_path):
    """A killed FAK has one panel, copy-scoped units, and no later retry."""
    async with _pg_pool() as pool:
        exec_svc = AsyncMock()
        exec_svc.execute.return_value = ExecutionResult(
            accepted=False,
            order_id=None,
            status="canceled",
            requested_amount=Decimal("5"),
            filled_amount=Decimal("0"),
            avg_price=Decimal("0"),
            fees_paid=Decimal("0"),
            error="order_not_fillable",
        )
        runtime = AsyncMock()
        runtime.ensure_token_ready_or_bootstrapped = AsyncMock(return_value=True)
        ctx = _make_context(
            pool,
            tmp_path,
            exec_svc=exec_svc,
            mds=_FakeMDS(_make_bundle()),
            runtime=runtime,
        )
        with (
            patch("orchestrators.trade_pipeline.print_skip_reason") as generic_panel,
            patch("orchestrators.trade_pipeline.print_trade_result") as detailed_panel,
            patch("orchestrators.trade_pipeline.log_trade_skip") as skip_log,
        ):
            results = await execute_trades(ctx, "leader1", [_signal()], time.time())
        generic_panel.assert_not_called()
        detailed_panel.assert_called_once()
        assert "order_not_fillable" in detailed_panel.call_args.kwargs["reason"]

        submitted_request = exec_svc.execute.await_args.args[0]
        payload = skip_log.call_args.kwargs
        assert payload["price"] == float(submitted_request.price_limit)
        assert payload["usdc_value"] == float(submitted_request.amount)
        assert payload["size"] * payload["price"] == pytest.approx(
            payload["usdc_value"],
            rel=1e-6,
        )
        persisted = await ctx.orders_repo.get_processed_trade(results[0].trade_key)
        assert persisted["status"] == "terminal_failed"


@pytest.mark.asyncio
async def test_fak_accepted_no_fill_prints_transient_panel(tmp_path):
    """A submitted FAK is logged as pending without a contradictory skip panel."""

    async with _pg_pool() as pool:
        exec_svc = AsyncMock()
        exec_svc.execute.return_value = ExecutionResult(
            accepted=True,
            order_id="ord-vis",
            status="delayed",
            requested_amount=Decimal("4.25"),
            filled_amount=Decimal("0"),
            avg_price=Decimal("0"),
            fees_paid=Decimal("0"),
            error=None,
            effective_price=Decimal("0.52"),
        )
        runtime = AsyncMock()
        runtime.ensure_token_ready_or_bootstrapped = AsyncMock(return_value=True)
        ctx = _make_context(
            pool,
            tmp_path,
            exec_svc=exec_svc,
            mds=_FakeMDS(_make_bundle()),
            runtime=runtime,
        )
        with (
            patch("orchestrators.trade_pipeline.print_skip_reason") as p,
            patch("orchestrators.trade_pipeline.log_trade_submitted") as log_sub,
        ):
            results = await execute_trades(ctx, "leader1", [_signal()], time.time())
        assert results[0].status == "submitted_unconfirmed"
        p.assert_not_called()
        # Copy-scoped size is logged (limit price + internally consistent
        # tokens*price ≈ usdc), not the leader's raw signal.size.
        log_sub.assert_called()
        sub_kwargs = log_sub.call_args.kwargs
        assert sub_kwargs["meta"]["submissionState"] == "submitted_unconfirmed"
        assert sub_kwargs["price"] == pytest.approx(0.52)
        assert sub_kwargs["usdc_value"] == pytest.approx(4.25)
        if sub_kwargs["size"] is not None and sub_kwargs["usdc_value"] is not None:
            assert sub_kwargs["size"] * sub_kwargs["price"] == pytest.approx(
                sub_kwargs["usdc_value"], rel=1e-6
            )
