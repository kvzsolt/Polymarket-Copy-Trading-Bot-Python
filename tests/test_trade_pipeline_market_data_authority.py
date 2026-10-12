"""PR 3: market-data authority in the trade pipeline.

Covers:
  - new signal token is subscribed via market_data_runtime before decision
  - signal is skipped when market data stays stale past the timeout
  - decision-service receives min_order_size / tick_size from the MDS bundle
"""

from __future__ import annotations

import contextlib
import time
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import pmcopy.config.app_config as app_config_mod
from pmcopy.cache.market_state_store import BookSnapshot, QuoteSnapshot, TickSnapshot
from tests.config_helpers import load_test_config
from pmcopy.application.trading_context import TradingContext
from pmcopy.domain.execution import ExecutionMetadata
from pmcopy.orchestrators.trade_pipeline import execute_trades
from pmcopy.services.accountingService import AccountingService
from pmcopy.services.copyDecisionService import CopyDecision
from pmcopy.services.executionService import ExecutionResult
from pmcopy.services.market_data_service import (
    MarketDataBundle,
    MarketDataSource,
    StaleDataError,
)
from pmcopy.services.pnlTracker import PnLTracker
from pmcopy.services.positionManager import PositionManager
from pmcopy.services.risk.risk_engine import RiskEngine
from pmcopy.storage.postgres.fills_repo import FillsRepo
from pmcopy.storage.postgres.orders_repo import OrdersRepo
from pmcopy.storage.postgres.positions_repo import PositionsRepo
from pmcopy.storage.postgres.risk_blocks_repo import RiskBlocksRepo

FAKE_TOKEN_ID = "71321045679252212594626385532706912750332728571942532223535764789812338637396"


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
    # Keep the MDS retry loop tight in tests.
    monkeypatch.setenv("MARKET_DATA_SUBSCRIBE_TIMEOUT_SECONDS", "0.4")


@pytest.fixture(autouse=True)
def _mock_balance(monkeypatch):
    async def _fake_balance(_addr):
        return 10000.0

    monkeypatch.setattr("pmcopy.orchestrators.trade_pipeline.get_my_balance", _fake_balance)


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
        # Legacy regression tests pin to WSS primary mode. PR10.4-specific
        # tests below opt into ``rest_snapshot`` explicitly.
        "MARKET_DATA_PRIMARY_SOURCE": "wss",
    }
    env.update(overrides)
    with patch.object(app_config_mod, "_runtime_python_supported", return_value=True):
        return load_test_config(env)


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
            target_notional_usdc=float(min(base_notional_usdc, max_notional_usdc)),
            target_size_tokens=float(min(base_notional_usdc, max_notional_usdc))
            / max(float(live_price), 1e-9),
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
    pool,
    tmp_path,
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
        positions_repo=PositionsRepo(pool),
        fills_repo=FillsRepo(pool),
        orders_repo=OrdersRepo(pool),
        risk_blocks_repo=RiskBlocksRepo(pool),
        pnl_tracker=PnLTracker(log_path=str(tmp_path / "pr3.pnl")),
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

    # PR6.1 — live mode requires a coordinator. Inject a pass-through so the
    # mocked ExecutionService keeps getting called.
    class _PassThrough:
        async def submit_copy_trade(self, intent):
            from pmcopy.services.execution.coordinator import ExecutionCoordinatorResult
            from pmcopy.services.execution.types import ExecutionRequest

            req = ExecutionRequest(
                user_id=intent.user_id,
                leader_id=intent.leader_wallet or "",
                token_id=intent.token_id,
                side=intent.side,
                amount=intent.notional_usdc if intent.side == "BUY" else intent.size,
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
                lifecycle_status=getattr(result, "status", ""),
                raw_result=result,
            )

    ctx.execution_coordinator = _PassThrough()
    if decision_svc is not None:
        ctx.copy_decision_service = decision_svc
    return ctx


@pytest.mark.asyncio
async def test_trade_pipeline_subscribes_new_token_before_decision(tmp_path):
    async with _pg_pool() as pool:
        exec_svc = AsyncMock()
        exec_svc.execute.return_value = _exec_result_filled()
        runtime = AsyncMock()
        runtime.ensure_token_ready_or_bootstrapped = AsyncMock(return_value=True)
        decision_svc = _RecordingDecisionService()

        ctx = _make_context(
            pool,
            tmp_path,
            exec_svc=exec_svc,
            mds=_FakeMDS(_make_bundle()),
            runtime=runtime,
            decision_svc=decision_svc,
        )

        await execute_trades(ctx, "leader1", [_signal()], time.time())

        # Pipeline calls ensure_token_ready_or_bootstrapped (which internally subscribes).
        runtime.ensure_token_ready_or_bootstrapped.assert_awaited()
        call_arg = runtime.ensure_token_ready_or_bootstrapped.await_args.args[0]
        assert call_arg == FAKE_TOKEN_ID
        exec_svc.execute.assert_awaited_once()


@pytest.mark.asyncio
async def test_trade_pipeline_skips_when_market_data_stale_after_subscribe(tmp_path):
    async with _pg_pool() as pool:
        exec_svc = AsyncMock()
        runtime = AsyncMock()
        runtime.ensure_token_ready_or_bootstrapped = AsyncMock(return_value=True)

        ctx = _make_context(
            pool,
            tmp_path,
            exec_svc=exec_svc,
            mds=_FakeMDS(_make_bundle(), raise_stale=True),
            runtime=runtime,
        )

        results = await execute_trades(ctx, "leader1", [_signal()], time.time())

        assert len(results) == 1
        assert results[0].status == "skipped"
        assert results[0].reason == "market_data_stale"
        exec_svc.execute.assert_not_awaited()
        # Bootstrap was attempted before MDS resolution.
        runtime.ensure_token_ready_or_bootstrapped.assert_awaited()


@pytest.mark.asyncio
async def test_trade_pipeline_sizing_uses_bundle_tick_and_min_order_size(tmp_path):
    async with _pg_pool() as pool:
        exec_svc = AsyncMock()
        exec_svc.execute.return_value = _exec_result_filled()
        runtime = AsyncMock()
        decision_svc = _RecordingDecisionService()
        bundle = _make_bundle(
            min_order_size=Decimal("7.5"),
            tick_size=Decimal("0.001"),
        )

        ctx = _make_context(
            pool,
            tmp_path,
            exec_svc=exec_svc,
            mds=_FakeMDS(bundle),
            runtime=runtime,
            decision_svc=decision_svc,
        )

        await execute_trades(ctx, "leader1", [_signal()], time.time())

        assert decision_svc.last_call is not None
        assert decision_svc.last_call["market_min_size"] == pytest.approx(7.5)
        assert decision_svc.last_call["tick_size"] == pytest.approx(0.001)


# ---------------------------------------------------------------------------
# PR10.3: ensure_token_ready_or_bootstrapped integration
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pipeline_calls_ensure_token_ready_for_each_signal(tmp_path):
    async with _pg_pool() as pool:
        exec_svc = AsyncMock()
        exec_svc.execute.return_value = _exec_result_filled()

        runtime = AsyncMock()
        runtime.ensure_token_ready_or_bootstrapped = AsyncMock(return_value=True)

        ctx = _make_context(
            pool,
            tmp_path,
            exec_svc=exec_svc,
            mds=_FakeMDS(_make_bundle()),
            runtime=runtime,
        )

        await execute_trades(ctx, "leader1", [_signal()], time.time())

        runtime.ensure_token_ready_or_bootstrapped.assert_awaited_once()
        call_arg = runtime.ensure_token_ready_or_bootstrapped.await_args.args[0]
        assert call_arg == FAKE_TOKEN_ID


@pytest.mark.asyncio
async def test_pipeline_logs_prefetch_start_for_each_token(tmp_path, capsys):
    async with _pg_pool() as pool:
        exec_svc = AsyncMock()
        exec_svc.execute.return_value = _exec_result_filled()

        runtime = AsyncMock()
        runtime.ensure_token_ready_or_bootstrapped = AsyncMock(return_value=True)

        ctx = _make_context(
            pool,
            tmp_path,
            exec_svc=exec_svc,
            mds=_FakeMDS(_make_bundle()),
            runtime=runtime,
        )

        await execute_trades(ctx, "leader1", [_signal()], time.time())

        out = capsys.readouterr().out
        assert "trade_pipeline_market_data_prefetch_start" in out


@pytest.mark.asyncio
async def test_pipeline_with_no_runtime_logs_prefetch_no_runtime(tmp_path, capsys):
    async with _pg_pool() as pool:
        exec_svc = AsyncMock()
        exec_svc.execute.return_value = _exec_result_filled()

        ctx = _make_context(
            pool,
            tmp_path,
            exec_svc=exec_svc,
            mds=_FakeMDS(_make_bundle()),
            runtime=None,
        )

        await execute_trades(ctx, "leader1", [_signal()], time.time())

        out = capsys.readouterr().out
        assert "trade_pipeline_market_data_prefetch_no_runtime" in out


@pytest.mark.asyncio
async def test_pipeline_skips_token_when_bootstrap_returns_false(tmp_path):
    async with _pg_pool() as pool:
        exec_svc = AsyncMock()

        runtime = AsyncMock()
        runtime.ensure_token_ready_or_bootstrapped = AsyncMock(return_value=False)

        ctx = _make_context(
            pool,
            tmp_path,
            exec_svc=exec_svc,
            mds=_FakeMDS(_make_bundle()),
            runtime=runtime,
        )

        results = await execute_trades(ctx, "leader1", [_signal()], time.time())

        assert len(results) == 1
        assert results[0].status == "skipped"
        exec_svc.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_pipeline_executes_when_bootstrap_returns_true_and_mds_fresh(tmp_path):
    async with _pg_pool() as pool:
        exec_svc = AsyncMock()
        exec_svc.execute.return_value = _exec_result_filled()

        runtime = AsyncMock()
        runtime.ensure_token_ready_or_bootstrapped = AsyncMock(return_value=True)

        ctx = _make_context(
            pool,
            tmp_path,
            exec_svc=exec_svc,
            mds=_FakeMDS(_make_bundle()),
            runtime=runtime,
        )

        results = await execute_trades(ctx, "leader1", [_signal()], time.time())

        assert len(results) == 1
        assert results[0].status != "skipped"
        exec_svc.execute.assert_awaited_once()


# ---------------------------------------------------------------------------
# Metadata-only copy prefetch in the legacy REST-primary configuration mode
# ---------------------------------------------------------------------------


def _rest_metadata(
    *,
    token_id: str = FAKE_TOKEN_ID,
    min_order_size: Decimal = Decimal("3"),
    tick_size: Decimal = Decimal("0.01"),
) -> ExecutionMetadata:
    """Return dynamic venue metadata without constructing an order book."""
    return ExecutionMetadata(
        token_id=token_id,
        tick_size=tick_size,
        min_order_size=min_order_size,
        neg_risk=False,
    )


def _rest_config(**overrides):
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
        "MARKET_DATA_PRIMARY_SOURCE": "rest_snapshot",
    }
    env.update(overrides)
    with patch.object(app_config_mod, "_runtime_python_supported", return_value=True):
        return load_test_config(env)


class _FakeRestMDS:
    def __init__(self, metadata=None, *, raise_exc=None):
        self._metadata = metadata
        self._raise = raise_exc
        self.metadata_calls: list[tuple[str, str | None]] = []
        self.fresh_calls: list[str] = []

    async def require_execution_metadata(self, token_id, *, condition_id=None):
        self.metadata_calls.append((token_id, condition_id))
        if self._raise is not None:
            raise self._raise
        return self._metadata

    async def require_fresh_data(self, token_id, **kwargs):
        self.fresh_calls.append(token_id)
        if self._raise is not None:
            raise self._raise
        return self._metadata


def _make_rest_context(pool, tmp_path, *, exec_svc, mds, runtime, decision_svc=None):
    ctx = TradingContext(
        user_id="user1",
        wallet_address="0x" + "a" * 40,
        client=MagicMock(),
        positions_repo=PositionsRepo(pool),
        fills_repo=FillsRepo(pool),
        orders_repo=OrdersRepo(pool),
        risk_blocks_repo=RiskBlocksRepo(pool),
        pnl_tracker=PnLTracker(log_path=str(tmp_path / "pr3-rest.pnl")),
        risk_engine=RiskEngine(),
        accounting_service=AccountingService(),
        position_manager=PositionManager(),
        execution_service=exec_svc,
        config=_rest_config(),
        allocation_pct=1.0,
        buy_min_amount=0.01,
        buy_max_amount=1000.0,
        market_data_service=mds,
        extra={"slippage_tolerance_bps": 200},
    )
    ctx.market_data_runtime = runtime

    class _PassThrough:
        async def submit_copy_trade(self, intent):
            from pmcopy.services.execution.coordinator import ExecutionCoordinatorResult
            from pmcopy.services.execution.types import ExecutionRequest

            req = ExecutionRequest(
                user_id=intent.user_id,
                leader_id=intent.leader_wallet or "",
                token_id=intent.token_id,
                side=intent.side,
                amount=intent.notional_usdc if intent.side == "BUY" else intent.size,
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
                lifecycle_status=getattr(result, "status", ""),
                raw_result=result,
            )

    ctx.execution_coordinator = _PassThrough()
    if decision_svc is not None:
        ctx.copy_decision_service = decision_svc
    return ctx


@pytest.mark.asyncio
async def test_rest_primary_calls_require_execution_metadata_once_per_token(tmp_path):
    async with _pg_pool() as pool:
        exec_svc = AsyncMock()
        exec_svc.execute.return_value = _exec_result_filled()

        mds = _FakeRestMDS(_rest_metadata())
        runtime = AsyncMock()
        runtime.ensure_token_ready_or_bootstrapped = AsyncMock(return_value=True)
        runtime.subscribe_tokens = AsyncMock()

        ctx = _make_rest_context(
            pool,
            tmp_path,
            exec_svc=exec_svc,
            mds=mds,
            runtime=runtime,
            decision_svc=_RecordingDecisionService(),
        )

        await execute_trades(ctx, "leader1", [_signal()], time.time())

        assert len(mds.metadata_calls) == 1
        token_id, condition_id = mds.metadata_calls[0]
        assert token_id == FAKE_TOKEN_ID
        assert condition_id == "cond-pr3"
        # The compatibility mode fetches metadata only, then warms WSS for
        # ongoing store updates without waiting for WSS readiness.
        runtime.subscribe_tokens.assert_awaited_once_with([FAKE_TOKEN_ID])
        runtime.ensure_token_ready_or_bootstrapped.assert_not_awaited()


@pytest.mark.asyncio
async def test_rest_primary_skips_with_specific_skip_code(tmp_path):
    async with _pg_pool() as pool:
        exec_svc = AsyncMock()

        err = StaleDataError(
            "metadata unavailable",
            skip_code="market_metadata_unavailable",
        )
        mds = _FakeRestMDS(raise_exc=err)
        runtime = AsyncMock()

        ctx = _make_rest_context(
            pool,
            tmp_path,
            exec_svc=exec_svc,
            mds=mds,
            runtime=runtime,
        )

        results = await execute_trades(ctx, "leader1", [_signal()], time.time())

        assert len(results) == 1
        assert results[0].status == "skipped"
        assert results[0].reason == "market_metadata_unavailable"
        exec_svc.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_rest_primary_logs_metadata_prefetch_without_snapshot(tmp_path, capsys):
    async with _pg_pool() as pool:
        exec_svc = AsyncMock()
        exec_svc.execute.return_value = _exec_result_filled()

        mds = _FakeRestMDS(_rest_metadata())
        runtime = AsyncMock()

        ctx = _make_rest_context(
            pool,
            tmp_path,
            exec_svc=exec_svc,
            mds=mds,
            runtime=runtime,
            decision_svc=_RecordingDecisionService(),
        )

        await execute_trades(ctx, "leader1", [_signal()], time.time())

        out = capsys.readouterr().out
        assert "trade_pipeline_copy_metadata_prefetch" in out
        assert "trade_pipeline_market_data_snapshot_start" not in out
        assert "trade_pipeline_market_data_snapshot_ready" not in out


@pytest.mark.asyncio
async def test_rest_primary_subscribes_wss_without_waiting_for_readiness(tmp_path):
    async with _pg_pool() as pool:
        exec_svc = AsyncMock()
        exec_svc.execute.return_value = _exec_result_filled()

        mds = _FakeRestMDS(_rest_metadata())
        runtime = AsyncMock()
        runtime.ensure_token_ready_or_bootstrapped = AsyncMock(return_value=False)
        runtime.subscribe_tokens = AsyncMock()

        ctx = _make_rest_context(
            pool,
            tmp_path,
            exec_svc=exec_svc,
            mds=mds,
            runtime=runtime,
        )

        await execute_trades(ctx, "leader1", [_signal()], time.time())

        runtime.subscribe_tokens.assert_awaited_once_with([FAKE_TOKEN_ID])
        runtime.ensure_token_ready_or_bootstrapped.assert_not_awaited()
        exec_svc.execute.assert_awaited_once()
