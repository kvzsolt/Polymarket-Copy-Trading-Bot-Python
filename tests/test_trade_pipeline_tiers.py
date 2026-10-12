"""Tests for tier-gating in the trade pipeline (PG-A6 port)."""

from __future__ import annotations

import contextlib
import time
from dataclasses import dataclass
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import pmcopy.config.app_config as app_config_mod
from tests.config_helpers import load_test_config
from pmcopy.application.trading_context import TradingContext
from pmcopy.domain.execution import ExecutionMetadata
from pmcopy.orchestrators.trade_pipeline import execute_trades, _resolve_leader_tier
from pmcopy.services.pnlTracker import PnLTracker
from pmcopy.services.risk.risk_engine import RiskEngine
from pmcopy.services.accountingService import AccountingService
from pmcopy.services.positionManager import PositionManager
from pmcopy.storage.postgres.fills_repo import FillsRepo
from pmcopy.storage.postgres.orders_repo import OrdersRepo
from pmcopy.storage.postgres.positions_repo import PositionsRepo
from pmcopy.storage.postgres.risk_blocks_repo import RiskBlocksRepo

FAKE_TOKEN_ID = "71321045679252212594626385532706912750332728571942532223535764789812338637396"
_VALID_PRIVATE_KEY = "0x" + "a" * 64
_VALID_FUNDER = "0x" + "b" * 40
_VALID_USER = "0x" + "c" * 40


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


def _config(**overrides):
    env = {
        "PRIVATE_KEY": _VALID_PRIVATE_KEY,
        "FUNDER_ADDRESS": _VALID_FUNDER,
        "USER_ADDRESS": _VALID_USER,
        "LEADER_WALLETS": "0x" + "d" * 40,
        "RPC_URL": "https://polygon-rpc.example.com",
        "CHAIN_ID": "137",
        "PERCENTAGE": "7",
        "BUY_MIN_AMOUNT": "3",
        "BUY_MAX_AMOUNT": "42",
        "MAX_MARKET_EXPOSURE_USDC": "123",
        "SLIPPAGE_TOLERANCE": "1.5",
        "ENABLE_TIER_GATE_WATCHLIST": "true",
        "ENABLE_TIER_GATE_WATCHLIST_LOG": "true",
        "MARKET_DATA_PRIMARY_SOURCE": "wss",
    }
    env.update(overrides)
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(app_config_mod, "_runtime_python_supported", lambda: True)
        return load_test_config(env)


@pytest.fixture(autouse=True)
def _reset_singletons(monkeypatch):
    monkeypatch.setenv("COPY_DECISION_MIN_SCORE", "0.01")
    monkeypatch.setenv("COPY_MAX_SIGNAL_AGE_SECONDS", "999999")
    monkeypatch.setenv("COPY_MAX_PRICE_DRIFT_PCT", "999")
    monkeypatch.setenv("COPY_MAX_EDGE_DECAY_PCT", "999")
    monkeypatch.setenv("COPY_MIN_SHARES", "0.001")
    monkeypatch.setenv("COPY_MIN_NOTIONAL_USDC", "0.01")
    monkeypatch.setenv("BUY_MIN_AMOUNT", "0.01")
    monkeypatch.setenv("ENABLE_TIER_GATE_WATCHLIST", "true")
    monkeypatch.setenv("ENABLE_TIER_GATE_WATCHLIST_LOG", "true")


@dataclass
class _FakeTierRecord:
    follow_tier: str


class _FakeLeadersRepo:
    """Minimal mock that satisfies _resolve_leader_tier."""

    def __init__(self, tier_map: dict[str, str] | None = None):
        self._tier_map = tier_map or {}

    async def get_tier(self, leader_id: str):
        tier = self._tier_map.get(leader_id)
        if tier is None:
            return None
        return _FakeTierRecord(follow_tier=tier)


def _make_fresh_bundle():
    """Tiny WSS-fresh MarketDataBundle used by tier tests for sizing inputs."""
    from pmcopy.cache.market_state_store import BookSnapshot, QuoteSnapshot, TickSnapshot
    from pmcopy.services.market_data_service import MarketDataBundle, MarketDataSource

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


class _TiersFakeMDS:
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


def _make_context(pool, *, leaders_repo=None, execution_service=None, config=None, tmp_path=None):
    pnl_log = str((tmp_path or "/tmp") / "pnl.jsonl") if tmp_path else "/tmp/pnl.jsonl"
    ctx = TradingContext(
        user_id="user1",
        wallet_address=_VALID_FUNDER,
        client=MagicMock(),
        positions_repo=PositionsRepo(pool),
        fills_repo=FillsRepo(pool),
        orders_repo=OrdersRepo(pool),
        risk_blocks_repo=RiskBlocksRepo(pool),
        pnl_tracker=PnLTracker(log_path=pnl_log),
        risk_engine=RiskEngine(),
        accounting_service=AccountingService(),
        position_manager=PositionManager(),
        execution_service=execution_service,
        leaders_repo=leaders_repo,
        config=config or _config(),
        allocation_pct=1.0,
        buy_min_amount=0.01,
        buy_max_amount=1000.0,
        market_data_service=_TiersFakeMDS(_make_fresh_bundle()),
        extra={"slippage_tolerance_bps": 200},
    )
    ctx.market_data_runtime = AsyncMock()
    return ctx


def _signal(
    *, side="BUY", tx_hash="0xabc", asset=FAKE_TOKEN_ID, condition_id="cond1", price=0.50, size=10.0
):
    return {
        "side": side,
        "transactionHash": tx_hash,
        "asset": asset,
        "conditionId": condition_id,
        "price": price,
        "size": size,
        "usdcSize": size * price,
        "timestamp": int(time.time()),
    }


# --- _resolve_leader_tier unit tests ---


class TestResolveLeaderTier:
    @pytest.mark.asyncio
    async def test_no_repo_returns_watchlist(self, tmp_path):
        # P0-2: fail-closed — an absent leaders repo must not auto-promote to live.
        async with _pg_pool() as pool:
            ctx = _make_context(pool, tmp_path=tmp_path)
            assert await _resolve_leader_tier(ctx, "0xleader") == "watchlist"

    @pytest.mark.asyncio
    async def test_repo_no_record_returns_watchlist(self, tmp_path):
        async with _pg_pool() as pool:
            repo = _FakeLeadersRepo({})
            ctx = _make_context(pool, leaders_repo=repo, tmp_path=tmp_path)
            assert await _resolve_leader_tier(ctx, "0xleader") == "watchlist"

    @pytest.mark.asyncio
    async def test_repo_watchlist(self, tmp_path):
        async with _pg_pool() as pool:
            repo = _FakeLeadersRepo({"0xleader": "watchlist"})
            ctx = _make_context(pool, leaders_repo=repo, tmp_path=tmp_path)
            assert await _resolve_leader_tier(ctx, "0xleader") == "watchlist"

    @pytest.mark.asyncio
    async def test_repo_paper_follow(self, tmp_path):
        async with _pg_pool() as pool:
            repo = _FakeLeadersRepo({"0xleader": "paper_follow"})
            ctx = _make_context(pool, leaders_repo=repo, tmp_path=tmp_path)
            assert await _resolve_leader_tier(ctx, "0xleader") == "paper_follow"

    @pytest.mark.asyncio
    async def test_repo_live_follow(self, tmp_path):
        async with _pg_pool() as pool:
            repo = _FakeLeadersRepo({"0xleader": "live_follow"})
            ctx = _make_context(pool, leaders_repo=repo, tmp_path=tmp_path)
            assert await _resolve_leader_tier(ctx, "0xleader") == "live_follow"

    @pytest.mark.asyncio
    async def test_repo_exception_returns_watchlist(self, tmp_path):
        async with _pg_pool() as pool:
            repo = MagicMock()
            repo.get_tier = AsyncMock(side_effect=RuntimeError("db gone"))
            ctx = _make_context(pool, leaders_repo=repo, tmp_path=tmp_path)
            assert await _resolve_leader_tier(ctx, "0xleader") == "watchlist"

    @pytest.mark.asyncio
    async def test_repo_unknown_tier_returns_watchlist(self, tmp_path):
        async with _pg_pool() as pool:
            repo = _FakeLeadersRepo({"0xleader": "something_unknown"})
            ctx = _make_context(pool, leaders_repo=repo, tmp_path=tmp_path)
            assert await _resolve_leader_tier(ctx, "0xleader") == "watchlist"


# --- execute_trades tier-gating integration tests ---


@pytest.mark.asyncio
async def test_watchlist_tier_skips_trade(tmp_path):
    async with _pg_pool() as pool:
        repo = _FakeLeadersRepo({"0xleader": "watchlist"})
        ctx = _make_context(
            pool, leaders_repo=repo, execution_service=MagicMock(), tmp_path=tmp_path
        )
        results = await execute_trades(ctx, "0xleader", [_signal()], time.time())
        assert len(results) == 1
        assert results[0].status == "skipped"
        assert results[0].reason == "tier_watchlist"


@pytest.mark.asyncio
async def test_paper_follow_tier_records_paper(tmp_path):
    async with _pg_pool() as pool:
        repo = _FakeLeadersRepo({"0xleader": "paper_follow"})
        ctx = _make_context(
            pool, leaders_repo=repo, execution_service=MagicMock(), tmp_path=tmp_path
        )
        results = await execute_trades(ctx, "0xleader", [_signal()], time.time())
        assert len(results) == 1
        assert results[0].status == "paper"
        assert results[0].reason == "tier_paper_follow"


@pytest.mark.asyncio
async def test_paper_follow_marks_processed(tmp_path):
    """Paper trades should be idempotent (marked processed)."""
    async with _pg_pool() as pool:
        repo = _FakeLeadersRepo({"0xleader": "paper_follow"})
        ctx = _make_context(
            pool, leaders_repo=repo, execution_service=MagicMock(), tmp_path=tmp_path
        )
        sig = _signal(tx_hash="0xpaper1")
        await execute_trades(ctx, "0xleader", [sig], time.time())
        results = await execute_trades(ctx, "0xleader", [sig], time.time())
        assert results[0].status == "skipped"
        assert results[0].reason == "already_processed"


@pytest.mark.asyncio
async def test_no_repo_skips_as_watchlist(tmp_path):
    """P0-2 fail-closed: without a leaders_repo a leader resolves to watchlist and
    the trade is skipped — never auto-executed as live."""
    async with _pg_pool() as pool:
        exec_svc = AsyncMock()
        ctx = _make_context(pool, leaders_repo=None, execution_service=exec_svc, tmp_path=tmp_path)
        results = await execute_trades(ctx, "0xleader", [_signal()], time.time())
        assert len(results) == 1
        assert results[0].status == "skipped"
        assert results[0].reason == "tier_watchlist"
        exec_svc.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_multiple_signals_tier_applied_to_all(tmp_path):
    async with _pg_pool() as pool:
        repo = _FakeLeadersRepo({"0xleader": "watchlist"})
        ctx = _make_context(
            pool, leaders_repo=repo, execution_service=MagicMock(), tmp_path=tmp_path
        )
        signals = [
            _signal(tx_hash="0x1", asset=FAKE_TOKEN_ID),
            _signal(tx_hash="0x2", asset=FAKE_TOKEN_ID),
            _signal(tx_hash="0x3", asset=FAKE_TOKEN_ID),
        ]
        results = await execute_trades(ctx, "0xleader", signals, time.time())
        assert len(results) == 3
        assert all(r.status == "skipped" and r.reason == "tier_watchlist" for r in results)


@pytest.mark.asyncio
async def test_watchlist_gate_can_be_disabled(tmp_path):
    from pmcopy.services.executionService import ExecutionResult

    async with _pg_pool() as pool:
        repo = _FakeLeadersRepo({"0xleader": "watchlist"})
        exec_svc = AsyncMock()
        exec_svc.execute = AsyncMock(
            return_value=ExecutionResult(
                accepted=True,
                order_id="ord123",
                status="filled",
                requested_amount=Decimal("5"),
                filled_amount=Decimal("5"),
                avg_price=Decimal("0.50"),
                fees_paid=Decimal("0.01"),
            )
        )
        ctx = _make_context(
            pool,
            leaders_repo=repo,
            execution_service=exec_svc,
            config=_config(ENABLE_TIER_GATE_WATCHLIST="false", PERCENTAGE="100"),
            tmp_path=tmp_path,
        )
        results = await execute_trades(
            ctx,
            "0xleader",
            [_signal(price=1.0, size=25.0)],
            time.time(),
        )

        assert len(results) == 1
        assert results[0].reason != "tier_watchlist"


# --- immutable paper trading-mode override tests ---


@pytest.mark.asyncio
async def test_paper_trade_only_overrides_live_follow(tmp_path):
    """Typed paper_trade_only must demote live_follow leaders to paper without executing."""
    async with _pg_pool() as pool:
        repo = _FakeLeadersRepo({"0xleader": "live_follow"})
        exec_svc = AsyncMock()
        ctx = _make_context(
            pool,
            leaders_repo=repo,
            execution_service=exec_svc,
            config=_config(TRADING_MODE="paper"),
            tmp_path=tmp_path,
        )
        results = await execute_trades(ctx, "0xleader", [_signal()], time.time())
        assert len(results) == 1
        assert results[0].status == "paper"
        assert results[0].reason == "tier_paper_follow"
        exec_svc.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_paper_trade_only_does_not_affect_watchlist(tmp_path):
    """Typed paper_trade_only should not change watchlist behavior (still skipped)."""
    async with _pg_pool() as pool:
        repo = _FakeLeadersRepo({"0xleader": "watchlist"})
        ctx = _make_context(
            pool,
            leaders_repo=repo,
            execution_service=MagicMock(),
            config=_config(TRADING_MODE="paper"),
            tmp_path=tmp_path,
        )
        results = await execute_trades(ctx, "0xleader", [_signal()], time.time())
        assert len(results) == 1
        assert results[0].status == "skipped"
        assert results[0].reason == "tier_watchlist"


@pytest.mark.asyncio
async def test_live_trading_enabled_allows_live(tmp_path):
    """Explicit live selection reaches the injected execution boundary for a live-follow tier."""
    from pmcopy.services.executionService import ExecutionResult
    from tests.test_trade_pipeline_safety import _PassThroughCoordinator

    async with _pg_pool() as pool:
        repo = _FakeLeadersRepo({"0xleader": "live_follow"})
        exec_svc = AsyncMock()
        exec_svc.execute = AsyncMock(
            return_value=ExecutionResult(
                accepted=True,
                order_id="ord456",
                status="filled",
                requested_amount=Decimal("5"),
                filled_amount=Decimal("5"),
                avg_price=Decimal("0.50"),
                fees_paid=Decimal("0.01"),
            )
        )
        ctx = _make_context(
            pool,
            leaders_repo=repo,
            execution_service=exec_svc,
            config=_config(TRADING_MODE="live"),
            tmp_path=tmp_path,
        )
        ctx.execution_coordinator = _PassThroughCoordinator(exec_svc)
        with patch(
            "pmcopy.orchestrators.trade_pipeline.get_my_balance", AsyncMock(return_value=Decimal(1000))
        ):
            results = await execute_trades(ctx, "0xleader", [_signal()], time.time())
        assert len(results) == 1
        assert results[0].status == "executed"
        exec_svc.execute.assert_awaited_once()


@pytest.mark.asyncio
async def test_watchlist_gate_requires_injected_config(tmp_path):
    async with _pg_pool() as pool:
        repo = _FakeLeadersRepo({"0xleader": "watchlist"})
        ctx = TradingContext(
            user_id="user1",
            wallet_address=_VALID_FUNDER,
            client=MagicMock(),
            positions_repo=PositionsRepo(pool),
            fills_repo=FillsRepo(pool),
            orders_repo=OrdersRepo(pool),
            risk_blocks_repo=RiskBlocksRepo(pool),
            pnl_tracker=PnLTracker(log_path=str(tmp_path / "pnl.jsonl")),
            risk_engine=RiskEngine(),
            accounting_service=AccountingService(),
            position_manager=PositionManager(),
            execution_service=MagicMock(),
            leaders_repo=repo,
            config=_config(),
            allocation_pct=1.0,
            buy_min_amount=0.01,
            buy_max_amount=1000.0,
            extra={"slippage_tolerance_bps": 200},
        )

        ctx.config = None
        with pytest.raises(
            RuntimeError, match="TradingContext.config is required for live tier gating"
        ):
            await execute_trades(ctx, "0xleader", [_signal()], time.time())
