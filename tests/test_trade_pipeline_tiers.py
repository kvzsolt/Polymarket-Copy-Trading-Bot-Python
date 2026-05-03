"""Tests for tier-gating in the trade pipeline."""

from __future__ import annotations

import time
from dataclasses import dataclass
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

import config.app_config as app_config_mod
from config.app_config import load_config
from config.env import ENV, rehydrate_env
from domain.trading_context import TradingContext
from orchestrators.trade_pipeline import execute_trades, _resolve_leader_tier
from services.pnlTracker import PnLTracker
from services.riskEngine import RiskEngine
from services.accountingService import AccountingService
from services.positionManager import PositionManager
from storage.sqlite.connection import create_connection
from storage.sqlite.fills_repo import FillsRepo
from storage.sqlite.migrations import apply_migrations
from storage.sqlite.orders_repo import OrdersRepo
from storage.sqlite.positions_repo import PositionsRepo

FAKE_TOKEN_ID = "71321045679252212594626385532706912750332728571942532223535764789812338637396"
_VALID_PRIVATE_KEY = "0x" + "a" * 64
_VALID_FUNDER = "0x" + "b" * 40
_VALID_USER = "0x" + "c" * 40


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
    }
    env.update(overrides)
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(app_config_mod, "_runtime_python_supported", lambda: True)
        return load_config(env)


@pytest.fixture(autouse=True)
def _reset_singletons(monkeypatch):
    monkeypatch.setenv("COPY_DECISION_MIN_SCORE", "0.01")
    monkeypatch.setenv("COPY_MAX_SIGNAL_AGE_SECONDS", "999999")
    monkeypatch.setenv("COPY_MAX_PRICE_DRIFT_PCT", "999")
    monkeypatch.setenv("COPY_MAX_EDGE_DECAY_PCT", "999")
    monkeypatch.setenv("COPY_MIN_SHARES", "0.001")
    monkeypatch.setenv("COPY_MIN_NOTIONAL_USDC", "0.01")
    monkeypatch.setenv("BUY_MIN_AMOUNT", "0.01")
    # Ensure tier gates are enabled so tier-gating tests actually gate
    monkeypatch.setenv("ENABLE_TIER_GATE_WATCHLIST", "true")
    monkeypatch.setenv("ENABLE_TIER_GATE_WATCHLIST_LOG", "true")
    rehydrate_env()


@dataclass
class _FakeTierRecord:
    follow_tier: str


class _FakeLeadersRepo:
    """Minimal mock that satisfies _resolve_leader_tier."""

    def __init__(self, tier_map: dict[str, str] | None = None):
        self._tier_map = tier_map or {}

    def get_tier(self, leader_id: str):
        tier = self._tier_map.get(leader_id)
        if tier is None:
            return None
        return _FakeTierRecord(follow_tier=tier)


def _init_db(tmp_path):
    db_path = str(tmp_path / "test.db")
    with create_connection(db_path) as conn:
        apply_migrations(conn)
    return db_path


def _make_context(db_path, *, leaders_repo=None, execution_service=None, config=None):
    return TradingContext(
        user_id="user1",
        wallet_address=_VALID_FUNDER,
        client=MagicMock(),
        positions_repo=PositionsRepo(db_path),
        fills_repo=FillsRepo(db_path),
        orders_repo=OrdersRepo(db_path),
        risk_blocks_repo=MagicMock(),
        pnl_tracker=PnLTracker(log_path=str(db_path) + ".pnl"),
        risk_engine=RiskEngine(),
        accounting_service=AccountingService(),
        position_manager=PositionManager(),
        execution_service=execution_service,
        leaders_repo=leaders_repo,
        config=config or _config(),
        allocation_pct=1.0,
        buy_min_amount=0.01,
        buy_max_amount=1000.0,
        extra={"slippage_tolerance_bps": 200},
    )


def _signal(*, side="BUY", tx_hash="0xabc", asset=FAKE_TOKEN_ID, condition_id="cond1", price=0.50, size=10.0):
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
    def test_no_repo_returns_live(self, tmp_path):
        ctx = _make_context(_init_db(tmp_path))
        assert _resolve_leader_tier(ctx, "0xleader") == "live_follow"

    def test_repo_no_record_returns_live(self, tmp_path):
        repo = _FakeLeadersRepo({})
        ctx = _make_context(_init_db(tmp_path), leaders_repo=repo)
        assert _resolve_leader_tier(ctx, "0xleader") == "live_follow"

    def test_repo_watchlist(self, tmp_path):
        repo = _FakeLeadersRepo({"0xleader": "watchlist"})
        ctx = _make_context(_init_db(tmp_path), leaders_repo=repo)
        assert _resolve_leader_tier(ctx, "0xleader") == "watchlist"

    def test_repo_paper_follow(self, tmp_path):
        repo = _FakeLeadersRepo({"0xleader": "paper_follow"})
        ctx = _make_context(_init_db(tmp_path), leaders_repo=repo)
        assert _resolve_leader_tier(ctx, "0xleader") == "paper_follow"

    def test_repo_live_follow(self, tmp_path):
        repo = _FakeLeadersRepo({"0xleader": "live_follow"})
        ctx = _make_context(_init_db(tmp_path), leaders_repo=repo)
        assert _resolve_leader_tier(ctx, "0xleader") == "live_follow"

    def test_repo_exception_returns_live(self, tmp_path):
        repo = MagicMock()
        repo.get_tier.side_effect = RuntimeError("db gone")
        ctx = _make_context(_init_db(tmp_path), leaders_repo=repo)
        assert _resolve_leader_tier(ctx, "0xleader") == "live_follow"

    def test_repo_unknown_tier_returns_live(self, tmp_path):
        repo = _FakeLeadersRepo({"0xleader": "something_unknown"})
        ctx = _make_context(_init_db(tmp_path), leaders_repo=repo)
        assert _resolve_leader_tier(ctx, "0xleader") == "live_follow"


# --- execute_trades tier-gating integration tests ---

@pytest.mark.asyncio
async def test_watchlist_tier_skips_trade(tmp_path):
    repo = _FakeLeadersRepo({"0xleader": "watchlist"})
    ctx = _make_context(_init_db(tmp_path), leaders_repo=repo, execution_service=MagicMock())
    results = await execute_trades(ctx, "0xleader", [_signal()], time.time())
    assert len(results) == 1
    assert results[0].status == "skipped"
    assert results[0].reason == "tier_watchlist"


@pytest.mark.asyncio
async def test_paper_follow_tier_records_paper(tmp_path):
    repo = _FakeLeadersRepo({"0xleader": "paper_follow"})
    ctx = _make_context(_init_db(tmp_path), leaders_repo=repo, execution_service=MagicMock())
    results = await execute_trades(ctx, "0xleader", [_signal()], time.time())
    assert len(results) == 1
    assert results[0].status == "paper"
    assert results[0].reason == "tier_paper_follow"


@pytest.mark.asyncio
async def test_paper_follow_marks_processed(tmp_path):
    """Paper trades should be idempotent (marked processed)."""
    repo = _FakeLeadersRepo({"0xleader": "paper_follow"})
    db = _init_db(tmp_path)
    ctx = _make_context(db, leaders_repo=repo, execution_service=MagicMock())
    sig = _signal(tx_hash="0xpaper1")
    await execute_trades(ctx, "0xleader", [sig], time.time())
    # Second call with same signal should be deduped
    results = await execute_trades(ctx, "0xleader", [sig], time.time())
    assert results[0].status == "skipped"
    assert results[0].reason == "already_processed"


@pytest.mark.asyncio
async def test_no_repo_allows_execution(tmp_path):
    """Without a leaders_repo, all trades proceed (backward compat for configured leaders)."""
    from unittest.mock import AsyncMock
    from services.executionService import ExecutionResult

    exec_svc = AsyncMock()
    exec_svc.execute = AsyncMock(return_value=ExecutionResult(
        accepted=True, order_id="ord123", status="filled",
        requested_amount=Decimal("5"), filled_amount=Decimal("5"),
        avg_price=Decimal("0.50"), fees_paid=Decimal("0.01"),
    ))
    ctx = _make_context(_init_db(tmp_path), leaders_repo=None, execution_service=exec_svc)
    results = await execute_trades(ctx, "0xleader", [_signal()], time.time())
    assert len(results) == 1
    # Without tier gate, trade should actually execute (not skipped/paper)
    assert results[0].status not in ("skipped", "paper") or results[0].reason not in ("tier_watchlist", "tier_paper_follow")


@pytest.mark.asyncio
async def test_multiple_signals_tier_applied_to_all(tmp_path):
    repo = _FakeLeadersRepo({"0xleader": "watchlist"})
    ctx = _make_context(_init_db(tmp_path), leaders_repo=repo, execution_service=MagicMock())
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
    from unittest.mock import AsyncMock
    from services.executionService import ExecutionResult

    repo = _FakeLeadersRepo({"0xleader": "watchlist"})
    exec_svc = AsyncMock()
    exec_svc.execute = AsyncMock(return_value=ExecutionResult(
        accepted=True, order_id="ord123", status="filled",
        requested_amount=Decimal("5"), filled_amount=Decimal("5"),
        avg_price=Decimal("0.50"), fees_paid=Decimal("0.01"),
    ))
    exec_svc.market_meta_service = AsyncMock()
    exec_svc.market_meta_service.get = AsyncMock(side_effect=Exception("no meta"))

    ctx = _make_context(
        _init_db(tmp_path),
        leaders_repo=repo,
        execution_service=exec_svc,
        config=_config(ENABLE_TIER_GATE_WATCHLIST="false", PERCENTAGE="100"),
    )
    results = await execute_trades(
        ctx,
        "0xleader",
        [_signal(price=1.0, size=25.0)],
        time.time(),
    )

    assert len(results) == 1
    assert results[0].reason != "tier_watchlist"


# --- PAPER_TRADE_ONLY global override tests ---

@pytest.mark.asyncio
async def test_paper_trade_only_overrides_live_follow(tmp_path):
    """Typed paper_trade_only must demote live_follow leaders to paper without executing."""
    repo = _FakeLeadersRepo({"0xleader": "live_follow"})
    from unittest.mock import AsyncMock
    exec_svc = AsyncMock()
    ctx = _make_context(
        _init_db(tmp_path),
        leaders_repo=repo,
        execution_service=exec_svc,
        config=_config(PAPER_TRADE_ONLY="true"),
    )
    results = await execute_trades(ctx, "0xleader", [_signal()], time.time())
    assert len(results) == 1
    assert results[0].status == "paper"
    assert results[0].reason == "tier_paper_follow"
    # Real execution must never be called
    exec_svc.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_paper_trade_only_does_not_affect_watchlist(tmp_path):
    """Typed paper_trade_only should not change watchlist behavior (still skipped)."""
    repo = _FakeLeadersRepo({"0xleader": "watchlist"})
    ctx = _make_context(
        _init_db(tmp_path),
        leaders_repo=repo,
        execution_service=MagicMock(),
        config=_config(PAPER_TRADE_ONLY="true"),
    )
    results = await execute_trades(ctx, "0xleader", [_signal()], time.time())
    assert len(results) == 1
    assert results[0].status == "skipped"
    assert results[0].reason == "tier_watchlist"


@pytest.mark.asyncio
async def test_paper_trade_only_false_allows_live(tmp_path):
    """Typed paper_trade_only=false must not interfere with live execution."""
    from unittest.mock import AsyncMock
    from services.executionService import ExecutionResult

    repo = _FakeLeadersRepo({"0xleader": "live_follow"})
    exec_svc = AsyncMock()
    exec_svc.execute = AsyncMock(return_value=ExecutionResult(
        accepted=True, order_id="ord456", status="filled",
        requested_amount=Decimal("5"), filled_amount=Decimal("5"),
        avg_price=Decimal("0.50"), fees_paid=Decimal("0.01"),
    ))
    ctx = _make_context(
        _init_db(tmp_path),
        leaders_repo=repo,
        execution_service=exec_svc,
        config=_config(PAPER_TRADE_ONLY="false"),
    )
    results = await execute_trades(ctx, "0xleader", [_signal()], time.time())
    assert len(results) == 1
    # With paper_trade_only disabled, execution should be attempted (not forced to paper)
    assert results[0].status != "paper"


@pytest.mark.asyncio
async def test_watchlist_gate_requires_injected_config(tmp_path):
    repo = _FakeLeadersRepo({"0xleader": "watchlist"})
    db_path = _init_db(tmp_path)
    ctx = TradingContext(
        user_id="user1",
        wallet_address=_VALID_FUNDER,
        client=MagicMock(),
        positions_repo=PositionsRepo(db_path),
        fills_repo=FillsRepo(db_path),
        orders_repo=OrdersRepo(db_path),
        risk_blocks_repo=MagicMock(),
        pnl_tracker=PnLTracker(log_path=str(db_path) + ".pnl"),
        risk_engine=RiskEngine(),
        accounting_service=AccountingService(),
        position_manager=PositionManager(),
        execution_service=MagicMock(),
        leaders_repo=repo,
        allocation_pct=1.0,
        buy_min_amount=0.01,
        buy_max_amount=1000.0,
        extra={"slippage_tolerance_bps": 200},
    )

    with pytest.raises(RuntimeError, match="TradingContext.config is required for live tier gating"):
        await execute_trades(ctx, "0xleader", [_signal()], time.time())
