"""Safety tests for the trade pipeline — duplicate prevention, failure handling, position updates."""

from __future__ import annotations

import time
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from config.env import ENV, rehydrate_env
from domain.trading_context import TradingContext
from orchestrators.trade_pipeline import execute_trades, build_trade_key
from services.copyDecisionService import CopyDecision
from services.executionService import ExecutionResult
from services.pnlTracker import PnLTracker
from services.positionManager import PositionManager
from services.riskEngine import RiskEngine
from services.accountingService import AccountingService
from storage.sqlite.connection import create_connection
from storage.sqlite.fills_repo import FillsRepo


@pytest.fixture(autouse=True)
def _lower_decision_gates_for_tests(monkeypatch):
    """Lower copy-decision thresholds so test signals pass the gate."""
    monkeypatch.setenv("COPY_DECISION_MIN_SCORE", "0.01")
    monkeypatch.setenv("COPY_MAX_SIGNAL_AGE_SECONDS", "999999")
    monkeypatch.setenv("COPY_MAX_PRICE_DRIFT_RATIO", "1.0")
    monkeypatch.setenv("COPY_MAX_EDGE_DECAY_RATIO", "1.0")
    monkeypatch.setenv("COPY_MIN_SHARES", "0.001")
    monkeypatch.setenv("COPY_MIN_NOTIONAL_USDC", "0.01")
    monkeypatch.setenv("BUY_MIN_AMOUNT", "0.01")
    monkeypatch.setenv("MIN_ORDER_SIZE", "0.00001")
    rehydrate_env()
    import orchestrators.trade_pipeline as _tp
    _tp._leader_scorer = None
    _tp._copy_decision = None
from storage.sqlite.migrations import apply_migrations
from storage.sqlite.orders_repo import OrdersRepo
from storage.sqlite.positions_repo import PositionsRepo

FAKE_TOKEN_ID = "71321045679252212594626385532706912750332728571942532223535764789812338637396"


def _init_db(tmp_path):
    db_path = str(tmp_path / "test.db")
    with create_connection(db_path) as conn:
        apply_migrations(conn)
    return db_path


def _make_context(db_path, *, execution_service=None):
    return TradingContext(
        user_id="user1",
        wallet_address="0x" + "a" * 40,
        client=MagicMock(),
        positions_repo=PositionsRepo(db_path),
        fills_repo=FillsRepo(db_path),
        orders_repo=OrdersRepo(db_path),
        pnl_tracker=PnLTracker(log_path=str(db_path) + ".pnl"),
        risk_engine=RiskEngine(),
        accounting_service=AccountingService(),
        position_manager=PositionManager(),
        execution_service=execution_service,
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
        "title": "Test Market",
        "eventSlug": "test-slug",
    }


def _exec_result(*, accepted=True, filled=Decimal("10"), status="filled", order_id="ord1", error=None):
    return ExecutionResult(
        accepted=accepted,
        order_id=order_id,
        status=status,
        requested_amount=Decimal("5"),
        filled_amount=filled,
        avg_price=Decimal("0.50") if filled > 0 else None,
        fees_paid=Decimal("0.01"),
        error=error,
    )


class _RecordingDecisionService:
    def __init__(self):
        self.buy_calls: list[dict[str, float]] = []

    def decide_buy(
        self,
        *,
        trade,
        leader_score,
        live_price,
        base_notional_usdc,
        max_notional_usdc,
        market_min_size,
    ):
        self.buy_calls.append(
            {
                "base_notional_usdc": float(base_notional_usdc),
                "max_notional_usdc": float(max_notional_usdc),
                "live_price": float(live_price),
                "market_min_size": float(market_min_size),
            }
        )
        target_notional = float(max_notional_usdc)
        target_size = target_notional / max(float(live_price), 1e-9)
        return CopyDecision(
            allow=True,
            skip_code="ok",
            reason="decision-approved BUY",
            side="BUY",
            decision_score=0.80,
            leader_score=float(leader_score.score),
            trade_score=0.80,
            live_price=float(live_price),
            reference_price=float(trade.get("price") or 0),
            target_notional_usdc=target_notional,
            target_size_tokens=target_size,
            price_drift_pct=0.0,
            edge_decay_pct=0.0,
            details={},
        )


@pytest.mark.asyncio
async def test_duplicate_trade_is_skipped(tmp_path):
    db_path = _init_db(tmp_path)
    exec_svc = AsyncMock()
    ctx = _make_context(db_path, execution_service=exec_svc)

    signal = _signal()
    trade_key = build_trade_key("user1", "leader1", signal)
    ctx.orders_repo.mark_processed_trade(
        trade_key=trade_key, user_id="user1", leader_id="leader1",
        tx_hash="0xabc", side="BUY", asset=FAKE_TOKEN_ID,
        condition_id="cond1", status="executed",
    )

    results = await execute_trades(ctx, "leader1", [signal], time.time())
    assert len(results) == 1
    assert results[0].status == "skipped"
    assert results[0].reason == "already_processed"
    exec_svc.execute.assert_not_called()


@pytest.mark.asyncio
async def test_transient_failure_not_marked_processed(tmp_path):
    db_path = _init_db(tmp_path)
    exec_svc = AsyncMock()
    exec_svc.execute.return_value = _exec_result(
        accepted=False, filled=Decimal("0"), status="failed", order_id=None, error="connection timeout"
    )
    ctx = _make_context(db_path, execution_service=exec_svc)

    signal = _signal()
    results = await execute_trades(ctx, "leader1", [signal], time.time())
    assert len(results) == 1
    assert results[0].status == "failed"

    # The trade should NOT be in processed_trades so it can be retried
    trade_key = build_trade_key("user1", "leader1", signal)
    assert ctx.orders_repo.has_processed_trade(trade_key) is False


@pytest.mark.asyncio
async def test_transient_failure_marked_processed_after_retry_budget(tmp_path, monkeypatch):
    db_path = _init_db(tmp_path)
    exec_svc = AsyncMock()
    exec_svc.execute.return_value = _exec_result(
        accepted=False, filled=Decimal("0"), status="failed", order_id=None, error="connection timeout"
    )
    ctx = _make_context(db_path, execution_service=exec_svc)
    monkeypatch.setitem(ENV, "NON_TERMINAL_FAILURE_MAX_ATTEMPTS", "2")

    signal = _signal(tx_hash="0xretry")
    trade_key = build_trade_key("user1", "leader1", signal)

    first = await execute_trades(ctx, "leader1", [signal], time.time())
    assert first[0].status == "failed"
    assert "retry budget exhausted" not in str(first[0].reason or "")
    assert ctx.orders_repo.has_processed_trade(trade_key) is False

    second = await execute_trades(ctx, "leader1", [signal], time.time())
    assert second[0].status == "failed"
    assert "retry budget exhausted" in str(second[0].reason or "")
    assert ctx.orders_repo.has_processed_trade(trade_key) is True

    third = await execute_trades(ctx, "leader1", [signal], time.time())
    assert third[0].status == "skipped"
    assert third[0].reason == "already_processed"
    assert exec_svc.execute.await_count == 2


@pytest.mark.asyncio
async def test_hard_rejection_is_marked_processed(tmp_path):
    db_path = _init_db(tmp_path)
    exec_svc = AsyncMock()
    exec_svc.execute.return_value = _exec_result(
        accepted=False, filled=Decimal("0"), status="rejected", order_id=None, error="amount below min_order_size"
    )
    ctx = _make_context(db_path, execution_service=exec_svc)

    signal = _signal()
    results = await execute_trades(ctx, "leader1", [signal], time.time())
    assert len(results) == 1
    assert results[0].status == "failed"

    trade_key = build_trade_key("user1", "leader1", signal)
    assert ctx.orders_repo.has_processed_trade(trade_key) is True


@pytest.mark.asyncio
async def test_submitted_unconfirmed_is_marked_processed(tmp_path):
    db_path = _init_db(tmp_path)
    exec_svc = AsyncMock()
    exec_svc.execute.return_value = _exec_result(
        accepted=True, filled=Decimal("0"), status="submitted_unconfirmed", order_id="ord1"
    )
    ctx = _make_context(db_path, execution_service=exec_svc)

    signal = _signal()
    results = await execute_trades(ctx, "leader1", [signal], time.time())
    assert len(results) == 1
    assert results[0].status == "submitted_unconfirmed"

    trade_key = build_trade_key("user1", "leader1", signal)
    assert ctx.orders_repo.has_processed_trade(trade_key) is True


@pytest.mark.asyncio
async def test_balance_tiered_policy_disabled_does_not_lookup_balance(tmp_path, monkeypatch):
    db_path = _init_db(tmp_path)
    exec_svc = AsyncMock()
    exec_svc.execute.return_value = _exec_result(
        accepted=True, filled=Decimal("10"), status="filled", order_id="ord-balance-off"
    )
    ctx = _make_context(db_path, execution_service=exec_svc)

    monkeypatch.setitem(ENV, "ENABLE_BALANCE_TIERED_POLICY", False)

    with patch("orchestrators.trade_pipeline.get_my_balance", new_callable=AsyncMock) as balance_lookup:
        results = await execute_trades(ctx, "leader1", [_signal(tx_hash="0xbal-off")], time.time())

    assert results[0].status == "executed"
    balance_lookup.assert_not_awaited()


@pytest.mark.asyncio
async def test_balance_tiered_policy_blocks_buy_without_tradable_cash(tmp_path, monkeypatch):
    db_path = _init_db(tmp_path)
    exec_svc = AsyncMock()
    ctx = _make_context(db_path, execution_service=exec_svc)

    monkeypatch.setitem(ENV, "ENABLE_BALANCE_TIERED_POLICY", True)
    monkeypatch.setitem(ENV, "DAY_START_EQUITY_USDC", 4)
    monkeypatch.setitem(ENV, "BALANCE_POLICY_RESERVE_PCT", 0.10)
    monkeypatch.setitem(ENV, "BALANCE_POLICY_RESERVE_FLOOR", 5)

    with patch(
        "orchestrators.trade_pipeline.get_my_balance",
        new_callable=AsyncMock,
        return_value=4.0,
    ), patch(
        "orchestrators.trade_pipeline.get_day_start_equity",
        return_value=Decimal("4"),
    ):
        results = await execute_trades(ctx, "leader1", [_signal(tx_hash="0xbal-block")], time.time())

    assert results[0].status == "skipped"
    assert results[0].reason == "no_tradable_cash_after_reserve"
    exec_svc.execute.assert_not_called()


@pytest.mark.asyncio
async def test_balance_tiered_policy_caps_buy_before_execution(tmp_path, monkeypatch):
    db_path = _init_db(tmp_path)
    exec_svc = AsyncMock()
    exec_svc.execute.return_value = _exec_result(
        accepted=True, filled=Decimal("10"), status="filled", order_id="ord-balance-cap"
    )
    ctx = _make_context(db_path, execution_service=exec_svc)

    monkeypatch.setitem(ENV, "ENABLE_BALANCE_TIERED_POLICY", True)
    monkeypatch.setitem(ENV, "DAY_START_EQUITY_USDC", 25)
    monkeypatch.setitem(ENV, "BALANCE_POLICY_RESERVE_PCT", 0.10)
    monkeypatch.setitem(ENV, "BALANCE_POLICY_RESERVE_FLOOR", 5)
    monkeypatch.setitem(ENV, "BALANCE_POLICY_HARD_CAP_PCT", 0.18)
    monkeypatch.setitem(ENV, "BALANCE_POLICY_HARD_CAP_CEILING", 4.5)

    import orchestrators.trade_pipeline as tp

    decision_service = _RecordingDecisionService()
    monkeypatch.setattr(tp, "_copy_decision", decision_service)

    with patch(
        "orchestrators.trade_pipeline.get_my_balance",
        new_callable=AsyncMock,
        return_value=25.0,
    ), patch(
        "orchestrators.trade_pipeline.get_day_start_equity",
        return_value=Decimal("25"),
    ):
        results = await execute_trades(ctx, "leader1", [_signal(tx_hash="0xbal-cap")], time.time())

    assert results[0].status == "executed"
    assert decision_service.buy_calls
    assert decision_service.buy_calls[0]["max_notional_usdc"] == pytest.approx(4.5)

    req = exec_svc.execute.await_args.args[0]
    assert req.side == "BUY"
    assert req.amount == Decimal("4.5")


@pytest.mark.asyncio
async def test_sell_with_no_position_skipped(tmp_path):
    db_path = _init_db(tmp_path)
    exec_svc = AsyncMock()
    ctx = _make_context(db_path, execution_service=exec_svc)

    signal = _signal(side="SELL")
    results = await execute_trades(ctx, "leader1", [signal], time.time())
    assert len(results) == 1
    assert results[0].status == "skipped"
    assert results[0].reason == "never_held"

    trade_key = build_trade_key("user1", "leader1", signal)
    assert ctx.orders_repo.has_processed_trade(trade_key) is True
    exec_svc.execute.assert_not_called()


@pytest.mark.asyncio
async def test_buy_fill_updates_position_repo(tmp_path):
    db_path = _init_db(tmp_path)
    exec_svc = AsyncMock()
    exec_svc.execute.return_value = _exec_result(
        accepted=True, filled=Decimal("10"), status="filled", order_id="ord1"
    )
    ctx = _make_context(db_path, execution_service=exec_svc)

    signal = _signal(price=0.50, size=20.0)
    results = await execute_trades(ctx, "leader1", [signal], time.time())
    assert len(results) == 1
    assert results[0].status == "executed"

    pos = ctx.positions_repo.get_position("user1", "cond1")
    assert pos is not None
    assert pos.size_tokens == Decimal("10")
    assert pos.avg_entry_price == Decimal("0.50")


@pytest.mark.asyncio
async def test_sell_reduces_position(tmp_path):
    db_path = _init_db(tmp_path)
    exec_svc = AsyncMock()
    ctx = _make_context(db_path, execution_service=exec_svc)

    # Set up existing position
    from storage.sqlite.positions_repo import PositionRecord
    ctx.positions_repo.upsert_position(PositionRecord(
        user_id="user1", condition_id="cond1", token_id=FAKE_TOKEN_ID,
        leader_id="leader1", size_tokens=Decimal("20"), avg_entry_price=Decimal("0.40"),
    ))

    exec_svc.execute.return_value = _exec_result(
        accepted=True, filled=Decimal("10"), status="filled", order_id="ord2"
    )

    signal = _signal(side="SELL", price=0.60, size=10.0)
    results = await execute_trades(ctx, "leader1", [signal], time.time())
    assert len(results) == 1
    assert results[0].status == "executed"

    pos = ctx.positions_repo.get_position("user1", "cond1")
    assert pos is not None
    assert pos.size_tokens == Decimal("10")


@pytest.mark.asyncio
async def test_sell_uses_notional_when_size_missing(tmp_path, monkeypatch):
    db_path = _init_db(tmp_path)
    exec_svc = AsyncMock()
    exec_svc.execute.return_value = _exec_result(
        accepted=True, filled=Decimal("4"), status="filled", order_id="ord-sell-derivation"
    )
    ctx = _make_context(db_path, execution_service=exec_svc)

    from storage.sqlite.positions_repo import PositionRecord
    ctx.positions_repo.upsert_position(PositionRecord(
        user_id="user1", condition_id="cond1", token_id=FAKE_TOKEN_ID,
        leader_id="leader1", size_tokens=Decimal("20"), avg_entry_price=Decimal("0.40"),
    ))

    monkeypatch.setitem(ENV, "ENABLE_COPY_DECISION_GATE", False)

    signal = _signal(side="SELL", price=0.50, size=0.0)
    signal["usdcSize"] = 2.0
    results = await execute_trades(ctx, "leader1", [signal], time.time())
    assert len(results) == 1
    assert results[0].status == "executed"

    req = exec_svc.execute.await_args.args[0]
    assert req.side == "SELL"
    assert req.amount == Decimal("4")


@pytest.mark.asyncio
async def test_sell_realized_pnl_falls_back_to_updated_position_or_signal_price(tmp_path):
    db_path = _init_db(tmp_path)
    exec_svc = AsyncMock()
    exec_svc.execute.return_value = _exec_result(
        accepted=True, filled=Decimal("10"), status="filled", order_id="ord-sell-fallback"
    )
    ctx = _make_context(db_path, execution_service=exec_svc)

    pnl_tracker = MagicMock()
    ctx.pnl_tracker = pnl_tracker
    ctx.positions_repo.get_position = MagicMock(
        return_value=SimpleNamespace(size_tokens=Decimal("20"), avg_entry_price=None)
    )
    ctx.positions_repo.apply_fill = MagicMock(
        return_value=SimpleNamespace(avg_entry_price=Decimal("0.45"))
    )

    signal = _signal(side="SELL", price=0.60, size=10.0)
    results = await execute_trades(ctx, "leader1", [signal], time.time())

    assert len(results) == 1
    assert results[0].status == "executed"

    record_sell_kwargs = pnl_tracker.record_sell.call_args.kwargs
    assert record_sell_kwargs["entry_price"] == pytest.approx(0.45)
    assert record_sell_kwargs["realized_pnl"] == pytest.approx(0.5)


@pytest.mark.asyncio
async def test_sell_realized_pnl_falls_back_to_signal_price_when_position_prices_missing(tmp_path):
    db_path = _init_db(tmp_path)
    exec_svc = AsyncMock()
    exec_svc.execute.return_value = ExecutionResult(
        accepted=True,
        order_id="ord-sell-signal-fallback",
        status="filled",
        requested_amount=Decimal("10"),
        filled_amount=Decimal("10"),
        avg_price=Decimal("0.50"),
        fees_paid=Decimal("0.01"),
        error=None,
    )
    ctx = _make_context(db_path, execution_service=exec_svc)

    pnl_tracker = MagicMock()
    ctx.pnl_tracker = pnl_tracker
    ctx.positions_repo.get_position = MagicMock(
        return_value=SimpleNamespace(size_tokens=Decimal("20"), avg_entry_price=None)
    )
    ctx.positions_repo.apply_fill = MagicMock(return_value=None)

    signal = _signal(side="SELL", price=0.60, size=10.0)
    results = await execute_trades(ctx, "leader1", [signal], time.time())

    assert len(results) == 1
    assert results[0].status == "executed"

    record_sell_kwargs = pnl_tracker.record_sell.call_args.kwargs
    assert record_sell_kwargs["entry_price"] == pytest.approx(0.60)
    assert record_sell_kwargs["realized_pnl"] == pytest.approx(-1.0)


# ---------------------------------------------------------------------------
# Regression: token_id field-mapping priority (tokenID > token_id > asset)
# ---------------------------------------------------------------------------

from orchestrators.trade_pipeline import normalize_signals

HEX_ASSET = "0x951403abcdef1234567890abcdef1234567890ab8665"


def _raw_trade(**overrides):
    """Minimal raw trade payload accepted by normalize_signals."""
    base = {
        "side": "BUY",
        "conditionId": "cond1",
        "transactionHash": "0xdeadbeef",
        "price": 0.55,
        "size": 5.0,
        "usdcSize": 2.75,
        "timestamp": int(time.time()),
    }
    base.update(overrides)
    return base


class TestTokenIdFieldPriority:
    """tokenID must win over asset when both are present."""

    def test_tokenID_preferred_over_hex_asset(self):
        """Live-ish payload: asset=hex, tokenID=numeric → tokenID must win."""
        raw = _raw_trade(asset=HEX_ASSET, tokenID=FAKE_TOKEN_ID)
        signals = normalize_signals([raw], leader_id="L1")
        assert len(signals) == 1
        assert signals[0][0].token_id == FAKE_TOKEN_ID

    def test_token_id_preferred_over_hex_asset(self):
        """Underscore variant: asset=hex, token_id=numeric → token_id must win."""
        raw = _raw_trade(asset=HEX_ASSET, token_id=FAKE_TOKEN_ID)
        signals = normalize_signals([raw], leader_id="L1")
        assert len(signals) == 1
        assert signals[0][0].token_id == FAKE_TOKEN_ID

    def test_asset_fallback_when_no_tokenID(self):
        """If tokenID/token_id absent, asset (valid numeric) is used as fallback."""
        raw = _raw_trade(asset=FAKE_TOKEN_ID)
        signals = normalize_signals([raw], leader_id="L1")
        assert len(signals) == 1
        assert signals[0][0].token_id == FAKE_TOKEN_ID

    def test_hex_asset_only_is_rejected(self):
        """Hex-only asset fails is_valid_clob_token_id and signal is skipped."""
        raw = _raw_trade(asset=HEX_ASSET)
        signals = normalize_signals([raw], leader_id="L1")
        assert len(signals) == 0

    def test_tokenID_only(self):
        """Payload with only tokenID (no asset) works fine."""
        raw = _raw_trade(tokenID=FAKE_TOKEN_ID)
        signals = normalize_signals([raw], leader_id="L1")
        assert len(signals) == 1
        assert signals[0][0].token_id == FAKE_TOKEN_ID

    def test_trade_metadata_primary_fields(self):
        raw = _raw_trade(
            tokenID=FAKE_TOKEN_ID,
            outcome="Blackhawks",
            submarket="Moneyline",
        )
        signals = normalize_signals([raw], leader_id="L1")
        assert len(signals) == 1
        signal = signals[0][0]
        assert signal.outcome == "Blackhawks"
        assert signal.submarket == "Moneyline"

    def test_trade_metadata_fallback_fields(self):
        raw = _raw_trade(
            tokenID=FAKE_TOKEN_ID,
            selectionName="Hurricanes",
            marketSubtype="Spread",
        )
        signals = normalize_signals([raw], leader_id="L1")
        assert len(signals) == 1
        signal = signals[0][0]
        assert signal.outcome == "Hurricanes"
        assert signal.submarket == "Spread"

    def test_build_trade_key_prefers_tokenID(self):
        """build_trade_key must also prefer tokenID over hex asset."""
        trade = {"transactionHash": "0xabc", "side": "BUY",
                 "asset": HEX_ASSET, "tokenID": FAKE_TOKEN_ID}
        key = build_trade_key("u1", "l1", trade)
        assert FAKE_TOKEN_ID in key
        assert HEX_ASSET not in key


@pytest.mark.asyncio
async def test_full_flow_persisted_token_id(tmp_path):
    """End-to-end: raw payload → normalize → execute → positions_repo stores correct token_id."""
    db_path = _init_db(tmp_path)
    exec_svc = AsyncMock()
    exec_svc.execute = AsyncMock(return_value=_exec_result(filled=Decimal("5")))
    ctx = _make_context(db_path, execution_service=exec_svc)

    # Simulate a live payload where asset is a hex string but tokenID is correct
    raw = _raw_trade(asset=HEX_ASSET, tokenID=FAKE_TOKEN_ID, size=100.0, price=0.50, usdcSize=50.0)
    results = await execute_trades(ctx, "leader1", [raw], time.time())

    assert len(results) == 1
    assert results[0].status == "executed"

    # Verify the persisted position uses the correct (numeric) token_id
    pos = ctx.positions_repo.get_position("user1", "cond1")
    assert pos is not None
    assert pos.token_id == FAKE_TOKEN_ID
    assert pos.token_id != HEX_ASSET
