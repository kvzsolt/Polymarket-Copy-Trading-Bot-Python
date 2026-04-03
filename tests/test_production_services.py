from __future__ import annotations

import sys
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.env import SignatureType, WalletConfig, validate_wallet_config
from services.backtest import BacktestConfig, BacktestEngine
from services.executionService import ExecutionRequest, ExecutionService, _sanitize_submission_response
from services.marketMetaService import MarketMetaService
from services.riskEngine import PortfolioSnapshot, ProposedTrade, RiskEngine, policy_from_env
from utils.createClobClient import MarketMeta


class TestWalletValidation:
    def test_valid_wallet_config(self):
        cfg = WalletConfig(
            private_key="0x" + ("ab" * 32),
            chain_id=137,
            signature_type=SignatureType.EOA,
            funder_address="0x" + ("1" * 40),
            polygon_rpc_url="https://polygon-rpc.com",
        )
        validate_wallet_config(cfg)

    def test_invalid_chain_raises(self):
        cfg = WalletConfig(
            private_key="0x" + ("ab" * 32),
            chain_id=1,
            signature_type=SignatureType.EOA,
            funder_address="0x" + ("1" * 40),
            polygon_rpc_url="https://polygon-rpc.com",
        )
        with pytest.raises(Exception):
            validate_wallet_config(cfg)


class TestMarketMetaService:
    @pytest.mark.asyncio
    async def test_caches_market_meta(self):
        meta = MarketMeta(
            token_id="t1",
            tick_size=Decimal("0.01"),
            min_order_size=Decimal("1"),
            neg_risk=False,
        )
        fake_clob = SimpleNamespace(get_market_meta=AsyncMock(return_value=meta))
        svc = MarketMetaService(fake_clob, ttl_seconds=60)
        first = await svc.get("t1")
        second = await svc.get("t1")
        assert first == second
        assert fake_clob.get_market_meta.call_count == 1


class _Repo:
    def __init__(self):
        self.store = {}
        self.fills = {}
        self.history = []

    def get_by_idempotency_key(self, key):
        return self.store.get(key)

    def save_submission(self, *, idempotency_key, request, response):
        self.store[idempotency_key] = {"request": request, "response": response, "order_id": response.get("orderID")}
        self.history.append({
            "idempotency_key": idempotency_key,
            "request": request,
            "response": response,
        })

    def mark_fill(self, *, order_id, fill):
        self.fills[order_id] = fill


@dataclass
class _FeeSvc:
    fee_bps: int = 20

    async def get_fee_bps(self, _token_id: str) -> int:
        return self.fee_bps


@dataclass
class _MetaSvc:
    async def get(self, token_id: str):
        return MarketMeta(
            token_id=token_id,
            tick_size=Decimal("0.01"),
            min_order_size=Decimal("1"),
            neg_risk=False,
            best_bid=Decimal("0.49"),
            best_ask=Decimal("0.51"),
        )


class TestExecutionService:
    @pytest.mark.asyncio
    async def test_rejects_non_positive_amount(self):
        clob = SimpleNamespace(get_price=AsyncMock(return_value={"price": "0.5"}))
        svc = ExecutionService(clob, _MetaSvc(), _FeeSvc(), _Repo())
        req = ExecutionRequest(
            user_id="u1",
            leader_id="l1",
            token_id="t1",
            side="BUY",
            amount=Decimal("0"),
            max_slippage_bps=100,
            reason="copy_trade",
            signal_id="sig_zero",
        )
        res = await svc.execute(req)
        assert res.accepted is False
        assert res.status == "rejected"

    @pytest.mark.asyncio
    async def test_rejects_buy_below_min_share_price(self):
        clob = SimpleNamespace(get_price=AsyncMock(return_value={"price": "0.3"}))
        svc = ExecutionService(clob, _MetaSvc(), _FeeSvc(), _Repo())
        req = ExecutionRequest(
            user_id="u1",
            leader_id="l1",
            token_id="t1",
            side="BUY",
            amount=Decimal("5"),
            max_slippage_bps=100,
            reason="copy_trade",
            signal_id="sig_minprice",
        )
        with patch("services.executionService.ENV", {"MIN_SHARE_PRICE": 0.4}):
            res = await svc.execute(req)

        assert res.accepted is False
        assert res.status == "rejected"
        assert res.error is not None
        assert "min_share_price" in res.error

    @pytest.mark.asyncio
    @patch("services.executionService.ENV", {"MIN_SHARE_PRICE": 0})
    async def test_executes_and_records_fill(self):
        clob = SimpleNamespace(
            get_price=AsyncMock(return_value={"price": "0.5"}),
            create_market_order=AsyncMock(return_value={"signed": True}),
            post_signed_order=AsyncMock(return_value={"success": True, "orderID": "o1", "status": "submitted"}),
            client=SimpleNamespace(get_order=lambda _order_id: {"status": "filled", "size_matched": "5", "avg_price": "0.5"}),
        )
        repo = _Repo()
        svc = ExecutionService(clob, _MetaSvc(), _FeeSvc(), repo)
        svc._confirm_fill = AsyncMock(return_value={"status": "filled", "size_matched": "5", "avg_price": "0.5"})
        req = ExecutionRequest(
            user_id="u1",
            leader_id="l1",
            token_id="t1",
            side="BUY",
            amount=Decimal("5"),
            max_slippage_bps=100,
            reason="copy_trade",
            signal_id="sig_fill",
        )
        res = await svc.execute(req)
        assert res.accepted is True
        assert res.order_id == "o1"
        assert "o1" in repo.fills

    def test_persists_sanitized_submission_payload(self):
        long_signal = "sig_" + ("x" * 600)
        repo = _Repo()
        svc = ExecutionService(
            clob_client=SimpleNamespace(),
            market_meta_service=SimpleNamespace(),
            fee_service=SimpleNamespace(),
            order_repo=repo,
        )
        idem_key = "persist-sanitized"
        svc._save_submission(
            idempotency_key=idem_key,
            request={
                "user_id": "u1",
                "leader_id": "l1",
                "token_id": "t1",
                "side": "BUY",
                "amount": "5",
                "signal_id": long_signal,
                "secret": "drop-me",
            },
            response={
                "success": True,
                "orderID": "o-sanitized",
                "status": "submitted",
                "data": {"sensitive": "should-not-persist"},
                "unexpected": "drop-me",
            },
        )

        stored = repo.store[idem_key]
        assert "data" not in stored["response"]
        assert "unexpected" not in stored["response"]
        assert stored["response"]["orderID"] == "o-sanitized"
        assert "secret" not in stored["request"]
        assert stored["request"]["signal_id"] != long_signal
        assert len(stored["request"]["signal_id"]) < len(long_signal)

    @pytest.mark.asyncio
    @patch("services.executionService.ENV", {"MIN_SHARE_PRICE": 0})
    async def test_duplicate_idempotency_is_ignored(self):
        clob = SimpleNamespace(get_price=AsyncMock(return_value={"price": "0.5"}))
        repo = _Repo()
        svc = ExecutionService(clob, _MetaSvc(), _FeeSvc(), repo)
        req = ExecutionRequest(
            user_id="u1",
            leader_id="l1",
            token_id="t1",
            side="BUY",
            amount=Decimal("5"),
            max_slippage_bps=100,
            reason="copy_trade",
            signal_id="sig_dup",
        )
        # Seed duplicate — must have order_id to block retry
        key = "d"
        repo.store = {key: {"order_id": "existing", "status": "filled"}}
        with patch("services.executionService._idempotency_key", return_value=key):
            res = await svc.execute(req)
        assert res.status == "duplicate_ignored"

    @pytest.mark.asyncio
    async def test_stale_pending_submission_expires_and_retries(self):
        clob = SimpleNamespace(
            get_price=AsyncMock(return_value={"price": "0.5"}),
            create_market_order=AsyncMock(return_value={"signed": True}),
            post_signed_order=AsyncMock(return_value={"success": True, "orderID": "o2", "status": "submitted"}),
            client=SimpleNamespace(get_order=lambda _order_id: {"status": "filled", "size_matched": "5", "avg_price": "0.5"}),
        )
        repo = _Repo()
        svc = ExecutionService(clob, _MetaSvc(), _FeeSvc(), repo)
        req = ExecutionRequest(
            user_id="u1",
            leader_id="l1",
            token_id="t1",
            side="BUY",
            amount=Decimal("5"),
            max_slippage_bps=100,
            reason="copy_trade",
            signal_id="sig_stale_pending",
        )

        key = "stale-pending-key"
        repo.store[key] = {
            "status": "pending",
            "order_id": None,
            "request": {"user_id": "u1", "leader_id": "l1"},
            "response": {"status": "pending", "pending_since": 0},
        }

        with patch("services.executionService.ENV", {"MIN_SHARE_PRICE": 0, "PENDING_SUBMISSION_TTL_SECONDS": 120}):
            with patch("services.executionService._idempotency_key", return_value=key):
                res = await svc.execute(req)

        assert res.accepted is True
        assert res.status != "duplicate_ignored"
        assert clob.create_market_order.await_count == 1

    @pytest.mark.asyncio
    async def test_stale_submitted_unconfirmed_without_order_id_expires_and_retries(self):
        clob = SimpleNamespace(
            get_price=AsyncMock(return_value={"price": "0.5"}),
            create_market_order=AsyncMock(return_value={"signed": True}),
            post_signed_order=AsyncMock(return_value={"success": True, "orderID": "o2", "status": "submitted"}),
            client=SimpleNamespace(get_order=lambda _order_id: {"status": "filled", "size_matched": "5", "avg_price": "0.5"}),
        )
        repo = _Repo()
        svc = ExecutionService(clob, _MetaSvc(), _FeeSvc(), repo)
        req = ExecutionRequest(
            user_id="u1",
            leader_id="l1",
            token_id="t1",
            side="BUY",
            amount=Decimal("5"),
            max_slippage_bps=100,
            reason="copy_trade",
            signal_id="sig_stale_submitted_unconfirmed",
        )

        key = "stale-submitted-unconfirmed-key"
        repo.store[key] = {
            "status": "submitted_unconfirmed",
            "order_id": None,
            "request": {"user_id": "u1", "leader_id": "l1"},
            "response": {"status": "submitted_unconfirmed", "pending_since": 0},
        }

        with patch("services.executionService.ENV", {"MIN_SHARE_PRICE": 0, "PENDING_SUBMISSION_TTL_SECONDS": 120}):
            with patch("services.executionService._idempotency_key", return_value=key):
                res = await svc.execute(req)

        assert res.accepted is True
        assert res.status != "duplicate_ignored"
        assert clob.create_market_order.await_count == 1

    @pytest.mark.asyncio
    async def test_pending_submission_captures_timestamps_and_ttl(self):
        clob = SimpleNamespace(
            get_price=AsyncMock(return_value={"price": "0.5"}),
            create_market_order=AsyncMock(return_value={"signed": True}),
            post_signed_order=AsyncMock(return_value={"success": True, "orderID": "o3", "status": "submitted"}),
            client=SimpleNamespace(get_order=lambda _order_id: {"status": "filled", "size_matched": "5", "avg_price": "0.5"}),
        )
        repo = _Repo()
        svc = ExecutionService(clob, _MetaSvc(), _FeeSvc(), repo)
        req = ExecutionRequest(
            user_id="u1",
            leader_id="l1",
            token_id="t1",
            side="BUY",
            amount=Decimal("5"),
            max_slippage_bps=100,
            reason="copy_trade",
            signal_id="sig_pending_ttl",
        )

        with patch("services.executionService.ENV", {"MIN_SHARE_PRICE": 0, "PENDING_SUBMISSION_TTL_SECONDS": 120}):
            await svc.execute(req)

        pending_write = next(item for item in repo.history if item["response"].get("status") == "pending")
        pending_response = pending_write["response"]
        assert "pending_since" in pending_response
        assert pending_response.get("pending_ttl_seconds") == 120
        assert pending_response.get("pending_expires_at") == pytest.approx(
            pending_response["pending_since"] + 120,
            rel=0,
            abs=1e-6,
        )

    def test_submission_response_sanitizer_drops_unknown_fields_and_truncates_error(self):
        long_error = "e" * 5000
        sanitized = _sanitize_submission_response(
            {
                "success": "false",
                "status": "failed",
                "error": long_error,
                "data": {"token": "secret"},
                "debug": "verbose",
            }
        )

        assert sanitized["success"] is False
        assert "data" not in sanitized
        assert "debug" not in sanitized
        assert sanitized["error"] != long_error
        assert len(sanitized["error"]) < len(long_error)

    @pytest.mark.asyncio
    @patch("services.executionService.ENV", {"MIN_SHARE_PRICE": 0})
    async def test_ambiguous_post_submit_returns_submitted_unconfirmed(self):
        clob = SimpleNamespace(
            get_price=AsyncMock(return_value={"price": "0.5"}),
            create_market_order=AsyncMock(return_value={"signed": True}),
            post_signed_order=AsyncMock(side_effect=RuntimeError("timeout after submit")),
        )
        repo = _Repo()
        svc = ExecutionService(clob, _MetaSvc(), _FeeSvc(), repo)
        req = ExecutionRequest(
            user_id="u1",
            leader_id="l1",
            token_id="t1",
            side="BUY",
            amount=Decimal("5"),
            max_slippage_bps=100,
            reason="copy_trade",
            signal_id="sig_ambiguous_post",
        )

        res = await svc.execute(req)

        assert res.accepted is True
        assert res.status == "submitted_unconfirmed"
        assert clob.create_market_order.await_count == 1


class TestRiskEngine:
    def test_policy_rejects_over_trade_limit(self):
        engine = RiskEngine()
        with patch("services.riskEngine.ENV", {
            "BUY_MAX_AMOUNT": 100,
            "MAX_POSITION_PCT": 0.25,
            "MAX_MARKET_EXPOSURE_USDC": 300,
            "MAX_CATEGORY_EXPOSURE_USDC": 600,
            "MAX_LEADER_EXPOSURE_USDC": 500,
            "MAX_OPEN_POSITIONS": 20,
            "MAX_DAILY_DRAWDOWN_PCT": 20,
            "SLIPPAGE_TOLERANCE": 2.0,
            "KILL_SWITCH": False,
        }):
            policy = policy_from_env()
        portfolio = PortfolioSnapshot(
            equity_usdc=Decimal("1000"),
            daily_drawdown_pct=Decimal("0"),
            open_positions_count=1,
            market_exposures={},
            category_exposures={},
            leader_exposures={},
        )
        trade = ProposedTrade(
            user_id="u",
            leader_id="l",
            market_id="m1",
            category="c1",
            token_id="t1",
            side="BUY",
            notional_usdc=Decimal("150"),
        )
        decision = engine.evaluate(portfolio, policy, trade)
        assert decision.allow is False


class TestBacktest:
    def test_backtest_runs(self):
        cfg = BacktestConfig(
            initial_equity=1000.0,
            max_trade_usdc=100.0,
            slippage_bps=50,
            fee_bps=20,
            latency_ms=200,
        )
        signals = [
            {"timestamp": 1, "token_id": "t1", "side": "BUY", "usdc_size": 50, "price": 0.5},
            {"timestamp": 2, "token_id": "t1", "side": "SELL", "size": 50, "price": 0.6},
        ]
        market = {"t1": 0.55}
        res = BacktestEngine().run(signals, market, cfg)
        assert "final_equity" in res
        assert res["num_points"] >= 1
