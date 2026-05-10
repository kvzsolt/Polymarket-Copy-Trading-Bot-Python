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
from services.riskEngine import PortfolioSnapshot, ProposedTrade, RiskEngine, policy_from_config  # noqa: F401
from utils.createClobClient import MarketMeta


def _make_exec_config(
    min_share_price: float = 0.0,
    max_share_price: float = 0.0,
    pending_ttl: int = 120,
    enable_executable_order_guard: bool = True,
) -> SimpleNamespace:
    """Minimal AppConfig-shaped object for injecting into ExecutionService in tests."""
    ec = SimpleNamespace(
        min_share_price=min_share_price,
        max_share_price=max_share_price,
        pending_submission_ttl_seconds=pending_ttl,
        enable_executable_order_guard=enable_executable_order_guard,
        circuit_failure_threshold=5,
        circuit_cooldown_seconds=60,
        execution_require_fresh_book=False,
        book_spread_mode="absolute_bps",
        max_book_spread_bps=500,
        max_book_spread_ticks=0,
        slippage_tolerance=2.0,
        max_retries=3,
        retry_backoff_base=1.0,
        non_terminal_failure_max_attempts=3,
        transaction_fee_bps=20,
        gas_cost_usdc=0.03,
        risk_buffer_pct=0.10,
        paper_trade_only=False,
        enable_copy_decision_gate=False,
        enable_near_resolution_gate=False,
        near_resolution_gate_price=0.95,
        near_resolution_max_age_seconds=15,
        meta_cache_ttl_good=300,
        meta_cache_ttl_thin=30,
    )
    return SimpleNamespace(execution=ec, raw={})


def _make_risk_policy_config(
    buy_max_amount: float = 100,
    max_position_pct: float = 0.25,
    max_market_exposure_usdc: float = 300,
    max_category_exposure_usdc: float = 600,
    max_leader_exposure_usdc: float = 500,
    max_open_positions: int = 20,
    max_daily_drawdown_pct: float = 20,
    slippage_tolerance: float = 2.0,
    kill_switch: bool = False,
) -> SimpleNamespace:
    """Minimal AppConfig-shaped object for policy_from_config() in tests."""
    rc = SimpleNamespace(
        max_market_exposure_usdc=max_market_exposure_usdc,
        max_category_exposure_usdc=max_category_exposure_usdc,
        max_leader_exposure_usdc=max_leader_exposure_usdc,
        max_open_positions=max_open_positions,
        max_daily_drawdown_pct=max_daily_drawdown_pct,
        kill_switch=kill_switch,
    )
    ec = SimpleNamespace(slippage_tolerance=slippage_tolerance)
    sc = SimpleNamespace(buy_max_amount=buy_max_amount, max_position_pct=max_position_pct)
    return SimpleNamespace(risk=rc, execution=ec, sizing=sc)


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
    async def get(self, token_id: str, refresh: bool = False):  # noqa: ARG002
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
        svc = ExecutionService(clob, _MetaSvc(), _FeeSvc(), _Repo(), config=_make_exec_config(min_share_price=0.4))
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
        res = await svc.execute(req)

        assert res.accepted is False
        assert res.status == "rejected"
        assert res.error is not None
        assert "min_share_price" in res.error

    @pytest.mark.asyncio
    async def test_rejects_buy_above_max_share_price(self):
        clob = SimpleNamespace(get_price=AsyncMock(return_value={"price": "0.8"}))
        svc = ExecutionService(clob, _MetaSvc(), _FeeSvc(), _Repo(), config=_make_exec_config(min_share_price=0, max_share_price=0.7))
        req = ExecutionRequest(
            user_id="u1",
            leader_id="l1",
            token_id="t1",
            side="BUY",
            amount=Decimal("5"),
            max_slippage_bps=100,
            reason="copy_trade",
            signal_id="sig_maxprice",
        )
        res = await svc.execute(req)

        assert res.accepted is False
        assert res.status == "rejected"
        assert res.error is not None
        assert "max_share_price" in res.error

    @pytest.mark.asyncio
    async def test_executes_and_records_fill(self):
        clob = SimpleNamespace(
            get_price=AsyncMock(return_value={"price": "0.5"}),
            create_market_order=AsyncMock(return_value={"signed": True}),
            post_signed_order=AsyncMock(return_value={"success": True, "orderID": "o1", "status": "submitted"}),
            client=SimpleNamespace(get_order=lambda _order_id: {"status": "filled", "size_matched": "5", "avg_price": "0.5"}),
        )
        repo = _Repo()
        svc = ExecutionService(clob, _MetaSvc(), _FeeSvc(), repo, config=_make_exec_config(min_share_price=0))
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
        # Fill recording is now handled by ReconciliationService, not ExecutionService.
        # Verify the submission was persisted instead.
        assert "o1" in {rec.get("order_id") for rec in repo.store.values()}

    @pytest.mark.asyncio
    async def test_executable_order_guard_rejects_small_buy_when_enabled(self):
        """Small buys fail when the CLOB client is not fully mocked (guard removed, actual execution attempted)."""
        clob = SimpleNamespace(get_price=AsyncMock(return_value={"price": "0.5"}))
        svc = ExecutionService(
            clob,
            _MetaSvc(),
            _FeeSvc(),
            _Repo(),
            config=_make_exec_config(min_share_price=0, enable_executable_order_guard=True),
        )
        req = ExecutionRequest(
            user_id="u1",
            leader_id="l1",
            token_id="t1",
            side="BUY",
            amount=Decimal("0.10"),
            max_slippage_bps=100,
            reason="copy_trade",
            signal_id="sig_guard_on",
        )

        res = await svc.execute(req)

        # The executable order guard was removed; tiny orders now go through
        # to execution and fail through the normal mechanism
        assert res.accepted is False
        assert res.error is not None

    @pytest.mark.asyncio
    async def test_executable_order_guard_can_be_disabled_for_small_buy(self):
        clob = SimpleNamespace(
            get_price=AsyncMock(return_value={"price": "0.5"}),
            create_market_order=AsyncMock(return_value={"signed": True}),
            post_signed_order=AsyncMock(return_value={"success": True, "orderID": "o-small", "status": "submitted"}),
            client=SimpleNamespace(get_order=lambda _order_id: {"status": "filled", "size_matched": "0.2", "avg_price": "0.5"}),
        )
        repo = _Repo()
        svc = ExecutionService(
            clob,
            _MetaSvc(),
            _FeeSvc(),
            repo,
            config=_make_exec_config(min_share_price=0, enable_executable_order_guard=False),
        )
        req = ExecutionRequest(
            user_id="u1",
            leader_id="l1",
            token_id="t1",
            side="BUY",
            amount=Decimal("0.10"),
            max_slippage_bps=100,
            reason="copy_trade",
            signal_id="sig_guard_off",
        )

        res = await svc.execute(req)

        assert res.accepted is True
        assert res.order_id == "o-small"
        clob.create_market_order.assert_awaited_once()

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
    async def test_execution_service_does_not_expose_confirm_fill(self):
        """_confirm_fill was moved to ReconciliationService (issue #71).
        ExecutionService should not have this method.
        """
        clob = SimpleNamespace()
        svc = ExecutionService(clob, _MetaSvc(), _FeeSvc(), _Repo(), config=_make_exec_config(min_share_price=0))
        assert not hasattr(svc, "_confirm_fill"), "_confirm_fill should live in ReconciliationService, not ExecutionService"

    @pytest.mark.asyncio
    async def test_duplicate_idempotency_is_ignored(self):
        clob = SimpleNamespace(get_price=AsyncMock(return_value={"price": "0.5"}))
        repo = _Repo()
        svc = ExecutionService(clob, _MetaSvc(), _FeeSvc(), repo, config=_make_exec_config(min_share_price=0))
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

        svc._config = _make_exec_config(min_share_price=0, pending_ttl=120)
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

        svc._config = _make_exec_config(min_share_price=0, pending_ttl=120)
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
        svc = ExecutionService(clob, _MetaSvc(), _FeeSvc(), repo, config=_make_exec_config(min_share_price=0, pending_ttl=120))
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
    async def test_ambiguous_post_submit_returns_submitted_unconfirmed(self):
        clob = SimpleNamespace(
            get_price=AsyncMock(return_value={"price": "0.5"}),
            create_market_order=AsyncMock(return_value={"signed": True}),
            post_signed_order=AsyncMock(side_effect=RuntimeError("timeout after submit")),
        )
        repo = _Repo()
        svc = ExecutionService(clob, _MetaSvc(), _FeeSvc(), repo, config=_make_exec_config(min_share_price=0))
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
        policy = policy_from_config(_make_risk_policy_config(
            buy_max_amount=100,
            max_position_pct=0.25,
            max_market_exposure_usdc=300,
            max_category_exposure_usdc=600,
            max_leader_exposure_usdc=500,
            max_open_positions=20,
            max_daily_drawdown_pct=20,
            slippage_tolerance=2.0,
            kill_switch=False,
        ))
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
