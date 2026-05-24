"""Issue #66 — CLOB V2 gateway isolation acceptance tests.

These tests use the canonical ``ClobGateway`` Protocol shape and a typed
``AppConfig``-shaped fixture so they exercise the gateway path end-to-end
without hitting the configless fail-closed short-circuit owned by issue #73.
"""

from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from integrations.polymarket.clob_v2_gateway import ClobV2Gateway, _build_submit_result
from integrations.polymarket.errors import (
    VenueAuthError,
    VenueInvalidOrderError,
    VenueUnavailableError,
)
from integrations.polymarket.models import (
    MarketableOrderRequest,
    VenueCancelResult,
    VenueOrderState,
    VenueSubmitResult,
)
from interfaces.clob_gateway import ClobGateway
from services.executionService import ExecutionRequest, ExecutionService
from services.orderRepository import OrderRepository
from storage.sqlite.connection import create_connection
from storage.sqlite.migrations import apply_migrations
from utils.createClobClient import MarketMeta
from cache.market_state_store import BookSnapshot, QuoteSnapshot, TickSnapshot
from services.market_data_service import MarketDataBundle, MarketDataSource


# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_circuit():
    import services.executionService as _es
    _es._CLOB_CIRCUIT.reset()
    with patch.dict("config.env.ENV", {"MAX_BOOK_SPREAD_BPS": "5000"}, clear=False):
        yield
    _es._CLOB_CIRCUIT.reset()


def _make_exec_config(min_share_price: float = 0.0) -> SimpleNamespace:
    ec = SimpleNamespace(
        min_share_price=min_share_price,
        max_share_price=0.0,
        pending_submission_ttl_seconds=120,
        enable_executable_order_guard=True,
        circuit_failure_threshold=5,
        circuit_cooldown_seconds=60,
        execution_require_fresh_book=False,
        book_spread_mode="absolute_bps",
        max_book_spread_bps=5000,
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
    return SimpleNamespace(execution=ec, raw={"EXECUTION_SUBMIT_CONCURRENCY": 2})


class _FeeSvc:
    async def get_fee_bps(self, _token_id: str) -> int:
        return 0


class _MetaSvc:
    def __init__(self, neg_risk: bool = False) -> None:
        self._neg_risk = neg_risk

    async def get(self, token_id: str, refresh: bool = False) -> MarketMeta:
        return MarketMeta(
            token_id=token_id,
            tick_size=Decimal("0.01"),
            min_order_size=Decimal("0"),
            neg_risk=self._neg_risk,
            best_bid=Decimal("0.49"),
            best_ask=Decimal("0.51"),
        )


import time as _time


def _make_mds(token_id: str = "t1", *, neg_risk: bool = False) -> AsyncMock:
    """Return a MarketDataService AsyncMock yielding a valid bundle."""
    now = _time.monotonic()
    bid = Decimal("0.49")
    ask = Decimal("0.51")
    bundle = MarketDataBundle(
        token_id=token_id,
        quote=QuoteSnapshot(token_id=token_id, best_bid=bid, best_ask=ask, last_trade_price=None, source="wss", monotonic_ts=now),
        book=BookSnapshot(token_id=token_id, bids={bid: Decimal("100")}, asks={ask: Decimal("100")}, best_bid=bid, best_ask=ask, source="wss", monotonic_ts=now),
        tick=TickSnapshot(token_id=token_id, tick_size=Decimal("0.01"), min_order_size=Decimal("1"), neg_risk=neg_risk, source="wss", monotonic_ts=now),
        source=MarketDataSource.WSS,
    )
    mds = AsyncMock()
    mds.require_fresh_data = AsyncMock(return_value=bundle)
    return mds


class _StubClob:
    """Minimal venue stub used only because ExecutionService still needs a
    ``clob_client`` arg for non-submit paths (e.g. ``get_price`` for slippage
    decoration). Submission goes through the injected gateway mock."""

    def __init__(self) -> None:
        self.get_price = AsyncMock(return_value={"price": "0.50"})
        self.client = SimpleNamespace(
            get_order=lambda _oid: {"status": "filled", "size_matched": "5", "avg_price": "0.50"},
        )


def _prepare_db(path: Path) -> None:
    with create_connection(str(path)) as conn:
        apply_migrations(conn)
        conn.commit()


def _make_req(side: str = "BUY", **overrides) -> ExecutionRequest:
    defaults = dict(
        user_id="u1",
        leader_id="l1",
        token_id="t1",
        side=side,
        amount=Decimal("5"),
        max_slippage_bps=100,
        reason="copy_trade",
        signal_id="sig-1",
    )
    defaults.update(overrides)
    return ExecutionRequest(**defaults)


def _make_gateway(submit_result: VenueSubmitResult | Exception) -> MagicMock:
    """Build a gateway mock that returns / raises on submit_marketable_order."""
    gateway = MagicMock(spec=ClobGateway)
    if isinstance(submit_result, Exception):
        gateway.submit_marketable_order = AsyncMock(side_effect=submit_result)
    else:
        gateway.submit_marketable_order = AsyncMock(return_value=submit_result)
    gateway.submit_limit_order = AsyncMock()
    gateway.get_order = AsyncMock(
        return_value=VenueOrderState(order_id="ord-1", status="filled", size=Decimal("5"), filled_size=Decimal("5"))
    )
    gateway.get_fills = AsyncMock(return_value=[])
    gateway.get_market_info = AsyncMock(return_value={"tick_size": "0.01", "neg_risk": False})
    gateway.get_orderbook = AsyncMock()
    gateway.cancel_order = AsyncMock(return_value=VenueCancelResult(success=True, order_id="ord-1"))
    gateway.check_compatibility = AsyncMock(return_value={"sdk_available": True})
    return gateway


# ---------------------------------------------------------------------------
# Gateway acceptance: BUY / SELL submit
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_buy_submission_uses_gateway_marketable_path(tmp_path):
    db_path = tmp_path / "state.db"
    _prepare_db(db_path)
    repo = OrderRepository(str(db_path))
    gateway = _make_gateway(
        VenueSubmitResult(success=True, order_id="ord-buy", status="submitted")
    )
    svc = ExecutionService(
        _StubClob(), _FeeSvc(), repo,
        config=_make_exec_config(),
        clob_gateway=gateway,
        market_data_service=_make_mds(),
    )
    result = await svc.execute(_make_req(side="BUY", signal_id="sig-buy"))
    assert result.accepted is True
    gateway.submit_marketable_order.assert_awaited_once()
    request = gateway.submit_marketable_order.await_args.args[0]
    assert isinstance(request, MarketableOrderRequest)
    assert request.token_id == "t1"
    assert request.side == "BUY"
    assert request.size == Decimal("5")


@pytest.mark.asyncio
async def test_sell_submission_uses_gateway_marketable_path(tmp_path):
    db_path = tmp_path / "state.db"
    _prepare_db(db_path)
    repo = OrderRepository(str(db_path))
    gateway = _make_gateway(
        VenueSubmitResult(success=True, order_id="ord-sell", status="submitted")
    )
    svc = ExecutionService(
        _StubClob(), _FeeSvc(), repo,
        config=_make_exec_config(),
        clob_gateway=gateway,
        market_data_service=_make_mds(),
    )
    result = await svc.execute(_make_req(side="SELL", signal_id="sig-sell"))
    assert result.accepted is True
    request = gateway.submit_marketable_order.await_args.args[0]
    assert request.side == "SELL"


@pytest.mark.asyncio
async def test_neg_risk_propagates_through_gateway_request(tmp_path):
    db_path = tmp_path / "state.db"
    _prepare_db(db_path)
    repo = OrderRepository(str(db_path))
    gateway = _make_gateway(
        VenueSubmitResult(success=True, order_id="ord-nr", status="submitted")
    )
    svc = ExecutionService(
        _StubClob(), _FeeSvc(), repo,
        config=_make_exec_config(),
        clob_gateway=gateway,
        market_data_service=_make_mds(neg_risk=True),
    )
    await svc.execute(_make_req(signal_id="sig-nr"))
    request = gateway.submit_marketable_order.await_args.args[0]
    assert request.neg_risk is True
    assert request.tick_size == Decimal("0.01")


# ---------------------------------------------------------------------------
# Error classification: non-retryable vs ambiguous
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_invalid_order_error_is_non_retryable(tmp_path):
    db_path = tmp_path / "state.db"
    _prepare_db(db_path)
    repo = OrderRepository(str(db_path))
    gateway = _make_gateway(VenueInvalidOrderError("invalid amount"))
    svc = ExecutionService(
        _StubClob(), _FeeSvc(), repo,
        config=_make_exec_config(),
        clob_gateway=gateway,
        market_data_service=_make_mds(),
    )
    result = await svc.execute(_make_req(signal_id="sig-bad"))
    assert result.accepted is False
    # Non-retryable means the gateway is called exactly once, not retried.
    assert gateway.submit_marketable_order.await_count == 1


@pytest.mark.asyncio
async def test_auth_error_is_non_retryable(tmp_path):
    db_path = tmp_path / "state.db"
    _prepare_db(db_path)
    repo = OrderRepository(str(db_path))
    gateway = _make_gateway(VenueAuthError("unauthorized"))
    svc = ExecutionService(
        _StubClob(), _FeeSvc(), repo,
        config=_make_exec_config(),
        clob_gateway=gateway,
        market_data_service=_make_mds(),
    )
    result = await svc.execute(_make_req(signal_id="sig-auth"))
    assert result.accepted is False
    assert gateway.submit_marketable_order.await_count == 1


@pytest.mark.asyncio
async def test_ambiguous_post_returns_submitted_unconfirmed(tmp_path):
    """A ``VenueSubmitResult`` with ``ambiguous_post_failure=True`` must be
    persisted as ``submitted_unconfirmed`` and the retry loop must stop —
    duplicate-fill risk is unacceptable.
    """
    db_path = tmp_path / "state.db"
    _prepare_db(db_path)
    repo = OrderRepository(str(db_path))
    gateway = _make_gateway(
        VenueSubmitResult(
            success=False,
            ambiguous_post_failure=True,
            error="timeout after POST",
        )
    )
    svc = ExecutionService(
        _StubClob(), _FeeSvc(), repo,
        config=_make_exec_config(),
        clob_gateway=gateway,
        market_data_service=_make_mds(),
    )
    result = await svc.execute(_make_req(signal_id="sig-ambig"))
    assert result.status == "submitted_unconfirmed"
    assert result.accepted is True
    assert gateway.submit_marketable_order.await_count == 1


@pytest.mark.asyncio
async def test_unavailable_error_is_treated_as_ambiguous(tmp_path):
    """``VenueUnavailableError`` could mean the POST was delivered. Must be
    persisted as ``submitted_unconfirmed`` rather than retried — issue #66.
    """
    db_path = tmp_path / "state.db"
    _prepare_db(db_path)
    repo = OrderRepository(str(db_path))
    gateway = _make_gateway(VenueUnavailableError("connection reset"))
    svc = ExecutionService(
        _StubClob(), _FeeSvc(), repo,
        config=_make_exec_config(),
        clob_gateway=gateway,
        market_data_service=_make_mds(),
    )
    result = await svc.execute(_make_req(signal_id="sig-unavail"))
    assert result.status == "submitted_unconfirmed"
    # Conservative: do not retry an ambiguous-POST.
    assert gateway.submit_marketable_order.await_count == 1


# ---------------------------------------------------------------------------
# Compatibility probe
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_check_compatibility_when_sdk_unavailable_returns_failure_report():
    class _NoClient:
        client = None

    gw = ClobV2Gateway(_NoClient())
    report = await gw.check_compatibility()
    assert report["sdk_available"] is False
    assert "error" in report


@pytest.mark.asyncio
async def test_check_compatibility_when_sdk_available_returns_ok_report():
    class _Wrapper:
        def __init__(self) -> None:
            self.client = SimpleNamespace(
                exchange_address="0xfake",
                chain_id=137,
                create_market_order=lambda **_: {"signed": True},
            )

    gw = ClobV2Gateway(_Wrapper())
    report = await gw.check_compatibility()
    assert report["sdk_available"] is True
    assert report["chain_id"] == 137
    assert report["negative_risk_supported"] is True


# ---------------------------------------------------------------------------
# VenueSubmitResult flag plumbing
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_submit_marketable_order_quantizes_size_with_fixed_step_not_price_tick():
    """PR #105 review (Cubic P1): order *size* uses a fixed 2-decimal step
    per the Polymarket SDK ROUNDING_CONFIG. Quantizing size with the price
    ``tick_size`` (e.g. ``"0.1"``) used to reject valid 2-decimal sizes
    such as ``10.55`` because the rounding delta against a 0.1 grid
    exceeded the adapter's epsilon.

    With the fix, submit_marketable_order quantizes size at ``"0.01"`` and
    price at the supplied tick_size, so a 10.55 / tick=0.1 order goes
    through and the SDK receives ``size=10.55`` unchanged.
    """
    seen: dict[str, object] = {}

    async def _capture_signed(**kwargs):
        seen.update(kwargs)
        return {"signed": True}

    async def _post(_signed):
        return {"success": True, "data": {"orderID": "ord-quant", "status": "submitted"}}

    wrapper = SimpleNamespace(
        create_market_order=_capture_signed,
        post_signed_order=_post,
    )
    gw = ClobV2Gateway(wrapper)

    req = MarketableOrderRequest(
        token_id="t1",
        side="BUY",
        size=Decimal("10.55"),
        price_limit=Decimal("0.40"),
        tick_size=Decimal("0.1"),
        neg_risk=False,
    )

    result = await gw.submit_marketable_order(req)

    assert isinstance(result, VenueSubmitResult)
    assert result.success is True
    # The SDK call must have received size=10.55 (quantized at 0.01) and
    # price=0.4 (quantized at the price tick 0.1).
    assert seen["amount"] == pytest.approx(10.55)
    assert seen["max_price"] == pytest.approx(0.4)
    assert seen["tick_size"] == "0.1"


def test_build_submit_result_surfaces_ambiguous_and_non_retryable_flags():
    result = _build_submit_result({
        "success": False,
        "ambiguous_post_failure": True,
        "non_retryable": False,
        "error": "timeout after POST",
        "data": {"orderID": "ord-ambig", "status": "pending"},
    })
    assert isinstance(result, VenueSubmitResult)
    assert result.ambiguous_post_failure is True
    assert result.non_retryable is False
    assert result.order_id == "ord-ambig"
    assert result.status == "pending"


def test_build_submit_result_handles_non_retryable_rejection():
    result = _build_submit_result({
        "success": False,
        "non_retryable": True,
        "error": "lower than the minimum",
    })
    assert result.non_retryable is True
    assert result.ambiguous_post_failure is False
    assert result.success is False
