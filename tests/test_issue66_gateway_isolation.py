"""Issue #66 — CLOB V2 gateway isolation acceptance tests (PG-A6 port).

These tests use the canonical ``ClobGateway`` Protocol shape and a typed
``AppConfig``-shaped fixture so they exercise the gateway path end-to-end
without hitting the configless fail-closed short-circuit owned by issue #73.
"""

from __future__ import annotations

import contextlib
import sys
import time as _time
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from cache.market_state_store import BookSnapshot, QuoteSnapshot, TickSnapshot
from integrations.polymarket.clob_v2_gateway import ClobV2Gateway, _build_submit_result
from integrations.polymarket.errors import (
    VenueAuthError,
    VenueInvalidOrderError,
    VenueOrderNotFoundError,
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
from services.market_data_service import MarketDataBundle, MarketDataSource
from storage.postgres.orders_repo import OrdersRepo
from utils.createClobClient import MarketMeta


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


def _make_gateway(submit_result):
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


def _build_svc(pool, gateway, *, neg_risk: bool = False) -> ExecutionService:
    return ExecutionService(
        _StubClob(),
        _FeeSvc(),
        OrdersRepo(pool),
        config=_make_exec_config(),
        clob_gateway=gateway,
        market_data_service=_make_mds(neg_risk=neg_risk),
    )


# ---------------------------------------------------------------------------
# Gateway acceptance: BUY / SELL submit
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_buy_submission_uses_gateway_marketable_path():
    async with _pg_pool() as pool:
        gateway = _make_gateway(
            VenueSubmitResult(success=True, order_id="ord-buy", status="submitted")
        )
        svc = _build_svc(pool, gateway)
        result = await svc.execute(_make_req(side="BUY", signal_id="sig-buy"))
        assert result.accepted is True
        gateway.submit_marketable_order.assert_awaited_once()
        request = gateway.submit_marketable_order.await_args.args[0]
        assert isinstance(request, MarketableOrderRequest)
        assert request.token_id == "t1"
        assert request.side == "BUY"
        assert request.size == Decimal("5")


@pytest.mark.asyncio
async def test_sell_submission_uses_gateway_marketable_path():
    async with _pg_pool() as pool:
        gateway = _make_gateway(
            VenueSubmitResult(success=True, order_id="ord-sell", status="submitted")
        )
        svc = _build_svc(pool, gateway)
        result = await svc.execute(_make_req(side="SELL", signal_id="sig-sell"))
        assert result.accepted is True
        request = gateway.submit_marketable_order.await_args.args[0]
        assert request.side == "SELL"


@pytest.mark.asyncio
async def test_neg_risk_propagates_through_gateway_request():
    async with _pg_pool() as pool:
        gateway = _make_gateway(
            VenueSubmitResult(success=True, order_id="ord-nr", status="submitted")
        )
        svc = _build_svc(pool, gateway, neg_risk=True)
        await svc.execute(_make_req(signal_id="sig-nr"))
        request = gateway.submit_marketable_order.await_args.args[0]
        assert request.neg_risk is True
        assert request.tick_size == Decimal("0.01")


# ---------------------------------------------------------------------------
# Error classification: non-retryable vs ambiguous
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_invalid_order_error_is_non_retryable():
    async with _pg_pool() as pool:
        gateway = _make_gateway(VenueInvalidOrderError("invalid amount"))
        svc = _build_svc(pool, gateway)
        result = await svc.execute(_make_req(signal_id="sig-bad"))
        assert result.accepted is False
        assert gateway.submit_marketable_order.await_count == 1


@pytest.mark.asyncio
async def test_auth_error_is_non_retryable():
    async with _pg_pool() as pool:
        gateway = _make_gateway(VenueAuthError("unauthorized"))
        svc = _build_svc(pool, gateway)
        result = await svc.execute(_make_req(signal_id="sig-auth"))
        assert result.accepted is False
        assert gateway.submit_marketable_order.await_count == 1


@pytest.mark.asyncio
async def test_ambiguous_post_returns_submitted_unconfirmed():
    """A ``VenueSubmitResult`` with ``ambiguous_post_failure=True`` must be
    persisted as ``submitted_unconfirmed`` and the retry loop must stop —
    duplicate-fill risk is unacceptable.
    """
    async with _pg_pool() as pool:
        gateway = _make_gateway(
            VenueSubmitResult(
                success=False,
                ambiguous_post_failure=True,
                error="timeout after POST",
            )
        )
        svc = _build_svc(pool, gateway)
        result = await svc.execute(_make_req(signal_id="sig-ambig"))
        assert result.status == "submitted_unconfirmed"
        assert result.accepted is True
        assert gateway.submit_marketable_order.await_count == 1


@pytest.mark.asyncio
async def test_unavailable_error_is_treated_as_ambiguous():
    """``VenueUnavailableError`` could mean the POST was delivered. Must be
    persisted as ``submitted_unconfirmed`` rather than retried — issue #66.
    """
    async with _pg_pool() as pool:
        gateway = _make_gateway(VenueUnavailableError("connection reset"))
        svc = _build_svc(pool, gateway)
        result = await svc.execute(_make_req(signal_id="sig-unavail"))
        assert result.status == "submitted_unconfirmed"
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
async def test_submit_marketable_order_quantizes_amount_with_fixed_step_not_price_tick():
    """MarketOrderArgs.amount uses SDK amount precision, not price tick.

    Context7/docs expose ROUNDING_CONFIG["0.01"] as
    RoundConfig(price=2, size=2, amount=4). Market orders use ``amount``
    (USDC for BUY, shares for SELL), so a 4-decimal filled position such as
    5.6818 must pass through unchanged instead of being rejected or rounded
    against a 2-decimal limit-order size grid.
    """
    seen: dict[str, object] = {}

    async def _capture_signed(**kwargs):
        seen.update(kwargs)
        return {"signed": True}

    async def _post(_signed, **_kwargs):
        return {"success": True, "data": {"orderID": "ord-quant", "status": "submitted"}}

    wrapper = SimpleNamespace(
        create_market_order=_capture_signed,
        post_signed_order=_post,
    )
    gw = ClobV2Gateway(wrapper)

    req = MarketableOrderRequest(
        token_id="t1",
        side="SELL",
        size=Decimal("5.6818"),
        price_limit=Decimal("0.40"),
        tick_size=Decimal("0.1"),
        neg_risk=False,
    )

    result = await gw.submit_marketable_order(req)

    assert isinstance(result, VenueSubmitResult)
    assert result.success is True
    assert seen["amount"] == pytest.approx(5.6818)
    assert seen["max_price"] == pytest.approx(0.4)
    assert seen["tick_size"] == "0.1"


@pytest.mark.asyncio
async def test_submit_marketable_order_accepts_half_unit_amount_noise():
    """Amounts generated from float sizing can land half a 4dp unit off-grid.

    The SDK rounds MarketOrderArgs.amount to 4 decimals, so a BUY notional
    like 4.999995 is an unambiguous 5.0000 order, not a terminal invalid
    order.
    """
    seen: dict[str, object] = {}

    async def _capture_signed(**kwargs):
        seen.update(kwargs)
        return {"signed": True}

    async def _post(_signed, **_kwargs):
        return {"success": True, "data": {"orderID": "ord-quant-buy", "status": "submitted"}}

    wrapper = SimpleNamespace(
        create_market_order=_capture_signed,
        post_signed_order=_post,
    )
    gw = ClobV2Gateway(wrapper)

    req = MarketableOrderRequest(
        token_id="t1",
        side="BUY",
        size=Decimal("4.999995"),
        price_limit=Decimal("0.72"),
        tick_size=Decimal("0.01"),
        neg_risk=False,
    )

    result = await gw.submit_marketable_order(req)

    assert result.success is True
    assert seen["amount"] == pytest.approx(5.0)
    assert seen["max_price"] == pytest.approx(0.72)


@pytest.mark.asyncio
async def test_submit_marketable_order_accepts_buy_amount_precision_noise():
    seen: dict[str, object] = {}

    async def _capture_signed(**kwargs):
        seen.update(kwargs)
        return {"signed": True}

    async def _post(_signed, **_kwargs):
        return {"success": True, "data": {"orderID": "ord-quant-buy-2", "status": "submitted"}}

    wrapper = SimpleNamespace(
        create_market_order=_capture_signed,
        post_signed_order=_post,
    )
    gw = ClobV2Gateway(wrapper)

    req = MarketableOrderRequest(
        token_id="t1",
        side="BUY",
        size=Decimal("4.679789"),
        price_limit=Decimal("0.81"),
        tick_size=Decimal("0.01"),
        neg_risk=False,
    )

    result = await gw.submit_marketable_order(req)

    assert result.success is True
    assert seen["amount"] == pytest.approx(4.6798)
    assert seen["max_price"] == pytest.approx(0.81)


@pytest.mark.asyncio
async def test_submit_marketable_order_floors_sell_amount_to_avoid_oversell():
    seen: dict[str, object] = {}

    async def _capture_signed(**kwargs):
        seen.update(kwargs)
        return {"signed": True}

    async def _post(_signed, **_kwargs):
        return {"success": True, "data": {"orderID": "ord-quant-sell", "status": "submitted"}}

    wrapper = SimpleNamespace(
        create_market_order=_capture_signed,
        post_signed_order=_post,
    )
    gw = ClobV2Gateway(wrapper)

    req = MarketableOrderRequest(
        token_id="t1",
        side="SELL",
        size=Decimal("5.681889"),
        price_limit=Decimal("0.40"),
        tick_size=Decimal("0.01"),
        neg_risk=False,
    )

    result = await gw.submit_marketable_order(req)

    assert result.success is True
    assert seen["amount"] == pytest.approx(5.6818)


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


def test_build_submit_result_accepts_sdk_order_id_key():
    result = _build_submit_result({
        "success": True,
        "data": {"order_id": "ord-sdk-v2", "status": "submitted"},
    })
    assert isinstance(result, VenueSubmitResult)
    assert result.order_id == "ord-sdk-v2"
    assert result.status == "submitted"


def test_build_submit_result_handles_non_retryable_rejection():
    result = _build_submit_result({
        "success": False,
        "non_retryable": True,
        "error": "lower than the minimum",
    })
    assert result.non_retryable is True
    assert result.ambiguous_post_failure is False
    assert result.success is False


# ---------------------------------------------------------------------------
# get_order — transport-disconnect retry + 404 not-found (Server-disconnected fix)
# ---------------------------------------------------------------------------


class _FakeHttpResp:
    """Minimal httpx.Response stand-in for building a real SDK PolyApiException."""

    def __init__(self, status_code: int, body):
        self.status_code = status_code
        self._body = body
        self.text = str(body)

    def json(self):
        return self._body


def _sdk_transport_exc():
    """The exact wrapper py-clob-client-v2 raises on an httpx transport failure."""
    from py_clob_client_v2.exceptions import PolyApiException as SDKExc

    return SDKExc(error_msg="Request exception!")


def _sdk_404_exc():
    from py_clob_client_v2.exceptions import PolyApiException as SDKExc

    return SDKExc(_FakeHttpResp(404, {"error": "order not found"}))


@pytest.mark.asyncio
async def test_get_order_retries_once_on_transport_disconnect_then_succeeds():
    """GET is idempotent — a 'Server disconnected' transport failure is retried
    exactly once; the retry opens a fresh connection and succeeds."""
    calls = {"n": 0}

    def _sync_get_order(order_id: str):
        calls["n"] += 1
        if calls["n"] == 1:
            raise _sdk_transport_exc()
        return {"id": order_id, "status": "live", "size_matched": "0"}

    wrapper = SimpleNamespace(client=SimpleNamespace(get_order=_sync_get_order))
    gw = ClobV2Gateway(wrapper)

    state = await gw.get_order("0xorder")

    assert calls["n"] == 2  # one failure + one retry
    assert isinstance(state, VenueOrderState)
    assert state.status == "live"
    assert state.order_id == "0xorder"


@pytest.mark.asyncio
async def test_get_order_retries_once_on_direct_connection_terminated_then_succeeds():
    """Some stacks surface HTTP/2 disconnects directly as ConnectionTerminated.

    That is still a safe-to-retry GET transport failure, not a terminal venue
    response.
    """
    calls = {"n": 0}

    class ConnectionTerminated(Exception):
        pass

    def _sync_get_order(order_id: str):
        calls["n"] += 1
        if calls["n"] == 1:
            raise ConnectionTerminated(
                "<ConnectionTerminated error_code:1, last_stream_id:10065>"
            )
        return {"id": order_id, "status": "matched", "size_matched": "1"}

    wrapper = SimpleNamespace(client=SimpleNamespace(get_order=_sync_get_order))
    gw = ClobV2Gateway(wrapper)

    state = await gw.get_order("0xorder")

    assert calls["n"] == 2
    assert state.status == "matched"
    assert state.filled_size == Decimal("1")


@pytest.mark.asyncio
async def test_get_order_retries_transport_only_once_then_raises_unavailable():
    """A persistent transport failure raises VenueUnavailableError after the
    single retry (no infinite loop)."""
    calls = {"n": 0}

    def _always_disconnect(order_id: str):
        calls["n"] += 1
        raise _sdk_transport_exc()

    wrapper = SimpleNamespace(client=SimpleNamespace(get_order=_always_disconnect))
    gw = ClobV2Gateway(wrapper)

    with pytest.raises(VenueUnavailableError) as exc_info:
        await gw.get_order("0xorder")

    assert calls["n"] == 2  # original + exactly one retry
    assert "poly_api_transport" in str(exc_info.value)


@pytest.mark.asyncio
async def test_get_order_404_raises_not_found_without_retry():
    """A 404 is terminal-not-retryable; it raises VenueOrderNotFoundError and
    is NOT retried (unlike transport failures)."""
    calls = {"n": 0}

    def _not_found(order_id: str):
        calls["n"] += 1
        raise _sdk_404_exc()

    wrapper = SimpleNamespace(client=SimpleNamespace(get_order=_not_found))
    gw = ClobV2Gateway(wrapper)

    with pytest.raises(VenueOrderNotFoundError):
        await gw.get_order("0xorder")

    assert calls["n"] == 1  # no retry on 404
