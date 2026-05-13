"""PR 2: ExecutionService uses only MarketDataService.

Tests:
  - dirty market state blocks execution
  - stale market state blocks execution
  - resolved market blocks execution
  - missing tick blocks execution
  - missing min_order_size blocks execution
  - ExecutionService does not call MarketMetaService
  - ExecutionService does not call get_price
  - quote/book/tick all come from MarketDataBundle
"""
from __future__ import annotations

import inspect
import time
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from cache.market_state_store import BookSnapshot, QuoteSnapshot, TickSnapshot
from integrations.polymarket.models import VenueSubmitResult
from services.executionService import ExecutionRequest, ExecutionService
from services.market_data_service import (
    MarketDataBundle,
    MarketDataNotReady,
    MarketDataSource,
    StaleDataError,
)
from services.orderRepository import OrderRepository
from tests.conftest import make_clob_gateway, make_exec_config


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

TOKEN = "0xtest_token"


def _make_bundle(
    token_id: str = TOKEN,
    *,
    best_bid: Decimal = Decimal("0.49"),
    best_ask: Decimal = Decimal("0.51"),
    tick_size: Decimal = Decimal("0.01"),
    min_order_size: Decimal = Decimal("1"),
    neg_risk: bool = False,
) -> MarketDataBundle:
    now = time.monotonic()
    return MarketDataBundle(
        token_id=token_id,
        quote=QuoteSnapshot(
            token_id=token_id,
            best_bid=best_bid,
            best_ask=best_ask,
            last_trade_price=None,
            source="wss",
            monotonic_ts=now,
        ),
        book=BookSnapshot(
            token_id=token_id,
            bids={best_bid: Decimal("100")},
            asks={best_ask: Decimal("100")},
            best_bid=best_bid,
            best_ask=best_ask,
            source="wss",
            monotonic_ts=now,
        ),
        tick=TickSnapshot(
            token_id=token_id,
            tick_size=tick_size,
            min_order_size=min_order_size,
            neg_risk=neg_risk,
            source="wss",
            monotonic_ts=now,
        ),
        source=MarketDataSource.WSS,
    )


class _FeeSvc:
    async def get_fee_bps(self, _: str) -> int:
        return 0


def _make_svc(
    mds,
    *,
    fresh_db_path: str,
    exec_config=None,
    submit_result: VenueSubmitResult | None = None,
) -> tuple[ExecutionService, MagicMock]:
    if exec_config is None:
        exec_config = make_exec_config()
    if submit_result is None:
        submit_result = VenueSubmitResult(
            success=True, order_id="ord-test", status="submitted"
        )
    repo = OrderRepository(fresh_db_path)
    gateway = make_clob_gateway(submit_result=submit_result)
    svc = ExecutionService(
        object(),  # clob_client — not used in PR 2 path
        None,      # market_meta_service — intentionally None
        _FeeSvc(),
        repo,
        config=exec_config,
        clob_gateway=gateway,
        market_data_service=mds,
    )
    return svc, gateway


def _request(**overrides) -> ExecutionRequest:
    defaults = dict(
        user_id="u1",
        leader_id="leader",
        token_id=TOKEN,
        side="BUY",
        amount=Decimal("5"),
        max_slippage_bps=100,
        reason="copy_trade",
        signal_id="sig-pr2",
    )
    defaults.update(overrides)
    return ExecutionRequest(**defaults)


@pytest.fixture(autouse=True)
def _reset_circuit():
    import services.executionService as _es
    _es._CLOB_CIRCUIT.reset()
    with patch.dict("config.env.ENV", {"MAX_BOOK_SPREAD_BPS": "5000"}, clear=False):
        yield
    _es._CLOB_CIRCUIT.reset()


# ---------------------------------------------------------------------------
# Dirty market state blocks execution
# ---------------------------------------------------------------------------

class TestDirtyMarketBlocks:
    @pytest.mark.asyncio
    async def test_dirty_market_returns_stale_status(self, fresh_db_path) -> None:
        mds = AsyncMock()
        mds.require_fresh_data = AsyncMock(
            side_effect=StaleDataError(f"market data not executable for {TOKEN}; dirty=True")
        )
        svc, _ = _make_svc(mds, fresh_db_path=fresh_db_path)
        result = await svc.execute(_request())

        assert result.accepted is False
        assert result.status == "stale_market_data"
        assert "dirty" in result.error or "not executable" in result.error


# ---------------------------------------------------------------------------
# Stale market state blocks execution
# ---------------------------------------------------------------------------

class TestStaleMarketBlocks:
    @pytest.mark.asyncio
    async def test_stale_market_returns_stale_status(self, fresh_db_path) -> None:
        mds = AsyncMock()
        mds.require_fresh_data = AsyncMock(
            side_effect=StaleDataError(
                f"market data not executable for {TOKEN}; REST fallback disabled"
            )
        )
        svc, _ = _make_svc(mds, fresh_db_path=fresh_db_path)
        result = await svc.execute(_request())

        assert result.accepted is False
        assert result.status == "stale_market_data"


# ---------------------------------------------------------------------------
# Resolved market blocks execution
# ---------------------------------------------------------------------------

class TestResolvedMarketBlocks:
    @pytest.mark.asyncio
    async def test_resolved_market_returns_not_ready(self, fresh_db_path) -> None:
        mds = AsyncMock()
        mds.require_fresh_data = AsyncMock(
            side_effect=MarketDataNotReady(f"market/token resolved: {TOKEN}")
        )
        svc, _ = _make_svc(mds, fresh_db_path=fresh_db_path)
        result = await svc.execute(_request())

        assert result.accepted is False
        assert result.status == "market_not_ready"
        assert "resolved" in result.error


# ---------------------------------------------------------------------------
# Missing tick blocks execution
# ---------------------------------------------------------------------------

class TestMissingTickBlocks:
    @pytest.mark.asyncio
    async def test_missing_tick_raises_stale_error(self, fresh_db_path) -> None:
        mds = AsyncMock()
        mds.require_fresh_data = AsyncMock(
            side_effect=StaleDataError(
                f"market data not executable for {TOKEN}; tick missing"
            )
        )
        svc, _ = _make_svc(mds, fresh_db_path=fresh_db_path)
        result = await svc.execute(_request())

        assert result.accepted is False
        assert result.status == "stale_market_data"


# ---------------------------------------------------------------------------
# Missing min_order_size blocks execution
# ---------------------------------------------------------------------------

class TestMissingMinOrderSizeBlocks:
    @pytest.mark.asyncio
    async def test_missing_min_order_size_raises_stale_error(self, fresh_db_path) -> None:
        mds = AsyncMock()
        mds.require_fresh_data = AsyncMock(
            side_effect=StaleDataError(
                f"market data not executable for {TOKEN}; min_order_size missing"
            )
        )
        svc, _ = _make_svc(mds, fresh_db_path=fresh_db_path)
        result = await svc.execute(_request())

        assert result.accepted is False
        assert result.status == "stale_market_data"

    @pytest.mark.asyncio
    async def test_amount_below_min_order_size_rejected(self, fresh_db_path) -> None:
        """Execution is rejected when req.amount < bundle.tick.min_order_size."""
        mds = AsyncMock()
        mds.require_fresh_data = AsyncMock(
            return_value=_make_bundle(min_order_size=Decimal("10"))
        )
        svc, gateway = _make_svc(mds, fresh_db_path=fresh_db_path)
        result = await svc.execute(_request(amount=Decimal("3")))

        assert result.accepted is False
        assert result.status == "rejected"
        assert "min_order_size" in result.error
        gateway.submit_marketable_order.assert_not_awaited()


# ---------------------------------------------------------------------------
# ExecutionService does not call MarketMetaService
# ---------------------------------------------------------------------------

class TestNoMarketMetaServiceUsage:
    def test_market_meta_service_not_referenced_in_execute(self) -> None:
        """execute() source must not reference market_meta_service."""
        source = inspect.getsource(ExecutionService.execute)
        assert "market_meta_service" not in source

    @pytest.mark.asyncio
    async def test_meta_svc_mock_never_called(self, fresh_db_path) -> None:
        mds = AsyncMock()
        mds.require_fresh_data = AsyncMock(return_value=_make_bundle())
        repo = OrderRepository(fresh_db_path)
        meta_svc = MagicMock()
        gateway = make_clob_gateway()
        svc = ExecutionService(
            object(), meta_svc, _FeeSvc(), repo,
            config=make_exec_config(),
            clob_gateway=gateway,
            market_data_service=mds,
        )
        await svc.execute(_request())
        meta_svc.get.assert_not_called()


# ---------------------------------------------------------------------------
# ExecutionService does not call get_price
# ---------------------------------------------------------------------------

class TestNoGetPriceCall:
    def test_get_price_not_in_execute_source(self) -> None:
        """execute() must not call raw get_price."""
        source = inspect.getsource(ExecutionService.execute)
        assert "get_price" not in source

    @pytest.mark.asyncio
    async def test_clob_client_get_price_never_called(self, fresh_db_path) -> None:
        mds = AsyncMock()
        mds.require_fresh_data = AsyncMock(return_value=_make_bundle())
        clob_client = MagicMock()
        clob_client.get_price = AsyncMock()
        repo = OrderRepository(fresh_db_path)
        gateway = make_clob_gateway()
        svc = ExecutionService(
            clob_client, None, _FeeSvc(), repo,
            config=make_exec_config(),
            clob_gateway=gateway,
            market_data_service=mds,
        )
        await svc.execute(_request())
        clob_client.get_price.assert_not_called()


# ---------------------------------------------------------------------------
# quote/book/tick all come from MarketDataBundle
# ---------------------------------------------------------------------------

class TestBundleDataFlows:
    @pytest.mark.asyncio
    async def test_buy_price_comes_from_bundle_best_ask(self, fresh_db_path) -> None:
        """BUY worst_price is derived from bundle.quote.best_ask (zero slippage)."""
        best_ask = Decimal("0.72")
        mds = AsyncMock()
        mds.require_fresh_data = AsyncMock(
            return_value=_make_bundle(best_ask=best_ask, best_bid=Decimal("0.70"))
        )
        svc, gateway = _make_svc(mds, fresh_db_path=fresh_db_path)
        result = await svc.execute(_request(side="BUY", max_slippage_bps=0))

        assert result.accepted is True
        submitted = gateway.submit_marketable_order.await_args.args[0]
        assert submitted.price_limit == best_ask

    @pytest.mark.asyncio
    async def test_sell_price_comes_from_bundle_best_bid(self, fresh_db_path) -> None:
        """SELL worst_price is derived from bundle.quote.best_bid (zero slippage)."""
        best_bid = Decimal("0.47")
        mds = AsyncMock()
        mds.require_fresh_data = AsyncMock(
            return_value=_make_bundle(best_bid=best_bid, best_ask=Decimal("0.52"))
        )
        svc, gateway = _make_svc(mds, fresh_db_path=fresh_db_path)
        result = await svc.execute(_request(side="SELL", max_slippage_bps=0))

        assert result.accepted is True
        submitted = gateway.submit_marketable_order.await_args.args[0]
        assert submitted.price_limit == best_bid

    @pytest.mark.asyncio
    async def test_tick_size_from_bundle_used_in_gateway_request(self, fresh_db_path) -> None:
        """tick_size in the gateway request matches bundle.tick.tick_size."""
        tick_size = Decimal("0.001")
        mds = AsyncMock()
        mds.require_fresh_data = AsyncMock(
            return_value=_make_bundle(tick_size=tick_size)
        )
        svc, gateway = _make_svc(mds, fresh_db_path=fresh_db_path)
        await svc.execute(_request())

        submitted = gateway.submit_marketable_order.await_args.args[0]
        assert submitted.tick_size == tick_size

    @pytest.mark.asyncio
    async def test_neg_risk_from_bundle_used_in_gateway_request(self, fresh_db_path) -> None:
        """neg_risk in gateway request matches bundle.tick.neg_risk."""
        mds = AsyncMock()
        mds.require_fresh_data = AsyncMock(
            return_value=_make_bundle(neg_risk=True)
        )
        svc, gateway = _make_svc(mds, fresh_db_path=fresh_db_path)
        await svc.execute(_request())

        submitted = gateway.submit_marketable_order.await_args.args[0]
        assert submitted.neg_risk is True

    @pytest.mark.asyncio
    async def test_require_fresh_data_called_once_with_token_id(self, fresh_db_path) -> None:
        """require_fresh_data is called exactly once per execute() call."""
        mds = AsyncMock()
        mds.require_fresh_data = AsyncMock(return_value=_make_bundle())
        svc, _ = _make_svc(mds, fresh_db_path=fresh_db_path)
        await svc.execute(_request())

        mds.require_fresh_data.assert_awaited_once_with(TOKEN)


# ---------------------------------------------------------------------------
# MDS not wired → fail closed
# ---------------------------------------------------------------------------

class TestMdsNotWiredFailsClosed:
    @pytest.mark.asyncio
    async def test_no_mds_returns_rejected(self, fresh_db_path) -> None:
        repo = OrderRepository(fresh_db_path)
        gateway = make_clob_gateway()
        svc = ExecutionService(
            object(), None, _FeeSvc(), repo,
            config=make_exec_config(),
            clob_gateway=gateway,
            market_data_service=None,
        )
        result = await svc.execute(_request())

        assert result.accepted is False
        assert result.status == "rejected"
        assert "market_data_service" in result.error
        gateway.submit_marketable_order.assert_not_awaited()


# ---------------------------------------------------------------------------
# Missing quote blocks execution
# ---------------------------------------------------------------------------

class TestMissingQuoteBlocks:
    @pytest.mark.asyncio
    async def test_missing_quote_returns_stale_status(self, fresh_db_path) -> None:
        mds = AsyncMock()
        mds.require_fresh_data = AsyncMock(
            side_effect=StaleDataError(
                f"market data not executable for {TOKEN}; quote missing"
            )
        )
        svc, gateway = _make_svc(mds, fresh_db_path=fresh_db_path)
        result = await svc.execute(_request())

        assert result.accepted is False
        assert result.status == "stale_market_data"
        assert "quote" in result.error
        gateway.submit_marketable_order.assert_not_awaited()


# ---------------------------------------------------------------------------
# Missing book blocks execution
# ---------------------------------------------------------------------------

class TestMissingBookBlocks:
    @pytest.mark.asyncio
    async def test_missing_book_returns_stale_status(self, fresh_db_path) -> None:
        mds = AsyncMock()
        mds.require_fresh_data = AsyncMock(
            side_effect=StaleDataError(
                f"market data not executable for {TOKEN}; book missing"
            )
        )
        svc, gateway = _make_svc(mds, fresh_db_path=fresh_db_path)
        result = await svc.execute(_request())

        assert result.accepted is False
        assert result.status == "stale_market_data"
        assert "book" in result.error
        gateway.submit_marketable_order.assert_not_awaited()


# ---------------------------------------------------------------------------
# Spread gate uses MarketDataBundle quote/book values
# ---------------------------------------------------------------------------

class TestSpreadGateUsesBundle:
    @pytest.mark.asyncio
    async def test_wide_spread_from_bundle_rejects(self, fresh_db_path) -> None:
        """Spread gate is driven by bundle.quote best_bid/best_ask + bundle.tick.tick_size."""
        exec_config = make_exec_config()
        exec_config.execution.book_spread_mode = "absolute_bps"
        exec_config.execution.max_book_spread_bps = 100  # 1% max

        mds = AsyncMock()
        mds.require_fresh_data = AsyncMock(
            return_value=_make_bundle(
                best_bid=Decimal("0.20"),
                best_ask=Decimal("0.90"),  # 70 cents wide → far above 100 bps
                tick_size=Decimal("0.01"),
            )
        )
        svc, gateway = _make_svc(
            mds, fresh_db_path=fresh_db_path, exec_config=exec_config
        )
        result = await svc.execute(_request())

        assert result.accepted is False
        assert result.status == "book_spread_wide"
        gateway.submit_marketable_order.assert_not_awaited()
