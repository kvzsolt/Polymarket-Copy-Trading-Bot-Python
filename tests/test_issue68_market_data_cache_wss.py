"""Tests for issue #68 — Market-data cache and WSS lifecycle.

Covers all acceptance criteria:
  - Cache TTL expiry (quote, book, tick)
  - Cache invalidation on own fill
  - Cache invalidation on tick-size change
  - Subscription payload uses assets_ids
  - Heartbeat sends PING every 5 seconds
  - Reconnect snapshots before deltas
  - Size 0 deletes a book level
  - MarketDataService freshness-aware reads
  - MarketDataService fail-closed on stale data
  - MarketDataService REST fallback
"""

from __future__ import annotations

import asyncio
import json
import time
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from clients.market_ws import MarketWebSocket, HEARTBEAT_INTERVAL
from clients.user_ws import UserWebSocket
from integrations.polymarket.models import OrderBook, OrderBookLevel
from services.market_data_service import MarketDataService, StaleDataError
from services.price_cache import (
    BookSnapshot,
    PositionPriceCache,
    QuoteSnapshot,
    TickSnapshot,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

TOKEN_A = "0xaaa_token_yes"
TOKEN_B = "0xbbb_token_no"
ASSET_IDS = ["0xaaa_yes", "0xbbb_no"]


@pytest.fixture
def cache() -> PositionPriceCache:
    return PositionPriceCache(quote_ttl=5.0, book_ttl=5.0, tick_ttl=300.0)


@pytest.fixture
def short_cache() -> PositionPriceCache:
    return PositionPriceCache(quote_ttl=0.01, book_ttl=0.01, tick_ttl=0.01)


@pytest.fixture
def mock_gateway():
    gw = AsyncMock()
    gw.get_orderbook = AsyncMock(return_value=OrderBook(
        token_id=TOKEN_A,
        bids=[
            OrderBookLevel(price=Decimal("0.50"), size=Decimal("100")),
            OrderBookLevel(price=Decimal("0.48"), size=Decimal("200")),
        ],
        asks=[
            OrderBookLevel(price=Decimal("0.55"), size=Decimal("150")),
            OrderBookLevel(price=Decimal("0.57"), size=Decimal("80")),
        ],
        best_bid=Decimal("0.50"),
        best_ask=Decimal("0.55"),
    ))
    gw.get_market_info = AsyncMock(return_value={
        "token_id": TOKEN_A,
        "tick_size": "0.01",
        "min_order_size": "5",
        "neg_risk": False,
        "best_bid": "0.50",
        "best_ask": "0.55",
    })
    return gw


@pytest.fixture
def mds(cache: PositionPriceCache, mock_gateway) -> MarketDataService:
    return MarketDataService(cache, mock_gateway)


@pytest.fixture
def market_ws(cache: PositionPriceCache) -> MarketWebSocket:
    return MarketWebSocket(cache, ws_url="wss://test.example.com/ws/market")


@pytest.fixture
def user_ws(cache: PositionPriceCache) -> UserWebSocket:
    return UserWebSocket(cache, ws_url="wss://test.example.com/ws/user")


# ===================================================================
# 1. Cache TTL expiry
# ===================================================================

class TestCacheTTLExpiry:
    """Verify cache entries expire after TTL."""

    def test_quote_ttl_expiry(self, short_cache: PositionPriceCache) -> None:
        short_cache.set_quote(TOKEN_A, best_bid=Decimal("0.50"), best_ask=Decimal("0.55"))
        time.sleep(0.02)
        assert short_cache.get_quote(TOKEN_A) is None

    def test_book_ttl_expiry(self, short_cache: PositionPriceCache) -> None:
        short_cache.set_book(TOKEN_A, bids={Decimal("0.50"): Decimal("100")}, asks={Decimal("0.55"): Decimal("80")})
        time.sleep(0.02)
        assert short_cache.get_book(TOKEN_A) is None

    def test_tick_ttl_expiry(self, short_cache: PositionPriceCache) -> None:
        short_cache.set_tick(TOKEN_A, tick_size=Decimal("0.01"), min_order_size=Decimal("5"), neg_risk=False)
        time.sleep(0.02)
        assert short_cache.get_tick(TOKEN_A) is None

    def test_quote_fresh_within_ttl(self, cache: PositionPriceCache) -> None:
        cache.set_quote(TOKEN_A, best_bid=Decimal("0.50"), best_ask=Decimal("0.55"))
        assert cache.get_quote(TOKEN_A) is not None

    def test_book_fresh_within_ttl(self, cache: PositionPriceCache) -> None:
        cache.set_book(TOKEN_A, bids={Decimal("0.50"): Decimal("100")}, asks={Decimal("0.55"): Decimal("80")})
        assert cache.get_book(TOKEN_A) is not None

    def test_tick_fresh_within_ttl(self, cache: PositionPriceCache) -> None:
        cache.set_tick(TOKEN_A, tick_size=Decimal("0.01"), min_order_size=Decimal("5"), neg_risk=False)
        assert cache.get_tick(TOKEN_A) is not None


# ===================================================================
# 2. Cache invalidation on own fill
# ===================================================================

class TestCacheInvalidationOnFill:
    """Verify own fill invalidates both quote and book."""

    def test_fill_invalidates_quote_and_book(self, cache: PositionPriceCache) -> None:
        cache.set_quote(TOKEN_A, best_bid=Decimal("0.50"), best_ask=Decimal("0.55"))
        cache.set_book(TOKEN_A, bids={Decimal("0.50"): Decimal("100")}, asks={Decimal("0.55"): Decimal("80")})
        cache.set_tick(TOKEN_A, tick_size=Decimal("0.01"), min_order_size=Decimal("5"), neg_risk=False)

        cache.invalidate_on_fill(TOKEN_A)

        assert cache.get_quote(TOKEN_A) is None
        assert cache.get_book(TOKEN_A) is None
        assert cache.get_tick(TOKEN_A) is not None  # tick survives

    def test_fill_only_affects_target_token(self, cache: PositionPriceCache) -> None:
        cache.set_quote(TOKEN_A, best_bid=Decimal("0.50"), best_ask=Decimal("0.55"))
        cache.set_quote(TOKEN_B, best_bid=Decimal("0.40"), best_ask=Decimal("0.45"))

        cache.invalidate_on_fill(TOKEN_A)

        assert cache.get_quote(TOKEN_A) is None
        assert cache.get_quote(TOKEN_B) is not None


# ===================================================================
# 3. Cache invalidation on tick-size change
# ===================================================================

class TestCacheInvalidationOnTickSizeChange:
    """Verify tick-size change invalidates quote and book."""

    def test_tick_size_change_clears_quote_and_book(self, cache: PositionPriceCache) -> None:
        cache.set_quote(TOKEN_A, best_bid=Decimal("0.50"), best_ask=Decimal("0.55"))
        cache.set_book(TOKEN_A, bids={Decimal("0.50"): Decimal("100")}, asks={Decimal("0.55"): Decimal("80")})
        cache.set_tick(TOKEN_A, tick_size=Decimal("0.01"), min_order_size=Decimal("5"), neg_risk=False)

        cache.on_tick_size_change(TOKEN_A, Decimal("0.001"))

        assert cache.get_quote(TOKEN_A) is None
        assert cache.get_book(TOKEN_A) is None
        tick = cache.get_tick(TOKEN_A)
        assert tick is not None
        assert tick.tick_size == Decimal("0.001")


# ===================================================================
# 4. Subscription payload uses assets_ids
# ===================================================================

class TestSubscriptionPayload:
    """Verify subscription message format uses assets_ids."""

    @pytest.mark.asyncio
    async def test_subscribe_uses_assets_ids(self, market_ws: MarketWebSocket) -> None:
        mock_ws = AsyncMock()
        market_ws._asset_ids = ASSET_IDS
        await market_ws._subscribe(mock_ws)

        mock_ws.send.assert_called_once()
        sent = json.loads(mock_ws.send.call_args[0][0])
        assert sent["assets_ids"] == ASSET_IDS
        assert sent["type"] == "market"


# ===================================================================
# 5. Heartbeat sends PING every 5 seconds
# ===================================================================

class TestHeartbeat:
    """Verify heartbeat sends PING every 5 seconds."""

    @pytest.mark.asyncio
    async def test_market_ws_heartbeat_interval(self, market_ws: MarketWebSocket) -> None:
        mock_ws = AsyncMock()
        market_ws._running = True
        call_count = 0

        async def mock_send(msg: str) -> None:
            nonlocal call_count
            if msg == "PING":
                call_count += 1
                if call_count >= 2:
                    market_ws._running = False

        mock_ws.send = mock_send

        with patch("clients.market_ws.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            await market_ws._heartbeat(mock_ws)
            assert mock_sleep.call_count >= 2
            for call in mock_sleep.call_args_list:
                assert call[0][0] == HEARTBEAT_INTERVAL

    @pytest.mark.asyncio
    async def test_user_ws_heartbeat_interval(self, user_ws: UserWebSocket) -> None:
        from clients.user_ws import HEARTBEAT_INTERVAL as USER_HB
        mock_ws = AsyncMock()
        user_ws._running = True
        call_count = 0

        async def mock_send(msg: str) -> None:
            nonlocal call_count
            if msg == "PING":
                call_count += 1
                if call_count >= 2:
                    user_ws._running = False

        mock_ws.send = mock_send

        with patch("clients.user_ws.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            await user_ws._heartbeat(mock_ws)
            assert mock_sleep.call_count >= 2
            for call in mock_sleep.call_args_list:
                assert call[0][0] == USER_HB


# ===================================================================
# 6. Reconnect snapshots before deltas
# ===================================================================

class TestReconnectSnapshots:
    """Verify reconnect fetches REST snapshot before applying deltas."""

    @pytest.mark.asyncio
    async def test_reconnect_calls_rest_bootstrap(self, cache: PositionPriceCache) -> None:
        bootstrap_calls: list[list[str]] = []

        async def track_bootstrap(asset_ids: list[str]) -> None:
            bootstrap_calls.append(list(asset_ids))

        ws = MarketWebSocket(
            cache,
            ws_url="wss://test.example.com/ws/market",
            rest_bootstrap=track_bootstrap,
        )
        ws._asset_ids = ASSET_IDS
        ws._running = True

        connect_count = 0

        async def fail_connect(*args: object, **kwargs: object) -> AsyncMock:
            nonlocal connect_count
            connect_count += 1
            if connect_count >= 2:
                ws._running = False
            raise OSError("connection refused")

        mock_ctx = AsyncMock()
        mock_ctx.__aenter__ = fail_connect
        mock_ctx.__aexit__ = AsyncMock(return_value=False)

        with patch("clients.market_ws.websockets.connect", return_value=mock_ctx):
            with patch("clients.market_ws.asyncio.sleep", new_callable=AsyncMock):
                await ws._reconnect_loop()

        assert len(bootstrap_calls) >= 1
        assert bootstrap_calls[0] == ASSET_IDS

    @pytest.mark.asyncio
    async def test_reconnect_invalidates_cache_before_bootstrap(
        self, cache: PositionPriceCache
    ) -> None:
        cache.set_quote(TOKEN_A, best_bid=Decimal("0.50"), best_ask=Decimal("0.55"))
        assert cache.quote_count() == 1

        invalidated = False

        async def check_cleared(asset_ids: list[str]) -> None:
            nonlocal invalidated
            if cache.quote_count() == 0:
                invalidated = True

        ws = MarketWebSocket(
            cache,
            ws_url="wss://test.example.com/ws/market",
            rest_bootstrap=check_cleared,
        )
        ws._asset_ids = ASSET_IDS
        ws._running = True

        connect_count = 0

        async def fail_connect(*args: object, **kwargs: object) -> AsyncMock:
            nonlocal connect_count
            connect_count += 1
            if connect_count >= 2:
                ws._running = False
            raise OSError("connection refused")

        mock_ctx = AsyncMock()
        mock_ctx.__aenter__ = fail_connect
        mock_ctx.__aexit__ = AsyncMock(return_value=False)

        with patch("clients.market_ws.websockets.connect", return_value=mock_ctx):
            with patch("clients.market_ws.asyncio.sleep", new_callable=AsyncMock):
                await ws._reconnect_loop()

        assert invalidated


# ===================================================================
# 7. Size 0 deletes a book level
# ===================================================================

class TestSizeZeroDeletesLevel:
    """Verify price_change with size=0 deletes a book level."""

    def test_size_zero_deletes_bid_level(self, cache: PositionPriceCache) -> None:
        cache.set_book(
            TOKEN_A,
            bids={
                Decimal("0.50"): Decimal("100"),
                Decimal("0.48"): Decimal("200"),
            },
            asks={Decimal("0.55"): Decimal("80")},
        )

        cache.apply_book_delta(TOKEN_A, "BUY", Decimal("0.50"), Decimal("0"))

        book = cache.get_book(TOKEN_A)
        assert book is not None
        assert Decimal("0.50") not in book.bids
        assert Decimal("0.48") in book.bids
        assert book.bids[Decimal("0.48")] == Decimal("200")

    def test_size_zero_deletes_ask_level(self, cache: PositionPriceCache) -> None:
        cache.set_book(
            TOKEN_A,
            bids={Decimal("0.50"): Decimal("100")},
            asks={
                Decimal("0.55"): Decimal("80"),
                Decimal("0.57"): Decimal("50"),
            },
        )

        cache.apply_book_delta(TOKEN_A, "SELL", Decimal("0.55"), Decimal("0"))

        book = cache.get_book(TOKEN_A)
        assert book is not None
        assert Decimal("0.55") not in book.asks
        assert Decimal("0.57") in book.asks

    def test_size_zero_on_empty_book_is_noop(self, cache: PositionPriceCache) -> None:
        cache.apply_book_delta(TOKEN_A, "BUY", Decimal("0.50"), Decimal("0"))

        book = cache.get_book(TOKEN_A)
        assert book is not None
        assert len(book.bids) == 0

    def test_market_ws_price_change_size_zero_deletes(
        self, market_ws: MarketWebSocket, cache: PositionPriceCache
    ) -> None:
        cache.set_book(
            TOKEN_A,
            bids={Decimal("0.50"): Decimal("100")},
            asks={Decimal("0.55"): Decimal("80")},
        )

        event = json.dumps({
            "event_type": "price_change",
            "asset_id": TOKEN_A,
            "changes": [
                {"side": "BUY", "price": "0.50", "size": "0"},
            ],
        })
        market_ws._handle_message(event)

        book = cache.get_book(TOKEN_A)
        assert book is not None
        assert Decimal("0.50") not in book.bids

    def test_market_ws_price_change_upsert_with_size(
        self, market_ws: MarketWebSocket, cache: PositionPriceCache
    ) -> None:
        event = json.dumps({
            "event_type": "price_change",
            "asset_id": TOKEN_A,
            "changes": [
                {"side": "BUY", "price": "0.50", "size": "150"},
                {"side": "SELL", "price": "0.55", "size": "80"},
            ],
        })
        market_ws._handle_message(event)

        book = cache.get_book(TOKEN_A)
        assert book is not None
        assert book.bids[Decimal("0.50")] == Decimal("150")
        assert book.asks[Decimal("0.55")] == Decimal("80")


# ===================================================================
# 8. User WS fill invalidation
# ===================================================================

class TestUserWSFillInvalidation:
    """Verify user fill events invalidate cache."""

    def test_fill_event_invalidates_quote_and_book(
        self, user_ws: UserWebSocket, cache: PositionPriceCache
    ) -> None:
        cache.set_quote(TOKEN_A, best_bid=Decimal("0.55"), best_ask=Decimal("0.60"))
        cache.set_book(TOKEN_A, bids={Decimal("0.55"): Decimal("100")}, asks={Decimal("0.60"): Decimal("50")})

        event = json.dumps({
            "event_type": "trade",
            "asset_id": TOKEN_A,
            "side": "BUY",
            "size": "100",
            "price": "0.55",
        })
        user_ws._handle_message(event)

        assert cache.get_quote(TOKEN_A) is None
        assert cache.get_book(TOKEN_A) is None


# ===================================================================
# 9. Reconcile invalidation
# ===================================================================

class TestReconcileInvalidation:
    """Verify reconciliation clears all cached data for a token."""

    def test_reconcile_clears_everything(self, cache: PositionPriceCache) -> None:
        cache.set_quote(TOKEN_A, best_bid=Decimal("0.50"), best_ask=Decimal("0.55"))
        cache.set_book(TOKEN_A, bids={Decimal("0.50"): Decimal("100")}, asks={Decimal("0.55"): Decimal("80")})
        cache.set_tick(TOKEN_A, tick_size=Decimal("0.01"), min_order_size=Decimal("5"), neg_risk=False)

        cache.invalidate_on_reconcile(TOKEN_A)

        assert cache.get_quote(TOKEN_A) is None
        assert cache.get_book(TOKEN_A) is None
        assert cache.get_tick(TOKEN_A) is None


# ===================================================================
# 10. Full invalidation (reconnect bootstrap)
# ===================================================================

class TestFullInvalidation:
    """Verify invalidate_all clears everything."""

    def test_invalidate_all(self, cache: PositionPriceCache) -> None:
        cache.set_quote(TOKEN_A, best_bid=Decimal("0.50"), best_ask=Decimal("0.55"))
        cache.set_book(TOKEN_A, bids={Decimal("0.50"): Decimal("100")}, asks={Decimal("0.55"): Decimal("80")})
        cache.set_tick(TOKEN_A, tick_size=Decimal("0.01"), min_order_size=Decimal("5"), neg_risk=False)
        cache.set_quote(TOKEN_B, best_bid=Decimal("0.40"), best_ask=Decimal("0.45"))

        cache.invalidate_all()

        assert cache.get_quote(TOKEN_A) is None
        assert cache.get_book(TOKEN_A) is None
        assert cache.get_tick(TOKEN_A) is None
        assert cache.get_quote(TOKEN_B) is None
        assert cache.quote_count() == 0
        assert cache.book_count() == 0
        assert cache.tick_count() == 0


# ===================================================================
# 11. MarketDataService — freshness-aware reads
# ===================================================================

class TestMarketDataServiceFreshness:
    """Verify MarketDataService serves fresh data from cache."""

    @pytest.mark.asyncio
    async def test_serves_fresh_quote_from_cache(
        self, mds: MarketDataService, cache: PositionPriceCache
    ) -> None:
        cache.set_quote(TOKEN_A, best_bid=Decimal("0.50"), best_ask=Decimal("0.55"))
        quote = await mds.require_fresh_quote(TOKEN_A)
        assert quote.best_bid == Decimal("0.50")
        assert quote.best_ask == Decimal("0.55")

    @pytest.mark.asyncio
    async def test_serves_fresh_book_from_cache(
        self, mds: MarketDataService, cache: PositionPriceCache
    ) -> None:
        cache.set_book(
            TOKEN_A,
            bids={Decimal("0.50"): Decimal("100")},
            asks={Decimal("0.55"): Decimal("80")},
        )
        book = await mds.require_fresh_book(TOKEN_A)
        assert Decimal("0.50") in book.bids
        assert Decimal("0.55") in book.asks

    @pytest.mark.asyncio
    async def test_serves_fresh_tick_from_cache(
        self, mds: MarketDataService, cache: PositionPriceCache
    ) -> None:
        cache.set_tick(TOKEN_A, tick_size=Decimal("0.01"), min_order_size=Decimal("5"), neg_risk=False)
        tick = await mds.require_fresh_tick(TOKEN_A)
        assert tick.tick_size == Decimal("0.01")

    @pytest.mark.asyncio
    async def test_require_fresh_data_returns_bundle(
        self, mds: MarketDataService, cache: PositionPriceCache
    ) -> None:
        cache.set_quote(TOKEN_A, best_bid=Decimal("0.50"), best_ask=Decimal("0.55"))
        cache.set_book(TOKEN_A, bids={Decimal("0.50"): Decimal("100")}, asks={Decimal("0.55"): Decimal("80")})
        cache.set_tick(TOKEN_A, tick_size=Decimal("0.01"), min_order_size=Decimal("5"), neg_risk=False)

        bundle = await mds.require_fresh_data(TOKEN_A)
        assert bundle.token_id == TOKEN_A
        assert bundle.quote.best_bid == Decimal("0.50")
        assert Decimal("0.50") in bundle.book.bids
        assert bundle.tick.tick_size == Decimal("0.01")


# ===================================================================
# 12. MarketDataService — fail-closed on stale data
# ===================================================================

class TestMarketDataServiceFailClosed:
    """Verify MarketDataService fails closed when data is unavailable."""

    @pytest.mark.asyncio
    async def test_stale_quote_raises(self, cache: PositionPriceCache, mock_gateway) -> None:
        mock_gateway.get_orderbook = AsyncMock(side_effect=RuntimeError("network down"))
        mds = MarketDataService(cache, mock_gateway)
        with pytest.raises(StaleDataError):
            await mds.require_fresh_quote(TOKEN_A)

    @pytest.mark.asyncio
    async def test_stale_book_raises(self, cache: PositionPriceCache, mock_gateway) -> None:
        mock_gateway.get_orderbook = AsyncMock(side_effect=RuntimeError("network down"))
        mds = MarketDataService(cache, mock_gateway)
        with pytest.raises(StaleDataError):
            await mds.require_fresh_book(TOKEN_A)

    @pytest.mark.asyncio
    async def test_stale_tick_raises(self, cache: PositionPriceCache, mock_gateway) -> None:
        mock_gateway.get_market_info = AsyncMock(side_effect=RuntimeError("network down"))
        mds = MarketDataService(cache, mock_gateway)
        with pytest.raises(StaleDataError):
            await mds.require_fresh_tick(TOKEN_A)

    @pytest.mark.asyncio
    async def test_stale_bundle_raises(self, cache: PositionPriceCache, mock_gateway) -> None:
        mock_gateway.get_orderbook = AsyncMock(side_effect=RuntimeError("network down"))
        mock_gateway.get_market_info = AsyncMock(side_effect=RuntimeError("network down"))
        mds = MarketDataService(cache, mock_gateway)
        with pytest.raises(StaleDataError):
            await mds.require_fresh_data(TOKEN_A)

    @pytest.mark.asyncio
    async def test_rest_failure_raises_stale(
        self, cache: PositionPriceCache, mock_gateway
    ) -> None:
        mock_gateway.get_orderbook = AsyncMock(side_effect=RuntimeError("network down"))
        mds = MarketDataService(cache, mock_gateway)

        with pytest.raises(StaleDataError):
            await mds.require_fresh_quote(TOKEN_A)

    @pytest.mark.asyncio
    async def test_get_or_none_returns_none(
        self, cache: PositionPriceCache, mock_gateway
    ) -> None:
        mock_gateway.get_orderbook = AsyncMock(side_effect=RuntimeError("network down"))
        mock_gateway.get_market_info = AsyncMock(side_effect=RuntimeError("network down"))
        mds = MarketDataService(cache, mock_gateway)

        result = await mds.get_quote_or_none(TOKEN_A)
        assert result is None

        result = await mds.get_book_or_none(TOKEN_A)
        assert result is None


# ===================================================================
# 13. MarketDataService — REST fallback
# ===================================================================

class TestMarketDataServiceRESTFallback:
    """Verify MarketDataService falls back to REST when cache is stale."""

    @pytest.mark.asyncio
    async def test_quote_rest_fallback(
        self, mds: MarketDataService, cache: PositionPriceCache, mock_gateway
    ) -> None:
        quote = await mds.require_fresh_quote(TOKEN_A)
        assert quote is not None
        assert quote.best_bid == Decimal("0.50")
        assert quote.best_ask == Decimal("0.55")
        mock_gateway.get_orderbook.assert_called_once_with(TOKEN_A)

    @pytest.mark.asyncio
    async def test_book_rest_fallback(
        self, mds: MarketDataService, cache: PositionPriceCache, mock_gateway
    ) -> None:
        book = await mds.require_fresh_book(TOKEN_A)
        assert book is not None
        assert Decimal("0.50") in book.bids
        assert book.bids[Decimal("0.50")] == Decimal("100")
        mock_gateway.get_orderbook.assert_called_once_with(TOKEN_A)

    @pytest.mark.asyncio
    async def test_tick_rest_fallback(
        self, mds: MarketDataService, cache: PositionPriceCache, mock_gateway
    ) -> None:
        tick = await mds.require_fresh_tick(TOKEN_A)
        assert tick is not None
        assert tick.tick_size == Decimal("0.01")
        assert tick.min_order_size == Decimal("5")
        assert tick.neg_risk is False
        mock_gateway.get_market_info.assert_called_once_with(TOKEN_A)

    @pytest.mark.asyncio
    async def test_rest_fallback_populates_cache(
        self, mds: MarketDataService, cache: PositionPriceCache, mock_gateway
    ) -> None:
        await mds.require_fresh_quote(TOKEN_A)

        assert cache.get_quote(TOKEN_A) is not None
        quote = cache.get_quote(TOKEN_A)
        assert quote.best_bid == Decimal("0.50")

    @pytest.mark.asyncio
    async def test_rest_fallback_only_called_when_stale(
        self, mds: MarketDataService, cache: PositionPriceCache, mock_gateway
    ) -> None:
        cache.set_quote(TOKEN_A, best_bid=Decimal("0.50"), best_ask=Decimal("0.55"))
        quote = await mds.require_fresh_quote(TOKEN_A)
        assert quote.best_bid == Decimal("0.50")
        mock_gateway.get_orderbook.assert_not_called()


# ===================================================================
# 14. BookSnapshot helpers
# ===================================================================

class TestBookSnapshot:
    """Verify BookSnapshot best_bid/best_ask helpers."""

    def test_best_bid(self) -> None:
        book = BookSnapshot(
            bids={Decimal("0.48"): Decimal("200"), Decimal("0.50"): Decimal("100")},
            asks={Decimal("0.55"): Decimal("80")},
            ts=time.monotonic(),
        )
        assert book.best_bid() == Decimal("0.50")

    def test_best_ask(self) -> None:
        book = BookSnapshot(
            bids={Decimal("0.50"): Decimal("100")},
            asks={Decimal("0.57"): Decimal("50"), Decimal("0.55"): Decimal("80")},
            ts=time.monotonic(),
        )
        assert book.best_ask() == Decimal("0.55")

    def test_best_bid_empty(self) -> None:
        book = BookSnapshot()
        assert book.best_bid() is None

    def test_best_ask_empty(self) -> None:
        book = BookSnapshot()
        assert book.best_ask() is None


# ===================================================================
# 15. Introspection with books
# ===================================================================

class TestIntrospectionWithBooks:
    """Verify cache introspection includes books."""

    def test_book_count(self, cache: PositionPriceCache) -> None:
        assert cache.book_count() == 0
        cache.set_book(TOKEN_A, bids={Decimal("0.50"): Decimal("100")}, asks={})
        assert cache.book_count() == 1

    def test_subscribed_tokens_includes_books(self, cache: PositionPriceCache) -> None:
        cache.set_book(TOKEN_A, bids={Decimal("0.50"): Decimal("100")}, asks={})
        tokens = cache.subscribed_tokens()
        assert TOKEN_A in tokens
