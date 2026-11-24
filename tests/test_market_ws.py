"""Tests for clients.market_ws — Market WebSocket client.

Coverage:
  - Subscription message shape (assets_ids, type, custom_feature_enabled)
  - Heartbeat interval compliance (PING)
  - Dispatch routing (book, price_change, best_bid_ask)
  - _asset_id helper
  - Book event parsing and MarketStateStore updates
  - Price-change event parsing (per-item asset_id from price_changes[])
  - best_bid_ask, tick_size_change, last_trade_price, market_resolved
  - Executability gates (best_bid_ask alone / tick alone / empty book)
  - Debug state in StaleDataError
  - Reconnect lifecycle
"""

from __future__ import annotations

import ast
import asyncio
import inspect
import json
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pmcopy.cache.market_state_store import MarketStateStore
from pmcopy.clients.ws.market_ws import MarketWebSocket, HEARTBEAT_INTERVAL, _asset_id

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "market_wss"

# ---------------------------------------------------------------------------
# Real Polymarket sample payloads
# ---------------------------------------------------------------------------

_ASSET_A = "81105658394977909179248100227432049093597910573611715290070739967741990478395"
_ASSET_B = "30540827094841843166170340938908793435352324701605293640314073169375762175099"
_MARKET = "0x860230aabe261b85f0689dda7a2ab1f70f30e5eff55026f3a5ce0fdf243d076f"

BOOK_EVENT_ONE_SIDED: dict = {
    "event_type": "book",
    "asset_id": _ASSET_A,
    "market": _MARKET,
    "bids": [],
    "asks": [{"price": "0.999", "size": "280080"}],
    "tick_size": "0.001",
    "timestamp": "1778759360028",
    "hash": "9ad54389e0d9e50220f24c4149c11cffd4fa08f3",
}

BOOK_EVENT_TWO_SIDED: dict = {
    "event_type": "book",
    "asset_id": _ASSET_A,
    "market": _MARKET,
    "bids": [{"price": "0.49", "size": "1000"}],
    "asks": [{"price": "0.51", "size": "1000"}],
    "tick_size": "0.01",
    "timestamp": "1778759360028",
    "hash": "abc123",
}

BOOK_EVENT_EMPTY: dict = {
    "event_type": "book",
    "asset_id": _ASSET_A,
    "market": _MARKET,
    "bids": [],
    "asks": [],
    "tick_size": "0.001",
    "timestamp": "1778759360028",
    "hash": "empty",
}

PRICE_CHANGE_EVENT: dict = {
    "event_type": "price_change",
    "market": _MARKET,
    "price_changes": [
        {
            "asset_id": _ASSET_B,
            "price": "0.01",
            "size": "428321.91",
            "side": "BUY",
            "hash": "542e64fe51a097ee5de8a23ef74c3753f8155b24",
            "best_bid": "0.999",
            "best_ask": "1",
        },
        {
            "asset_id": _ASSET_A,
            "price": "0.99",
            "size": "428321.91",
            "side": "SELL",
            "hash": "26e6f74b60c80b24b76ecc8fdbbbe714d6113b66",
            "best_bid": "0",
            "best_ask": "0.001",
        },
    ],
    "timestamp": "1778759362413",
}

BEST_BID_ASK_EVENT: dict = {
    "event_type": "best_bid_ask",
    "market": _MARKET,
    "asset_id": _ASSET_A,
    "best_bid": "0.48",
    "best_ask": "0.52",
    "spread": "0.04",
    "timestamp": "1778759371033",
}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def cache() -> MarketStateStore:
    return MarketStateStore()


@pytest.fixture
def market_ws(cache: MarketStateStore) -> MarketWebSocket:
    return MarketWebSocket(
        cache,
        ws_url="wss://test.example.com/ws/market",
    )


ASSET_IDS = ["0xaaa_yes", "0xbbb_no"]


# ---------------------------------------------------------------------------
# Subscription
# ---------------------------------------------------------------------------


class TestSubscription:
    """Verify subscription message format."""

    @pytest.mark.asyncio
    async def test_subscribe_message_shape(
        self,
        market_ws: MarketWebSocket,
    ) -> None:
        """Correct assets_ids + type + custom_feature_enabled payload."""
        mock_ws = AsyncMock()
        market_ws._asset_ids = ASSET_IDS
        await market_ws._subscribe(mock_ws)

        mock_ws.send.assert_called_once()
        sent = json.loads(mock_ws.send.call_args[0][0])
        assert sent["assets_ids"] == ASSET_IDS
        assert sent["type"] == "market"
        assert sent["custom_feature_enabled"] is True


# ---------------------------------------------------------------------------
# Heartbeat
# ---------------------------------------------------------------------------


class TestHeartbeat:
    """Verify heartbeat sends PING every 5 seconds."""

    @pytest.mark.asyncio
    async def test_heartbeat_interval(
        self,
        market_ws: MarketWebSocket,
    ) -> None:
        """Heartbeat sends PING every 5 seconds."""
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

        with patch("pmcopy.clients.ws.market_ws.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            await market_ws._heartbeat(mock_ws)
            # Should have slept for HEARTBEAT_INTERVAL each time
            assert mock_sleep.call_count >= 2
            for call in mock_sleep.call_args_list:
                assert call[0][0] == HEARTBEAT_INTERVAL

    @pytest.mark.asyncio
    async def test_heartbeat_exits_on_send_failure(
        self,
        market_ws: MarketWebSocket,
    ) -> None:
        """Heartbeat returns (triggering reconnect) on send failure."""
        mock_ws = AsyncMock()
        mock_ws.send.side_effect = ConnectionError("pipe broken")
        market_ws._running = True

        with patch("pmcopy.clients.ws.market_ws.asyncio.sleep", new_callable=AsyncMock):
            await market_ws._heartbeat(mock_ws)
            # Should return cleanly, not raise


# ---------------------------------------------------------------------------
# Message handling — price changes
# ---------------------------------------------------------------------------


class TestPriceChange:
    """Verify price change events update the cache."""

    def test_price_change_updates_cache(
        self,
        market_ws: MarketWebSocket,
        cache: MarketStateStore,
    ) -> None:
        """price_change with Polymarket format (per-item asset_id) updates cache."""
        event = json.dumps(
            {
                "event_type": "price_change",
                "market": "0xmarket",
                "price_changes": [
                    {"asset_id": "0xaaa_yes", "side": "BUY", "price": "0.55", "size": "100"},
                    {"asset_id": "0xaaa_yes", "side": "SELL", "price": "0.60", "size": "100"},
                ],
                "timestamp": "1000",
            }
        )
        market_ws._handle_message(event)

        quote = cache.get_quote("0xaaa_yes")
        assert quote is not None
        assert quote.best_bid == Decimal("0.55")
        assert quote.best_ask == Decimal("0.60")

    def test_price_change_fallback_fields(
        self,
        market_ws: MarketWebSocket,
        cache: MarketStateStore,
    ) -> None:
        """Book event with bids and asks sets both sides in cache."""
        event = json.dumps(
            {
                "event_type": "book",
                "asset_id": "0xaaa_yes",
                "market": "0xmarket",
                "bids": [{"price": "0.48", "size": "100"}],
                "asks": [{"price": "0.52", "size": "100"}],
            }
        )
        market_ws._handle_message(event)

        quote = cache.get_quote("0xaaa_yes")
        assert quote is not None
        assert quote.best_bid == Decimal("0.48")
        assert quote.best_ask == Decimal("0.52")

    def test_non_json_message_ignored(
        self,
        market_ws: MarketWebSocket,
        cache: MarketStateStore,
    ) -> None:
        """Non-JSON messages are silently ignored."""
        market_ws._handle_message("PONG")
        market_ws._handle_message("not-json{broken")
        assert cache.get_quote("nonexistent") is None

    def test_unknown_event_type_ignored(
        self,
        market_ws: MarketWebSocket,
        cache: MarketStateStore,
    ) -> None:
        """Unknown event types are ignored without error."""
        event = json.dumps({"event_type": "heartbeat_ack", "status": "ok"})
        market_ws._handle_message(event)
        assert cache.get_quote("nonexistent") is None


# ---------------------------------------------------------------------------
# Message handling — tick size changes
# ---------------------------------------------------------------------------


class TestTickSizeChange:
    """Verify tick size change events update the cache."""

    def test_tick_size_change_updates_cache(
        self,
        market_ws: MarketWebSocket,
        cache: MarketStateStore,
    ) -> None:
        """Tick size change event updates cache."""
        event = json.dumps(
            {
                "event_type": "tick_size_change",
                "asset_id": "0xaaa_yes",
                "tick_size": "0.001",
            }
        )
        market_ws._handle_message(event)

        # A tick-size-only event updates raw state and invalidates sizing, but
        # cannot create executable metadata without min_order_size/neg_risk.
        state = cache.get_state("0xaaa_yes")
        assert state is not None
        assert state.tick_size == Decimal("0.001")
        assert cache.get_tick("0xaaa_yes") is None


# ---------------------------------------------------------------------------
# Reconnect behavior
# ---------------------------------------------------------------------------


class TestReconnect:
    """Verify reconnect lifecycle."""

    @pytest.mark.asyncio
    async def test_reconnect_on_heartbeat_failure(
        self,
        cache: MarketStateStore,
    ) -> None:
        """Heartbeat failure triggers reconnect cycle.

        We mock websockets.connect to simulate one connection that dies
        on heartbeat, then stop the loop.
        """
        bootstrap_called = False

        async def mock_bootstrap(asset_ids: list[str]) -> None:
            nonlocal bootstrap_called
            bootstrap_called = True

        ws = MarketWebSocket(
            cache,
            ws_url="wss://test.example.com/ws/market",
            rest_bootstrap=mock_bootstrap,
        )
        ws._asset_ids = ASSET_IDS
        ws._running = True

        connect_count = 0

        # Create a mock context manager for websockets.connect
        mock_ws_instance = AsyncMock()

        # First call: send subscribe ok, then heartbeat fails
        send_call = 0

        async def mock_send(msg: str) -> None:
            nonlocal send_call
            send_call += 1
            if send_call > 1:  # first is subscribe, second is PING
                raise ConnectionError("dead")

        mock_ws_instance.send = mock_send

        async def mock_aiter(self_ws: object) -> None:
            # Block forever until cancel
            await asyncio.sleep(100)
            return
            yield  # make it an async generator  # noqa: E501

        mock_ws_instance.__aiter__ = mock_aiter

        async def connect_factory(*args: object, **kwargs: object) -> AsyncMock:
            nonlocal connect_count
            connect_count += 1
            if connect_count >= 2:
                ws._running = False
                raise OSError("stop")
            return mock_ws_instance

        mock_ctx = AsyncMock()
        mock_ctx.__aenter__ = connect_factory
        mock_ctx.__aexit__ = AsyncMock(return_value=False)

        with patch("pmcopy.clients.ws.market_ws.websockets.connect", return_value=mock_ctx):
            with patch("pmcopy.clients.ws.market_ws.asyncio.sleep", new_callable=AsyncMock):
                await ws._reconnect_loop()

        # Bootstrap should have been called
        assert bootstrap_called

    @pytest.mark.asyncio
    async def test_reconnect_rest_bootstrap(
        self,
        cache: MarketStateStore,
    ) -> None:
        """Reconnect fetches REST snapshot before resubscribing."""
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

        # Mock connection that fails immediately
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

        with patch("pmcopy.clients.ws.market_ws.websockets.connect", return_value=mock_ctx):
            with patch("pmcopy.clients.ws.market_ws.asyncio.sleep", new_callable=AsyncMock):
                await ws._reconnect_loop()

        # Bootstrap should have been called at least once with our asset IDs
        assert len(bootstrap_calls) >= 1
        assert bootstrap_calls[0] == ASSET_IDS


# ---------------------------------------------------------------------------
# Tests 1–3: Dispatch routing
# ---------------------------------------------------------------------------


class TestDispatch:
    """_dispatch_event reads actual event_type and routes correctly."""

    def test_dispatch_routes_book(
        self, market_ws: MarketWebSocket, cache: MarketStateStore
    ) -> None:
        market_ws._handle_message(json.dumps(BOOK_EVENT_TWO_SIDED))
        book = cache.get_book(_ASSET_A)
        assert book is not None
        assert book.source == "wss"

    def test_dispatch_routes_price_change(
        self, market_ws: MarketWebSocket, cache: MarketStateStore
    ) -> None:
        market_ws._handle_message(json.dumps(PRICE_CHANGE_EVENT))
        state_a = cache.get_state(_ASSET_A)
        assert state_a is not None

    def test_dispatch_routes_best_bid_ask(
        self, market_ws: MarketWebSocket, cache: MarketStateStore
    ) -> None:
        market_ws._handle_message(json.dumps(BEST_BID_ASK_EVENT))
        quote = cache.get_quote(_ASSET_A)
        assert quote is not None
        assert quote.source == "wss"


# ---------------------------------------------------------------------------
# Tests 4–6: _asset_id helper
# ---------------------------------------------------------------------------


class TestAssetId:
    """_asset_id module-level helper."""

    def test_asset_id_from_asset_id_key(self) -> None:
        assert _asset_id({"asset_id": "abc123"}) == "abc123"

    def test_asset_id_fallback_keys(self) -> None:
        assert _asset_id({"asset": "from_asset"}) == "from_asset"
        assert _asset_id({"token_id": "from_token_id"}) == "from_token_id"
        assert _asset_id({"token": "from_token"}) == "from_token"
        assert _asset_id({}) is None

    def test_asset_id_no_print(self) -> None:
        import pmcopy.clients.ws.market_ws as mws_module

        src = inspect.getsource(mws_module._asset_id)
        assert "print(" not in src


# ---------------------------------------------------------------------------
# Tests 7–12: Book event handling
# ---------------------------------------------------------------------------


class TestBookHandling:
    """WSS book events update MarketStateStore correctly."""

    def test_book_two_sided_sets_book_source_wss(
        self, market_ws: MarketWebSocket, cache: MarketStateStore
    ) -> None:
        market_ws._handle_message(json.dumps(BOOK_EVENT_TWO_SIDED))
        assert cache.book_source_of(_ASSET_A) == "wss"

    def test_book_asks_only_sets_book_source_wss(
        self, market_ws: MarketWebSocket, cache: MarketStateStore
    ) -> None:
        market_ws._handle_message(json.dumps(BOOK_EVENT_ONE_SIDED))
        assert cache.book_source_of(_ASSET_A) == "wss"
        book = cache.get_book(_ASSET_A)
        assert book is not None
        assert book.ask_count if hasattr(book, "ask_count") else len(book.asks) == 1

    def test_book_bids_only_sets_book_source_wss(
        self, market_ws: MarketWebSocket, cache: MarketStateStore
    ) -> None:
        event = {**BOOK_EVENT_ONE_SIDED, "bids": [{"price": "0.49", "size": "500"}], "asks": []}
        market_ws._handle_message(json.dumps(event))
        assert cache.book_source_of(_ASSET_A) == "wss"
        book = cache.get_book(_ASSET_A)
        assert book is not None
        assert len(book.bids) == 1

    def test_book_empty_bids_and_asks_not_executable(
        self, market_ws: MarketWebSocket, cache: MarketStateStore
    ) -> None:
        market_ws._handle_message(json.dumps(BOOK_EVENT_EMPTY))
        assert cache.get_book(_ASSET_A) is None
        # book_source should NOT be set to wss for empty book
        assert cache.book_source_of(_ASSET_A) != "wss"

    def test_book_sets_quote_source_wss(
        self, market_ws: MarketWebSocket, cache: MarketStateStore
    ) -> None:
        market_ws._handle_message(json.dumps(BOOK_EVENT_TWO_SIDED))
        assert cache.quote_source_of(_ASSET_A) == "wss"

    def test_book_stores_condition_id_from_market(
        self, market_ws: MarketWebSocket, cache: MarketStateStore
    ) -> None:
        market_ws._handle_message(json.dumps(BOOK_EVENT_TWO_SIDED))
        state = cache.get_state(_ASSET_A)
        assert state is not None
        assert state.condition_id == _MARKET


# ---------------------------------------------------------------------------
# Tests 13–18: price_change handling
# ---------------------------------------------------------------------------


class TestPriceChangeHandling:
    """price_change events use per-item asset_id from price_changes[]."""

    def test_uses_price_changes_key(
        self, market_ws: MarketWebSocket, cache: MarketStateStore
    ) -> None:
        market_ws._handle_message(json.dumps(PRICE_CHANGE_EVENT))
        # Both per-item assets should be updated
        assert cache.get_state(_ASSET_A) is not None
        assert cache.get_state(_ASSET_B) is not None

    def test_no_toplevel_asset_id_required(
        self, market_ws: MarketWebSocket, cache: MarketStateStore
    ) -> None:
        # Confirm the event has no top-level asset_id
        assert "asset_id" not in PRICE_CHANGE_EVENT
        # Should still process without error
        market_ws._handle_message(json.dumps(PRICE_CHANGE_EVENT))

    def test_uses_nested_change_asset_id(
        self, market_ws: MarketWebSocket, cache: MarketStateStore
    ) -> None:
        market_ws._handle_message(json.dumps(PRICE_CHANGE_EVENT))
        # _ASSET_B gets a BUY delta at price 0.01
        state_b = cache.get_state(_ASSET_B)
        assert state_b is not None
        assert Decimal("0.01") in state_b.bids

    def test_size_zero_deletes_level(
        self, market_ws: MarketWebSocket, cache: MarketStateStore
    ) -> None:
        # First add a level
        setup = {
            "event_type": "price_change",
            "market": _MARKET,
            "price_changes": [
                {"asset_id": _ASSET_A, "side": "BUY", "price": "0.45", "size": "100"},
            ],
            "timestamp": "1",
        }
        market_ws._handle_message(json.dumps(setup))
        state = cache.get_state(_ASSET_A)
        assert Decimal("0.45") in state.bids  # type: ignore[union-attr]

        # Now delete it (size=0)
        delete = {
            "event_type": "price_change",
            "market": _MARKET,
            "price_changes": [
                {"asset_id": _ASSET_A, "side": "BUY", "price": "0.45", "size": "0"},
            ],
            "timestamp": "2",
        }
        market_ws._handle_message(json.dumps(delete))
        state = cache.get_state(_ASSET_A)
        assert Decimal("0.45") not in (state.bids if state else {})

    def test_size_positive_upserts_level(
        self, market_ws: MarketWebSocket, cache: MarketStateStore
    ) -> None:
        event = {
            "event_type": "price_change",
            "market": _MARKET,
            "price_changes": [
                {"asset_id": _ASSET_A, "side": "SELL", "price": "0.99", "size": "500"},
            ],
            "timestamp": "1",
        }
        market_ws._handle_message(json.dumps(event))
        state = cache.get_state(_ASSET_A)
        assert state is not None
        assert state.asks.get(Decimal("0.99")) == Decimal("500")

    def test_updates_quote_source_wss_from_nested_best_bid_ask(
        self, market_ws: MarketWebSocket, cache: MarketStateStore
    ) -> None:
        market_ws._handle_message(json.dumps(PRICE_CHANGE_EVENT))
        # _ASSET_B's change has best_bid=0.999, best_ask=1
        assert cache.quote_source_of(_ASSET_B) == "wss"
        state_b = cache.get_state(_ASSET_B)
        assert state_b is not None
        assert state_b.best_bid == Decimal("0.999")


# ---------------------------------------------------------------------------
# Tests 19–21: best_bid_ask handling
# ---------------------------------------------------------------------------


class TestBestBidAskHandling:
    """best_bid_ask updates quote only; does not touch book."""

    def test_sets_quote_source_wss(
        self, market_ws: MarketWebSocket, cache: MarketStateStore
    ) -> None:
        market_ws._handle_message(json.dumps(BEST_BID_ASK_EVENT))
        assert cache.quote_source_of(_ASSET_A) == "wss"

    def test_does_not_set_book_source(
        self, market_ws: MarketWebSocket, cache: MarketStateStore
    ) -> None:
        market_ws._handle_message(json.dumps(BEST_BID_ASK_EVENT))
        assert cache.book_source_of(_ASSET_A) != "wss"

    def test_alone_does_not_make_token_executable(
        self, market_ws: MarketWebSocket, cache: MarketStateStore
    ) -> None:
        market_ws._handle_message(json.dumps(BEST_BID_ASK_EVENT))
        # No book, no tick → not executable
        book = cache.get_book(_ASSET_A)
        assert book is None


# ---------------------------------------------------------------------------
# Tests 22–23: tick_size_change handling
# ---------------------------------------------------------------------------


class TestTickSizeChangeSafety:
    """tick_size_change updates tick only; does not promote quote/book."""

    def test_tick_size_change_updates_tick_source_wss(
        self, market_ws: MarketWebSocket, cache: MarketStateStore
    ) -> None:
        event = json.dumps(
            {
                "event_type": "tick_size_change",
                "asset_id": _ASSET_A,
                "tick_size": "0.001",
            }
        )
        market_ws._handle_message(event)
        assert cache.tick_source_of(_ASSET_A) == "wss"

    def test_tick_size_change_alone_not_executable(
        self, market_ws: MarketWebSocket, cache: MarketStateStore
    ) -> None:
        event = json.dumps(
            {
                "event_type": "tick_size_change",
                "asset_id": _ASSET_A,
                "tick_size": "0.001",
            }
        )
        market_ws._handle_message(event)
        # tick_size_change marks dirty → is_dirty must be True
        assert cache.is_dirty(_ASSET_A)
        # book not set → not executable
        assert cache.get_book(_ASSET_A) is None


# ---------------------------------------------------------------------------
# Test 24: last_trade_price
# ---------------------------------------------------------------------------


class TestLastTradePrice:
    """last_trade_price is observability only; does not affect executability."""

    def test_last_trade_price_not_executable(
        self, market_ws: MarketWebSocket, cache: MarketStateStore
    ) -> None:
        event = json.dumps(
            {
                "event_type": "last_trade_price",
                "asset_id": _ASSET_A,
                "price": "0.55",
            }
        )
        market_ws._handle_message(event)
        assert cache.get_book(_ASSET_A) is None
        assert cache.book_source_of(_ASSET_A) != "wss"


# ---------------------------------------------------------------------------
# Test 25: market_resolved
# ---------------------------------------------------------------------------


class TestMarketResolvedHandling:
    """market_resolved marks all listed asset_ids non-executable."""

    def test_market_resolved_marks_assets_ids_non_executable(
        self, market_ws: MarketWebSocket, cache: MarketStateStore
    ) -> None:
        # First give both assets a live state
        cache.replace_book_snapshot(
            _ASSET_A,
            {Decimal("0.49"): Decimal("100")},
            {Decimal("0.51"): Decimal("100")},
            {"source": "wss"},
        )
        cache.replace_book_snapshot(
            _ASSET_B,
            {Decimal("0.49"): Decimal("100")},
            {Decimal("0.51"): Decimal("100")},
            {"source": "wss"},
        )

        event = json.dumps(
            {
                "event_type": "market_resolved",
                "market": _MARKET,
                "assets_ids": [_ASSET_A, _ASSET_B],
            }
        )
        market_ws._handle_message(event)

        assert cache.is_resolved(_ASSET_A)
        assert cache.is_resolved(_ASSET_B)


# ---------------------------------------------------------------------------
# Test 26: StaleDataError debug state
# ---------------------------------------------------------------------------


class TestStaleErrorDebugState:
    """StaleDataError message includes MarketStateStore debug fields."""

    @pytest.mark.asyncio
    async def test_stale_error_includes_debug_state(self, cache: MarketStateStore) -> None:
        from pmcopy.services.market_data.market_data_service import MarketDataService, MarketDataPolicy, StaleDataError

        gateway = MagicMock()
        policy = MarketDataPolicy(
            max_quote_age_seconds=10.0,
            max_book_age_seconds=10.0,
            max_tick_age_seconds=300.0,
            require_wss_fresh=True,
            allow_rest_fallback=False,
        )
        svc = MarketDataService(cache, gateway, policy=policy)

        with pytest.raises(StaleDataError) as exc_info:
            await svc.require_fresh_data(_ASSET_A)

        msg = str(exc_info.value)
        assert "quote_source=" in msg
        assert "book_source=" in msg
        assert "bid_count=" in msg


# ---------------------------------------------------------------------------
# Tests 27–29: No print / no canary
# ---------------------------------------------------------------------------


class TestCodeQuality:
    """Verify no raw print() calls and no canary references remain."""

    def test_no_print_in_market_ws(self) -> None:
        import pmcopy.clients.ws.market_ws as mws_module

        src = inspect.getsource(mws_module)
        # Allow print only inside string literals (unlikely here); reject bare calls
        lines = [ln for ln in src.splitlines() if "print(" in ln and not ln.strip().startswith("#")]
        assert lines == [], f"Raw print() found in market_ws.py: {lines}"

    def test_no_canary_in_market_ws(self) -> None:
        import pmcopy.clients.ws.market_ws as mws_module

        src = inspect.getsource(mws_module)
        assert "canary" not in src.lower()

    def test_no_canary_in_market_state_store(self) -> None:
        import pmcopy.cache.market_state_store as mss_module

        src = inspect.getsource(mss_module)
        assert "canary" not in src.lower()


# ---------------------------------------------------------------------------
# PR10.2: Fixture-based tests (real observed and docs-shaped payloads)
# ---------------------------------------------------------------------------


class TestFixtureLoading:
    """Load fixture files and verify parsing against MarketStateStore."""

    @staticmethod
    def _load(filename: str) -> dict:
        return json.loads((FIXTURE_DIR / filename).read_text())

    def test_live_book_fixture_accepted(
        self, market_ws: MarketWebSocket, cache: MarketStateStore
    ) -> None:
        event = self._load("book_live_one_sided_asks.json")
        market_ws._handle_message(json.dumps(event))
        book = cache.get_book(_ASSET_A)
        assert book is not None
        assert cache.book_source_of(_ASSET_A) == "wss"

    def test_live_price_change_fixture_touches_both_assets(
        self, market_ws: MarketWebSocket, cache: MarketStateStore
    ) -> None:
        event = self._load("price_change_live_nested.json")
        market_ws._handle_message(json.dumps(event))
        assert cache.quote_source_of(_ASSET_A) == "wss"
        assert cache.quote_source_of(_ASSET_B) == "wss"

    def test_live_best_bid_ask_fixture_updates_quote_only(
        self, market_ws: MarketWebSocket, cache: MarketStateStore
    ) -> None:
        event = self._load("best_bid_ask_live.json")
        market_ws._handle_message(json.dumps(event))
        assert cache.quote_source_of(_ASSET_A) == "wss"
        assert cache.book_source_of(_ASSET_A) != "wss"

    def test_book_docs_shape_accepted(
        self, market_ws: MarketWebSocket, cache: MarketStateStore
    ) -> None:
        event = self._load("book_docs_shape.json")
        market_ws._handle_message(json.dumps(event))
        assert cache.book_source_of(_ASSET_A) == "wss"

    def test_tick_size_change_docs_shape_updates_tick(
        self, market_ws: MarketWebSocket, cache: MarketStateStore
    ) -> None:
        event = self._load("tick_size_change_docs_shape.json")
        market_ws._handle_message(json.dumps(event))
        state = cache.get_state(_ASSET_A)
        assert state is not None
        assert state.tick_size == Decimal("0.001")
        assert state.tick_source == "wss"

    def test_last_trade_price_docs_shape_does_not_set_book(
        self, market_ws: MarketWebSocket, cache: MarketStateStore
    ) -> None:
        event = self._load("last_trade_price_docs_shape.json")
        market_ws._handle_message(json.dumps(event))
        assert cache.get_book(_ASSET_A) is None

    def test_new_market_docs_shape_does_not_set_book(
        self, market_ws: MarketWebSocket, cache: MarketStateStore
    ) -> None:
        event = self._load("new_market_docs_shape.json")
        market_ws._handle_message(json.dumps(event))
        assert cache.get_book(_ASSET_A) is None

    def test_market_resolved_docs_shape_marks_resolved(
        self, market_ws: MarketWebSocket, cache: MarketStateStore
    ) -> None:
        event = self._load("market_resolved_docs_shape.json")
        market_ws._handle_message(json.dumps(event))
        assert cache.is_resolved(_ASSET_A)
        assert cache.is_resolved(_ASSET_B)


# ---------------------------------------------------------------------------
# PR10.2: Book event tick_size metadata (test 7, test 8)
# ---------------------------------------------------------------------------


class TestBookTickSizeHandling:
    """Tick_size from book snapshot updates tick metadata without dirtying state."""

    def test_book_tick_size_sets_tick_source_wss(
        self, market_ws: MarketWebSocket, cache: MarketStateStore
    ) -> None:
        market_ws._handle_message(json.dumps(BOOK_EVENT_TWO_SIDED))
        state = cache.get_state(_ASSET_A)
        assert state is not None
        assert state.tick_source == "wss"
        assert state.tick_size == Decimal("0.01")

    def test_book_tick_size_does_not_dirty_state(
        self, market_ws: MarketWebSocket, cache: MarketStateStore
    ) -> None:
        market_ws._handle_message(json.dumps(BOOK_EVENT_TWO_SIDED))
        assert not cache.is_dirty(_ASSET_A)

    def test_book_tick_size_does_not_affect_book_or_quote_source(
        self, market_ws: MarketWebSocket, cache: MarketStateStore
    ) -> None:
        market_ws._handle_message(json.dumps(BOOK_EVENT_TWO_SIDED))
        assert cache.book_source_of(_ASSET_A) == "wss"
        assert cache.quote_source_of(_ASSET_A) == "wss"

    def test_book_last_trade_price_field_does_not_change_sources(
        self, market_ws: MarketWebSocket, cache: MarketStateStore
    ) -> None:
        event = {**BOOK_EVENT_TWO_SIDED, "last_trade_price": "0.50"}
        market_ws._handle_message(json.dumps(event))
        assert cache.book_source_of(_ASSET_A) == "wss"
        assert cache.quote_source_of(_ASSET_A) == "wss"

    def test_book_without_tick_size_does_not_dirty(
        self, market_ws: MarketWebSocket, cache: MarketStateStore
    ) -> None:
        event = {k: v for k, v in BOOK_EVENT_TWO_SIDED.items() if k != "tick_size"}
        market_ws._handle_message(json.dumps(event))
        assert not cache.is_dirty(_ASSET_A)


# ---------------------------------------------------------------------------
# PR10.2: One-sided book quote derivation (test 6)
# ---------------------------------------------------------------------------


class TestOneSidedBookQuote:
    """One-sided WSS book derives best_ask / best_bid from the populated side."""

    def test_asks_only_sets_best_ask_from_min_asks(
        self, market_ws: MarketWebSocket, cache: MarketStateStore
    ) -> None:
        market_ws._handle_message(json.dumps(BOOK_EVENT_ONE_SIDED))
        state = cache.get_state(_ASSET_A)
        assert state is not None
        assert state.best_ask == Decimal("0.999")
        assert state.quote_source == "wss"

    def test_bids_only_sets_best_bid_from_max_bids(
        self, market_ws: MarketWebSocket, cache: MarketStateStore
    ) -> None:
        event = {**BOOK_EVENT_ONE_SIDED, "bids": [{"price": "0.49", "size": "500"}], "asks": []}
        market_ws._handle_message(json.dumps(event))
        state = cache.get_state(_ASSET_A)
        assert state is not None
        assert state.best_bid == Decimal("0.49")
        assert state.quote_source == "wss"


# ---------------------------------------------------------------------------
# PR10.2: new_market event (test 19)
# ---------------------------------------------------------------------------


class TestNewMarketHandling:
    """new_market events may store metadata but must not create quote/book."""

    def test_new_market_does_not_set_book(
        self, market_ws: MarketWebSocket, cache: MarketStateStore
    ) -> None:
        event = {"event_type": "new_market", "asset_id": _ASSET_A, "condition_id": _MARKET}
        market_ws._handle_message(json.dumps(event))
        assert cache.get_book(_ASSET_A) is None

    def test_new_market_does_not_set_quote(
        self, market_ws: MarketWebSocket, cache: MarketStateStore
    ) -> None:
        event = {"event_type": "new_market", "asset_id": _ASSET_A, "condition_id": _MARKET}
        market_ws._handle_message(json.dumps(event))
        assert cache.get_quote(_ASSET_A) is None

    def test_new_market_stores_condition_id(
        self, market_ws: MarketWebSocket, cache: MarketStateStore
    ) -> None:
        event = {"event_type": "new_market", "asset_id": _ASSET_A, "condition_id": _MARKET}
        market_ws._handle_message(json.dumps(event))
        state = cache.get_state(_ASSET_A)
        assert state is not None
        assert state.condition_id == _MARKET


# ---------------------------------------------------------------------------
# PR10.2: End-to-end executability (tests 21, 22)
# ---------------------------------------------------------------------------


class TestTokenExecutability:
    """WSS book + pre-set tick → clean state; quote-only → no book."""

    def test_wss_book_with_pretick_leaves_clean_state(
        self, market_ws: MarketWebSocket, cache: MarketStateStore
    ) -> None:
        cache.set_tick(
            _ASSET_A,
            tick_size=Decimal("0.01"),
            min_order_size=Decimal("1"),
            neg_risk=False,
        )
        market_ws._handle_message(json.dumps(BOOK_EVENT_TWO_SIDED))

        assert not cache.is_dirty(_ASSET_A)
        assert cache.book_source_of(_ASSET_A) == "wss"
        assert cache.quote_source_of(_ASSET_A) == "wss"
        assert cache.get_book(_ASSET_A) is not None
        assert cache.get_tick(_ASSET_A) is not None

    def test_quote_only_no_book_returns_no_book(
        self, market_ws: MarketWebSocket, cache: MarketStateStore
    ) -> None:
        market_ws._handle_message(json.dumps(BEST_BID_ASK_EVENT))
        assert cache.get_book(_ASSET_A) is None

    def test_wss_book_without_min_order_size_returns_no_tick(
        self, market_ws: MarketWebSocket, cache: MarketStateStore
    ) -> None:
        market_ws._handle_message(json.dumps(BOOK_EVENT_TWO_SIDED))
        # update_tick_size_from_book sets tick_size but not min_order_size
        # → get_tick() returns None until min_order_size is provided
        assert cache.get_tick(_ASSET_A) is None


# ---------------------------------------------------------------------------
# PR10.2: get_debug_state includes tick_size (Part E)
# ---------------------------------------------------------------------------


class TestDebugStateTick:
    """get_debug_state returns tick_size field."""

    def test_debug_state_has_tick_size_when_set(self, cache: MarketStateStore) -> None:
        cache.set_tick(
            _ASSET_A,
            tick_size=Decimal("0.01"),
            min_order_size=Decimal("1"),
            neg_risk=False,
        )
        debug = cache.get_debug_state(_ASSET_A)
        assert "tick_size" in debug
        assert debug["tick_size"] == "0.01"

    def test_debug_state_tick_size_none_when_not_set(self, cache: MarketStateStore) -> None:
        cache.ensure_state(_ASSET_A)
        debug = cache.get_debug_state(_ASSET_A)
        assert "tick_size" in debug
        assert debug["tick_size"] is None


# ---------------------------------------------------------------------------
# PR10.2: canonical mark_resolved in market_resolved handler
# ---------------------------------------------------------------------------


class TestMarketResolvedCanonical:
    """_handle_market_resolved stores condition_id via mark_resolved."""

    def test_resolved_stores_condition_id_from_market_field(
        self, market_ws: MarketWebSocket, cache: MarketStateStore
    ) -> None:
        event = {
            "event_type": "market_resolved",
            "market": _MARKET,
            "assets_ids": [_ASSET_A],
        }
        market_ws._handle_message(json.dumps(event))
        state = cache.get_state(_ASSET_A)
        assert state is not None
        assert state.condition_id == _MARKET
        assert cache.is_resolved(_ASSET_A)


# ---------------------------------------------------------------------------
# PR10.2: tick_size_change prefers new_tick_size field
# ---------------------------------------------------------------------------


class TestTickSizeChangePriority:
    """_handle_tick_size_change prefers new_tick_size over tick_size."""

    def test_new_tick_size_field_takes_priority(
        self, market_ws: MarketWebSocket, cache: MarketStateStore
    ) -> None:
        event = {
            "event_type": "tick_size_change",
            "asset_id": _ASSET_A,
            "new_tick_size": "0.001",
            "tick_size": "0.01",
        }
        market_ws._handle_message(json.dumps(event))
        state = cache.get_state(_ASSET_A)
        assert state is not None
        assert state.tick_size == Decimal("0.001")

    def test_tick_size_field_used_when_no_new_tick_size(
        self, market_ws: MarketWebSocket, cache: MarketStateStore
    ) -> None:
        event = {
            "event_type": "tick_size_change",
            "asset_id": _ASSET_A,
            "tick_size": "0.01",
        }
        market_ws._handle_message(json.dumps(event))
        state = cache.get_state(_ASSET_A)
        assert state is not None
        assert state.tick_size == Decimal("0.01")


# ---------------------------------------------------------------------------
# PR10.2: capture script quality checks (test 24)
# ---------------------------------------------------------------------------


class TestCaptureScriptQuality:
    """capture_market_wss_events.py is a safe read-only diagnostic script."""

    SCRIPT_PATH = Path(__file__).parent.parent / "scripts" / "capture_market_wss_events.py"

    @classmethod
    def _src(cls) -> str:
        return cls.SCRIPT_PATH.read_text()

    def test_capture_script_exists(self) -> None:
        assert self.SCRIPT_PATH.exists()

    def test_capture_script_no_app_import(self) -> None:
        src = self._src()
        assert "import app" not in src
        assert "from app" not in src

    def test_capture_script_no_trading_services(self) -> None:
        src = self._src()
        for forbidden in ("ExecutionService", "TradingContext", "TradeDecisionEngine"):
            assert forbidden not in src, f"Forbidden import '{forbidden}' found in capture script"

    def test_capture_script_print_goes_to_stderr(self) -> None:
        tree = ast.parse(self._src())
        offending = [
            node.lineno
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "print"
            and not any(
                kw.arg == "file" and ast.unparse(kw.value) == "sys.stderr" for kw in node.keywords
            )
        ]
        assert offending == [], f"print() without file=sys.stderr at lines: {offending}"
