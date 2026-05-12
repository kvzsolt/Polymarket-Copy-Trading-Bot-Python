"""Tests for clients.market_ws — Market WebSocket client.

Phase 2 test coverage:
  - Subscription message shape (assets_ids, type, custom_feature_enabled)
  - Heartbeat interval compliance (5s PING)
  - Price change event updates cache
  - Tick size change event updates cache
  - Reconnect on heartbeat failure
  - REST bootstrap on reconnect
"""
from __future__ import annotations

import asyncio
import json
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from cache.market_state_store import MarketStateStore
from clients.market_ws import MarketWebSocket, HEARTBEAT_INTERVAL


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

        with patch("clients.market_ws.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
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

        with patch("clients.market_ws.asyncio.sleep", new_callable=AsyncMock):
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
        """Market price change event updates cache."""
        event = json.dumps({
            "event_type": "price_change",
            "asset_id": "0xaaa_yes",
            "changes": [
                {"side": "BUY", "price": "0.55"},
                {"side": "SELL", "price": "0.60"},
            ],
        })
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
        """Price change with top-level best_bid/best_ask also works."""
        event = json.dumps({
            "event_type": "book",
            "asset_id": "0xaaa_yes",
            "best_bid": "0.48",
            "best_ask": "0.52",
        })
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
        event = json.dumps({
            "event_type": "tick_size_change",
            "asset_id": "0xaaa_yes",
            "tick_size": "0.001",
        })
        market_ws._handle_message(event)

        tick = cache.get_tick("0xaaa_yes")
        assert tick is not None
        assert tick.tick_size == Decimal("0.001")


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

        # Message loop returns immediately (no messages)
        msg_queue: asyncio.Queue[str] = asyncio.Queue()

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

        with patch("clients.market_ws.websockets.connect", return_value=mock_ctx):
            with patch("clients.market_ws.asyncio.sleep", new_callable=AsyncMock):
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

        with patch("clients.market_ws.websockets.connect", return_value=mock_ctx):
            with patch("clients.market_ws.asyncio.sleep", new_callable=AsyncMock):
                await ws._reconnect_loop()

        # Bootstrap should have been called at least once with our asset IDs
        assert len(bootstrap_calls) >= 1
        assert bootstrap_calls[0] == ASSET_IDS