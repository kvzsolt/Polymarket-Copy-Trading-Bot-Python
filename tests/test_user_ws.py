"""Tests for clients.user_ws — User WebSocket client.

Phase 2 test coverage:
  - Fill event invalidates cache
  - Order update events are logged
  - Heartbeat interval compliance (5s PING)
  - Reconnect on connection failure
"""

from __future__ import annotations

import asyncio
import json
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest

from clients.user_ws import UserWebSocket, _HEARTBEAT_INTERVAL
from services.price_cache import PositionPriceCache


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def cache() -> PositionPriceCache:
    return PositionPriceCache(quote_ttl=5.0, tick_ttl=300.0)


@pytest.fixture
def user_ws(cache: PositionPriceCache) -> UserWebSocket:
    return UserWebSocket(
        cache,
        ws_url="wss://test.example.com/ws/user",
    )


TOKEN_A = "0xaaa_token_yes"
API_CREDS = {"api_key": "test_key", "api_secret": "test_secret"}


# ---------------------------------------------------------------------------
# Fill event handling
# ---------------------------------------------------------------------------

class TestFillEvent:
    """Verify fill events trigger cache invalidation."""

    def test_fill_event_invalidates_cache(
        self,
        user_ws: UserWebSocket,
        cache: PositionPriceCache,
    ) -> None:
        """User fill event triggers cache invalidation."""
        # Pre-populate cache
        cache.set_quote(TOKEN_A, best_bid=Decimal("0.55"), best_ask=Decimal("0.60"))
        assert cache.get_quote(TOKEN_A) is not None

        # Simulate fill event
        event = json.dumps({
            "event_type": "trade",
            "asset_id": TOKEN_A,
            "side": "BUY",
            "size": "100",
            "price": "0.55",
        })
        user_ws._handle_message(event)

        # Cache should be invalidated
        assert cache.get_quote(TOKEN_A) is None

    def test_fill_event_order_fill_type(
        self,
        user_ws: UserWebSocket,
        cache: PositionPriceCache,
    ) -> None:
        """order_fill event type also triggers invalidation."""
        cache.set_quote(TOKEN_A, best_bid=Decimal("0.55"), best_ask=Decimal("0.60"))

        event = json.dumps({
            "event_type": "order_fill",
            "asset_id": TOKEN_A,
            "side": "SELL",
        })
        user_ws._handle_message(event)

        assert cache.get_quote(TOKEN_A) is None

    def test_fill_event_fill_type(
        self,
        user_ws: UserWebSocket,
        cache: PositionPriceCache,
    ) -> None:
        """fill event type also triggers invalidation."""
        cache.set_quote(TOKEN_A, best_bid=Decimal("0.55"), best_ask=Decimal("0.60"))

        event = json.dumps({
            "event_type": "fill",
            "token_id": TOKEN_A,
            "side": "BUY",
        })
        user_ws._handle_message(event)

        assert cache.get_quote(TOKEN_A) is None

    def test_fill_without_token_id_ignored(
        self,
        user_ws: UserWebSocket,
        cache: PositionPriceCache,
    ) -> None:
        """Fill event missing token_id is safely ignored."""
        cache.set_quote(TOKEN_A, best_bid=Decimal("0.55"), best_ask=Decimal("0.60"))

        event = json.dumps({
            "event_type": "trade",
            "side": "BUY",
            # no asset_id/market/token_id
        })
        user_ws._handle_message(event)

        # Cache NOT invalidated (can't identify which token)
        assert cache.get_quote(TOKEN_A) is not None


# ---------------------------------------------------------------------------
# Order update handling
# ---------------------------------------------------------------------------

class TestOrderUpdate:
    """Verify order lifecycle events are handled without side effects."""

    def test_order_update_logged(
        self,
        user_ws: UserWebSocket,
        cache: PositionPriceCache,
    ) -> None:
        """Order update events are logged but don't invalidate cache."""
        cache.set_quote(TOKEN_A, best_bid=Decimal("0.55"), best_ask=Decimal("0.60"))

        event = json.dumps({
            "event_type": "order_update",
            "order_id": "ord_123",
            "status": "open",
            "asset_id": TOKEN_A,
        })
        user_ws._handle_message(event)

        # Cache NOT invalidated for order updates
        assert cache.get_quote(TOKEN_A) is not None


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------

class TestAuthentication:
    """Verify authentication message format."""

    @pytest.mark.asyncio
    async def test_authenticate_sends_creds(
        self,
        user_ws: UserWebSocket,
    ) -> None:
        """Authentication sends type + API credentials."""
        mock_ws = AsyncMock()
        user_ws._api_creds = API_CREDS

        await user_ws._authenticate(mock_ws)

        mock_ws.send.assert_called_once()
        sent = json.loads(mock_ws.send.call_args[0][0])
        assert sent["type"] == "auth"
        assert sent["api_key"] == "test_key"
        assert sent["api_secret"] == "test_secret"

    def test_auth_success_logged(
        self,
        user_ws: UserWebSocket,
    ) -> None:
        """Auth success response is handled gracefully."""
        event = json.dumps({
            "type": "auth",
            "status": "success",
        })
        user_ws._handle_message(event)  # Should not raise


# ---------------------------------------------------------------------------
# Heartbeat
# ---------------------------------------------------------------------------

class TestHeartbeat:
    """Verify heartbeat sends PING every 5 seconds."""

    @pytest.mark.asyncio
    async def test_heartbeat_interval(
        self,
        user_ws: UserWebSocket,
    ) -> None:
        """User WS heartbeat sends PING every 5s."""
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
                assert call[0][0] == _HEARTBEAT_INTERVAL

    @pytest.mark.asyncio
    async def test_heartbeat_exits_on_failure(
        self,
        user_ws: UserWebSocket,
    ) -> None:
        """Heartbeat returns cleanly on send failure."""
        mock_ws = AsyncMock()
        mock_ws.send.side_effect = ConnectionError("pipe broken")
        user_ws._running = True

        with patch("clients.user_ws.asyncio.sleep", new_callable=AsyncMock):
            await user_ws._heartbeat(mock_ws)
            # Should return without raising


# ---------------------------------------------------------------------------
# Reconnect
# ---------------------------------------------------------------------------

class TestReconnect:
    """Verify reconnect lifecycle."""

    @pytest.mark.asyncio
    async def test_reconnect_on_failure(
        self,
        cache: PositionPriceCache,
    ) -> None:
        """Connection failure triggers reconnect with backoff."""
        ws = UserWebSocket(cache, ws_url="wss://test.example.com/ws/user")
        ws._api_creds = API_CREDS
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

        with patch("clients.user_ws.websockets.connect", return_value=mock_ctx):
            with patch("clients.user_ws.asyncio.sleep", new_callable=AsyncMock):
                await ws._reconnect_loop()

        # Should have tried to connect at least twice
        assert connect_count >= 2


# ---------------------------------------------------------------------------
# Message edge cases
# ---------------------------------------------------------------------------

class TestMessageEdgeCases:
    """Edge cases in message handling."""

    def test_pong_ignored(self, user_ws: UserWebSocket) -> None:
        """PONG messages are silently consumed."""
        user_ws._handle_message("PONG")
        user_ws._handle_message("pong")
        user_ws._handle_message("")

    def test_non_json_ignored(self, user_ws: UserWebSocket) -> None:
        """Non-JSON messages are silently consumed."""
        user_ws._handle_message("not-json{broken")

    def test_list_of_events(
        self,
        user_ws: UserWebSocket,
        cache: PositionPriceCache,
    ) -> None:
        """Array of events is processed individually."""
        cache.set_quote(TOKEN_A, best_bid=Decimal("0.55"), best_ask=Decimal("0.60"))

        events = json.dumps([
            {"event_type": "trade", "asset_id": TOKEN_A, "side": "BUY"},
            {"event_type": "order_update", "order_id": "ord_1", "status": "open"},
        ])
        user_ws._handle_message(events)

        # Fill invalidated the cache
        assert cache.get_quote(TOKEN_A) is None
