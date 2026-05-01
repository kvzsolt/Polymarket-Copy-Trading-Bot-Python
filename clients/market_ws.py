"""Market WebSocket client for real-time price and tick data streaming.

Phase 2 deliverable: streams live market data from Polymarket via WSS,
feeding the ``PositionPriceCache`` with price-change and tick-size events.

Connection lifecycle:
  1. Connect to market WSS endpoint
  2. Subscribe to ``assets_ids`` with ``type: "market"``
  3. Start heartbeat (5s PING) + message processing loop
  4. On disconnect/failure → REST snapshot bootstrap → WSS reconnect

The heartbeat sends a literal ``"PING"`` string every 5 seconds.
If the send fails, the connection is considered dead and the reconnect
loop takes over.
"""

from __future__ import annotations

import asyncio
import json
import time
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any, Callable, Coroutine

import websockets
from websockets.exceptions import (
    ConnectionClosed,
    ConnectionClosedError,
    ConnectionClosedOK,
)

from utils.logger import get_logger

if TYPE_CHECKING:
    from services.price_cache import PositionPriceCache

logger = get_logger("MarketWS")

# Default Polymarket market WSS endpoint
DEFAULT_MARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

# Reconnect parameters
_MIN_RECONNECT_DELAY = 1.0
_MAX_RECONNECT_DELAY = 30.0
_HEARTBEAT_INTERVAL = 5  # seconds — per spec


class MarketWebSocket:
    """Async WebSocket client for Polymarket market data streams.

    Subscribes to real-time price changes and tick-size changes for
    the specified ``asset_ids`` (token IDs).
    """

    def __init__(
        self,
        cache: PositionPriceCache,
        *,
        ws_url: str = DEFAULT_MARKET_WS_URL,
        rest_bootstrap: Callable[
            [list[str]], Coroutine[Any, Any, None]
        ] | None = None,
    ) -> None:
        self._cache = cache
        self._ws_url = ws_url
        self._rest_bootstrap = rest_bootstrap
        self._asset_ids: list[str] = []
        self._ws: Any = None
        self._running = False
        self._reconnect_delay = _MIN_RECONNECT_DELAY

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def connect(self, asset_ids: list[str]) -> None:
        """Connect, subscribe, and run heartbeat + message loop.

        This coroutine runs until ``stop()`` is called or an
        unrecoverable error occurs. Reconnects are handled internally.
        """
        self._asset_ids = list(asset_ids)
        self._running = True
        logger.info(
            "MarketWS: starting for %d asset(s)",
            len(self._asset_ids),
        )
        await self._reconnect_loop()

    async def stop(self) -> None:
        """Signal the client to disconnect gracefully."""
        self._running = False
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
        logger.info("MarketWS: stopped")

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    async def _reconnect_loop(self) -> None:
        """Main reconnect loop: REST bootstrap → WSS connect → run."""
        while self._running:
            try:
                # 1. REST snapshot bootstrap (if available)
                if self._rest_bootstrap and self._asset_ids:
                    try:
                        logger.info("MarketWS: REST bootstrap for %d assets", len(self._asset_ids))
                        self._cache.invalidate_all()
                        await self._rest_bootstrap(self._asset_ids)
                    except Exception as exc:
                        logger.warning("MarketWS: REST bootstrap failed: %s", exc)

                # 2. WSS connect
                async with websockets.connect(self._ws_url) as ws:
                    self._ws = ws
                    self._reconnect_delay = _MIN_RECONNECT_DELAY
                    logger.info("MarketWS: connected to %s", self._ws_url)

                    # 3. Subscribe
                    await self._subscribe(ws)

                    # 4. Run heartbeat + message loop concurrently
                    heartbeat_task = asyncio.create_task(self._heartbeat(ws))
                    try:
                        await self._message_loop(ws)
                    finally:
                        heartbeat_task.cancel()
                        try:
                            await heartbeat_task
                        except asyncio.CancelledError:
                            pass

            except (ConnectionClosed, ConnectionClosedError, ConnectionClosedOK) as exc:
                logger.warning("MarketWS: connection closed: %s", exc)
            except OSError as exc:
                logger.warning("MarketWS: network error: %s", exc)
            except Exception as exc:
                logger.error("MarketWS: unexpected error: %s", exc)

            if not self._running:
                break

            # Exponential backoff
            logger.info(
                "MarketWS: reconnecting in %.1fs",
                self._reconnect_delay,
            )
            await asyncio.sleep(self._reconnect_delay)
            self._reconnect_delay = min(
                self._reconnect_delay * 2,
                _MAX_RECONNECT_DELAY,
            )

    async def _subscribe(self, ws: Any) -> None:
        """Send subscription message for asset IDs."""
        payload = {
            "assets_ids": self._asset_ids,
            "type": "market",
            "custom_feature_enabled": True,
        }
        await ws.send(json.dumps(payload))
        logger.info(
            "MarketWS: subscribed to %d asset(s)",
            len(self._asset_ids),
        )

    async def _heartbeat(self, ws: Any) -> None:
        """Send PING every 5 seconds. Exits on send failure."""
        while self._running:
            await asyncio.sleep(_HEARTBEAT_INTERVAL)
            try:
                await ws.send("PING")
            except Exception as exc:
                logger.warning("MarketWS: heartbeat send failed: %s", exc)
                return  # Triggers reconnect via message_loop exit

    async def _message_loop(self, ws: Any) -> None:
        """Receive and dispatch messages until disconnect."""
        async for raw_msg in ws:
            if not self._running:
                break
            try:
                self._handle_message(raw_msg)
            except Exception as exc:
                logger.warning("MarketWS: message handling error: %s", exc)

    # ------------------------------------------------------------------
    # Message handling
    # ------------------------------------------------------------------

    def _handle_message(self, raw_msg: str) -> None:
        """Parse incoming WSS message and update cache."""
        # Ignore PONG or heartbeat ack
        if raw_msg in ("PONG", "pong", ""):
            return

        try:
            data = json.loads(raw_msg)
        except (json.JSONDecodeError, TypeError):
            logger.debug("MarketWS: non-JSON message: %s", raw_msg[:100])
            return

        # Handle list of events
        events = data if isinstance(data, list) else [data]

        for event in events:
            if not isinstance(event, dict):
                continue
            event_type = event.get("event_type") or event.get("type", "")
            if event_type in ("price_change", "book"):
                self._handle_price_change(event)
            elif event_type == "tick_size_change":
                self._handle_tick_size_change(event)

    def _handle_price_change(self, event: dict[str, Any]) -> None:
        """Process a price change event into the cache."""
        asset_id = event.get("asset_id") or event.get("market")
        if not asset_id:
            return
        try:
            changes = event.get("changes", [])
            best_bid: Decimal | None = None
            best_ask: Decimal | None = None
            for change in changes:
                if isinstance(change, dict):
                    side = change.get("side", "").upper()
                    price_str = change.get("price")
                    if price_str is not None and side == "BUY":
                        best_bid = Decimal(str(price_str))
                    elif price_str is not None and side == "SELL":
                        best_ask = Decimal(str(price_str))

            # Fallback: top-level bid/ask
            if best_bid is None:
                bid_raw = event.get("best_bid") or event.get("bid")
                if bid_raw is not None:
                    best_bid = Decimal(str(bid_raw))
            if best_ask is None:
                ask_raw = event.get("best_ask") or event.get("ask")
                if ask_raw is not None:
                    best_ask = Decimal(str(ask_raw))

            if best_bid is not None or best_ask is not None:
                self._cache.on_price_update(asset_id, best_bid, best_ask)
        except (InvalidOperation, ValueError) as exc:
            logger.debug("MarketWS: price parse error: %s", exc)

    def _handle_tick_size_change(self, event: dict[str, Any]) -> None:
        """Process a tick size change event into the cache."""
        asset_id = event.get("asset_id") or event.get("market")
        new_tick = event.get("tick_size") or event.get("new_tick_size")
        if not asset_id or new_tick is None:
            return
        try:
            self._cache.on_tick_size_change(asset_id, Decimal(str(new_tick)))
        except (InvalidOperation, ValueError) as exc:
            logger.debug("MarketWS: tick_size parse error: %s", exc)
