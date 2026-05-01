"""User WebSocket client for trade and order lifecycle events.

Phase 2 deliverable: streams user-specific events (fills, order updates)
from Polymarket, triggering cache invalidation on fills.

Connection lifecycle mirrors ``MarketWebSocket``:
  1. Connect to user WSS endpoint
  2. Authenticate using API credentials
  3. Start heartbeat (5s PING) + message processing loop
  4. On disconnect/failure → reconnect with backoff
"""

from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING, Any

import websockets
from websockets.exceptions import (
    ConnectionClosed,
    ConnectionClosedError,
    ConnectionClosedOK,
)

from utils.logger import get_logger

if TYPE_CHECKING:
    from services.price_cache import PositionPriceCache

logger = get_logger("UserWS")

# Default Polymarket user WSS endpoint
DEFAULT_USER_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/user"

# Reconnect parameters
_MIN_RECONNECT_DELAY = 1.0
_MAX_RECONNECT_DELAY = 30.0
_HEARTBEAT_INTERVAL = 5  # seconds — per spec


class UserWebSocket:
    """Async WebSocket client for Polymarket user-specific events.

    Monitors fills and order lifecycle updates. On fill events,
    invalidates the relevant token's price cache entry so the
    execution path fetches fresh data.
    """

    def __init__(
        self,
        cache: PositionPriceCache,
        *,
        ws_url: str = DEFAULT_USER_WS_URL,
    ) -> None:
        self._cache = cache
        self._ws_url = ws_url
        self._api_creds: dict[str, str] = {}
        self._ws: Any = None
        self._running = False
        self._reconnect_delay = _MIN_RECONNECT_DELAY

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def connect(self, api_creds: dict[str, str]) -> None:
        """Connect, authenticate, and run heartbeat + message loop.

        Args:
            api_creds: Dict with keys needed for authentication
                       (e.g. ``api_key``, ``api_secret``, ``api_passphrase``).
        """
        self._api_creds = dict(api_creds)
        self._running = True
        logger.info("UserWS: starting")
        await self._reconnect_loop()

    async def stop(self) -> None:
        """Signal the client to disconnect gracefully."""
        self._running = False
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
        logger.info("UserWS: stopped")

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    async def _reconnect_loop(self) -> None:
        """Main reconnect loop with exponential backoff."""
        while self._running:
            try:
                async with websockets.connect(self._ws_url) as ws:
                    self._ws = ws
                    self._reconnect_delay = _MIN_RECONNECT_DELAY
                    logger.info("UserWS: connected to %s", self._ws_url)

                    # Authenticate
                    await self._authenticate(ws)

                    # Run heartbeat + message loop concurrently
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
                logger.warning("UserWS: connection closed: %s", exc)
            except OSError as exc:
                logger.warning("UserWS: network error: %s", exc)
            except Exception as exc:
                logger.error("UserWS: unexpected error: %s", exc)

            if not self._running:
                break

            # Exponential backoff
            logger.info(
                "UserWS: reconnecting in %.1fs",
                self._reconnect_delay,
            )
            await asyncio.sleep(self._reconnect_delay)
            self._reconnect_delay = min(
                self._reconnect_delay * 2,
                _MAX_RECONNECT_DELAY,
            )

    async def _authenticate(self, ws: Any) -> None:
        """Send authentication payload on connect."""
        auth_payload = {
            "type": "auth",
            **self._api_creds,
        }
        await ws.send(json.dumps(auth_payload))
        logger.info("UserWS: authentication sent")

    async def _heartbeat(self, ws: Any) -> None:
        """Send PING every 5 seconds. Exits on send failure."""
        while self._running:
            await asyncio.sleep(_HEARTBEAT_INTERVAL)
            try:
                await ws.send("PING")
            except Exception as exc:
                logger.warning("UserWS: heartbeat send failed: %s", exc)
                try:
                    await ws.close()
                except Exception:
                    pass
                return

    async def _message_loop(self, ws: Any) -> None:
        """Receive and dispatch messages until disconnect."""
        async for raw_msg in ws:
            if not self._running:
                break
            try:
                self._handle_message(raw_msg)
            except Exception as exc:
                logger.warning("UserWS: message handling error: %s", exc)

    # ------------------------------------------------------------------
    # Message handling
    # ------------------------------------------------------------------

    def _handle_message(self, raw_msg: str) -> None:
        """Parse incoming WSS message and trigger appropriate actions."""
        # Ignore PONG or heartbeat ack
        if raw_msg in ("PONG", "pong", ""):
            return

        try:
            data = json.loads(raw_msg)
        except (json.JSONDecodeError, TypeError):
            logger.debug("UserWS: non-JSON message: %s", raw_msg[:100])
            return

        events = data if isinstance(data, list) else [data]

        for event in events:
            if not isinstance(event, dict):
                continue
            event_type = event.get("event_type") or event.get("type", "")
            if event_type in ("trade", "order_fill", "fill"):
                self._handle_fill(event)
            elif event_type in ("order_update", "order"):
                self._handle_order_update(event)
            elif event_type == "auth":
                status = event.get("status", "")
                if status == "success":
                    logger.info("UserWS: authenticated successfully")
                else:
                    logger.warning("UserWS: auth response: %s", event)

    def _handle_fill(self, event: dict[str, Any]) -> None:
        """Process a fill event: invalidate cache for the token."""
        # Polymarket fill events include the asset_id / market
        token_id = (
            event.get("asset_id")
            or event.get("market")
            or event.get("token_id")
        )
        if not token_id:
            logger.debug("UserWS: fill event missing token_id: %s", event)
            return

        self._cache.invalidate_on_fill(token_id)
        logger.info(
            "UserWS: fill detected → cache invalidated for %s (side=%s)",
            token_id[:16] + "..." if len(token_id) > 16 else token_id,
            event.get("side", "?"),
        )

    def _handle_order_update(self, event: dict[str, Any]) -> None:
        """Log order lifecycle events for diagnostics."""
        order_id = event.get("order_id") or event.get("id", "?")
        status = event.get("status") or event.get("state", "?")
        logger.debug(
            "UserWS: order update: id=%s status=%s",
            order_id,
            status,
        )
