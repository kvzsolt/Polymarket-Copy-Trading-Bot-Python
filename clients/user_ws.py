"""User WebSocket client for trade and order lifecycle events.

Streams user-specific events from Polymarket:
  - fills / trades
  - order updates
  - auth responses

This client does not mutate positions directly. It emits lightweight cache
invalidation and lifecycle callbacks. Reconciliation/position settlement should
remain owned by the reconciliation service.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

import websockets
from websockets.exceptions import (
    ConnectionClosed,
    ConnectionClosedError,
    ConnectionClosedOK,
)

from services.metrics import (
    METRIC_WSS_HEARTBEAT_STALL,
    METRIC_WSS_RECONNECT,
    metrics,
)
from utils.logger import get_logger

if TYPE_CHECKING:
    from cache.market_state_store import MarketStateStore

logger = get_logger("UserWS")

DEFAULT_USER_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/user"

_MIN_RECONNECT_DELAY = 1.0
_MAX_RECONNECT_DELAY = 30.0

try:
    from clients.ws_constants import HEARTBEAT_INTERVAL
except Exception:  # pragma: no cover
    HEARTBEAT_INTERVAL = 5.0


LifecycleCallback = Callable[[], Awaitable[None] | None]
EventCallback = Callable[[dict[str, Any]], Awaitable[None] | None]


class UserWebSocket:
    """Async WebSocket client for Polymarket user-specific events.

    Readiness rule:
      - socket connected is not enough
      - auth success is required before on_authenticated fires
    """

    def __init__(
        self,
        cache: MarketStateStore,
        *,
        ws_url: str = DEFAULT_USER_WS_URL,
        on_authenticated: LifecycleCallback | None = None,
        on_disconnected: LifecycleCallback | None = None,
        on_fill: EventCallback | None = None,
        on_order_update: EventCallback | None = None,
    ) -> None:
        self._cache = cache
        self._ws_url = ws_url
        self._api_creds: dict[str, str] = {}
        self._ws: Any = None
        self._running = False
        self._authenticated = False
        self._reconnect_delay = _MIN_RECONNECT_DELAY

        self._on_authenticated = on_authenticated
        self._on_disconnected = on_disconnected
        self._on_fill = on_fill
        self._on_order_update = on_order_update

    async def connect(self, api_creds: dict[str, str]) -> None:
        self._api_creds = dict(api_creds)
        self._running = True
        logger.info("UserWS: starting")
        await self._reconnect_loop()

    async def stop(self) -> None:
        self._running = False
        self._authenticated = False

        if self._ws:
            with contextlib.suppress(Exception):
                await self._ws.close()

        logger.info("UserWS: stopped")

    async def _reconnect_loop(self) -> None:
        while self._running:
            try:
                self._authenticated = False

                async with websockets.connect(
                    self._ws_url,
                    ping_interval=None,
                    close_timeout=5,
                    max_queue=None,
                ) as ws:
                    self._ws = ws
                    self._reconnect_delay = _MIN_RECONNECT_DELAY
                    logger.info("UserWS: connected to %s", self._ws_url)

                    await self._authenticate(ws)

                    heartbeat_task = asyncio.create_task(self._heartbeat(ws))
                    try:
                        await self._message_loop(ws)
                    finally:
                        heartbeat_task.cancel()
                        with contextlib.suppress(asyncio.CancelledError):
                            await heartbeat_task

            except (ConnectionClosed, ConnectionClosedError, ConnectionClosedOK) as exc:
                logger.warning("UserWS: connection closed: %s", exc)
            except OSError as exc:
                logger.warning("UserWS: network error: %s", exc)
            except Exception as exc:
                logger.exception("UserWS: unexpected error: %s", exc)
            finally:
                self._ws = None
                self._authenticated = False
                await self._run_lifecycle_callback(self._on_disconnected)

            if not self._running:
                break

            self._inc_metric(METRIC_WSS_RECONNECT, {"stream": "user"})
            logger.info("UserWS: reconnecting in %.1fs", self._reconnect_delay)
            await asyncio.sleep(self._reconnect_delay)
            self._reconnect_delay = min(
                self._reconnect_delay * 2,
                _MAX_RECONNECT_DELAY,
            )

    async def _authenticate(self, ws: Any) -> None:
        auth_payload = {
            "type": "auth",
            **self._api_creds,
        }
        await ws.send(json.dumps(auth_payload))
        logger.info("UserWS: authentication sent")

    async def _heartbeat(self, ws: Any) -> None:
        while self._running:
            await asyncio.sleep(HEARTBEAT_INTERVAL)
            try:
                await ws.send("PING")
            except Exception as exc:
                logger.warning("UserWS: heartbeat send failed: %s", exc)
                self._inc_metric(METRIC_WSS_HEARTBEAT_STALL, {"stream": "user"})
                with contextlib.suppress(Exception):
                    await ws.close()
                return

    async def _message_loop(self, ws: Any) -> None:
        async for raw_msg in ws:
            if not self._running:
                break

            try:
                await self._handle_message(raw_msg)
            except Exception as exc:
                logger.warning("UserWS: message handling error: %s", exc)

    async def _handle_message(self, raw_msg: str | bytes) -> None:
        if isinstance(raw_msg, bytes):
            raw_msg = raw_msg.decode("utf-8", errors="replace")

        if raw_msg in ("PONG", "pong", ""):
            return

        try:
            data = json.loads(raw_msg)
        except (json.JSONDecodeError, TypeError):
            logger.debug("UserWS: non-JSON message: %s", str(raw_msg)[:100])
            return

        events = data if isinstance(data, list) else [data]

        for event in events:
            if not isinstance(event, dict):
                continue

            event_type = event.get("event_type") or event.get("type", "")

            if event_type in ("trade", "order_fill", "fill"):
                await self._handle_fill(event)
            elif event_type in ("order_update", "order"):
                await self._handle_order_update(event)
            elif event_type == "auth":
                await self._handle_auth(event)
            else:
                logger.debug("UserWS: unhandled event_type=%r event=%s", event_type, event)

    async def _handle_auth(self, event: dict[str, Any]) -> None:
        status = str(event.get("status", "")).lower()

        if status in {"success", "authenticated", "ok"}:
            if not self._authenticated:
                self._authenticated = True
                logger.info("UserWS: authenticated successfully")
                await self._run_lifecycle_callback(self._on_authenticated)
            return

        logger.warning("UserWS: auth response: %s", event)

    async def _handle_fill(self, event: dict[str, Any]) -> None:
        token_id = _token_id_from_event(event)

        if not token_id:
            logger.debug("UserWS: fill event missing token_id: %s", event)
            return

        # Own fill invalidates local market assumptions. This should not settle
        # the position directly; reconciler remains the settlement owner.
        self._cache.mark_dirty(token_id, "own_fill")
        self._cache.invalidate(token_id)

        logger.info(
            "UserWS: fill detected -> market state invalidated for %s side=%s",
            _short_token(token_id),
            event.get("side", "?"),
        )

        await self._run_event_callback(self._on_fill, event)

    async def _handle_order_update(self, event: dict[str, Any]) -> None:
        order_id = event.get("order_id") or event.get("id", "?")
        status = event.get("status") or event.get("state", "?")

        logger.debug("UserWS: order update: id=%s status=%s", order_id, status)

        await self._run_event_callback(self._on_order_update, event)

    async def _run_lifecycle_callback(
        self,
        callback: LifecycleCallback | None,
    ) -> None:
        if callback is None:
            return

        result = callback()
        if asyncio.iscoroutine(result):
            await result

    async def _run_event_callback(
        self,
        callback: EventCallback | None,
        event: dict[str, Any],
    ) -> None:
        if callback is None:
            return

        result = callback(event)
        if asyncio.iscoroutine(result):
            await result

    @staticmethod
    def _inc_metric(metric_name: str, labels: dict[str, str]) -> None:
        try:
            metrics.inc(metric_name, labels=labels)
        except Exception:
            pass


def _token_id_from_event(event: dict[str, Any]) -> str | None:
    raw = (
        event.get("asset_id")
        or event.get("asset")
        or event.get("market")
        or event.get("token_id")
        or event.get("tokenId")
    )
    if raw is None:
        return None

    token_id = str(raw).strip()
    return token_id or None


def _short_token(token_id: str) -> str:
    return token_id[:16] + "..." if len(token_id) > 16 else token_id