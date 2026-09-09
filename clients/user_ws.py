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
import orjson
import time
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
from services.execution.user_order_evidence import UserOrderEvidenceParser
from utils.logger import get_logger

if TYPE_CHECKING:
    from cache.market_state_store import MarketStateStore

logger = get_logger("UserWS")

DEFAULT_USER_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/user"

_MIN_RECONNECT_DELAY = 1.0
_MAX_RECONNECT_DELAY = 30.0

try:
    from clients.ws_constants import USER_HEARTBEAT_INTERVAL as HEARTBEAT_INTERVAL
    from clients.ws_constants import PONG_TIMEOUT_SECONDS
except Exception:  # pragma: no cover
    HEARTBEAT_INTERVAL = 5
    PONG_TIMEOUT_SECONDS = 10.0


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
        heartbeat_interval: float | None = None,
        pong_timeout_seconds: float | None = None,
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
        self._heartbeat_interval = (
            float(heartbeat_interval)
            if heartbeat_interval is not None
            else float(HEARTBEAT_INTERVAL)
        )
        self._pong_timeout_seconds = (
            float(pong_timeout_seconds)
            if pong_timeout_seconds is not None
            else float(PONG_TIMEOUT_SECONDS)
        )
        self._last_ping_monotonic: float | None = None
        self._last_pong_monotonic: float | None = None
        self._tasks: set[asyncio.Task] = set()

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
                self._last_ping_monotonic = None
                self._last_pong_monotonic = None

                async with websockets.connect(
                    self._ws_url,
                    ping_interval=None,
                    close_timeout=5,
                    max_queue=None,
                ) as ws:
                    self._ws = ws
                    self._reconnect_delay = _MIN_RECONNECT_DELAY
                    # Seed an "alive" marker at connect time so the missed-pong
                    # detector has a stable anchor even when no inbound frame
                    # ever arrives (silent server case).
                    self._last_pong_monotonic = time.monotonic()
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
            "type": "user",
            "auth": {
                "apiKey": self._api_creds.get("api_key", ""),
                "secret": self._api_creds.get("api_secret", ""),
                "passphrase": self._api_creds.get("api_passphrase", ""),
            },
        }
        await ws.send(orjson.dumps(auth_payload).decode())
        logger.info("UserWS: authentication sent")
        # Polymarket user WS has no auth-ack response; connection alive = authenticated.
        if not self._authenticated:
            self._authenticated = True
            self._fire_lifecycle_callback(self._on_authenticated)

    async def _heartbeat(self, ws: Any) -> None:
        # Mirror MarketWS: send PING first, then sleep, then check for
        # missed pong. Pre-fix the order was sleep-then-send, which left
        # _last_ping_monotonic unset for the first heartbeat_interval
        # seconds; during that window _is_pong_missed always returned
        # False and a silent-server scenario went undetected.
        while self._running:
            try:
                await ws.send("PING")
                self._last_ping_monotonic = time.monotonic()
            except Exception as exc:
                logger.warning("UserWS: heartbeat send failed: %s", exc)
                self._inc_metric(METRIC_WSS_HEARTBEAT_STALL, {"stream": "user"})
                with contextlib.suppress(Exception):
                    await ws.close()
                return

            await asyncio.sleep(self._heartbeat_interval)

            # Missed-pong detector: if a PING was sent and no inbound frame
            # arrived within the pong-timeout window, close the socket and
            # let the reconnect loop bring up a fresh authenticated stream.
            if self._pong_timeout_seconds > 0 and self._is_pong_missed():
                logger.warning(
                    "UserWS: pong timeout (no inbound activity within %.1fs) — reconnecting",
                    self._pong_timeout_seconds,
                )
                self._inc_metric(METRIC_WSS_HEARTBEAT_STALL, {"stream": "user"})
                with contextlib.suppress(Exception):
                    await ws.close()
                return

    def _is_pong_missed(self) -> bool:
        # Pre-fix the formula was ``last_ping - last_pong`` which goes
        # negative whenever an inbound frame arrives between two PING
        # sends. Correct formula: ``now - last_alive`` — any inbound
        # activity within the timeout keeps the connection healthy.
        if self._last_ping_monotonic is None:
            return False
        last_alive = self._last_pong_monotonic
        if last_alive is None:
            elapsed = time.monotonic() - self._last_ping_monotonic
        else:
            elapsed = time.monotonic() - last_alive
        return elapsed > self._pong_timeout_seconds

    async def _message_loop(self, ws: Any) -> None:
        async for raw_msg in ws:
            if not self._running:
                break

            msg = (
                raw_msg.decode("utf-8", errors="replace") if isinstance(raw_msg, bytes) else raw_msg
            )
            # Any inbound frame proves the pipe is alive; treat as a pong
            # for the missed-pong detector.
            self._last_pong_monotonic = time.monotonic()
            if msg in ("PING", "ping"):
                with contextlib.suppress(Exception):
                    await ws.send("PONG")
                continue

            try:
                self._handle_message(msg)
            except Exception as exc:
                logger.warning("UserWS: message handling error: %s", exc)

    def _handle_message(self, raw_msg: str) -> None:
        if raw_msg in ("PONG", "pong", ""):
            return

        try:
            data = orjson.loads(raw_msg)
        except (orjson.JSONDecodeError, TypeError):
            logger.debug("UserWS: non-JSON message rejected")
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
                self._handle_auth(event)
            else:
                logger.debug("UserWS: unhandled event_type=%r event=%s", event_type, event)

    def _handle_auth(self, event: dict[str, Any]) -> None:
        status = str(event.get("status", "")).lower()

        if status in {"success", "authenticated", "ok"}:
            if not self._authenticated:
                self._authenticated = True
                logger.info("UserWS: authenticated successfully")
                self._fire_lifecycle_callback(self._on_authenticated)
            return

        logger.warning("UserWS: auth response: %s", event)

    def _handle_fill(self, event: dict[str, Any]) -> None:
        token_id = _token_id_from_event(event)

        if not token_id:
            logger.debug("UserWS: fill event missing token_id: %s", event)
            return

        # Own fill invalidates local market assumptions. Evict removes the data
        # so get_quote/get_book return None until WSS delivers fresh state.
        # Reconciler remains the settlement owner — this is cache hygiene only.
        self._cache.evict(token_id)

        logger.info(
            "UserWS: fill detected -> market state invalidated for %s side=%s",
            _short_token(token_id),
            event.get("side", "?"),
        )

        # Lifecycle log — classify the WSS trade status. UserWebSocket never
        # mutates positions or reservations; the reconciler converges state.
        _emit_lifecycle_for_event(event, token_id=token_id)

        self._fire_event_callback(self._on_fill, event)

    def _handle_order_update(self, event: dict[str, Any]) -> None:
        order_id = event.get("order_id") or event.get("id", "?")
        status = event.get("status") or event.get("state", "?")

        logger.debug("UserWS: order update: id=%s status=%s", order_id, status)

        token_id = _token_id_from_event(event)
        _emit_lifecycle_for_event(event, token_id=token_id)

        self._fire_event_callback(self._on_order_update, event)

    def _fire_lifecycle_callback(self, callback: LifecycleCallback | None) -> None:
        if callback is None:
            return
        result = callback()
        if asyncio.iscoroutine(result):
            self._spawn(result)

    def _fire_event_callback(
        self,
        callback: EventCallback | None,
        event: dict[str, Any],
    ) -> None:
        if callback is None:
            return
        result = callback(event)
        if asyncio.iscoroutine(result):
            self._spawn(result)

    def _spawn(self, coro: Any) -> None:
        # Hold a strong reference so background tasks aren't GC'd mid-flight.
        try:
            task = asyncio.get_running_loop().create_task(coro)
        except RuntimeError:
            coro.close()
            return
        self._tasks.add(task)

        def _on_done(done: asyncio.Task) -> None:
            self._tasks.discard(done)
            if done.cancelled():
                return
            try:
                exc = done.exception()
            except Exception as callback_exc:
                logger.warning(
                    "UserWS: callback task inspection failed: %s",
                    callback_exc,
                )
                return
            if exc is not None:
                logger.warning(
                    "UserWS: callback task failed: %s: %s",
                    type(exc).__name__,
                    exc,
                )

        task.add_done_callback(_on_done)

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


# Maps Polymarket WSS trade statuses → canonical lifecycle status strings.
# Reservation/state mutation belongs to the reconciler; UserWebSocket only
# emits the lifecycle log.
_WSS_TRADE_STATUS_MAP: dict[str, str] = {
    "MATCHED": "matched",
    "MINED": "mined",
    "CONFIRMED": "confirmed",
    "RETRYING": "retrying",
    "FAILED": "failed",
}


def _emit_lifecycle_for_event(event: dict[str, Any], *, token_id: str | None) -> None:
    """Classify a WSS event status and emit a lifecycle log if recognised."""
    raw = (
        event.get("trade_status")
        or event.get("tradeStatus")
        or event.get("status")
        or event.get("state")
        or ""
    )
    canonical = _WSS_TRADE_STATUS_MAP.get(str(raw).strip().upper())
    if canonical is None:
        return
    order_id = UserOrderEvidenceParser().order_id_for(event)
    # WSS user 'trade' events carry a stable trade UUID in ``id`` (Context7:
    # Polymarket User Channel trade message). Surface it for correlation across
    # the MATCHED → MINED → CONFIRMED status churn emitted for a single trade.
    trade_id = event.get("id") or event.get("trade_id")
    side = str(event.get("side") or event.get("makerSide") or "").upper() or "?"
    size_raw = event.get("size") or event.get("matched_size") or event.get("filled_size")
    price_raw = event.get("price") or event.get("avg_price") or event.get("average_price")
    condition_id = event.get("condition_id") or event.get("conditionId") or event.get("market")
    # WSS trade events carry no tx hash. Only the REST getTrades Trade does,
    # under the snake_case ``transaction_hash`` — check it first so a
    # REST-sourced event is not silently dropped (the camelCase variant the
    # code previously checked never matched the real field name).
    tx_hash = (
        event.get("transaction_hash")
        or event.get("tx_hash")
        or event.get("txHash")
        or event.get("transactionHash")
    )

    def _coerce_float(v: Any) -> float | None:
        try:
            return float(v) if v is not None else None
        except (TypeError, ValueError):
            return None

    try:
        from utils.logger import log_order_lifecycle as _log_lc

        size = _coerce_float(size_raw)
        price = _coerce_float(price_raw)
        usdc = None
        if size is not None and price is not None:
            usdc = size * price
        _log_lc(
            logger,
            status=canonical,
            side=side,
            condition_id=str(condition_id) if condition_id else None,
            asset=token_id,
            order_id=str(order_id) if order_id else None,
            size=size,
            price=price,
            usdc_value=usdc,
            venue_status=str(raw).upper(),
            tx_hash=str(tx_hash) if tx_hash else None,
            meta={"trade_id": str(trade_id)} if trade_id else None,
        )
    except Exception:
        # Logging never breaks the WSS loop.
        pass


def _token_id_from_event(event: dict[str, Any]) -> str | None:
    # ``event["market"]`` is a CONDITION id in Polymarket's protocol, not
    # a token / asset id. Pre-fix the fallback chain accepted it and the
    # cache eviction at the call sites would silently miss (wrong key) —
    # stale book/quote data could survive own-fill events. Drop it.
    raw = (
        event.get("asset_id") or event.get("asset") or event.get("token_id") or event.get("tokenId")
    )
    if raw is None:
        return None

    token_id = str(raw).strip()
    return token_id or None


def _short_token(token_id: str) -> str:
    return token_id[:16] + "..." if len(token_id) > 16 else token_id
