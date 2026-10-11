"""WSS-backed Polymarket market data client.

Consumes the market WebSocket stream and updates a MarketStateStore.
Intended production rule: execution reads market state only from this
WSS-backed store and fails closed if state is stale, dirty, missing,
or resolved.

Polymarket market/user docs specify a literal "PING" heartbeat every 5 seconds,
paired with a 10-second pong-timeout window; missing the window closes the
socket so the reconnect loop performs a fresh bootstrap + resync.
"""

from __future__ import annotations

import asyncio
import contextlib
import orjson
import time
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any, Callable, Coroutine, Iterable

import websockets
from websockets.exceptions import ConnectionClosed, ConnectionClosedError, ConnectionClosedOK

from services.metrics import METRIC_WSS_HEARTBEAT_STALL, METRIC_WSS_RECONNECT, metrics
from clients.market_book_decoder import MarketBookDecoder
from utils.logger import get_logger

try:
    from clients.ws_constants import MARKET_HEARTBEAT_INTERVAL, PONG_TIMEOUT_SECONDS
except Exception:  # pragma: no cover
    MARKET_HEARTBEAT_INTERVAL = 5
    PONG_TIMEOUT_SECONDS = 10.0

HEARTBEAT_INTERVAL = MARKET_HEARTBEAT_INTERVAL

if TYPE_CHECKING:
    from cache.market_state_store import MarketStateStore
    from services.market_evidence import MarketEvidenceTap

logger = get_logger("MarketWS")

DEFAULT_MARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
_MIN_RECONNECT_DELAY = 1.0
_MAX_RECONNECT_DELAY = 30.0

RestBootstrap = Callable[[list[str]], Coroutine[Any, Any, None]]


class MarketWebSocket:
    """Polymarket market WebSocket client.

    Handles:
      - book: authoritative full-book snapshot
      - price_change: incremental level update; size 0 deletes a level
      - last_trade_price: observability only, not executable price
      - tick_size_change: update tick and invalidate prepared sizing
      - best_bid_ask: top-of-book update
      - new_market: metadata/discovery hook
      - market_resolved: mark asset/market non-tradable
    """

    def __init__(
        self,
        cache: MarketStateStore,
        *,
        ws_url: str = DEFAULT_MARKET_WS_URL,
        rest_bootstrap: RestBootstrap | None = None,
        heartbeat_interval: float | None = None,
        pong_timeout_seconds: float | None = None,
        reconnect_min_delay: float = _MIN_RECONNECT_DELAY,
        reconnect_max_delay: float = _MAX_RECONNECT_DELAY,
        on_connected: Callable[[], Coroutine[Any, Any, None]] | None = None,
        on_disconnected: Callable[[], Coroutine[Any, Any, None]] | None = None,
        on_state_change: Callable[[str, str], Coroutine[Any, Any, None]] | None = None,
        on_quote_invalidated: Callable[[str], None] | None = None,
        max_pending_callbacks: int = 128,
        evidence: MarketEvidenceTap | None = None,
    ) -> None:
        if (
            isinstance(max_pending_callbacks, bool)
            or not isinstance(max_pending_callbacks, int)
            or max_pending_callbacks <= 0
        ):
            raise ValueError("max_pending_callbacks must be a positive integer")
        self._cache = cache
        self._ws_url = ws_url
        self._rest_bootstrap = rest_bootstrap
        self._asset_ids: list[str] = []
        self._ws: Any = None
        self._running = False
        self._reconnect_delay = reconnect_min_delay
        self._reconnect_min_delay = reconnect_min_delay
        self._reconnect_max_delay = reconnect_max_delay
        self._heartbeat_interval = (
            float(heartbeat_interval)
            if heartbeat_interval is not None
            else float(MARKET_HEARTBEAT_INTERVAL)
        )
        self._pong_timeout_seconds = (
            float(pong_timeout_seconds)
            if pong_timeout_seconds is not None
            else float(PONG_TIMEOUT_SECONDS)
        )
        self._last_ping_monotonic: float | None = None
        self._last_pong_monotonic: float | None = None
        self._bootstrap_in_flight = False
        self._on_connected = on_connected
        self._on_disconnected = on_disconnected
        self._on_state_change = on_state_change
        self._on_quote_invalidated = on_quote_invalidated
        self._tasks: set[asyncio.Task[None]] = set()
        self._max_pending_callbacks = max_pending_callbacks
        self._closed = False
        self._evidence = evidence

    async def connect(self, asset_ids: list[str]) -> None:
        """Open the market WebSocket connection."""
        self._asset_ids = _dedupe_asset_ids(asset_ids)
        self._closed = False
        self._running = True
        logger.info("MarketWS: starting for %d asset(s)", len(self._asset_ids))
        await self._reconnect_loop()

    async def stop(self) -> None:
        """Stop the market WebSocket client and release its tasks."""
        self._running = False
        self._closed = True
        if self._ws is not None:
            with contextlib.suppress(Exception):
                await self._ws.close()
        self._mark_all_stale("stopped")
        tasks = tuple(self._tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        logger.info("MarketWS: stopped")

    async def subscribe(self, asset_ids: list[str]) -> None:
        """Subscribe the connection to the supplied market tokens."""
        new_ids = [a for a in _dedupe_asset_ids(asset_ids) if a not in self._asset_ids]
        if not new_ids:
            return
        self._asset_ids.extend(new_ids)
        if self._ws is not None:
            await self._ws.send(
                orjson.dumps(
                    {
                        "assets_ids": new_ids,
                        "operation": "subscribe",
                        "level": 2,
                        "custom_feature_enabled": True,
                    }
                ).decode()
            )
            logger.info("MarketWS: dynamically subscribed to %d asset(s)", len(new_ids))

    async def unsubscribe(self, asset_ids: list[str]) -> None:
        """Remove the supplied market-token subscriptions."""
        remove_ids = _dedupe_asset_ids(asset_ids)
        if not remove_ids:
            return
        remove_set = set(remove_ids)
        self._asset_ids = [a for a in self._asset_ids if a not in remove_set]
        if self._ws is not None:
            await self._ws.send(
                orjson.dumps(
                    {
                        "assets_ids": remove_ids,
                        "operation": "unsubscribe",
                    }
                ).decode()
            )
            logger.info("MarketWS: dynamically unsubscribed from %d asset(s)", len(remove_ids))
        for asset_id in remove_ids:
            self._mark_asset_stale(asset_id, "unsubscribed")

    async def _reconnect_loop(self) -> None:
        while self._running:
            try:
                self._mark_all_stale("reconnecting")
                self._last_ping_monotonic = None
                self._last_pong_monotonic = None

                if self._rest_bootstrap and self._asset_ids:
                    await self._run_rest_bootstrap()

                async with websockets.connect(
                    self._ws_url,
                    ping_interval=None,  # Polymarket uses literal text PING
                    close_timeout=5,
                    max_queue=16,
                    max_size=2**20,
                ) as ws:
                    self._ws = ws
                    self._reconnect_delay = self._reconnect_min_delay
                    # Seed an "alive" marker at connect time so the missed-pong
                    # detector has a stable anchor even when no inbound frame
                    # ever arrives (silent server case).
                    self._last_pong_monotonic = time.monotonic()
                    logger.info("MarketWS: connected")

                    await self._subscribe(ws)

                    if self._on_connected is not None:
                        try:
                            await self._on_connected()
                        except Exception as exc:
                            logger.warning(
                                "MarketWS: on_connected callback failed | error_type=%s",
                                type(exc).__name__,
                            )

                    heartbeat_task = asyncio.create_task(self._heartbeat(ws))
                    try:
                        await self._message_loop(ws)
                    finally:
                        heartbeat_task.cancel()
                        with contextlib.suppress(asyncio.CancelledError):
                            await heartbeat_task

            except (ConnectionClosed, ConnectionClosedError, ConnectionClosedOK) as exc:
                logger.warning("MarketWS: connection closed | error_type=%s", type(exc).__name__)
            except OSError as exc:
                logger.warning("MarketWS: network error | error_type=%s", type(exc).__name__)
            except Exception as exc:
                logger.warning("MarketWS: unexpected error | error_type=%s", type(exc).__name__)
            finally:
                self._ws = None
                if self._on_disconnected is not None:
                    try:
                        await self._on_disconnected()
                    except Exception as exc:
                        logger.warning(
                            "MarketWS: on_disconnected callback failed | error_type=%s",
                            type(exc).__name__,
                        )
                self._mark_all_stale("disconnected")

            if not self._running:
                break

            self._inc_metric(METRIC_WSS_RECONNECT, {"stream": "market"})
            logger.info("MarketWS: reconnecting in %.1fs", self._reconnect_delay)
            await asyncio.sleep(self._reconnect_delay)
            self._reconnect_delay = min(self._reconnect_delay * 2, self._reconnect_max_delay)

    async def _run_rest_bootstrap(self) -> None:
        # Dedupe overlapping resync triggers (e.g. cascading disconnects); a
        # single in-flight bootstrap is enough to seed the next connect cycle.
        if self._bootstrap_in_flight:
            logger.info(
                "MarketWS: REST bootstrap already in flight; skipping duplicate resync",
            )
            return
        self._bootstrap_in_flight = True
        try:
            logger.info("MarketWS: REST bootstrap for %d asset(s)", len(self._asset_ids))
            self._call_cache_optional("invalidate_all")
            rest_bootstrap = self._rest_bootstrap
            if rest_bootstrap is None:
                self._mark_all_dirty("rest_bootstrap_unavailable")
                return
            await rest_bootstrap(self._asset_ids)
        except Exception as exc:
            logger.warning("MarketWS: REST bootstrap failed: %s", exc)
            self._mark_all_dirty("rest_bootstrap_failed")
        finally:
            self._bootstrap_in_flight = False

    async def _subscribe(self, ws: Any) -> None:
        payload = {
            "type": "market",
            "assets_ids": self._asset_ids,
            "initial_dump": True,
            "level": 2,
            "custom_feature_enabled": True,
        }
        await ws.send(orjson.dumps(payload).decode())
        logger.info("MarketWS: subscribed to %d asset(s)", len(self._asset_ids))

    async def _heartbeat(self, ws: Any) -> None:
        # First PING is sent immediately on connect, not after the interval.
        # This records connection liveness inside the startup window so the
        # market-data freshness gate does not reject a freshly-connected
        # stream that has not yet had a full heartbeat cycle.
        #
        # The missed-pong detector runs AFTER the sleep (i.e. before sending
        # the next PING) so a silent server is detected: comparing the
        # most recent PING send against the most recent inbound activity
        # (seeded at connect time) lets the gap grow each iteration.
        while self._running:
            try:
                await ws.send("PING")
                now = time.monotonic()
                self._last_ping_monotonic = now
                self._call_cache_optional("on_market_ws_heartbeat", now)
            except Exception as exc:
                logger.warning("MarketWS: heartbeat send failed: %s", exc)
                self._inc_metric(METRIC_WSS_HEARTBEAT_STALL, {"stream": "market"})
                with contextlib.suppress(Exception):
                    await ws.close()
                return

            await asyncio.sleep(self._heartbeat_interval)

            # Missed-pong detector: if a PING was sent and no PONG (or any
            # inbound frame, which proves the pipe is alive) arrived within
            # the pong-timeout window, close the socket and let the
            # reconnect loop trigger a fresh bootstrap + resync.
            if self._pong_timeout_seconds > 0 and self._is_pong_missed():
                logger.warning(
                    "MarketWS: pong timeout (no inbound activity within %.1fs) — reconnecting",
                    self._pong_timeout_seconds,
                )
                self._inc_metric(METRIC_WSS_HEARTBEAT_STALL, {"stream": "market"})
                with contextlib.suppress(Exception):
                    await ws.close()
                return

    def _is_pong_missed(self) -> bool:
        # Silent-server detection: compare NOW against the most recent
        # inbound activity. Pre-fix the formula was
        # ``last_ping - last_pong`` which produced a negative or
        # falsely-small value whenever an inbound frame arrived between
        # two PING sends, causing the detector to miss real stalls and
        # only catch them on the next PING cycle (15s+ latency for a
        # 10s threshold). The correct formula is ``now - last_alive``:
        # any inbound activity within the timeout window keeps the
        # connection healthy, anything beyond it trips reconnect.
        if self._last_ping_monotonic is None:
            return False
        last_alive = self._last_pong_monotonic
        if last_alive is None:
            # No inbound observed yet — fall back to time-since-last-ping
            # so a stuck loop still detects the stall.
            elapsed = time.monotonic() - self._last_ping_monotonic
        else:
            elapsed = time.monotonic() - last_alive
        return elapsed > self._pong_timeout_seconds

    async def _message_loop(self, ws: Any) -> None:
        async for raw_msg in ws:
            if not self._running:
                break
            if self._evidence is not None:
                self._evidence.observe(raw_msg)
            msg = (
                raw_msg.decode("utf-8", errors="replace") if isinstance(raw_msg, bytes) else raw_msg
            )
            # Any inbound frame proves the pipe is alive; treat it as a pong
            # both for the in-loop missed-pong detector AND for the cache
            # liveness anchor (P1-d), so data frames — not only literal PONG
            # frames — keep the stream marked live and a server that stops
            # sending anything trips staleness on the inbound clock.
            now = time.monotonic()
            self._last_pong_monotonic = now
            self._call_cache_optional("on_market_ws_pong", now)
            if msg in ("PING", "ping"):
                with contextlib.suppress(Exception):
                    await ws.send("PONG")
                continue
            try:
                self._handle_message(msg)
            except Exception as exc:
                logger.warning("MarketWS: message handling error: %s", exc)

    def _handle_message(self, raw_msg: str) -> None:
        if raw_msg in ("PONG", "pong", ""):
            now = time.monotonic()
            self._last_pong_monotonic = now
            self._call_cache_optional("on_market_ws_pong", now)
            return

        try:
            data = orjson.loads(raw_msg)
        except (orjson.JSONDecodeError, TypeError):
            logger.debug("MarketWS: non-JSON message rejected")
            self._mark_all_stale("invalid_frame")
            return

        events = data if isinstance(data, list) else [data]
        for event in events:
            if isinstance(event, dict):
                self._dispatch_event(event)

    def _dispatch_event(self, event: dict[str, Any]) -> None:
        event_type = str(event.get("event_type") or event.get("type") or "").strip()

        if event_type == "book":
            self._handle_book(event)
        elif event_type == "price_change":
            self._handle_price_change(event)
        elif event_type == "last_trade_price":
            self._handle_last_trade_price(event)
        elif event_type == "tick_size_change":
            self._handle_tick_size_change(event)
        elif event_type == "best_bid_ask":
            self._handle_best_bid_ask(event)
        elif event_type == "new_market":
            self._handle_new_market(event)
        elif event_type == "market_resolved":
            self._handle_market_resolved(event)
        else:
            logger.debug(
                "market_wss_event_ignored | eventType=%r rawKeys=%s",
                event_type,
                list(event.keys()),
            )

    def _handle_book(self, event: dict[str, Any]) -> None:
        asset_id = _asset_id(event)
        if not asset_id:
            self._mark_all_stale("unattributable_book_snapshot")
            return

        try:
            bids = MarketBookDecoder.levels(event.get("bids", event.get("buys", [])))
            asks = MarketBookDecoder.levels(event.get("asks", event.get("sells", [])))
        except ValueError:
            self._mark_asset_stale(asset_id, "invalid_book_snapshot")
            return

        if not bids and not asks:
            self._mark_asset_stale(asset_id, "empty_book_snapshot")
            logger.debug(
                "market_wss_event_ignored | type=book assetId=%s reason=empty_book",
                asset_id,
            )
            self._notify_state_change(asset_id, "book")
            return

        condition_id = str(event.get("market") or "").strip() or None
        prior_book_source = self._get_book_source(asset_id)
        replaced = self._replace_book_snapshot(
            asset_id, bids, asks, event, condition_id=condition_id
        )
        if not replaced:
            self._mark_asset_stale(asset_id, "book_snapshot_not_replaced")
            return
        if replaced and prior_book_source == "rest_bootstrap":
            logger.info(
                "market_data_rest_bootstrap_wss_override | token_id=%s event_type=book",
                asset_id,
            )

        best_bid = max(bids) if bids else _to_decimal(event.get("best_bid") or event.get("bid"))
        best_ask = min(asks) if asks else _to_decimal(event.get("best_ask") or event.get("ask"))

        if best_bid is not None or best_ask is not None:
            self._on_price_update(asset_id, best_bid, best_ask)

        tick = _to_decimal(event.get("tick_size") or event.get("minimum_tick_size"))
        if tick is not None:
            if not self._call_cache_optional("update_tick_size_from_book", asset_id, tick):
                self._on_tick_size_change(asset_id, tick)

        ltp = _to_decimal(event.get("last_trade_price"))
        if ltp is not None:
            self._call_cache_optional("on_last_trade_price", asset_id, ltp, event)

        if not replaced:
            self._mark_asset_dirty(asset_id, "book_snapshot_not_replaced")

        logger.debug(
            "market_wss_book_received | assetId=%s conditionId=%s bidCount=%d askCount=%d"
            " bestBid=%s bestAsk=%s hash=%s ts=%s",
            asset_id,
            condition_id,
            len(bids),
            len(asks),
            best_bid,
            best_ask,
            event.get("hash"),
            event.get("timestamp"),
        )
        self._notify_state_change(asset_id, "book")

    def _handle_price_change(self, event: dict[str, Any]) -> None:
        changes = event.get("price_changes") or event.get("changes") or []
        try:
            decoded = MarketBookDecoder.changes(changes)
        except ValueError:
            affected = (
                {asset for row in changes if isinstance(row, dict) and (asset := _asset_id(row))}
                if isinstance(changes, list)
                else set(self._asset_ids)
            )
            if not affected or (
                isinstance(changes, list)
                and any(not isinstance(row, dict) or not _asset_id(row) for row in changes)
            ):
                self._mark_all_stale("unattributable_invalid_book_delta")
                return
            for asset in affected:
                self._mark_asset_stale(asset, "invalid_book_delta_batch")
            return
        if not isinstance(changes, list):
            logger.debug(
                "market_wss_event_ignored | type=price_change reason=price_changes_not_list"
            )
            return

        condition_id = str(event.get("market") or "").strip() or None
        touched: set[str] = set()

        batch_hook = getattr(self._cache, "apply_book_deltas", None)
        applied_batch = False
        if batch_hook is not None:
            try:
                result = batch_hook(
                    tuple(
                        (change.token_id, change.side, change.price, change.size)
                        for change in decoded
                    )
                )
                if asyncio.iscoroutine(result):
                    result.close()
                    raise TypeError("Critical cache updates must complete synchronously")
                applied_batch = True
            except Exception as exc:
                logger.warning("market_book_batch_failed | error_type=%s", type(exc).__name__)
                for asset in {change.token_id for change in decoded}:
                    self._mark_asset_stale(asset, "book_batch_failed")
                return

        for change in changes:
            if not isinstance(change, dict):
                continue
            asset_id = _asset_id(change)
            if not asset_id:
                continue
            side = _normalize_side(change.get("side"))
            price = _to_decimal(change.get("price"))
            size = _to_decimal(change.get("size"))
            if side is None or price is None or size is None:
                logger.debug(
                    "market_wss_event_ignored | type=price_change_item assetId=%s"
                    " reason=missing_side_or_price_or_size",
                    asset_id,
                )
                continue

            prior_source = self._get_book_source(asset_id)
            if not applied_batch:
                self._apply_book_delta(asset_id, side, price, size)
            if prior_source == "rest_bootstrap":
                logger.info(
                    "market_data_rest_bootstrap_wss_override | token_id=%s event_type=price_change",
                    asset_id,
                )
            touched.add(asset_id)

            best_bid = _to_decimal(change.get("best_bid"))
            best_ask = _to_decimal(change.get("best_ask"))
            if best_bid is not None or best_ask is not None:
                self._on_price_update(asset_id, best_bid, best_ask)

            self._touch_asset(asset_id, "price_change")
            self._notify_state_change(asset_id, "price_change")

        logger.debug(
            "market_wss_price_change_received | conditionId=%s changeCount=%d assetIds=%s ts=%s",
            condition_id,
            len(changes),
            list(touched),
            event.get("timestamp"),
        )

    def _handle_best_bid_ask(self, event: dict[str, Any]) -> None:
        asset_id = _asset_id(event)
        if not asset_id:
            self._mark_all_stale("unattributable_book_quote")
            return
        try:
            best_bid = MarketBookDecoder.optional_quote(event, "best_bid", "bid", "best_bid_price")
            best_ask = MarketBookDecoder.optional_quote(event, "best_ask", "ask", "best_ask_price")
        except ValueError:
            self._mark_asset_stale(asset_id, "invalid_book_quote")
            return
        if best_bid is None and best_ask is None:
            return
        self._on_price_update(asset_id, best_bid, best_ask)
        self._call_cache_optional("on_best_bid_ask", asset_id, best_bid, best_ask, event)
        self._touch_asset(asset_id, "best_bid_ask")
        logger.debug(
            "market_wss_best_bid_ask_received | assetId=%s conditionId=%s"
            " bestBid=%s bestAsk=%s spread=%s ts=%s",
            asset_id,
            str(event.get("market") or "").strip() or None,
            best_bid,
            best_ask,
            event.get("spread"),
            event.get("timestamp"),
        )
        self._notify_state_change(asset_id, "best_bid_ask")

    def _handle_last_trade_price(self, event: dict[str, Any]) -> None:
        asset_id = _asset_id(event)
        if not asset_id:
            return
        price = _to_decimal(
            event.get("price") or event.get("last_trade_price") or event.get("last_price")
        )
        if price is None:
            return
        self._call_cache_optional("on_last_trade_price", asset_id, price, event)
        self._touch_asset(asset_id, "last_trade_price")

    def _handle_tick_size_change(self, event: dict[str, Any]) -> None:
        asset_id = _asset_id(event)
        tick = _to_decimal(event.get("new_tick_size") or event.get("tick_size"))
        if not asset_id or tick is None:
            return
        self._on_tick_size_change(asset_id, tick)
        self._mark_asset_dirty(asset_id, "tick_size_change")
        self._call_cache_optional("invalidate_sizing_decision", asset_id)
        self._notify_state_change(asset_id, "tick_size_change")

    def _handle_new_market(self, event: dict[str, Any]) -> None:
        self._call_cache_optional("on_new_market", event)
        asset_id = _asset_id(event)
        if asset_id:
            self._touch_asset(asset_id, "new_market")

    def _handle_market_resolved(self, event: dict[str, Any]) -> None:
        assets_ids: list[str] = []
        raw_list = event.get("assets_ids") or []
        if isinstance(raw_list, str):
            raw_list = [raw_list]
        for item in raw_list:
            s = str(item).strip()
            if s:
                assets_ids.append(s)

        single = _asset_id(event)
        if single and single not in assets_ids:
            assets_ids.append(single)

        condition_id = str(event.get("market") or "").strip() or None
        for asset_id in assets_ids:
            if not self._call_cache_optional("mark_resolved", asset_id, condition_id):
                self._call_cache_optional("on_market_resolved", asset_id, event)
                self._mark_asset_dirty(asset_id, "market_resolved")
                self._mark_asset_stale(asset_id, "market_resolved")
            self._notify_state_change(asset_id, "market_resolved")

        if not assets_ids:
            self._call_cache_optional("on_market_resolved", event)

    # Cache adapter methods -------------------------------------------------

    def _mark_all_stale(self, reason: str) -> None:
        self._call_cache_optional("mark_all_stale", reason)
        for asset_id in self._asset_ids:
            self._mark_asset_stale(asset_id, reason)

    def _mark_all_dirty(self, reason: str) -> None:
        self._call_cache_optional("mark_all_dirty", reason)
        for asset_id in self._asset_ids:
            self._mark_asset_dirty(asset_id, reason)

    def _mark_asset_stale(self, asset_id: str, reason: str) -> None:
        if not self._call_cache_optional("mark_stale", asset_id, reason):
            self._call_cache_optional("invalidate", asset_id)
        if self._on_quote_invalidated is not None:
            try:
                self._on_quote_invalidated(asset_id)
            except Exception as exc:
                logger.error("market_quote_invalidation_failed | error_type=%s", type(exc).__name__)
        self._notify_state_change(asset_id, "book_invalidated")

    def _mark_asset_dirty(self, asset_id: str, reason: str) -> None:
        self._call_cache_optional("mark_dirty", asset_id, reason)

    def _mark_asset_fresh(self, asset_id: str, source: str) -> None:
        self._call_cache_optional("mark_fresh", asset_id, source, time.monotonic())

    def _touch_asset(self, asset_id: str, source: str) -> None:
        self._call_cache_optional("touch", asset_id, source, time.monotonic())

    def _replace_book_snapshot(
        self,
        asset_id: str,
        bids: dict[Decimal, Decimal],
        asks: dict[Decimal, Decimal],
        raw_event: dict[str, Any],
        *,
        condition_id: str | None = None,
    ) -> bool:
        event_with_meta = dict(raw_event)
        if condition_id:
            event_with_meta["condition_id"] = condition_id
        for method_name in ("replace_book_snapshot", "on_book_snapshot"):
            method = getattr(self._cache, method_name, None)
            if method is None:
                continue
            try:
                result = method(asset_id, bids, asks, event_with_meta)
                if asyncio.iscoroutine(result):
                    result.close()
                    raise TypeError("Critical cache updates must complete synchronously")
                return True
            except Exception as exc:
                logger.warning("market_book_snapshot_failed | error_type=%s", type(exc).__name__)
                return False
        logger.warning(
            "MarketWS: cache lacks replace_book_snapshot/on_book_snapshot; "
            "book for asset %s cannot be authoritatively replaced",
            asset_id,
        )
        return False

    def _get_book_source(self, asset_id: str) -> str | None:
        """Return current book_source for *asset_id* from cache, or None."""
        try:
            return getattr(self._cache, "book_source_of", lambda _: None)(asset_id)
        except Exception:
            return None

    def _apply_book_delta(self, asset_id: str, side: str, price: Decimal, size: Decimal) -> None:
        if not self._call_cache_optional("apply_book_delta", asset_id, side, price, size):
            logger.debug("MarketWS: cache lacks apply_book_delta for asset %s", asset_id)

    def _on_price_update(
        self, asset_id: str, best_bid: Decimal | None, best_ask: Decimal | None
    ) -> None:
        self._call_cache_optional("on_price_update", asset_id, best_bid, best_ask)

    def _on_tick_size_change(self, asset_id: str, tick: Decimal) -> None:
        self._call_cache_optional("on_tick_size_change", asset_id, tick)

    def _notify_state_change(self, asset_id: str, event_type: str) -> None:
        if self._on_state_change is None or not asset_id:
            return
        try:
            coro = self._on_state_change(asset_id, event_type)
            if asyncio.iscoroutine(coro):
                self._spawn(coro)
        except Exception as exc:
            logger.warning(
                "MarketWS: on_state_change callback failed | error_type=%s",
                type(exc).__name__,
            )

    def _call_cache_optional(self, method_name: str, *args: Any) -> bool:
        method = getattr(self._cache, method_name, None)
        if method is None:
            return False
        try:
            result = method(*args)
            if asyncio.iscoroutine(result):
                self._spawn(result)
            return True
        except Exception as exc:
            logger.warning(
                "MarketWS: cache hook %s failed | error_type=%s", method_name, type(exc).__name__
            )
            return False

    def _spawn(self, coro: Coroutine[Any, Any, None]) -> asyncio.Task[None] | None:
        if self._closed or len(self._tasks) >= self._max_pending_callbacks:
            coro.close()
            self._inc_metric(
                "market_ws_callback_dropped_total",
                {"reason": "closed" if self._closed else "capacity"},
            )
            return None
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._callback_completed)
        return task

    def _callback_completed(self, task: asyncio.Task[None]) -> None:
        self._tasks.discard(task)
        if task.cancelled():
            return
        error = task.exception()
        if error is not None:
            logger.warning("market_ws_callback_failed | error_type=%s", type(error).__name__)
            self._inc_metric("market_ws_callback_failed_total", {})

    @staticmethod
    def _inc_metric(metric_name: str, labels: dict[str, str]) -> None:
        try:
            metrics.inc(metric_name, labels=labels)
        except Exception:
            pass


def _dedupe_asset_ids(asset_ids: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for raw in asset_ids:
        asset_id = str(raw).strip()
        if asset_id and asset_id not in seen:
            seen.add(asset_id)
            result.append(asset_id)
    return result


def _asset_id(event: dict[str, Any]) -> str | None:
    raw = event.get("asset_id") or event.get("asset") or event.get("token_id") or event.get("token")
    if raw is None:
        return None
    asset_id = str(raw).strip()
    return asset_id or None


def _to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _normalize_side(side: Any) -> str | None:
    value = str(side or "").upper()
    if value in {"BUY", "BID"}:
        return "BUY"
    if value in {"SELL", "ASK"}:
        return "SELL"
    return None


def _max_decimal(a: Decimal | None, b: Decimal) -> Decimal:
    return b if a is None or b > a else a


def _min_decimal(a: Decimal | None, b: Decimal) -> Decimal:
    return b if a is None or b < a else a
