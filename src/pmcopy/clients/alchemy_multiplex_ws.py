"""Singleton multiplexed Alchemy WebSocket client with 2s debounced resubscription.

Maintains at most ONE shared WebSocket connection per process across all active
confirmed-log and mempool subscriptions:
  * ``logs_maker`` (``OrderFilled`` server-side filtered by ``topics[2]``)
  * ``logs_taker`` (``OrderFilled`` server-side filtered by ``topics[3]``)
  * ``pending_exchange`` (``alchemy_pendingTransactions`` locked to Exchange ``toAddress``)
  * optional ``pending_direct_users_*`` chunks when ``PMCOPY__TRACK_DIRECT_USER_TXS=true``

Every raw incoming frame is metered in UTF-8 bytes by ``AlchemyWsMeter`` before
JSON decoding. Dynamic roster updates coalesce through a configurable debounce
window (2.0s default) and reconcile atomically via ``eth_subscribe`` followed by
``eth_unsubscribe`` on the same open socket without reconnecting or dropping fills.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import random
from collections import OrderedDict
from collections.abc import Callable, Coroutine, Mapping, Sequence
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

import orjson
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from pmcopy.adapters.clock import SystemReceiptClock
from pmcopy.clients.onchain_ws import OnchainWsStateListener
from pmcopy.interfaces.source_time import ReceiptClock
from pmcopy.services.onchain_detection.alchemy_ws_meter import AlchemyWsMeter
from pmcopy.utils.logger import get_logger, redact_error_detail

logger = get_logger("AlchemyMultiplexWS")

MessageCallback = Callable[[dict[str, Any]], Coroutine[Any, Any, None]]

_DEFAULT_DEBOUNCE_SECONDS = 2.0
_DEFAULT_RPC_TIMEOUT_SECONDS = 10.0
_MIN_RECONNECT_DELAY = 1.0
_MAX_RECONNECT_DELAY = 30.0
_NOTIFICATION_QUEUE_MAXSIZE = 2048
_RETIRED_SUB_GRACE_CAP = 32


class _NoRedirectConnect(connect):
    """Reject credential-bearing WebSocket handshake redirects."""

    def process_redirect(self, exc: Exception) -> Exception:
        """Convert HTTP redirect responses into a hard connection error."""
        del exc
        return ConnectionError("Alchemy WebSocket handshake redirect rejected")


def _build_silent_ws_logger() -> logging.Logger:
    ws_logger = logging.Logger("pmcopy.private_alchemy_ws")
    ws_logger.disabled = True
    return ws_logger


_SILENT_WS_LOGGER = _build_silent_ws_logger()


def _default_connect_factory(ws_url: str) -> Any:
    return _NoRedirectConnect(
        ws_url,
        proxy=None,
        open_timeout=10,
        close_timeout=3,
        ping_interval=20,
        ping_timeout=20,
        max_size=2_000_000,
        max_queue=1024,
        logger=_SILENT_WS_LOGGER,
    )


def _canonical_params_bytes(params: Sequence[Any]) -> bytes:
    return orjson.dumps(list(params), option=orjson.OPT_SORT_KEYS)


@dataclass(frozen=True, slots=True)
class SlotSubscription:
    """Desired JSON-RPC ``eth_subscribe`` specification for one logical slot."""

    params: Sequence[Any]
    on_message: MessageCallback


class AlchemyMultiplexWebSocket:
    """Single shared Alchemy WebSocket connection multiplexing all subscriptions."""

    def __init__(
        self,
        *,
        ws_url: str,
        meter: AlchemyWsMeter | None = None,
        debounce_seconds: float = _DEFAULT_DEBOUNCE_SECONDS,
        rpc_timeout_seconds: float = _DEFAULT_RPC_TIMEOUT_SECONDS,
        reconnect_min_delay: float = _MIN_RECONNECT_DELAY,
        reconnect_max_delay: float = _MAX_RECONNECT_DELAY,
        state_listener: OnchainWsStateListener | None = None,
        receipt_clock: ReceiptClock | None = None,
        connect_factory: Callable[..., Any] | None = None,
    ) -> None:
        """Initialize the singleton multiplexed WebSocket manager.

        Args:
            ws_url: Validated ``wss://`` Alchemy Polygon endpoint.
            meter: Optional UTF-8 byte / CU meter and circuit breaker.
            debounce_seconds: Coalescing delay for dynamic roster updates.
            rpc_timeout_seconds: Per-request timeout for JSON-RPC control calls.
            reconnect_min_delay: Initial exponential backoff delay in seconds.
            reconnect_max_delay: Maximum exponential backoff delay in seconds.
            state_listener: Optional health/readiness observer.
            receipt_clock: Monotonic/wall receipt clock for frame arrival stamps.
            connect_factory: Optional WebSocket dialer override for tests.
        """
        cleaned_url = str(ws_url or "").strip()
        parsed = urlparse(cleaned_url)
        if parsed.scheme != "wss" or not parsed.hostname:
            raise ValueError("AlchemyMultiplexWebSocket requires a wss:// URL")

        self._ws_url = cleaned_url
        self._redacted_endpoint = f"{parsed.scheme}://{parsed.hostname}/<redacted>"
        self._meter = meter
        self._debounce_seconds = max(0.0, float(debounce_seconds))
        self._rpc_timeout_seconds = max(0.5, float(rpc_timeout_seconds))
        self._reconnect_min_delay = max(0.05, float(reconnect_min_delay))
        self._reconnect_max_delay = max(self._reconnect_min_delay, float(reconnect_max_delay))
        self._reconnect_delay = self._reconnect_min_delay
        self._state_listener = state_listener
        self._receipt_clock = receipt_clock if receipt_clock is not None else SystemReceiptClock()
        self._connect_factory = (
            connect_factory if connect_factory is not None else _default_connect_factory
        )

        self._desired_slots: dict[str, SlotSubscription] = {}
        self._pending_debounced_slots: dict[str, SlotSubscription] | None = None
        self._active_subs: dict[str, tuple[str, bytes, SlotSubscription]] = {}
        self._sub_id_to_callback: dict[str, MessageCallback] = {}
        self._retired_sub_callbacks: OrderedDict[str, MessageCallback] = OrderedDict()

        self._ws: Any = None
        self._running = False
        self._connected = False
        self._in_reconnect_backoff = False
        self._next_rpc_id = 0
        self._pending_rpc: dict[int, asyncio.Future[Any]] = {}
        self._send_lock = asyncio.Lock()
        self._reconcile_lock = asyncio.Lock()
        self._wake_event = asyncio.Event()
        self._breaker_event = asyncio.Event()
        self._subscribed_event = asyncio.Event()
        self._notification_queue: asyncio.Queue[tuple[str, dict[str, Any]]] = asyncio.Queue(
            maxsize=_NOTIFICATION_QUEUE_MAXSIZE
        )
        self._dropped_notifications = 0

        self._supervisor_task: asyncio.Task[None] | None = None
        self._debounce_task: asyncio.Task[None] | None = None

    def __repr__(self) -> str:
        return (
            f"AlchemyMultiplexWebSocket(endpoint={self._redacted_endpoint!r}, "
            f"connected={self._connected}, active_subscriptions={len(self._active_subs)})"
        )

    @property
    def redacted_endpoint(self) -> str:
        """Return ``wss://<host>/<redacted>`` without exposing the API key."""
        return self._redacted_endpoint

    @property
    def connected(self) -> bool:
        """Return True when the shared WebSocket is open and initial slots are active."""
        if not self._desired_slots:
            return self._running
        return self._connected and bool(self._active_subs)

    @property
    def active_subscription_count(self) -> int:
        """Return number of currently active ``eth_subscribe`` streams on the socket."""
        return len(self._active_subs)

    @property
    def active_slots(self) -> frozenset[str]:
        """Return logical slot names currently subscribed on the shared socket."""
        return frozenset(self._active_subs.keys())

    @property
    def dropped_notifications(self) -> int:
        """Return count of ring-evicted notifications under extreme queue saturation."""
        return self._dropped_notifications

    async def start(self, initial_slots: Mapping[str, SlotSubscription] | None = None) -> None:
        """Start the singleton WebSocket supervisor without blocking on empty slots."""
        if self._running:
            return
        self._running = True
        self._desired_slots = dict(initial_slots or {})
        if self._desired_slots:
            self._subscribed_event.clear()
            self._wake_event.set()
        else:
            self._subscribed_event.set()
        self._supervisor_task = asyncio.create_task(
            self._supervisor_loop(),
            name="alchemy-multiplex-ws-supervisor",
        )

    async def wait_until_subscribed(self, wait_seconds: float = 5.0) -> None:
        """Wait until desired slots are reconciled on the shared socket.

        Args:
            wait_seconds: Maximum wait duration in seconds.
        """
        async with asyncio.timeout(wait_seconds):
            await self._subscribed_event.wait()

    def schedule_reconcile(self, desired_slots: Mapping[str, SlotSubscription]) -> None:
        """Coalesce dynamic subscription updates over ``debounce_seconds``."""
        self._pending_debounced_slots = dict(desired_slots)
        if not self._running:
            self._desired_slots = dict(desired_slots)
            return
        if self._debounce_task is None or self._debounce_task.done():
            self._debounce_task = asyncio.create_task(
                self._run_debounce_timer(),
                name="alchemy-multiplex-ws-debounce",
            )

    async def reconcile_now(self, desired_slots: Mapping[str, SlotSubscription]) -> None:
        """Immediately reconcile ``desired_slots`` on the single shared connection."""
        self._cancel_debounce_task()
        self._desired_slots = dict(desired_slots)
        if not self._running:
            return

        ws = self._ws
        if not self._desired_slots:
            if ws is not None and self._connected:
                await self._reconcile_on_open_socket(ws)
            self._subscribed_event.set()
            self._wake_event.set()
            return

        if ws is not None and self._connected:
            await self._reconcile_on_open_socket(ws)
            return

        self._subscribed_event.clear()
        self._wake_event.set()
        if not self._in_reconnect_backoff:
            await self.wait_until_subscribed(wait_seconds=self._rpc_timeout_seconds)

    async def unsubscribe_slot(self, slot: str) -> bool:
        """Immediately ``eth_unsubscribe`` one logical slot without dropping the socket."""
        if self._pending_debounced_slots is not None:
            self._pending_debounced_slots.pop(slot, None)
        existed = self._desired_slots.pop(slot, None) is not None or (slot in self._active_subs)
        ws = self._ws
        if ws is not None and self._connected:
            await self._reconcile_on_open_socket(ws)
        if not self._desired_slots:
            self._subscribed_event.set()
            self._wake_event.set()
        return existed

    async def stop(self) -> None:
        """Gracefully close the single connection and stop all background tasks."""
        self._running = False
        self._cancel_debounce_task()
        self._wake_event.set()
        self._breaker_event.set()

        ws = self._ws
        if ws is not None:
            with contextlib.suppress(Exception):
                await ws.close()

        if self._supervisor_task is not None:
            self._supervisor_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._supervisor_task
            self._supervisor_task = None

        self._clear_connection_state(ConnectionError("AlchemyMultiplexWebSocket stopped"))

    def _cancel_debounce_task(self) -> None:
        self._pending_debounced_slots = None
        if self._debounce_task is not None and not self._debounce_task.done():
            self._debounce_task.cancel()
        self._debounce_task = None

    async def _run_debounce_timer(self) -> None:
        try:
            while self._running and self._pending_debounced_slots is not None:
                if self._debounce_seconds > 0.0:
                    await asyncio.sleep(self._debounce_seconds)
                pending = self._pending_debounced_slots
                self._pending_debounced_slots = None
                if pending is None or not self._running:
                    return
                self._desired_slots = pending
                ws = self._ws
                if ws is not None and self._connected:
                    await self._reconcile_on_open_socket(ws)
                    if not self._desired_slots:
                        self._wake_event.set()
                elif self._desired_slots:
                    self._subscribed_event.clear()
                    self._wake_event.set()
                else:
                    self._subscribed_event.set()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(
                "alchemy_ws_debounced_reconcile_failed | endpoint=%s error=%s",
                self._redacted_endpoint,
                redact_error_detail(exc),
            )
            self._wake_event.set()

    async def _supervisor_loop(self) -> None:
        stream_name = "alchemy_multiplex"
        while self._running:
            if not self._desired_slots:
                self._subscribed_event.set()
                await self._wake_event.wait()
                self._wake_event.clear()
                continue

            unexpected_disconnect = False
            notified_connected = False
            try:
                async with self._connect_factory(self._ws_url) as ws:
                    self._ws = ws
                    self._connected = True
                    recv_task = asyncio.create_task(
                        self._ws_recv_loop(ws),
                        name="alchemy-multiplex-ws-recv",
                    )
                    dispatch_task = asyncio.create_task(
                        self._dispatch_loop(),
                        name="alchemy-multiplex-ws-dispatch",
                    )
                    breaker_task = asyncio.create_task(
                        self._breaker_loop(),
                        name="alchemy-multiplex-ws-breaker",
                    )
                    try:
                        self._wake_event.clear()
                        await self._reconcile_on_open_socket(ws)
                        self._reconnect_delay = self._reconnect_min_delay
                        self._notify_connected(stream_name)
                        notified_connected = True

                        while self._running and self._desired_slots:
                            wake_waiter = asyncio.create_task(self._wake_event.wait())
                            done, _pending = await asyncio.wait(
                                {recv_task, dispatch_task, breaker_task, wake_waiter},
                                return_when=asyncio.FIRST_COMPLETED,
                            )
                            if not wake_waiter.done():
                                wake_waiter.cancel()
                                with contextlib.suppress(asyncio.CancelledError):
                                    await wake_waiter

                            worker_terminated = False
                            for worker in (recv_task, dispatch_task, breaker_task):
                                if worker in done:
                                    worker_terminated = True
                                    exc = worker.exception()
                                    if exc is not None:
                                        raise exc
                            if worker_terminated:
                                if self._running and self._desired_slots:
                                    unexpected_disconnect = True
                                break

                            self._wake_event.clear()
                            await self._reconcile_on_open_socket(ws)
                            if not self._desired_slots:
                                break
                    finally:
                        for task in (breaker_task, dispatch_task, recv_task):
                            task.cancel()
                        for task in (breaker_task, dispatch_task, recv_task):
                            with contextlib.suppress(asyncio.CancelledError, Exception):
                                await task
                        self._clear_connection_state(
                            ConnectionError("Alchemy WebSocket session ended")
                        )
                        if notified_connected:
                            self._notify_disconnected(stream_name)
            except asyncio.CancelledError:
                raise
            except (ConnectionClosed, OSError, RuntimeError, TimeoutError, ValueError) as exc:
                if self._running and self._desired_slots:
                    unexpected_disconnect = True
                    logger.warning(
                        "alchemy_ws_disconnected | endpoint=%s error=%s",
                        self._redacted_endpoint,
                        redact_error_detail(exc),
                    )
            except Exception as exc:
                if self._running and self._desired_slots:
                    unexpected_disconnect = True
                    logger.error(
                        "alchemy_ws_unexpected_error | endpoint=%s error=%s",
                        self._redacted_endpoint,
                        redact_error_detail(exc),
                    )

            if not self._running or not self._desired_slots:
                continue

            if unexpected_disconnect:
                if self._meter is not None:
                    self._meter.record_reconnect()
                self._notify_reconnect_scheduled(stream_name)
                jitter = random.uniform(0.85, 1.15)
                delay = min(self._reconnect_delay * jitter, self._reconnect_max_delay)
                try:
                    self._in_reconnect_backoff = True
                    wake_waiter = asyncio.create_task(self._wake_event.wait())
                    sleep_task = asyncio.create_task(asyncio.sleep(delay))
                    try:
                        await asyncio.wait(
                            {wake_waiter, sleep_task},
                            return_when=asyncio.FIRST_COMPLETED,
                        )
                    finally:
                        self._in_reconnect_backoff = False
                        for helper in (wake_waiter, sleep_task):
                            if not helper.done():
                                helper.cancel()
                                with contextlib.suppress(asyncio.CancelledError):
                                    await helper
                except asyncio.CancelledError:
                    raise
                self._reconnect_delay = min(self._reconnect_delay * 2.0, self._reconnect_max_delay)

    async def _reconcile_on_open_socket(self, ws: Any) -> None:
        async with self._reconcile_lock:
            while True:
                desired = dict(self._desired_slots)

                # 1. Unsubscribe slots that were completely removed (e.g. circuit breaker).
                for slot in list(self._active_subs.keys()):
                    if slot in desired:
                        continue
                    old_sub_id, _old_canonical, _old_spec = self._active_subs[slot]
                    await self._rpc_call(ws, "eth_unsubscribe", [old_sub_id])
                    self._active_subs.pop(slot, None)
                    self._sub_id_to_callback.pop(old_sub_id, None)

                # 2. Zero-gap atomic update / subscribe for added or modified slots:
                #    subscribe the new filter FIRST, then unsubscribe any superseded sub_id.
                for slot, spec in desired.items():
                    canonical = _canonical_params_bytes(spec.params)
                    existing = self._active_subs.get(slot)
                    if existing is not None:
                        old_sub_id, old_canonical, old_spec = existing
                        if canonical == old_canonical:
                            self._active_subs[slot] = (old_sub_id, old_canonical, spec)
                            self._sub_id_to_callback[old_sub_id] = spec.on_message
                            continue

                        new_sub_id = await asyncio.shield(
                            self._subscribe_slot_rpc(ws, slot, spec, canonical)
                        )
                        self._sub_id_to_callback.pop(old_sub_id, None)
                        self._retired_sub_callbacks[old_sub_id] = old_spec.on_message
                        self._retired_sub_callbacks.move_to_end(old_sub_id)
                        while len(self._retired_sub_callbacks) > _RETIRED_SUB_GRACE_CAP:
                            self._retired_sub_callbacks.popitem(last=False)
                        await self._rpc_call(ws, "eth_unsubscribe", [old_sub_id])
                        logger.info(
                            "alchemy_ws_slot_subscribed | slot=%s sub_id=%s active_subscriptions=%d endpoint=%s",
                            slot,
                            new_sub_id,
                            len(self._active_subs),
                            self._redacted_endpoint,
                        )
                        continue

                    new_sub_id = await asyncio.shield(
                        self._subscribe_slot_rpc(ws, slot, spec, canonical)
                    )
                    logger.info(
                        "alchemy_ws_slot_subscribed | slot=%s sub_id=%s active_subscriptions=%d endpoint=%s",
                        slot,
                        new_sub_id,
                        len(self._active_subs),
                        self._redacted_endpoint,
                    )

                if desired == self._desired_slots:
                    break

            if set(self._active_subs.keys()) == set(self._desired_slots.keys()):
                self._subscribed_event.set()

    async def _subscribe_slot_rpc(
        self,
        ws: Any,
        slot: str,
        spec: SlotSubscription,
        canonical: bytes,
    ) -> str:
        sub_id = await self._rpc_call(ws, "eth_subscribe", list(spec.params))
        if not isinstance(sub_id, str) or not sub_id:
            raise RuntimeError(f"eth_subscribe for slot {slot!r} returned invalid subscription id")
        self._active_subs[slot] = (sub_id, canonical, spec)
        self._sub_id_to_callback[sub_id] = spec.on_message
        return sub_id

    async def _rpc_call(self, ws: Any, method: str, params: list[Any]) -> Any:
        self._next_rpc_id += 1
        req_id = self._next_rpc_id
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[Any] = loop.create_future()
        self._pending_rpc[req_id] = fut
        payload = orjson.dumps(
            {
                "jsonrpc": "2.0",
                "id": req_id,
                "method": method,
                "params": params,
            }
        ).decode("utf-8")
        try:
            async with self._send_lock:
                await ws.send(payload)
            async with asyncio.timeout(self._rpc_timeout_seconds):
                return await fut
        finally:
            self._pending_rpc.pop(req_id, None)

    def _enqueue_notification_nonblocking(self, sub_id: str, payload: dict[str, Any]) -> None:
        """Enqueue a subscription notification without ever blocking ``_ws_recv_loop``."""
        try:
            self._notification_queue.put_nowait((sub_id, payload))
        except asyncio.QueueFull:
            with contextlib.suppress(asyncio.QueueEmpty):
                self._notification_queue.get_nowait()
            self._dropped_notifications += 1
            with contextlib.suppress(asyncio.QueueFull):
                self._notification_queue.put_nowait((sub_id, payload))

    async def _ws_recv_loop(self, ws: Any) -> None:
        async for raw in ws:
            if not self._running:
                return
            if self._meter is not None:
                self._meter.record_frame(raw)
                if (
                    not self._meter.breaker_tripped
                    and self._meter.rolling_hour_bytes > self._meter.max_hourly_bytes
                ):
                    self._breaker_event.set()

            try:
                msg = orjson.loads(raw)
            except orjson.JSONDecodeError:
                continue

            if not isinstance(msg, dict):
                continue

            if "id" in msg and "method" not in msg:
                req_id = msg.get("id")
                fut = self._pending_rpc.get(req_id) if isinstance(req_id, int) else None
                if fut is not None and not fut.done():
                    if msg.get("error") is not None:
                        fut.set_exception(
                            RuntimeError(
                                f"Alchemy JSON-RPC error: {redact_error_detail(msg['error'])}"
                            )
                        )
                    else:
                        fut.set_result(msg.get("result"))
                continue

            if msg.get("method") == "eth_subscription":
                params = msg.get("params")
                if isinstance(params, dict):
                    sub_id = params.get("subscription")
                    result = params.get("result")
                    if isinstance(sub_id, str) and isinstance(result, dict):
                        enriched = dict(result)
                        enriched["_source_receipt"] = self._receipt_clock.capture()
                        self._enqueue_notification_nonblocking(sub_id, enriched)

    async def _breaker_loop(self) -> None:
        while self._running:
            await self._breaker_event.wait()
            self._breaker_event.clear()
            if not self._running or self._meter is None:
                continue
            try:
                await self._meter.check_and_enforce()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error(
                    "alchemy_ws_breaker_enforce_error | endpoint=%s error=%s",
                    self._redacted_endpoint,
                    redact_error_detail(exc),
                )

    async def _dispatch_loop(self) -> None:
        while self._running:
            sub_id, payload = await self._notification_queue.get()
            callback = self._sub_id_to_callback.get(sub_id) or self._retired_sub_callbacks.get(
                sub_id
            )
            if callback is None:
                continue
            try:
                await callback(payload)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error(
                    "alchemy_ws_callback_error | sub_id=%s error=%s",
                    sub_id,
                    redact_error_detail(exc),
                )

    def _clear_connection_state(self, err: Exception) -> None:
        self._ws = None
        self._connected = False
        self._active_subs.clear()
        self._sub_id_to_callback.clear()
        self._retired_sub_callbacks.clear()
        if self._desired_slots:
            self._subscribed_event.clear()
        for fut in list(self._pending_rpc.values()):
            if not fut.done():
                fut.set_exception(err)
        self._pending_rpc.clear()
        while not self._notification_queue.empty():
            with contextlib.suppress(asyncio.QueueEmpty):
                self._notification_queue.get_nowait()

    def _notify_connected(self, stream: str) -> None:
        if self._state_listener is None:
            return
        with contextlib.suppress(Exception):
            self._state_listener.on_connected(stream)

    def _notify_disconnected(self, stream: str) -> None:
        if self._state_listener is None:
            return
        with contextlib.suppress(Exception):
            self._state_listener.on_disconnected(stream)

    def _notify_reconnect_scheduled(self, stream: str) -> None:
        if self._state_listener is None:
            return
        with contextlib.suppress(Exception):
            self._state_listener.on_reconnect_scheduled(stream)


__all__ = [
    "AlchemyMultiplexWebSocket",
    "MessageCallback",
    "SlotSubscription",
]
