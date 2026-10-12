"""Alchemy Ethereum WebSocket client for on-chain detection (PR2).

A minimal, self-contained ``websockets``-based client that:
  * opens a persistent connection to an Alchemy WebSocket endpoint,
  * sends an ``eth_subscribe`` request for either ``logs`` (mined
    ``OrderFilled``) or ``alchemy_pendingTransactions`` (pending tx),
  * drains JSON-RPC subscription notifications into an ``asyncio.Queue``,
  * reconnects with exponential backoff + jitter on close/error,
  * reports connect / disconnect / reconnect-scheduled to an optional
    ``OnchainWsStateListener`` (operator visibility only — see
    ``services.onchain_detection.ws_health_reporter``),
  * exposes a graceful ``stop()`` that cancels its tasks and closes the socket.

This mirrors the lifecycle discipline of ``clients/market_ws.py`` (heartbeat,
backoff, structured logging, ``redact_error_detail`` on errors) but is
intentionally separate because the Alchemy JSON-RPC framing differs from the
Polymarket market/user WebSocket protocol.
"""

from __future__ import annotations

import asyncio
import contextlib
import orjson
import random
from typing import Any, Callable, Coroutine, Protocol, runtime_checkable
from urllib.parse import urlparse

import websockets
from websockets.exceptions import (
    ConnectionClosed,
    ConnectionClosedError,
    ConnectionClosedOK,
)

from pmcopy.utils.logger import get_logger, redact_error_detail
from pmcopy.adapters.clock import SystemReceiptClock
from pmcopy.interfaces.source_time import ReceiptClock

logger = get_logger("OnchainWS")

_MIN_RECONNECT_DELAY = 1.0
_MAX_RECONNECT_DELAY = 30.0
_SUBSCRIBE_TIMEOUT_SECONDS = 10.0
_RECEIVE_TIMEOUT_SECONDS = 60.0
# Bounded incoming-message queue cap (backpressure). 1000 messages is
# roughly one Polygon block worth of OrderFilled events at peak activity;
# overflow triggers a connection-close + reconnect instead of OOM.
_MAX_QUEUE: int | None = 1000

MessageCallback = Callable[[dict[str, Any]], Coroutine[Any, Any, None]]


@runtime_checkable
class OnchainWsStateListener(Protocol):
    """Observer of one subscription's connection lifecycle.

    Every method is **synchronous, non-blocking and best-effort**: the client
    calls them from inside its connect loop, swallows anything they raise, and
    never awaits them. Implementations must therefore not perform IO — the
    production listener only touches an in-memory metrics registry and the
    readiness tracker.
    """

    def on_connected(self, stream: str) -> None:
        """Report that ``stream`` is subscribed and receiving notifications."""
        ...

    def on_disconnected(self, stream: str) -> None:
        """Report that ``stream`` lost its connection (for any reason)."""
        ...

    def on_reconnect_scheduled(self, stream: str) -> None:
        """Report that ``stream`` is about to wait out its backoff delay."""
        ...


class OnchainWebSocket:
    """A single Alchemy WebSocket subscription.

    One instance owns one subscription (either the pending-tx sub or the
    mined-log sub). The runtime creates two instances.
    """

    def __init__(
        self,
        *,
        ws_url: str,
        subscribe_params: list[Any],
        name: str,
        on_message: MessageCallback,
        reconnect_min_delay: float = _MIN_RECONNECT_DELAY,
        reconnect_max_delay: float = _MAX_RECONNECT_DELAY,
        state_listener: OnchainWsStateListener | None = None,
        receipt_clock: ReceiptClock | None = None,
    ) -> None:
        self._ws_url = ws_url
        self._subscribe_params = subscribe_params
        self._name = name
        self._on_message = on_message
        self._state_listener = state_listener
        self._receipt_clock = receipt_clock if receipt_clock is not None else SystemReceiptClock()
        self._reconnect_min_delay = reconnect_min_delay
        self._reconnect_max_delay = reconnect_max_delay
        self._reconnect_delay = reconnect_min_delay
        self._ws: Any = None
        self._running = False
        self._sub_id: str | None = None
        self._task: asyncio.Task | None = None

    @property
    def name(self) -> str:
        return self._name

    @property
    def running(self) -> bool:
        return self._running

    @property
    def redacted_endpoint(self) -> str:
        """Return the WebSocket host only; the path carries the provider API key."""
        return urlparse(self._ws_url).netloc or "<unset>"

    @property
    def connected(self) -> bool:
        return self._ws is not None and self._sub_id is not None

    def start(self) -> None:
        """Start the reconnect loop as a background task (fire-and-forget)."""
        if self._task is not None and not self._task.done():
            return
        self._running = True
        self._task = asyncio.create_task(self._run(), name=f"onchain-ws-{self._name}")

    async def stop(self) -> None:
        """Gracefully stop: cancel the loop, close the socket."""
        self._running = False
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        if self._ws is not None:
            with contextlib.suppress(Exception):
                await self._ws.close()
            self._ws = None
        self._sub_id = None

    async def _run(self) -> None:
        while self._running:
            try:
                async with websockets.connect(
                    self._ws_url,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=5,
                    # Backpressure cap: when the on_message callback (which
                    # may block on _resolve_condition_id HTTP/RPC lookups)
                    # falls behind the producer during a burst (e.g. a
                    # high-volume Polygon block with 100s of OrderFilled
                    # events), the WS library's incoming buffer fills. With
                    # max_queue=None (unbounded) this can grow without limit
                    # and OOM the host. With a bounded cap the library
                    # closes the connection with code 1011 on overflow,
                    # triggering the reconnect+exponential-backoff below —
                    # exactly the desired fail-safe (the runtime already
                    # tolerates WSS drops per its own docstring).
                    max_queue=_MAX_QUEUE,
                ) as ws:
                    self._ws = ws
                    self._reconnect_delay = self._reconnect_min_delay
                    logger.info(
                        "OnchainWS[%s]: connected to %s", self._name, self.redacted_endpoint
                    )

                    await self._send_subscribe(ws)

                    async for raw in ws:
                        if not self._running:
                            break
                        await self._handle_raw(raw)
            except (ConnectionClosed, ConnectionClosedError, ConnectionClosedOK) as exc:
                logger.warning(
                    "OnchainWS[%s]: connection closed: %s", self._name, type(exc).__name__
                )
            except OSError as exc:
                logger.warning("OnchainWS[%s]: network error: %s", self._name, type(exc).__name__)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "OnchainWS[%s]: unexpected error: %s",
                    self._name,
                    redact_error_detail(exc),
                )
            finally:
                self._ws = None
                self._sub_id = None
                self._notify_state("on_disconnected")

            if not self._running:
                break

            delay = self._reconnect_delay + random.uniform(0, 0.5)
            logger.info("OnchainWS[%s]: reconnecting in %.1fs", self._name, delay)
            self._notify_state("on_reconnect_scheduled")
            await asyncio.sleep(delay)
            self._reconnect_delay = min(self._reconnect_delay * 2, self._reconnect_max_delay)

    async def _send_subscribe(self, ws: Any) -> None:
        request = {
            "id": 1,
            "jsonrpc": "2.0",
            "method": "eth_subscribe",
            "params": self._subscribe_params,
        }
        await ws.send(orjson.dumps(request).decode())
        # Wait for the subscription ack. The first frame after the request is
        # the ack (``{"id":1,"result":"<sub_id>"}``); subsequent frames are
        # notifications. We read synchronously here with a timeout.
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=_SUBSCRIBE_TIMEOUT_SECONDS)
        except asyncio.TimeoutError as exc:
            logger.warning("OnchainWS[%s]: subscribe ack timed out", self._name)
            raise ConnectionClosed(None, None) from exc  # trigger reconnect
        try:
            ack = orjson.loads(raw)
        except (orjson.JSONDecodeError, TypeError) as exc:
            logger.warning("OnchainWS[%s]: non-JSON acknowledgement rejected", self._name)
            raise ConnectionClosed(None, None) from exc
        if not isinstance(ack, dict) or "result" not in ack:
            logger.warning("OnchainWS[%s]: subscribe failed: %s", self._name, ack)
            raise ConnectionClosed(None, None)
        self._sub_id = str(ack["result"])
        logger.info("OnchainWS[%s]: subscribed (sub_id=%s)", self._name, self._sub_id)
        self._notify_state("on_connected")

    def _notify_state(self, event: str) -> None:
        """Fire one lifecycle callback on the optional state listener.

        The listener is an operator-visibility observer, never a participant:
        it is looked up by name (so a partial implementation is legal), called
        synchronously, and anything it raises is swallowed at debug level. A
        health reporter must never be able to break the detection socket.

        Args:
            event: Listener method name (``on_connected`` / ``on_disconnected``
                / ``on_reconnect_scheduled``).
        """

        listener = self._state_listener
        if listener is None:
            return
        callback = getattr(listener, event, None)
        if not callable(callback):
            return
        try:
            callback(self._name)
        except Exception as exc:
            logger.debug(
                "OnchainWS[%s]: state listener %s failed: %s",
                self._name,
                event,
                redact_error_detail(exc),
            )

    async def _handle_raw(self, raw: str | bytes) -> None:
        receipt = self._receipt_clock.capture()
        if raw in ("", b""):
            return
        try:
            data = orjson.loads(raw)
        except (orjson.JSONDecodeError, TypeError):
            logger.debug("OnchainWS[%s]: non-JSON frame rejected", self._name)
            return
        if not isinstance(data, dict):
            return
        # JSON-RPC notification: {"method":"eth_subscription","params":{"subscription","result"}}
        if data.get("method") == "eth_subscription":
            params = data.get("params") or {}
            result = params.get("result")
            if not isinstance(result, dict):
                return
            result = {**result, "_source_receipt": receipt}
            try:
                await self._on_message(result)
            except Exception as exc:
                logger.warning(
                    "OnchainWS[%s]: on_message error: %s",
                    self._name,
                    redact_error_detail(exc),
                )
            return
        # Other messages (e.g. subscription ack duplicates) are ignored.
