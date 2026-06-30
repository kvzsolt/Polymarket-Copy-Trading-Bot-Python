"""Alchemy Ethereum WebSocket client for on-chain detection (PR2).

A minimal, self-contained ``websockets``-based client that:
  * opens a persistent connection to an Alchemy WebSocket endpoint,
  * sends an ``eth_subscribe`` request for either ``logs`` (mined
    ``OrderFilled``) or ``alchemy_pendingTransactions`` (pending tx),
  * drains JSON-RPC subscription notifications into an ``asyncio.Queue``,
  * reconnects with exponential backoff + jitter on close/error,
  * exposes a graceful ``stop()`` that cancels its tasks and closes the socket.

This mirrors the lifecycle discipline of ``clients/market_ws.py`` (heartbeat,
backoff, structured logging, ``redact_error_detail`` on errors) but is
intentionally separate because the Alchemy JSON-RPC framing differs from the
Polymarket market/user WebSocket protocol.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import random
from typing import Any, Callable, Coroutine

import websockets
from websockets.exceptions import (
    ConnectionClosed,
    ConnectionClosedError,
    ConnectionClosedOK,
)

from utils.logger import get_logger, redact_error_detail

logger = get_logger("OnchainWS")

_MIN_RECONNECT_DELAY = 1.0
_MAX_RECONNECT_DELAY = 30.0
_SUBSCRIBE_TIMEOUT_SECONDS = 10.0
_RECEIVE_TIMEOUT_SECONDS = 60.0

MessageCallback = Callable[[dict[str, Any]], Coroutine[Any, Any, None]]


class OnchainWebSocket:
    """A single Alchemy WebSocket subscription.

    One instance owns one subscription (either the pending-tx sub or the
    mined-log sub). The runtime creates two instances.
    """

    def __init__(
        self,
        *,
        ws_url: str,
        subscribe_params: dict[str, Any],
        name: str,
        on_message: MessageCallback,
        reconnect_min_delay: float = _MIN_RECONNECT_DELAY,
        reconnect_max_delay: float = _MAX_RECONNECT_DELAY,
    ) -> None:
        self._ws_url = ws_url
        self._subscribe_params = subscribe_params
        self._name = name
        self._on_message = on_message
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
                    max_queue=None,
                ) as ws:
                    self._ws = ws
                    self._reconnect_delay = self._reconnect_min_delay
                    logger.info("OnchainWS[%s]: connected to %s", self._name, self._ws_url)

                    await self._send_subscribe(ws)

                    async for raw in ws:
                        if not self._running:
                            break
                        await self._handle_raw(raw)
            except (ConnectionClosed, ConnectionClosedError, ConnectionClosedOK) as exc:
                logger.warning("OnchainWS[%s]: connection closed: %s", self._name, exc)
            except OSError as exc:
                logger.warning("OnchainWS[%s]: network error: %s", self._name, exc)
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

            if not self._running:
                break

            delay = self._reconnect_delay + random.uniform(0, 0.5)
            logger.info("OnchainWS[%s]: reconnecting in %.1fs", self._name, delay)
            await asyncio.sleep(delay)
            self._reconnect_delay = min(self._reconnect_delay * 2, self._reconnect_max_delay)

    async def _send_subscribe(self, ws: Any) -> None:
        request = {
            "id": 1,
            "jsonrpc": "2.0",
            "method": "eth_subscribe",
            "params": self._subscribe_params,
        }
        await ws.send(json.dumps(request))
        # Wait for the subscription ack. The first frame after the request is
        # the ack (``{"id":1,"result":"<sub_id>"}``); subsequent frames are
        # notifications. We read synchronously here with a timeout.
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=_SUBSCRIBE_TIMEOUT_SECONDS)
        except asyncio.TimeoutError:
            logger.warning("OnchainWS[%s]: subscribe ack timed out", self._name)
            raise ConnectionClosed(None, None)  # trigger reconnect
        try:
            ack = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            logger.warning("OnchainWS[%s]: non-JSON ack: %s", self._name, str(raw)[:100])
            raise ConnectionClosed(None, None)
        if not isinstance(ack, dict) or "result" not in ack:
            logger.warning("OnchainWS[%s]: subscribe failed: %s", self._name, ack)
            raise ConnectionClosed(None, None)
        self._sub_id = str(ack["result"])
        logger.info("OnchainWS[%s]: subscribed (sub_id=%s)", self._name, self._sub_id)

    async def _handle_raw(self, raw: str | bytes) -> None:
        if raw in ("", b""):
            return
        try:
            data = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            logger.debug("OnchainWS[%s]: non-JSON frame: %s", self._name, str(raw)[:100])
            return
        if not isinstance(data, dict):
            return
        # JSON-RPC notification: {"method":"eth_subscription","params":{"subscription","result"}}
        if data.get("method") == "eth_subscription":
            params = data.get("params") or {}
            result = params.get("result")
            if result is None:
                return
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