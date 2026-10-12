"""Metered owner-filtered WebSocket sessions with finite buffers and no hidden retries."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Callable, Sequence
from typing import Protocol

from websockets.asyncio.client import connect

from pmcopy.clients.network.provider_transport import ProviderEndpoints
from pmcopy.domain.provider_budget import BudgetExhausted
from pmcopy.domain.source_time import TransportReceipt
from pmcopy.interfaces.grouped_detection import (
    DetectionStateStore,
    NativeObservationSink,
    RawObservationSink,
)
from pmcopy.interfaces.provider_budget import ProviderAdmission
from pmcopy.interfaces.source_time import ReceiptClock
from pmcopy.services.grouped_detection import DetectionSubscription
from pmcopy.services.onchain_detection.bounded_recovery import BoundedLogRecovery


class StreamUnavailable(RuntimeError):
    """Sanitized source failure; never contains a credential URL or provider payload."""


class StreamAdmission(ProviderAdmission, Protocol):
    """Charge received bytes even for irrelevant or malformed upstream messages."""

    def charge_stream(self, provider: str, byte_count: int, *, category: str = "essential") -> None:
        """Account delivered bytes and reject exhausted shared quota."""
        ...


class StreamSocket(Protocol):
    """Small asynchronous transport surface suitable for deterministic fakes."""

    async def send(self, message: str) -> None:
        """Send one already-admitted JSON-RPC control message."""
        ...

    async def recv(self) -> str | bytes:
        """Return one received frame or raise a transport failure."""
        ...

    async def close(self) -> None:
        """Close without implicit subscriptions or retries."""
        ...


class StreamConnector(Protocol):
    """Resolve only validated configured aliases inside a credential-owning adapter."""

    async def open(self, provider: str) -> StreamSocket:
        """Open a single bounded connection attempt."""
        ...


class GroupRecovery(Protocol):
    """Recover evidence before enabling fresh dispatch for an acknowledged group."""

    async def recover(self, group: DetectionSubscription) -> int:
        """Return the fully recovered head or fail closed."""
        ...


class _NoRedirectConnect(connect):
    """A provider cannot redirect credential-bearing handshakes to another URL."""

    def process_redirect(self, exc: Exception) -> Exception:
        return StreamUnavailable("provider WebSocket handshake unavailable")


class ProviderWebSocketConnector:
    """Use fixed TLS endpoints without proxies, redirects, URL logs, or retry loops."""

    def __init__(self, endpoints: ProviderEndpoints) -> None:
        """Retain endpoints privately; no network operation occurs during composition."""
        self._endpoints = endpoints
        self._logger = logging.Logger("aethertrade.private_provider_websocket")
        self._logger.disabled = True

    async def open(self, provider: str) -> StreamSocket:
        """Attempt one credential-safe handshake with finite frame and queue bounds."""
        if provider != "alchemy":
            raise StreamUnavailable("standing stream billing unavailable")
        try:
            return await _NoRedirectConnect(
                self._endpoints.websocket_url(provider),
                proxy=None,
                open_timeout=10,
                close_timeout=3,
                ping_interval=20,
                ping_timeout=20,
                max_size=1_000_000,
                max_queue=16,
                logger=self._logger,
            )
        except Exception:
            raise StreamUnavailable("provider WebSocket connection unavailable") from None


class GroupedWebSocketSession:
    """Multiplex native-owner groups on one metered Alchemy connection.

    All groups are acknowledged before recovery; early messages are buffered with
    their original receipt clocks. Recovery records old data as evidence only.
    This class makes one session attempt; its owner decides bounded reconnects.
    """

    def __init__(
        self,
        *,
        groups: Sequence[DetectionSubscription],
        connector: StreamConnector,
        admission: StreamAdmission,
        observer: NativeObservationSink,
        recovery: GroupRecovery,
        raw: RawObservationSink,
        clock: ReceiptClock,
        state: DetectionStateStore,
        status_changed: Callable[[], None] | None = None,
        maximum_buffer: int = 1024,
    ) -> None:
        """Inject transports and local evidence; reject unsupported standing feeds."""
        if (
            not groups
            or len(groups) > 100
            or any(
                group.provider != "alchemy"
                or group.kind != "mined"
                or not 1 <= len(group.wallets) <= 100
                for group in groups
            )
        ):
            raise ValueError("only bounded Alchemy mined owner groups are supported")
        if type(maximum_buffer) is not int or not 1 <= maximum_buffer <= 4096:
            raise ValueError("invalid source buffer bound")
        self._groups, self._connector, self._admission = tuple(groups), connector, admission
        self._observer, self._recovery, self._raw = observer, recovery, raw
        self._clock, self._state, self._maximum = clock, state, maximum_buffer
        self._changed = status_changed or (lambda: None)
        self.connected = False
        self._wallets = tuple(wallet for group in groups for wallet in group.wallets)
        self._frames: asyncio.Queue[tuple[dict[str, object], TransportReceipt] | Exception] = (
            asyncio.Queue(maximum_buffer + 1)
        )

    async def run(self) -> None:
        """Serve a single admitted session; every exit visibly pauses source coverage."""
        socket: StreamSocket | None = None
        reader: asyncio.Task[None] | None = None
        try:
            socket = await self._connector.open("alchemy")
            reader = asyncio.create_task(self._pump(socket), name="metered-owner-stream-reader")
            subscriptions: dict[str, DetectionSubscription] = {}
            buffered: list[tuple[dict[str, object], TransportReceipt]] = []
            for request_id, group in enumerate(self._groups, 1):
                self._admission.admit_rpc("alchemy", "eth_subscribe", category="essential")
                await socket.send(
                    json.dumps(
                        {
                            "jsonrpc": "2.0",
                            "id": request_id,
                            "method": "eth_subscribe",
                            "params": group.subscribe_params(),
                        }
                    )
                )
                async with asyncio.timeout(10):
                    while True:
                        message, receipt = await self._receive()
                        if message.get("method") == "eth_subscription":
                            if len(buffered) >= self._maximum:
                                raise StreamUnavailable("subscription buffer exhausted")
                            buffered.append((message, receipt))
                            continue
                        subscription = message.get("result")
                        if (
                            type(message.get("id")) is not int
                            or message["id"] != request_id
                            or "error" in message
                            or not isinstance(subscription, str)
                            or not subscription
                            or len(subscription) > 256
                            or subscription in subscriptions
                        ):
                            raise StreamUnavailable("invalid subscription acknowledgment")
                        subscriptions[subscription] = group
                        break
            heads = {
                subscription: await self._recovery.recover(group)
                for subscription, group in subscriptions.items()
            }
            self.connected = True
            self._state.set_effective(self._wallets, "mined", None)
            pending = tuple(
                state.wallet
                for state in self._state.states()
                if state.wallet in self._wallets and state.desired_mode == "pending"
            )
            self._state.set_effective(pending, "mined", "pending_disabled")
            self._changed()
            for message, receipt in buffered:
                await self._dispatch(message, receipt, subscriptions, heads)
            while True:
                message, receipt = await self._receive()
                await self._dispatch(message, receipt, subscriptions, heads)
        except BudgetExhausted:
            raise
        except Exception:
            raise StreamUnavailable("grouped source session unavailable") from None
        finally:
            self.connected = False
            self._state.set_effective(self._wallets, "paused", "source_unavailable")
            self._changed()
            if reader is not None:
                reader.cancel()
                await asyncio.gather(reader, return_exceptions=True)
            if socket is not None:
                try:
                    await socket.close()
                except Exception:
                    # Preserve sanitized failures and cancellation at this boundary.
                    pass

    async def _pump(self, socket: StreamSocket) -> None:
        """Capture and persist arrivals independently from enrichment/recovery I/O."""
        try:
            while True:
                frame = await self._read_wire(socket)
                if self._frames.qsize() >= self._maximum:
                    raise StreamUnavailable("source processing buffer exhausted")
                self._frames.put_nowait(frame)
        except Exception as exc:
            self._state.set_effective(self._wallets, "paused", "source_unavailable")
            self._changed()
            self._frames.put_nowait(exc)

    async def _receive(self) -> tuple[dict[str, object], TransportReceipt]:
        frame = await self._frames.get()
        if isinstance(frame, Exception):
            raise frame
        return frame

    async def _read_wire(self, socket: StreamSocket) -> tuple[dict[str, object], TransportReceipt]:
        raw = await socket.recv()
        receipt = self._clock.capture()
        byte_count = len(raw.encode("utf-8")) if isinstance(raw, str) else len(raw)
        self._admission.charge_stream("alchemy", byte_count, category="essential")
        if byte_count > 1_000_000:
            raise StreamUnavailable("source frame exceeds bound")
        message = json.loads(raw)
        if not isinstance(message, dict) or message.get("jsonrpc") != "2.0":
            raise StreamUnavailable("invalid source envelope")
        params = message.get("params")
        if message.get("method") == "eth_subscription" and isinstance(params, dict):
            self._raw.record("alchemy", "mined", params.get("result"), receipt, recovered=False)
        return message, receipt

    async def _dispatch(
        self,
        message: dict[str, object],
        receipt: TransportReceipt,
        subscriptions: dict[str, DetectionSubscription],
        heads: dict[str, int],
    ) -> None:
        params = message.get("params")
        if message.get("method") != "eth_subscription" or not isinstance(params, dict):
            raise StreamUnavailable("invalid source notification")
        subscription, payload = params.get("subscription"), params.get("result")
        if (
            not isinstance(subscription, str)
            or subscription not in subscriptions
            or not isinstance(payload, dict)
        ):
            raise StreamUnavailable("unknown source subscription")
        group = subscriptions[subscription]
        number = BoundedLogRecovery._quantity(payload.get("blockNumber"))
        recovered = number <= heads[subscription]
        await self._observer.observe(payload, receipt, recovered=recovered)
        self._state.advance_checkpoint(BoundedLogRecovery.source_key(group), number)
