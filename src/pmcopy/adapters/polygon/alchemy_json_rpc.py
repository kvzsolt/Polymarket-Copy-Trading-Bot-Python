"""Multiplex Alchemy subscriptions and read-only RPC over one warm socket."""

from __future__ import annotations

from pmcopy.adapters.clock import SystemReceiptClock
from pmcopy.interfaces.source_time import ReceiptClock

import asyncio
import contextlib
from dataclasses import dataclass
from enum import StrEnum

import orjson

from pmcopy.domain.source_transactions import EvmHead, EvmReceipt, ReceiptStatus
from pmcopy.interfaces.evm_events import EvmEventSink, JsonRpcSocket
from pmcopy.interfaces.source_transactions import Clock


class AlchemyRpcError(RuntimeError):
    """Base failure raised by the Alchemy JSON-RPC session."""


class AlchemyRpcDisconnectedError(AlchemyRpcError):
    """Raised when an in-flight RPC request loses its socket."""


class AlchemyRpcResponseError(AlchemyRpcError):
    """Raised when Alchemy returns an error or malformed result."""


class _SubscriptionKind(StrEnum):
    """Internal subscription routes on the multiplexed socket."""

    PENDING = "pending"
    HEADS = "heads"
    LOGS = "logs"


class _AddressNormalizer:
    """Normalize adapter configuration without exposing provider payloads."""

    @staticmethod
    def normalize(value: str) -> str:
        normalized = str(value or "").strip().lower()
        if len(normalized) != 42 or not normalized.startswith("0x"):
            raise ValueError("subscription address must be a 20-byte hex address")
        try:
            int(normalized[2:], 16)
        except ValueError as exc:
            raise ValueError("subscription address must be a 20-byte hex address") from exc
        return normalized


@dataclass(frozen=True, slots=True)
class AlchemySubscriptionOptions:
    """Configured subscription filters for one Polygon session."""

    chain_id: int
    pending_targets: tuple[str, ...]
    log_addresses: tuple[str, ...]

    def __post_init__(self) -> None:
        """Validate and normalize chain and address filters."""

        if self.chain_id <= 0:
            raise ValueError("chain_id must be positive")
        pending_targets = tuple(
            dict.fromkeys(_AddressNormalizer.normalize(value) for value in self.pending_targets)
        )
        log_addresses = tuple(
            dict.fromkeys(_AddressNormalizer.normalize(value) for value in self.log_addresses)
        )
        if not pending_targets:
            raise ValueError("pending_targets must contain at least one address")
        object.__setattr__(self, "pending_targets", pending_targets)
        object.__setattr__(self, "log_addresses", log_addresses)


class AlchemyJsonRpcSession:
    """Route subscriptions and read-only RPC on a single connected socket."""

    def __init__(
        self,
        socket: JsonRpcSocket,
        options: AlchemySubscriptionOptions,
        sink: EvmEventSink,
        clock: Clock,
        max_pending_events: int = 1024,
        receipt_clock: ReceiptClock | None = None,
    ) -> None:
        """Initialize a not-yet-started Alchemy session.

        Args:
            socket: Already-connected full-duplex JSON-RPC WebSocket.
            options: Polygon chain and subscription filters.
            sink: Typed event consumer.
            clock: Deterministic head observation clock.
            max_pending_events: Bounded notification backlog before reconnect.

        Raises:
            ValueError: If ``max_pending_events`` is not positive.
        """

        if max_pending_events <= 0:
            raise ValueError("max_pending_events must be positive")
        self._socket = socket
        self._options = options
        self._sink = sink
        self._clock = clock
        self._receipt_clock = receipt_clock if receipt_clock is not None else SystemReceiptClock()
        self._next_request_id = 1
        self._pending_requests: dict[int, asyncio.Future[object]] = {}
        self._subscriptions: dict[str, _SubscriptionKind] = {}
        self._event_queue: asyncio.Queue[tuple[_SubscriptionKind, object]] = asyncio.Queue(
            maxsize=max_pending_events
        )
        self._reader_task: asyncio.Task[None] | None = None
        self._event_task: asyncio.Task[None] | None = None
        self._disconnected = asyncio.Event()
        self._disconnect_lock = asyncio.Lock()
        self._disconnect_notified = False
        self._running = False
        self._stopping = False

    @property
    def running(self) -> bool:
        """Return whether the session reader is active."""

        return self._running

    async def start(self) -> None:
        """Start the reader and establish pending, head, and log subscriptions."""

        if self._running:
            return
        self._running = True
        self._stopping = False
        self._disconnected.clear()
        self._disconnect_notified = False
        self._event_task = asyncio.create_task(
            self._event_loop(),
            name="alchemy-json-rpc-events",
        )
        self._reader_task = asyncio.create_task(
            self._reader_loop(),
            name="alchemy-json-rpc-reader",
        )
        try:
            await self._subscribe(
                _SubscriptionKind.PENDING,
                [
                    "alchemy_pendingTransactions",
                    {
                        "toAddress": list(self._options.pending_targets),
                        "hashesOnly": False,
                    },
                ],
            )
            await self._subscribe(_SubscriptionKind.HEADS, ["newHeads"])
            if self._options.log_addresses:
                await self._subscribe(
                    _SubscriptionKind.LOGS,
                    ["logs", {"address": list(self._options.log_addresses)}],
                )
        except BaseException:
            await self.stop()
            raise

    async def stop(self) -> None:
        """Stop the reader, fail waiters, and close the socket."""

        self._stopping = True
        self._running = False
        reader = self._reader_task
        self._reader_task = None
        event_worker = self._event_task
        self._event_task = None
        if reader is not None and reader is not asyncio.current_task():
            reader.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await reader
        if event_worker is not None and event_worker is not asyncio.current_task():
            event_worker.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await event_worker
        self._fail_pending_requests(AlchemyRpcDisconnectedError("Alchemy session stopped"))
        await self._socket.close()
        self._subscriptions.clear()
        self._disconnected.set()

    async def wait_disconnected(self) -> None:
        """Wait until the socket reader exits or the session is stopped."""

        await self._disconnected.wait()

    async def get_transaction_receipt(
        self,
        chain_id: int,
        transaction_hash: str,
    ) -> EvmReceipt | None:
        """Return a normalized receipt over the warm WebSocket.

        Args:
            chain_id: Expected configured Polygon chain identifier.
            transaction_hash: Source transaction hash.

        Returns:
            Mined receipt, or ``None`` while pending or unknown.

        Raises:
            ValueError: If ``chain_id`` differs from session configuration.
            AlchemyRpcError: If the socket disconnects or response is malformed.
        """

        self._require_chain(chain_id)
        result = await self._request("eth_getTransactionReceipt", [transaction_hash])
        if result is None:
            return None
        if not isinstance(result, dict):
            raise AlchemyRpcResponseError("Alchemy returned a malformed receipt")
        try:
            raw_logs = result.get("logs")
            logs = (
                tuple(dict(item) for item in raw_logs if isinstance(item, dict))
                if isinstance(raw_logs, list)
                else ()
            )
            return EvmReceipt(
                chain_id=chain_id,
                transaction_hash=str(result.get("transactionHash") or "").lower(),
                status=ReceiptStatus(self._parse_quantity(result.get("status"))),
                block_number=self._parse_quantity(result.get("blockNumber")),
                block_hash=str(result.get("blockHash") or "").lower(),
                logs=logs,
            )
        except (TypeError, ValueError) as exc:
            raise AlchemyRpcResponseError("Alchemy returned a malformed receipt") from exc

    async def get_confirmed_nonce(self, chain_id: int, address: str) -> int:
        """Return the confirmed account nonce over the warm WebSocket.

        Args:
            chain_id: Expected configured Polygon chain identifier.
            address: Transaction sender address.

        Returns:
            Non-negative confirmed nonce.

        Raises:
            ValueError: If ``chain_id`` differs from session configuration.
            AlchemyRpcError: If the socket disconnects or response is malformed.
        """

        self._require_chain(chain_id)
        result = await self._request("eth_getTransactionCount", [address, "latest"])
        try:
            return self._parse_quantity(result)
        except (TypeError, ValueError) as exc:
            raise AlchemyRpcResponseError("Alchemy returned a malformed nonce") from exc

    async def _subscribe(self, kind: _SubscriptionKind, params: list[object]) -> None:
        result = await self._request("eth_subscribe", params)
        subscription_id = str(result or "").strip()
        if not subscription_id:
            raise AlchemyRpcResponseError("Alchemy returned an empty subscription id")
        self._subscriptions[subscription_id] = kind

    async def _request(self, method: str, params: list[object]) -> object:
        if not self._running:
            raise AlchemyRpcDisconnectedError("Alchemy session is not running")
        request_id = self._next_request_id
        self._next_request_id += 1
        future: asyncio.Future[object] = asyncio.get_running_loop().create_future()
        self._pending_requests[request_id] = future
        request: dict[str, object] = {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
            "params": params,
        }
        try:
            await self._socket.send(orjson.dumps(request).decode("utf-8"))
            return await future
        except BaseException:
            self._pending_requests.pop(request_id, None)
            if not future.done():
                future.cancel()
            raise

    async def _reader_loop(self) -> None:
        try:
            while self._running:
                raw = await self._socket.recv()
                await self._handle_frame(raw)
        except asyncio.CancelledError:
            raise
        except Exception:
            await self._handle_disconnect(
                AlchemyRpcDisconnectedError("Alchemy WebSocket disconnected")
            )
        finally:
            self._disconnected.set()

    async def _event_loop(self) -> None:
        try:
            while self._running:
                kind, payload = await self._event_queue.get()
                try:
                    await self._dispatch_event(kind, payload)
                finally:
                    self._event_queue.task_done()
        except asyncio.CancelledError:
            raise
        except Exception:
            await self._handle_disconnect(
                AlchemyRpcDisconnectedError("Alchemy event dispatch failed")
            )
        finally:
            self._disconnected.set()

    async def _handle_frame(self, raw: str | bytes) -> None:
        receipt = self._receipt_clock.capture()
        try:
            payload = orjson.loads(raw)
        except (orjson.JSONDecodeError, TypeError):
            return
        if not isinstance(payload, dict):
            return
        if "id" in payload:
            self._complete_request(payload)
            return
        if payload.get("method") != "eth_subscription":
            return
        params = payload.get("params")
        if not isinstance(params, dict):
            return
        subscription_id = str(params.get("subscription") or "")
        kind = self._subscriptions.get(subscription_id)
        result = params.get("result")
        if kind is _SubscriptionKind.PENDING and isinstance(result, dict):
            self._enqueue_event(kind, {**result, "_source_receipt": receipt})
        elif kind is _SubscriptionKind.HEADS and isinstance(result, dict):
            head = self._parse_head(result)
            if head is not None:
                self._enqueue_event(kind, head)
        elif kind is _SubscriptionKind.LOGS and isinstance(result, dict):
            self._enqueue_event(kind, {**result, "_source_receipt": receipt})

    def _enqueue_event(self, kind: _SubscriptionKind, payload: object) -> None:
        try:
            self._event_queue.put_nowait((kind, payload))
        except asyncio.QueueFull as exc:
            raise AlchemyRpcResponseError("Alchemy event backlog exceeded its bound") from exc

    async def _dispatch_event(self, kind: _SubscriptionKind, payload: object) -> None:
        if kind is _SubscriptionKind.PENDING and isinstance(payload, dict):
            await self._sink.on_pending_transaction(dict(payload))
        elif kind is _SubscriptionKind.HEADS and isinstance(payload, EvmHead):
            await self._sink.on_new_head(payload)
        elif kind is _SubscriptionKind.LOGS and isinstance(payload, dict):
            await self._sink.on_log(dict(payload))

    async def _handle_disconnect(self, error: AlchemyRpcError) -> None:
        self._running = False
        self._fail_pending_requests(error)
        if self._stopping:
            return
        async with self._disconnect_lock:
            if self._disconnect_notified:
                return
            self._disconnect_notified = True
            await self._sink.on_disconnect()

    def _complete_request(self, payload: dict[object, object]) -> None:
        raw_request_id = payload.get("id")
        if not isinstance(raw_request_id, (str, int)):
            return
        try:
            request_id = int(raw_request_id)
        except ValueError:
            return
        future = self._pending_requests.pop(request_id, None)
        if future is None or future.done():
            return
        error = payload.get("error")
        if isinstance(error, dict):
            code = error.get("code")
            future.set_exception(
                AlchemyRpcResponseError(f"Alchemy RPC request failed with code {code}")
            )
            return
        future.set_result(payload.get("result"))

    def _parse_head(self, result: dict[object, object]) -> EvmHead | None:
        try:
            return EvmHead(
                chain_id=self._options.chain_id,
                number=self._parse_quantity(result.get("number")),
                block_hash=str(result.get("hash") or "").lower(),
                parent_hash=str(result.get("parentHash") or "").lower(),
                observed_at=self._clock.now(),
            )
        except (TypeError, ValueError):
            return None

    def _require_chain(self, chain_id: int) -> None:
        if chain_id != self._options.chain_id:
            raise ValueError(
                f"chain_id {chain_id} does not match configured chain {self._options.chain_id}"
            )

    def _fail_pending_requests(self, error: AlchemyRpcError) -> None:
        futures = tuple(self._pending_requests.values())
        self._pending_requests.clear()
        for future in futures:
            if not future.done():
                future.set_exception(error)

    @staticmethod
    def _parse_quantity(value: object) -> int:
        if isinstance(value, bool):
            raise TypeError("boolean is not an EVM quantity")
        if isinstance(value, int):
            if value < 0:
                raise ValueError("EVM quantity must be non-negative")
            return value
        if isinstance(value, str):
            parsed = int(value, 16) if value.lower().startswith("0x") else int(value)
            if parsed < 0:
                raise ValueError("EVM quantity must be non-negative")
            return parsed
        raise TypeError("EVM quantity must be an integer or string")


__all__ = [
    "AlchemyJsonRpcSession",
    "AlchemyRpcDisconnectedError",
    "AlchemyRpcError",
    "AlchemyRpcResponseError",
    "AlchemySubscriptionOptions",
]
