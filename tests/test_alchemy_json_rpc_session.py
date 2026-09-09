"""Deterministic tests for multiplexed Alchemy WebSocket JSON-RPC handling."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime

import pytest

from adapters.polygon.alchemy_json_rpc import (
    AlchemyJsonRpcSession,
    AlchemyRpcDisconnectedError,
    AlchemySubscriptionOptions,
)
from domain.source_transactions import EvmHead, EvmReceipt, ReceiptStatus

_CHAIN_ID = 137
_NOW = datetime(2026, 8, 13, 10, 0, tzinfo=UTC)
_TX_HASH = "0x" + "ab" * 32
_BLOCK_HASH = "0x" + "cd" * 32
_PARENT_HASH = "0x" + "ef" * 32
_EXCHANGE = "0x" + "11" * 20
_SAFE = "0x" + "22" * 20


class FakeClock:
    """Fixed clock for decoded head timestamps."""

    def now(self) -> datetime:
        """Return the deterministic instant."""

        return _NOW


class FakeJsonRpcSocket:
    """Queue-backed WebSocket transport with deterministic send barriers."""

    def __init__(self) -> None:
        self.sent: list[dict[str, object]] = []
        self.closed = False
        self.recv_calls = 0
        self._incoming: asyncio.Queue[str | BaseException] = asyncio.Queue()
        self._send_condition = asyncio.Condition()

    async def send(self, payload: str) -> None:
        """Record one JSON-RPC request."""

        decoded = json.loads(payload)
        assert isinstance(decoded, dict)
        async with self._send_condition:
            self.sent.append(decoded)
            self._send_condition.notify_all()

    async def recv(self) -> str:
        """Return or raise the next queued incoming frame."""

        self.recv_calls += 1
        item = await self._incoming.get()
        if isinstance(item, BaseException):
            raise item
        return item

    async def close(self) -> None:
        """Mark the fake socket closed."""

        self.closed = True

    async def feed(self, payload: dict[str, object]) -> None:
        """Queue one server frame."""

        await self._incoming.put(json.dumps(payload))

    async def fail(self, error: BaseException) -> None:
        """Queue one receive failure."""

        await self._incoming.put(error)

    async def wait_for_send_count(self, count: int) -> None:
        """Wait deterministically until ``count`` requests were sent."""

        async with self._send_condition:
            await self._send_condition.wait_for(lambda: len(self.sent) >= count)


class FakeEventSink:
    """Capture typed pending, head, log, and disconnect events."""

    def __init__(self) -> None:
        self.pending: list[dict[str, object]] = []
        self.heads: list[EvmHead] = []
        self.logs: list[dict[str, object]] = []
        self.disconnects = 0
        self.pending_ready = asyncio.Event()
        self.head_ready = asyncio.Event()
        self.log_ready = asyncio.Event()
        self.disconnect_ready = asyncio.Event()
        self.session: AlchemyJsonRpcSession | None = None
        self.receipt: EvmReceipt | None = None
        self.receipt_ready = asyncio.Event()

    async def on_pending_transaction(self, transaction: dict[str, object]) -> None:
        """Record one full pending transaction."""

        self.pending.append(transaction)
        self.pending_ready.set()

    async def on_new_head(self, head: EvmHead) -> None:
        """Record one parsed head."""

        self.heads.append(head)
        self.head_ready.set()
        if self.session is not None:
            self.receipt = await self.session.get_transaction_receipt(_CHAIN_ID, _TX_HASH)
            self.receipt_ready.set()

    async def on_log(self, log: dict[str, object]) -> None:
        """Record one raw EVM log."""

        self.logs.append(log)
        self.log_ready.set()

    async def on_disconnect(self) -> None:
        """Record one unexpected connection loss."""

        self.disconnects += 1
        self.disconnect_ready.set()


async def _started_session() -> tuple[
    AlchemyJsonRpcSession,
    FakeJsonRpcSocket,
    FakeEventSink,
]:
    socket = FakeJsonRpcSocket()
    sink = FakeEventSink()
    session = AlchemyJsonRpcSession(
        socket=socket,
        options=AlchemySubscriptionOptions(
            chain_id=_CHAIN_ID,
            pending_targets=(_EXCHANGE, _SAFE),
            log_addresses=(_EXCHANGE,),
        ),
        sink=sink,
        clock=FakeClock(),
    )
    start_task = asyncio.create_task(session.start())
    await socket.wait_for_send_count(1)
    await socket.feed({"jsonrpc": "2.0", "id": 1, "result": "pending-sub"})
    await socket.wait_for_send_count(2)
    await socket.feed({"jsonrpc": "2.0", "id": 2, "result": "heads-sub"})
    await socket.wait_for_send_count(3)
    await socket.feed({"jsonrpc": "2.0", "id": 3, "result": "logs-sub"})
    await start_task
    return session, socket, sink


@pytest.mark.asyncio
async def test_session_subscribes_pending_heads_and_logs_on_one_socket() -> None:
    """One warm connection carries all required Alchemy subscriptions."""

    session, socket, _ = await _started_session()
    try:
        assert [request["method"] for request in socket.sent] == [
            "eth_subscribe",
            "eth_subscribe",
            "eth_subscribe",
        ]
        assert socket.sent[0]["params"] == [
            "alchemy_pendingTransactions",
            {"toAddress": [_EXCHANGE, _SAFE], "hashesOnly": False},
        ]
        assert socket.sent[1]["params"] == ["newHeads"]
        assert socket.sent[2]["params"] == [
            "logs",
            {"address": [_EXCHANGE]},
        ]
    finally:
        await session.stop()


@pytest.mark.asyncio
async def test_session_routes_full_pending_head_and_log_notifications() -> None:
    """Subscription identifiers route typed events without polling."""

    session, socket, sink = await _started_session()
    pending = {"hash": _TX_HASH, "from": _SAFE, "to": _EXCHANGE, "input": "0x"}
    raw_head = {
        "number": "0x65",
        "hash": _BLOCK_HASH,
        "parentHash": _PARENT_HASH,
    }
    raw_log = {"transactionHash": _TX_HASH, "logIndex": "0x3"}
    try:
        await socket.feed(
            {
                "jsonrpc": "2.0",
                "method": "eth_subscription",
                "params": {"subscription": "pending-sub", "result": pending},
            }
        )
        await socket.feed(
            {
                "jsonrpc": "2.0",
                "method": "eth_subscription",
                "params": {"subscription": "heads-sub", "result": raw_head},
            }
        )
        await socket.feed(
            {
                "jsonrpc": "2.0",
                "method": "eth_subscription",
                "params": {"subscription": "logs-sub", "result": raw_log},
            }
        )
        await sink.pending_ready.wait()
        await sink.head_ready.wait()
        await sink.log_ready.wait()

        assert len(sink.pending) == len(sink.logs) == 1
        assert {
            key: value for key, value in sink.pending[0].items() if key != "_source_receipt"
        } == pending
        assert {
            key: value for key, value in sink.logs[0].items() if key != "_source_receipt"
        } == raw_log
        assert sink.heads == [
            EvmHead(
                chain_id=_CHAIN_ID,
                number=101,
                block_hash=_BLOCK_HASH,
                parent_hash=_PARENT_HASH,
                observed_at=_NOW,
            )
        ]
    finally:
        await session.stop()


@pytest.mark.asyncio
async def test_receipt_lookup_uses_same_socket_and_parses_success() -> None:
    """A head-triggered receipt request shares the warm subscription socket."""

    session, socket, _ = await _started_session()
    try:
        receipt_task = asyncio.create_task(session.get_transaction_receipt(_CHAIN_ID, _TX_HASH))
        await socket.wait_for_send_count(4)
        assert socket.sent[3]["method"] == "eth_getTransactionReceipt"
        assert socket.sent[3]["params"] == [_TX_HASH]
        await socket.feed(
            {
                "jsonrpc": "2.0",
                "id": 4,
                "result": {
                    "transactionHash": _TX_HASH,
                    "status": "0x1",
                    "blockNumber": "0x65",
                    "blockHash": _BLOCK_HASH,
                    "logs": [],
                },
            }
        )

        receipt = await receipt_task

        assert receipt == EvmReceipt(
            chain_id=_CHAIN_ID,
            transaction_hash=_TX_HASH,
            status=ReceiptStatus.SUCCESS,
            block_number=101,
            block_hash=_BLOCK_HASH,
            logs=(),
        )
    finally:
        await session.stop()


@pytest.mark.asyncio
async def test_head_callback_receipt_lookup_cannot_block_socket_reader() -> None:
    """Notification dispatch leaves the sole reader free for RPC responses."""

    socket = FakeJsonRpcSocket()
    sink = FakeEventSink()
    session = AlchemyJsonRpcSession(
        socket=socket,
        options=AlchemySubscriptionOptions(
            chain_id=_CHAIN_ID,
            pending_targets=(_EXCHANGE,),
            log_addresses=(),
        ),
        sink=sink,
        clock=FakeClock(),
    )
    sink.session = session
    start_task = asyncio.create_task(session.start())
    await socket.wait_for_send_count(1)
    await socket.feed({"jsonrpc": "2.0", "id": 1, "result": "pending-sub"})
    await socket.wait_for_send_count(2)
    await socket.feed({"jsonrpc": "2.0", "id": 2, "result": "heads-sub"})
    await start_task
    try:
        await socket.feed(
            {
                "jsonrpc": "2.0",
                "method": "eth_subscription",
                "params": {
                    "subscription": "heads-sub",
                    "result": {
                        "number": "0x65",
                        "hash": _BLOCK_HASH,
                        "parentHash": _PARENT_HASH,
                    },
                },
            }
        )
        await socket.wait_for_send_count(3)

        assert socket.recv_calls >= 4

        await socket.feed(
            {
                "jsonrpc": "2.0",
                "id": 3,
                "result": {
                    "transactionHash": _TX_HASH,
                    "status": "0x1",
                    "blockNumber": "0x65",
                    "blockHash": _BLOCK_HASH,
                    "logs": [],
                },
            }
        )
        await sink.receipt_ready.wait()
        assert sink.receipt is not None
    finally:
        await session.stop()


@pytest.mark.asyncio
async def test_null_receipt_and_confirmed_nonce_are_parsed_without_retry_loop() -> None:
    """Unknown receipt returns None and nonce lookup remains event-triggered."""

    session, socket, _ = await _started_session()
    try:
        receipt_task = asyncio.create_task(session.get_transaction_receipt(_CHAIN_ID, _TX_HASH))
        await socket.wait_for_send_count(4)
        await socket.feed({"jsonrpc": "2.0", "id": 4, "result": None})
        assert await receipt_task is None

        nonce_task = asyncio.create_task(session.get_confirmed_nonce(_CHAIN_ID, _SAFE))
        await socket.wait_for_send_count(5)
        assert socket.sent[4]["method"] == "eth_getTransactionCount"
        assert socket.sent[4]["params"] == [_SAFE, "latest"]
        await socket.feed({"jsonrpc": "2.0", "id": 5, "result": "0x8"})
        assert await nonce_task == 8
    finally:
        await session.stop()


@pytest.mark.asyncio
async def test_disconnect_notifies_sink_and_fails_pending_rpc() -> None:
    """Connection loss fails closed and releases outstanding RPC waiters."""

    session, socket, sink = await _started_session()
    receipt_task = asyncio.create_task(session.get_transaction_receipt(_CHAIN_ID, _TX_HASH))
    await socket.wait_for_send_count(4)

    await socket.fail(ConnectionError("socket closed"))
    await sink.disconnect_ready.wait()

    with pytest.raises(AlchemyRpcDisconnectedError):
        await receipt_task
    assert sink.disconnects == 1
    await session.stop()
