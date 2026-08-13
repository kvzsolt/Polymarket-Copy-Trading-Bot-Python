"""Tests for deterministic Alchemy reconnect and resynchronization handling."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta

import pytest

from adapters.polygon.alchemy_connection_manager import AlchemyConnectionManager
from adapters.polygon.alchemy_json_rpc import AlchemySubscriptionOptions
from domain.source_transactions import EvmHead

_CHAIN_ID = 137
_NOW = datetime(2026, 8, 13, 10, 0, tzinfo=UTC)
_EXCHANGE = "0x" + "11" * 20


class FakeClock:
    """Fixed adapter clock."""

    def now(self) -> datetime:
        """Return the deterministic instant."""

        return _NOW


class FakeSocket:
    """Queue-backed socket for connection-manager tests."""

    def __init__(self, prefix: str) -> None:
        self.prefix = prefix
        self.sent: list[dict[str, object]] = []
        self.closed = False
        self._incoming: asyncio.Queue[str | BaseException] = asyncio.Queue()
        self._condition = asyncio.Condition()

    async def send(self, payload: str) -> None:
        """Record one outgoing frame."""

        decoded = json.loads(payload)
        assert isinstance(decoded, dict)
        async with self._condition:
            self.sent.append(decoded)
            self._condition.notify_all()

    async def recv(self) -> str:
        """Return or raise the next frame."""

        item = await self._incoming.get()
        if isinstance(item, BaseException):
            raise item
        return item

    async def close(self) -> None:
        """Mark the socket closed."""

        self.closed = True

    async def feed(self, payload: dict[str, object]) -> None:
        """Queue one incoming JSON-RPC frame."""

        await self._incoming.put(json.dumps(payload))

    async def fail(self) -> None:
        """Disconnect the socket reader."""

        await self._incoming.put(ConnectionError("closed"))

    async def wait_for_sends(self, count: int) -> None:
        """Wait until ``count`` outgoing frames exist."""

        async with self._condition:
            await self._condition.wait_for(lambda: len(self.sent) >= count)


class FakeConnector:
    """Return queued sockets and expose deterministic connection barriers."""

    def __init__(self, sockets: tuple[FakeSocket, ...]) -> None:
        self._sockets: asyncio.Queue[FakeSocket] = asyncio.Queue()
        for socket in sockets:
            self._sockets.put_nowait(socket)
        self.urls: list[str] = []
        self._condition = asyncio.Condition()

    async def connect(self, url: str) -> FakeSocket:
        """Return the next configured socket."""

        socket = await self._sockets.get()
        async with self._condition:
            self.urls.append(url)
            self._condition.notify_all()
        return socket

    async def wait_for_connections(self, count: int) -> None:
        """Wait until ``count`` connect calls occurred."""

        async with self._condition:
            await self._condition.wait_for(lambda: len(self.urls) >= count)


class FakeSleeper:
    """Block reconnect delay until explicitly released."""

    def __init__(self) -> None:
        self.delays: list[timedelta] = []
        self.called = asyncio.Event()
        self._releases: asyncio.Queue[None] = asyncio.Queue()

    async def sleep(self, delay: timedelta) -> None:
        """Record the delay and wait for test-controlled release."""

        self.delays.append(delay)
        self.called.set()
        await self._releases.get()

    def release(self) -> None:
        """Release one pending reconnect wait."""

        self._releases.put_nowait(None)


class FakeReconnectPolicy:
    """Return a fixed deterministic reconnect delay."""

    def delay_for(self, attempt: int) -> timedelta:
        """Return a distinct delay for ``attempt``."""

        return timedelta(seconds=attempt + 1)


class FakePipelineSink:
    """Capture disconnect and reconnect resynchronization calls."""

    def __init__(self) -> None:
        self.disconnects = 0
        self.resyncs = 0
        self.disconnect_ready = asyncio.Event()
        self.resync_ready = asyncio.Event()

    async def on_pending_transaction(self, transaction: dict[str, object]) -> None:
        """Ignore pending events for this lifecycle test."""

        del transaction

    async def on_new_head(self, head: EvmHead) -> None:
        """Ignore head events for this lifecycle test."""

        del head

    async def on_log(self, log: dict[str, object]) -> None:
        """Ignore log events for this lifecycle test."""

        del log

    async def on_disconnect(self) -> None:
        """Record fail-closed connection loss."""

        self.disconnects += 1
        self.disconnect_ready.set()

    async def resynchronize(self) -> None:
        """Record one completed subscription resynchronization."""

        self.resyncs += 1
        self.resync_ready.set()


async def _ack_subscriptions(socket: FakeSocket) -> None:
    await socket.wait_for_sends(1)
    await socket.feed({"jsonrpc": "2.0", "id": 1, "result": f"{socket.prefix}-pending"})
    await socket.wait_for_sends(2)
    await socket.feed({"jsonrpc": "2.0", "id": 2, "result": f"{socket.prefix}-heads"})


@pytest.mark.asyncio
async def test_disconnect_reconnects_and_resynchronizes_before_reuse() -> None:
    """Reconnect re-subscribes then deterministically resynchronizes gate state."""

    first = FakeSocket("first")
    second = FakeSocket("second")
    connector = FakeConnector((first, second))
    sleeper = FakeSleeper()
    sink = FakePipelineSink()
    manager = AlchemyConnectionManager(
        url="wss://polygon.example.invalid/v2/redacted",
        connector=connector,
        options=AlchemySubscriptionOptions(
            chain_id=_CHAIN_ID,
            pending_targets=(_EXCHANGE,),
            log_addresses=(),
        ),
        sink=sink,
        clock=FakeClock(),
        reconnect_policy=FakeReconnectPolicy(),
        sleeper=sleeper,
    )

    start_task = asyncio.create_task(manager.start())
    await connector.wait_for_connections(1)
    await _ack_subscriptions(first)
    await start_task
    assert sink.resyncs == 1

    await first.fail()
    await sink.disconnect_ready.wait()
    await sleeper.called.wait()
    assert sleeper.delays == [timedelta(seconds=1)]

    sink.resync_ready.clear()
    sleeper.release()
    await connector.wait_for_connections(2)
    await _ack_subscriptions(second)
    await sink.resync_ready.wait()

    assert sink.disconnects == 1
    assert sink.resyncs == 2
    assert connector.urls == [
        "wss://polygon.example.invalid/v2/redacted",
        "wss://polygon.example.invalid/v2/redacted",
    ]
    await manager.stop()
    assert second.closed is True


@pytest.mark.asyncio
async def test_rpc_fails_closed_before_first_connection() -> None:
    """Receipt calls cannot fall back to polling or another lazy transport."""

    manager = AlchemyConnectionManager(
        url="wss://polygon.example.invalid/v2/redacted",
        connector=FakeConnector((FakeSocket("unused"),)),
        options=AlchemySubscriptionOptions(
            chain_id=_CHAIN_ID,
            pending_targets=(_EXCHANGE,),
            log_addresses=(),
        ),
        sink=FakePipelineSink(),
        clock=FakeClock(),
        reconnect_policy=FakeReconnectPolicy(),
        sleeper=FakeSleeper(),
    )

    with pytest.raises(RuntimeError, match="not connected"):
        await manager.get_transaction_receipt(_CHAIN_ID, "0x" + "aa" * 32)
