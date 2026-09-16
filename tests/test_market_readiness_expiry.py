"""Expired book observations must change health even while the socket is silent."""

import asyncio
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, AsyncMock

import pytest

from cache.market_state_store import BookSnapshot, QuoteSnapshot, TickSnapshot
from orchestrators.market_data_runtime import MarketDataRuntime
from services.healthServer import ReadinessTracker


class ControlledSleep:
    """Release one loop iteration explicitly instead of waiting on wall time."""

    def __init__(self) -> None:
        self.entered: asyncio.Queue[float] = asyncio.Queue()
        self.advance: asyncio.Queue[None] = asyncio.Queue()
        self.cancelled = False

    async def __call__(self, delay: float) -> None:
        self.entered.put_nowait(delay)
        try:
            await self.advance.get()
        except asyncio.CancelledError:
            self.cancelled = True
            raise


@pytest.mark.asyncio
async def test_silent_quote_expiry_updates_readiness_and_owned_timer_is_awaited(
    monkeypatch,
) -> None:
    transport_stopped = asyncio.Event()

    async def connect(tokens):
        await transport_stopped.wait()

    monkeypatch.setattr(
        "orchestrators.market_data_runtime.MarketWebSocket",
        lambda **kwargs: SimpleNamespace(connect=connect, stop=AsyncMock()),
    )
    store = MagicMock()
    store.is_resolved.return_value = False
    store.is_dirty.return_value = False
    store.is_stale.return_value = False
    store.get_quote.return_value = QuoteSnapshot(
        "token", Decimal(".4"), Decimal(".6"), None, "wss", 10.0
    )
    store.get_book.return_value = BookSnapshot(
        "token", {}, {}, Decimal(".4"), Decimal(".6"), "wss", 10.0
    )
    store.get_tick.return_value = TickSnapshot(
        "token", Decimal(".005"), Decimal(5), False, "rest", 10.0
    )
    now = [10.0]
    sleeper = ControlledSleep()
    readiness = ReadinessTracker()
    runtime = MarketDataRuntime(
        market_store=store,
        gateway=AsyncMock(),
        readiness=readiness,
        config=SimpleNamespace(),
        monotonic=lambda: now[0],
        sleep=sleeper,
    )
    try:
        await runtime.start(["token"])
        await runtime._on_wss_connected()
        assert await sleeper.entered.get() == 1.0
        assert readiness.snapshot()["market_data_ready"] is True
        now[0] = 12.1
        sleeper.advance.put_nowait(None)
        assert await sleeper.entered.get() == 1.0
        assert readiness.snapshot()["market_data_ready"] is False
        assert readiness.snapshot()["stale_token_count"] == 1
    finally:
        transport_stopped.set()
        await runtime.stop()
    assert sleeper.cancelled is True


@pytest.mark.asyncio
async def test_cancelled_socket_shutdown_still_awaits_owned_freshness_loop(monkeypatch) -> None:
    from tests.test_book_update_atomicity import seeded_store

    stopping = asyncio.Event()
    release = asyncio.Event()

    async def connect(tokens):
        await release.wait()

    async def stop_socket():
        stopping.set()
        await release.wait()

    monkeypatch.setattr(
        "orchestrators.market_data_runtime.MarketWebSocket",
        lambda **kwargs: SimpleNamespace(connect=connect, stop=stop_socket),
    )
    sleeper = ControlledSleep()
    runtime = MarketDataRuntime(
        market_store=seeded_store(),
        gateway=AsyncMock(),
        readiness=ReadinessTracker(),
        config=SimpleNamespace(),
        monotonic=lambda: 0,
        sleep=sleeper,
    )
    await runtime.start(["token"])
    await sleeper.entered.get()
    shutdown = asyncio.create_task(runtime.stop())
    try:
        await stopping.wait()
        shutdown.cancel()
        with pytest.raises(asyncio.CancelledError):
            await shutdown
        assert sleeper.cancelled is True
        assert runtime._freshness_task is None
        assert runtime._ws_task is None
    finally:
        release.set()
        await runtime.stop()
