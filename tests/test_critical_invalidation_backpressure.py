"""Critical quote invalidation cannot be dropped with optional state callbacks."""

import asyncio

import pytest

from pmcopy.clients.ws.market_ws import MarketWebSocket
from tests.test_book_update_atomicity import seeded_store
from tests.test_market_data_runtime import RecordingQuoteSink, _make_runtime


@pytest.mark.asyncio
async def test_real_runtime_invalidates_quote_even_when_callback_queue_is_full(monkeypatch):
    released = asyncio.Event()

    async def connect(socket, tokens):
        await released.wait()

    monkeypatch.setattr(MarketWebSocket, "connect", connect)
    sink = RecordingQuoteSink()
    runtime = _make_runtime(store=seeded_store(), quote_sink=sink)
    await runtime.start(["token"])
    try:
        await runtime._on_market_state_change("token", "book")
        assert len(sink.quotes) == 1
        socket = runtime._ws_client
        assert socket is not None
        for _ in range(128):
            socket._notify_state_change("token", "book")
        assert len(socket._tasks) == 128
        socket._handle_book({"asset_id": "token", "bids": [{"price": "bad", "size": "1"}]})
        assert "token" in sink.discarded
        assert len(socket._tasks) == 128
    finally:
        released.set()
        await runtime.stop()
