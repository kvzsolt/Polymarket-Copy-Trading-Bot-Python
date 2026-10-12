"""Market callbacks have bounded ownership, observable failure and deterministic cleanup."""

import asyncio
from unittest.mock import Mock

import pytest

from pmcopy.clients import market_ws
from tests.test_book_update_atomicity import seeded_store


@pytest.mark.asyncio
async def test_stop_cancels_and_awaits_state_callback() -> None:
    started, cleaned = asyncio.Event(), asyncio.Event()

    async def observer(token: str, event: str) -> None:
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cleaned.set()

    socket = market_ws.MarketWebSocket(seeded_store(), on_state_change=observer)
    socket._notify_state_change("token", "book")
    await started.wait()
    try:
        await socket.stop()
        assert cleaned.is_set()
        assert not socket._tasks
    finally:
        for task in tuple(socket._tasks):
            task.cancel()
        await asyncio.gather(*socket._tasks, return_exceptions=True)


@pytest.mark.asyncio
async def test_pending_callbacks_are_bounded_and_overflow_is_counted(monkeypatch) -> None:
    metrics = Mock()
    monkeypatch.setattr(market_ws, "metrics", metrics)

    async def observer(token: str, event: str) -> None:
        await asyncio.Event().wait()

    socket = market_ws.MarketWebSocket(
        seeded_store(), on_state_change=observer, max_pending_callbacks=2
    )
    try:
        for _ in range(5):
            socket._notify_state_change("token", "book")
        assert len(socket._tasks) == 2
        assert (
            metrics.inc.call_args_list.count(
                (("market_ws_callback_dropped_total",), {"labels": {"reason": "capacity"}})
            )
            == 3
        )
    finally:
        await socket.stop()


@pytest.mark.asyncio
async def test_failed_callback_is_observed_and_error_text_is_private(monkeypatch) -> None:
    logger = Mock()
    monkeypatch.setattr(market_ws, "logger", logger)

    async def observer(token: str, event: str) -> None:
        raise RuntimeError("SYNTHETIC_CALLBACK_SECRET")

    socket = market_ws.MarketWebSocket(seeded_store(), on_state_change=observer)
    socket._notify_state_change("token", "book")
    await asyncio.gather(*socket._tasks, return_exceptions=True)
    assert logger.warning.called
    assert "SYNTHETIC_CALLBACK_SECRET" not in str(logger.mock_calls)
    assert not socket._tasks


@pytest.mark.asyncio
async def test_market_transport_has_bounded_receive_memory(monkeypatch) -> None:
    socket = market_ws.MarketWebSocket(seeded_store())
    options = {}

    class StoppedConnection:
        async def __aenter__(self):
            socket._running = False
            raise OSError("SYNTHETIC_CONNECTION_SECRET")

        async def __aexit__(self, *args):
            return False

    def connect(url, **kwargs):
        options.update(kwargs)
        return StoppedConnection()

    logger = Mock()
    monkeypatch.setattr(market_ws, "logger", logger)
    monkeypatch.setattr(market_ws.websockets, "connect", connect)
    await socket.connect(["token"])
    assert isinstance(options["max_queue"], int) and options["max_queue"] > 0
    assert isinstance(options["max_size"], int) and options["max_size"] > 0
    assert "SYNTHETIC_CONNECTION_SECRET" not in str(logger.mock_calls)
