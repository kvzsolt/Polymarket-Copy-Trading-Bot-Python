from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock, patch

import pytest

from services.tradeMonitor import LeaderMonitorState, fetch_trade_data
from storage.sqlite.cursors_repo import TradeCursor


@pytest.mark.asyncio
async def test_late_trade_newer_than_cursor_is_processed_once():
    now = int(time.time())
    late_trade = {
        "transactionHash": "0xlate",
        "timestamp": now - 120,  # older than 60s, must still be processed
        "type": "TRADE",
        "side": "BUY",
        "asset": "token-1",
        "conditionId": "cond-1",
        "size": 10,
        "price": 0.5,
        "usdcSize": 5,
    }
    calls: list[list[dict]] = []

    async def on_trade(trades: list[dict]):
        calls.append(trades)

    state = LeaderMonitorState(
        last_cursor=TradeCursor(timestamp=now - 300, tx_hash="", trade_id=None),
        last_processed_ts=float(now - 300),
    )

    with patch("services.tradeMonitor.fetch_data", new_callable=AsyncMock, return_value=[late_trade]):
        await fetch_trade_data("0xleader", on_trade, state)
        await fetch_trade_data("0xleader", on_trade, state)

    assert len(calls) == 1
    assert calls[0][0]["transactionHash"] == "0xlate"
    assert int(state.last_cursor.timestamp) == int(now - 120)
    assert state.last_cursor.tx_hash == "0xlate"


@pytest.mark.asyncio
async def test_new_trade_schedules_research_capture_without_blocking_execution():
    now = int(time.time())
    late_trade = {
        "transactionHash": "0xlate",
        "timestamp": now - 120,
        "type": "TRADE",
        "side": "BUY",
        "asset": "token-1",
        "conditionId": "cond-1",
        "size": 10,
        "price": 0.5,
        "usdcSize": 5,
    }
    on_trade = AsyncMock()
    research_capture = AsyncMock()
    research_capture.capture_observed_trades = AsyncMock()
    state = LeaderMonitorState(
        last_cursor=TradeCursor(timestamp=now - 300, tx_hash="", trade_id=None),
        last_processed_ts=float(now - 300),
    )

    scheduled: list[asyncio.Task] = []
    original_create_task = asyncio.create_task

    def _capture_task(coro):
        task = original_create_task(coro)
        scheduled.append(task)
        return task

    with patch("services.tradeMonitor.fetch_data", new_callable=AsyncMock, return_value=[late_trade]), \
         patch("services.tradeMonitor.asyncio.create_task", side_effect=_capture_task):
        await fetch_trade_data(
            "0xleader",
            on_trade,
            state,
            research_capture_service=research_capture,
        )
        if scheduled:
            await asyncio.gather(*scheduled)

    on_trade.assert_awaited_once()
    research_capture.capture_observed_trades.assert_awaited_once()
