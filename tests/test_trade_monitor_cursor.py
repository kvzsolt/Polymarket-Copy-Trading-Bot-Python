from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock, patch

import pytest

from services.tradeMonitor import LeaderMonitorState, fetch_trade_data, init
from storage.sqlite.cursors_repo import TradeCursor


@pytest.mark.asyncio
async def test_init_sets_startup_watermark_from_latest_activity_when_no_cursor_exists():
    activities = [
        {
            "id": "old",
            "transactionHash": "0xold",
            "timestamp": 1_700_000_000,
            "type": "TRADE",
        },
        {
            "id": "new",
            "transactionHash": "0xnew",
            "timestamp": 1_700_000_010,
            "type": "TRADE",
        },
    ]
    state = LeaderMonitorState()

    with patch("services.tradeMonitor.fetch_data", new_callable=AsyncMock, return_value=activities):
        await init("0x1111111111111111111111111111111111111111", state)

    assert int(state.last_cursor.timestamp) == 1_700_000_010
    assert state.last_cursor.tx_hash == "0xnew"
    assert state.last_cursor.trade_id == "new"
    assert state.last_processed_ts == float(1_700_000_010)


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
async def test_init_preserves_existing_cursor_on_restart():
    activities = [
        {
            "id": "newer-but-should-not-overwrite",
            "transactionHash": "0xnew",
            "timestamp": 1_700_000_010,
            "type": "TRADE",
        }
    ]
    state = LeaderMonitorState(
        last_cursor=TradeCursor(timestamp=1_700_000_100, tx_hash="0xpersisted", trade_id="persisted"),
        last_processed_ts=1_700_000_100.0,
    )

    with patch("services.tradeMonitor.fetch_data", new_callable=AsyncMock, return_value=activities):
        await init("0x1111111111111111111111111111111111111111", state)

    assert int(state.last_cursor.timestamp) == 1_700_000_100
    assert state.last_cursor.tx_hash == "0xpersisted"
    assert state.last_cursor.trade_id == "persisted"


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


@pytest.mark.asyncio
async def test_cursor_comparison_normalizes_hash_and_trade_id_on_restart():
    state = LeaderMonitorState(
        last_cursor=TradeCursor(timestamp=1_700_000_010, tx_hash="0XABC", trade_id="TRADE-1"),
        last_processed_ts=1_700_000_010.0,
    )
    duplicate_trade = {
        "id": "trade-1",
        "transactionHash": "0xabc",
        "timestamp": 1_700_000_010,
        "type": "TRADE",
        "side": "BUY",
        "asset": "token-1",
        "conditionId": "cond-1",
        "size": 5,
        "price": 0.4,
        "usdcSize": 2,
    }
    on_trade = AsyncMock()

    with patch("services.tradeMonitor.fetch_data", new_callable=AsyncMock, return_value=[duplicate_trade]):
        await fetch_trade_data("0xleader", on_trade, state)

    on_trade.assert_not_awaited()


@pytest.mark.asyncio
async def test_detected_trade_logs_emit_before_trade_callback():
    now = int(time.time())
    trade = {
        "id": "trade-2",
        "transactionHash": "0xabc2",
        "timestamp": now,
        "type": "TRADE",
        "side": "BUY",
        "asset": "token-2",
        "conditionId": "cond-2",
        "size": 8,
        "price": 0.25,
        "usdcSize": 2,
    }
    state = LeaderMonitorState(
        last_cursor=TradeCursor(timestamp=now - 100, tx_hash="", trade_id=None),
        last_processed_ts=float(now - 100),
    )
    events: list[str] = []

    async def on_trade(_trades: list[dict]):
        events.append("callback")

    with patch("services.tradeMonitor.fetch_data", new_callable=AsyncMock, return_value=[trade]), \
         patch(
             "services.tradeMonitor.print_new_trade_detected",
             side_effect=lambda _trade: events.append("console"),
         ) as print_detected, \
         patch(
             "services.tradeMonitor.log_trade_detected",
             side_effect=lambda *_args, **_kwargs: events.append("structured"),
         ) as log_detected:
        await fetch_trade_data("0xleader", on_trade, state)

    print_detected.assert_called_once()
    log_detected.assert_called_once()
    assert events == ["console", "structured", "callback"]
