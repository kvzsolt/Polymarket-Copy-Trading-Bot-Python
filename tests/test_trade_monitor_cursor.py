from __future__ import annotations

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
