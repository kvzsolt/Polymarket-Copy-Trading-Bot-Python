"""Release-gate: WSS heartbeat cadence (Issue #75 item 4).

Asserts that the market WebSocket client:
* uses the 5-second ``HEARTBEAT_INTERVAL`` mandated by the Polymarket spec;
* sends the literal string ``"PING"`` per the spec (no JSON wrapping);
* exits the heartbeat loop on a send failure so the reconnect loop can take
  over rather than busy-looping on a dead socket;
* sends a ``type: "market"`` subscription payload that uses the
  ``assets_ids`` key (spec) and not the wrong-cased ``asset_ids``.
"""
from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from clients.market_ws import MarketWebSocket
from clients.ws_constants import HEARTBEAT_INTERVAL
from services.price_cache import PositionPriceCache


def test_heartbeat_interval_is_five_seconds() -> None:
    assert HEARTBEAT_INTERVAL == 5


@pytest.mark.asyncio
async def test_heartbeat_sends_ping_string_at_cadence() -> None:
    cache = PositionPriceCache()
    ws_client = MarketWebSocket(cache)
    ws_client._asset_ids = ["tok"]
    ws_client._running = True
    fake_ws = MagicMock()
    fake_ws.send = AsyncMock()
    fake_ws.close = AsyncMock()

    sleep_calls: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)
        if len(sleep_calls) >= 2:
            ws_client._running = False

    with patch("clients.market_ws.asyncio.sleep", side_effect=fake_sleep):
        await ws_client._heartbeat(fake_ws)

    assert sleep_calls and all(s == HEARTBEAT_INTERVAL for s in sleep_calls)
    fake_ws.send.assert_awaited()
    fake_ws.send.assert_called_with("PING")


@pytest.mark.asyncio
async def test_heartbeat_exits_on_send_failure() -> None:
    cache = PositionPriceCache()
    ws_client = MarketWebSocket(cache)
    ws_client._asset_ids = ["tok"]
    ws_client._running = True
    fake_ws = MagicMock()
    fake_ws.send = AsyncMock(side_effect=ConnectionError("boom"))
    fake_ws.close = AsyncMock()

    async def instant_sleep(_seconds: float) -> None:
        return None

    with patch("clients.market_ws.asyncio.sleep", side_effect=instant_sleep):
        await asyncio.wait_for(ws_client._heartbeat(fake_ws), timeout=1.0)

    fake_ws.send.assert_awaited_once_with("PING")
    fake_ws.close.assert_awaited()


@pytest.mark.asyncio
async def test_subscribe_payload_uses_assets_ids() -> None:
    cache = PositionPriceCache()
    ws_client = MarketWebSocket(cache)
    ws_client._asset_ids = ["tokA", "tokB"]
    fake_ws = MagicMock()
    fake_ws.send = AsyncMock()
    await ws_client._subscribe(fake_ws)
    fake_ws.send.assert_awaited_once()
    payload = json.loads(fake_ws.send.await_args.args[0])
    assert payload["type"] == "market"
    assert payload["assets_ids"] == ["tokA", "tokB"]
    assert "asset_ids" not in payload
