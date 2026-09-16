"""Malformed public holdings evidence cannot erase an account's inventory."""

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from integrations.polymarket.data_api_positions import DataApiPositionSizeReader
from orchestrators import trade_pipeline
from services.risk import risk_monitor


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "reader",
    [trade_pipeline._fetch_live_position_size, risk_monitor._fetch_live_risk_position_size],
)
@pytest.mark.parametrize(
    "payload",
    [
        None,
        {},
        "bad",
        [None],
        [{"asset": "123", "size": True}],
        [{"asset": "123", "size": "NaN"}],
        [{"asset": "123", "size": "Infinity"}],
        [{"asset": "123", "size": "-1"}],
        [{"asset": "123"}],
    ],
)
async def test_invalid_holdings_remain_unknown(reader, payload, monkeypatch) -> None:
    fetch = AsyncMock(return_value=payload)
    http = SimpleNamespace(get_json=fetch)
    context = SimpleNamespace(
        user_id="account",
        wallet_address="0x" + "1" * 40,
        holdings_reader=DataApiPositionSizeReader(lambda: http),
    )
    assert await reader(context, token_id="123") is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "reader",
    [trade_pipeline._fetch_live_position_size, risk_monitor._fetch_live_risk_position_size],
)
async def test_dust_holdings_are_requested_explicitly(reader, monkeypatch) -> None:
    fetch = AsyncMock(return_value=[{"asset": "123", "size": "0.25"}])
    http = SimpleNamespace(get_json=fetch)
    context = SimpleNamespace(
        user_id="account",
        wallet_address="0x" + "1" * 40,
        holdings_reader=DataApiPositionSizeReader(lambda: http),
    )
    assert await reader(context, token_id="123") == Decimal("0.25")
    assert fetch.await_args.kwargs["params"]["sizeThreshold"] == 0
