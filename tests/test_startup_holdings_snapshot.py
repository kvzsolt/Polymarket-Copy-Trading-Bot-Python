"""Startup snapshots must retain zero-value shares and validate completeness."""

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from pmcopy.orchestrators import runtime_bootstrap

WALLET = "0x" + "1" * 40


@pytest.mark.asyncio
@pytest.mark.parametrize("value", ["0", "0.000000000000000001"])
async def test_startup_keeps_existing_shares_regardless_of_mark_value(value, monkeypatch) -> None:
    row = {"asset": "123", "conditionId": "0x" + "a" * 64, "size": "0.25", "currentValue": value}
    fetch = AsyncMock(return_value=[row])
    monkeypatch.setattr(
        runtime_bootstrap, "_get_http_client", lambda: SimpleNamespace(get_json=fetch)
    )
    result = await runtime_bootstrap.fetch_live_positions(WALLET)
    assert result is not None
    assert len(result) == 1
    assert Decimal(result[0]["size"]) == Decimal("0.25")


@pytest.mark.asyncio
@pytest.mark.parametrize("row", [None, {}, {"asset": "123", "size": "bad"}])
async def test_startup_invalid_row_returns_unknown(row, monkeypatch) -> None:
    monkeypatch.setattr(
        runtime_bootstrap,
        "_get_http_client",
        lambda: SimpleNamespace(get_json=AsyncMock(return_value=[row])),
    )
    assert await runtime_bootstrap.fetch_live_positions(WALLET) is None
