"""Contract tests for read-only leader SELL fraction data adapters."""

from __future__ import annotations

from decimal import Decimal
from urllib.parse import parse_qs, urlparse

import pytest
from web3 import Web3

from adapters.polygon.historical_erc1155_balance import (
    Web3HistoricalErc1155BalanceReader,
)
from integrations.polymarket.leader_sell_fraction_data_api import (
    DataApiLeaderSellFractionReader,
)
from services.onchain_detection.trade_detection_models import (
    DetectionConfidence,
    OnchainTradeSignal,
)

_LEADER = "0x" + "ab" * 20
_TOKEN = "123456"
_TX = "0x" + "cd" * 32


class _ContractCall:
    """Capture the historical block passed to one contract call."""

    def __init__(self, raw_balance: int) -> None:
        self._raw_balance = raw_balance
        self.block_identifiers: list[int] = []

    def call(self, *, block_identifier: int) -> int:
        """Return a fixed six-decimal ERC-1155 balance."""

        self.block_identifiers.append(block_identifier)
        return self._raw_balance


class _ContractFunctions:
    """Expose the minimal ``balanceOf`` web3 contract surface."""

    def __init__(self, call: _ContractCall) -> None:
        self._call = call
        self.arguments: list[tuple[str, int]] = []

    def balanceOf(self, wallet: str, token_id: int) -> _ContractCall:  # noqa: N802
        """Capture normalized balance arguments and return the fake call."""

        self.arguments.append((wallet, token_id))
        return self._call


class _Contract:
    """Provide fake contract functions."""

    def __init__(self, call: _ContractCall) -> None:
        self.functions = _ContractFunctions(call)


@pytest.mark.asyncio
async def test_historical_reader_returns_exact_six_decimal_balance() -> None:
    """Raw ERC-1155 micro-units are converted with ``Decimal`` only."""

    call = _ContractCall(4_106_993_477)
    contract = _Contract(call)
    reader = Web3HistoricalErc1155BalanceReader(contract=contract)

    balance = await reader.balance_at_block(_LEADER, _TOKEN, 92_749_234)

    assert balance == Decimal("4106.993477")
    assert contract.functions.arguments == [(Web3.to_checksum_address(_LEADER), 123456)]
    assert call.block_identifiers == [92_749_234]


def _signal() -> OnchainTradeSignal:
    """Build a canonical SELL signal for the public-data fallback."""

    return OnchainTradeSignal(
        leader_wallet=_LEADER,
        token_id=_TOKEN,
        side="SELL",
        size=Decimal("40"),
        price=Decimal("0.2"),
        usdc_size=Decimal("8"),
        tx_hash=_TX,
        confidence=DetectionConfidence.MINED_AUTHORITATIVE,
        maker_side=1,
        watched_role="maker",
        event_ts=1_700_000_000,
        log_index=7,
        condition_id="0x" + "ef" * 32,
        provisional=False,
        exchange="0x" + "11" * 20,
        order_hash=None,
        block_number=101,
    )


@pytest.mark.asyncio
async def test_data_api_reader_rewinds_newer_activity_to_pre_sell_balance() -> None:
    """A lagged lookup reconstructs the balance immediately before the target tx."""

    calls: list[str] = []

    async def fetch_json(url: str) -> object:
        calls.append(url)
        if "/positions" in url:
            return [{"asset": _TOKEN, "size": "10"}]
        return [
            {
                "transactionHash": "0x" + "ff" * 32,
                "asset": _TOKEN,
                "side": "SELL",
                "size": "10",
                "timestamp": 1_700_000_002,
            },
            {
                "transactionHash": _TX,
                "asset": _TOKEN,
                "side": "SELL",
                "size": "40",
                "timestamp": 1_700_000_001,
            },
        ]

    reader = DataApiLeaderSellFractionReader(fetch_json=fetch_json)

    fraction = await reader.fraction_for(_signal())

    assert fraction == Decimal("0.6666666666666666666666666667")
    assert len(calls) == 2
    assert all(_LEADER in url for url in calls)
    activity_url = next(url for url in calls if "/activity" in url)
    activity_query = parse_qs(urlparse(activity_url).query)
    assert activity_query["market"] == [_signal().condition_id]
    assert activity_query["limit"] == ["500"]
    positions_url = next(url for url in calls if "/positions" in url)
    positions_query = parse_qs(urlparse(positions_url).query)
    assert positions_query["market"] == [_signal().condition_id]
    assert positions_query["sizeThreshold"] == ["0"]
    assert positions_query["limit"] == ["500"]


@pytest.mark.asyncio
async def test_data_api_reader_preserves_stable_order_for_equal_timestamps() -> None:
    """API-provided order disambiguates trades sharing one second timestamp."""

    async def fetch_json(url: str) -> object:
        if "/positions" in url:
            return [{"asset": _TOKEN, "size": "10"}]
        return [
            {
                "transactionHash": "0x" + "00" * 32,
                "asset": _TOKEN,
                "side": "SELL",
                "size": "10",
                "timestamp": 1_700_000_001,
            },
            {
                "transactionHash": _TX,
                "asset": _TOKEN,
                "side": "SELL",
                "size": "40",
                "timestamp": 1_700_000_001,
            },
        ]

    reader = DataApiLeaderSellFractionReader(fetch_json=fetch_json)

    fraction = await reader.fraction_for(_signal())

    assert fraction == Decimal("0.6666666666666666666666666667")


@pytest.mark.asyncio
async def test_data_api_reader_returns_none_without_canonical_transaction() -> None:
    """A snapshot without the target transaction is not accepted as evidence."""

    async def fetch_json(url: str) -> object:
        if "/positions" in url:
            return [{"asset": _TOKEN, "size": "10"}]
        return []

    reader = DataApiLeaderSellFractionReader(fetch_json=fetch_json)

    assert await reader.fraction_for(_signal()) is None
