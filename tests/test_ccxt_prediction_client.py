from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from integrations.polymarket.ccxt_prediction_client import (
    CcxtPredictionConfigurationError,
    CcxtPredictionUnavailable,
    create_ccxt_prediction_client,
)


class FakeExchange:
    def __init__(self, settings):
        self.settings = settings
        self.options = settings["options"]
        self.privateKey = settings.get("privateKey")
        self.walletAddress = settings.get("walletAddress")
        self.close = AsyncMock()
        self.load_api_credentials = AsyncMock(return_value={"apiKey": "derived"})

    def polymarket_order_raw_amounts(self, *_args):
        return {"makerAmount": "old", "takerAmount": "old"}


@pytest.mark.asyncio
async def test_public_client_never_receives_secrets() -> None:
    created: list[FakeExchange] = []

    def factory(settings):
        exchange = FakeExchange(settings)
        created.append(exchange)
        return exchange

    client = await create_ccxt_prediction_client(
        private_key="0x" + "a" * 64,
        wallet_address="0x" + "b" * 40,
        authenticated=False,
        exchange_factory=factory,
        ccxt_version="4.5.70",
    )

    assert "privateKey" not in created[0].settings
    assert "walletAddress" not in created[0].settings
    assert client.authenticated is False
    assert client.signature_type is None


@pytest.mark.asyncio
async def test_authenticated_client_requires_private_key_and_verified_deposit_wallet() -> None:
    with pytest.raises(CcxtPredictionConfigurationError, match="private key"):
        await create_ccxt_prediction_client(
            wallet_address="0x" + "b" * 40,
            exchange_factory=FakeExchange,
            ccxt_version="4.5.70",
        )
    with pytest.raises(CcxtPredictionConfigurationError, match="Deposit Wallet"):
        await create_ccxt_prediction_client(
            private_key="0x" + "a" * 64,
            exchange_factory=FakeExchange,
            ccxt_version="4.5.70",
        )


@pytest.mark.asyncio
async def test_authenticated_client_is_type3_and_patched() -> None:
    wallet = "0x" + "b" * 40
    client = await create_ccxt_prediction_client(
        private_key="0x" + "a" * 64,
        wallet_config=SimpleNamespace(funder_address=wallet),
        exchange_factory=FakeExchange,
        ccxt_version="4.5.70",
    )

    assert client.authenticated is True
    assert client.wallet_address == wallet
    assert client.signature_type == 3
    assert client.exchange.options["signatureType"] == 3
    assert client.exchange.options["funder"] == wallet
    assert client.exchange._pmcopy_precision_patch == "pmcopy-arbitrary-tick-v1"
    client.exchange.load_api_credentials.assert_awaited_once()


@pytest.mark.asyncio
async def test_client_closes_once() -> None:
    client = await create_ccxt_prediction_client(
        authenticated=False,
        exchange_factory=FakeExchange,
        ccxt_version="4.5.70",
    )
    await client.close()
    await client.close()
    client.exchange.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_old_ccxt_is_rejected_before_client_creation() -> None:
    with pytest.raises(CcxtPredictionUnavailable, match="too old"):
        await create_ccxt_prediction_client(
            authenticated=False,
            exchange_factory=FakeExchange,
            ccxt_version="4.5.69",
        )


@pytest.mark.asyncio
async def test_authenticated_bootstrap_failure_closes_exchange() -> None:
    created: list[FakeExchange] = []

    def factory(settings):
        exchange = FakeExchange(settings)
        exchange.load_api_credentials.side_effect = RuntimeError("auth bootstrap failed")
        created.append(exchange)
        return exchange

    with pytest.raises(RuntimeError, match="auth bootstrap failed"):
        await create_ccxt_prediction_client(
            private_key="0x" + "a" * 64,
            wallet_address="0x" + "b" * 40,
            authenticated=True,
            exchange_factory=factory,
            ccxt_version="4.5.70",
        )

    created[0].close.assert_awaited_once()
