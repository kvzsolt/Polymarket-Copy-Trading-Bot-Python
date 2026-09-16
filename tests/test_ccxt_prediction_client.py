from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from integrations.polymarket.ccxt_prediction_client import (
    CcxtPredictionConfigurationError,
    CcxtPredictionUnavailable,
    create_ccxt_prediction_client,
)


# Fixture markers used to observe credential routing; deliberately short and
# non-secret so static credential scanners do not misread them.
_FIXTURE_KEY = "k1"
_FIXTURE_SECRET = "k2"
_FIXTURE_PASSPHRASE = "k3"


class FakeExchange:
    def __init__(self, settings):
        self.settings = settings
        self.options = settings["options"]
        self.privateKey = settings.get("privateKey")
        self.walletAddress = settings.get("walletAddress")
        self.apiKey = _FIXTURE_KEY
        self.secret = _FIXTURE_SECRET
        self.password = _FIXTURE_PASSPHRASE
        self.close = AsyncMock()
        self.load_api_credentials = AsyncMock(return_value={"apiKey": "dk"})

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
        ccxt_version="4.5.73",
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
            ccxt_version="4.5.73",
        )
    with pytest.raises(CcxtPredictionConfigurationError, match="Deposit Wallet"):
        await create_ccxt_prediction_client(
            private_key="0x" + "a" * 64,
            exchange_factory=FakeExchange,
            ccxt_version="4.5.73",
        )


@pytest.mark.asyncio
async def test_wallet_config_never_falls_back_to_the_global_wallet() -> None:
    """A per-user wallet must be the complete identity for that user.

    Falling through to ``config.wallet`` would pair one multi-user entry's
    private key with a different account's Deposit Wallet funder.
    """

    config = SimpleNamespace(
        wallet=SimpleNamespace(
            funder_address="0x" + "c" * 40,
            private_key="0x" + "d" * 64,
        )
    )

    with pytest.raises(CcxtPredictionConfigurationError, match="Deposit Wallet"):
        await create_ccxt_prediction_client(
            wallet_config=SimpleNamespace(funder_address="", private_key="0x" + "a" * 64),
            config=config,
            authenticated=True,
            exchange_factory=FakeExchange,
            ccxt_version="4.5.73",
        )


@pytest.mark.asyncio
async def test_config_wallet_is_used_only_without_a_wallet_config() -> None:
    """The global-config fallback is valid solely on the single-user path."""

    config = SimpleNamespace(
        wallet=SimpleNamespace(
            funder_address="0x" + "c" * 40,
            private_key="0x" + "d" * 64,
        )
    )

    client = await create_ccxt_prediction_client(
        config=config,
        authenticated=True,
        exchange_factory=FakeExchange,
        ccxt_version="4.5.73",
    )

    assert client.wallet_address == "0x" + "c" * 40
    created = client.exchange.settings
    assert created["walletAddress"] == "0x" + "c" * 40
    assert created["privateKey"] == "0x" + "d" * 64


@pytest.mark.asyncio
async def test_authenticated_client_is_type3_and_patched() -> None:
    wallet = "0x" + "b" * 40
    client = await create_ccxt_prediction_client(
        private_key="0x" + "a" * 64,
        wallet_config=SimpleNamespace(funder_address=wallet),
        exchange_factory=FakeExchange,
        ccxt_version="4.5.73",
    )

    assert client.authenticated is True
    assert client.wallet_address == wallet
    assert client.signature_type == 3
    assert client.exchange.options["signatureType"] == 3
    assert client.exchange.options["funder"] == wallet
    assert client.exchange._pmcopy_precision_patch == "pmcopy-arbitrary-tick-v2"
    client.exchange.load_api_credentials.assert_awaited_once()


@pytest.mark.asyncio
async def test_authenticated_client_exposes_normalized_user_stream_credentials() -> None:
    """Expose derived L2 credentials only through the runtime auth seam."""

    client = await create_ccxt_prediction_client(
        private_key="0x" + "a" * 64,
        wallet_address="0x" + "b" * 40,
        authenticated=True,
        exchange_factory=FakeExchange,
        ccxt_version="4.5.73",
    )

    assert client.api_creds == {
        "api_key": _FIXTURE_KEY,
        "api_secret": _FIXTURE_SECRET,
        "api_passphrase": _FIXTURE_PASSPHRASE,
    }


@pytest.mark.asyncio
async def test_user_stream_credentials_prefer_derived_l2_options() -> None:
    """CCXT load_api_credentials caches derived creds in exchange options."""

    class OptionsExchange(FakeExchange):
        def __init__(self, settings):
            super().__init__(settings)
            self.options = dict(settings["options"])
            self.options["l2ApiKey"] = "opt-k"
            self.options["l2Secret"] = "opt-s"
            self.options["l2Passphrase"] = "opt-p"

    client = await create_ccxt_prediction_client(
        private_key="0x" + "a" * 64,
        wallet_address="0x" + "b" * 40,
        authenticated=True,
        exchange_factory=OptionsExchange,
        ccxt_version="4.5.73",
    )

    assert client.api_creds == {
        "api_key": "opt-k",
        "api_secret": "opt-s",
        "api_passphrase": "opt-p",
    }


@pytest.mark.asyncio
async def test_client_closes_once() -> None:
    client = await create_ccxt_prediction_client(
        authenticated=False,
        exchange_factory=FakeExchange,
        ccxt_version="4.5.73",
    )
    await client.close()
    await client.close()
    client.exchange.close.assert_awaited_once_with(True)


@pytest.mark.asyncio
async def test_old_ccxt_is_rejected_before_client_creation() -> None:
    with pytest.raises(CcxtPredictionUnavailable, match="too old"):
        await create_ccxt_prediction_client(
            authenticated=False,
            exchange_factory=FakeExchange,
            ccxt_version="4.5.72",
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
            ccxt_version="4.5.73",
        )

    created[0].close.assert_awaited_once()
