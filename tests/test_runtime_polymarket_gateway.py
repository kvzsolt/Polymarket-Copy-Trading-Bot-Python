from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from integrations.polymarket.backend_factory import VenueBackend
from integrations.polymarket.hybrid_gateway import HybridClobGateway
from integrations.polymarket.runtime_gateway import build_runtime_polymarket_gateway


class SdkGateway:
    pass


class Exchange:
    def __init__(self, settings):
        self.settings = settings
        self.options = settings["options"]
        self.has = {
            "prediction": True,
            "createOrder": True,
            "fetchOrderBook": True,
            "fetchOrder": True,
            "fetchOpenOrders": True,
            "fetchMyTrades": True,
            "cancelOrder": True,
        }
        self.close = AsyncMock()
        self.load_api_credentials = AsyncMock(return_value={"apiKey": "derived"})

    def polymarket_order_raw_amounts(self, *_args):
        return {}


@pytest.mark.asyncio
async def test_sdk_default_does_not_construct_ccxt() -> None:
    called = False

    def factory(_settings):
        nonlocal called
        called = True
        return Exchange(_settings)

    sdk = SdkGateway()
    bundle = await build_runtime_polymarket_gateway(
        sdk_gateway=sdk,
        config=SimpleNamespace(execution=SimpleNamespace(trading_mode="live")),
        exchange_factory=factory,
        ccxt_version="4.5.70",
    )

    assert bundle.gateway is sdk
    assert bundle.backend is VenueBackend.SDK
    assert bundle.ccxt_client is None
    assert called is False


@pytest.mark.asyncio
async def test_paper_ccxt_backend_is_public_only_even_when_key_is_configured() -> None:
    wallet = SimpleNamespace(
        private_key="0x" + "a" * 64,
        funder_address="0x" + "b" * 40,
    )
    bundle = await build_runtime_polymarket_gateway(
        sdk_gateway=SdkGateway(),
        config=SimpleNamespace(
            execution=SimpleNamespace(trading_mode="paper"),
            wallet=wallet,
        ),
        backend=VenueBackend.CCXT,
        exchange_factory=Exchange,
        ccxt_version="4.5.70",
    )

    assert bundle.ccxt_client is not None
    assert bundle.ccxt_client.authenticated is False
    assert "privateKey" not in bundle.ccxt_client.exchange.settings


@pytest.mark.asyncio
async def test_live_shadow_is_sdk_primary_and_authenticated_for_private_reads() -> None:
    wallet = SimpleNamespace(
        private_key="0x" + "a" * 64,
        funder_address="0x" + "b" * 40,
    )
    sdk = SdkGateway()
    bundle = await build_runtime_polymarket_gateway(
        sdk_gateway=sdk,
        config=SimpleNamespace(
            execution=SimpleNamespace(trading_mode="live"),
            wallet=wallet,
        ),
        backend=VenueBackend.SDK_SHADOW_CCXT,
        exchange_factory=Exchange,
        ccxt_version="4.5.70",
    )

    assert isinstance(bundle.gateway, HybridClobGateway)
    assert bundle.gateway.primary is sdk
    assert bundle.ccxt_client is not None
    assert bundle.ccxt_client.authenticated is True


@pytest.mark.asyncio
async def test_live_ccxt_primary_fails_before_client_construction_without_gate() -> None:
    wallet = SimpleNamespace(
        private_key="0x" + "a" * 64,
        funder_address="0x" + "b" * 40,
    )
    factory = AsyncMock()

    with pytest.raises(RuntimeError, match="explicit"):
        await build_runtime_polymarket_gateway(
            sdk_gateway=SdkGateway(),
            config=SimpleNamespace(
                execution=SimpleNamespace(trading_mode="live"),
                wallet=wallet,
            ),
            backend=VenueBackend.CCXT,
            exchange_factory=factory,
            ccxt_version="4.5.70",
        )

    factory.assert_not_awaited()
