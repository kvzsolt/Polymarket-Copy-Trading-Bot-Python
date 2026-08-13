"""Contracts for SDK-default, CCXT-optional runtime backend selection."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from integrations.polymarket.backend_factory import (
    VenueBackend,
    resolve_backend_settings,
    select_polymarket_gateway,
)
from integrations.polymarket.hybrid_gateway import HybridClobGateway
from integrations.polymarket.models import MarketableOrderRequest


class FakeGateway:
    """Record read and write operations through the venue-neutral port."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.get_market_info = AsyncMock(return_value={"backend": name})
        self.get_orderbook = AsyncMock(return_value=None)
        self.submit_marketable_order = AsyncMock(return_value={"backend": name})
        self.submit_limit_order = AsyncMock(return_value={"backend": name})
        self.get_order = AsyncMock(return_value=None)
        self.list_open_orders = AsyncMock(return_value=[])
        self.get_fills = AsyncMock(return_value=[])
        self.cancel_order = AsyncMock(return_value={"backend": name})
        self.check_compatibility = AsyncMock(return_value={"sdk_available": True})


def test_backend_settings_default_to_official_sdk() -> None:
    """Missing backend configuration never silently selects CCXT."""

    backend, allow_live = resolve_backend_settings(SimpleNamespace(execution=SimpleNamespace()))

    assert backend is VenueBackend.SDK
    assert allow_live is False


def test_official_sdk_gateway_import_is_available() -> None:
    """Production composition retains the first-party SDK adapter path."""

    from integrations.polymarket.clob_v2_gateway import PolymarketGateway
    from integrations.polymarket.sdk_client import create_polymarket_client

    assert PolymarketGateway is not None
    assert create_polymarket_client is not None


def test_sdk_is_selected_for_paper_and_live_by_default() -> None:
    """The first-party SDK remains primary in both trading modes."""

    sdk = FakeGateway("sdk")
    ccxt = FakeGateway("ccxt")

    assert select_polymarket_gateway(sdk_gateway=sdk, ccxt_gateway=ccxt) is sdk
    assert (
        select_polymarket_gateway(
            sdk_gateway=sdk,
            ccxt_gateway=ccxt,
            trading_mode="live",
        )
        is sdk
    )


def test_ccxt_live_primary_requires_separate_experimental_gate() -> None:
    """Explicit live mode alone cannot promote the experimental adapter."""

    with pytest.raises(RuntimeError, match="explicit"):
        select_polymarket_gateway(
            sdk_gateway=FakeGateway("sdk"),
            ccxt_gateway=FakeGateway("ccxt"),
            backend=VenueBackend.CCXT,
            trading_mode="live",
            allow_experimental_ccxt_live=False,
        )


@pytest.mark.asyncio
async def test_shadow_mode_never_mirrors_marketable_write() -> None:
    """SDK shadow CCXT submits only through the SDK primary."""

    sdk = FakeGateway("sdk")
    ccxt = FakeGateway("ccxt")
    hybrid = HybridClobGateway(primary=sdk, shadow=ccxt)
    request = MarketableOrderRequest(
        token_id="1001",
        side="BUY",
        size=1,
        price_limit=1,
        condition_id="condition",
    )

    await hybrid.submit_marketable_order(request)

    sdk.submit_marketable_order.assert_awaited_once_with(request)
    ccxt.submit_marketable_order.assert_not_awaited()
