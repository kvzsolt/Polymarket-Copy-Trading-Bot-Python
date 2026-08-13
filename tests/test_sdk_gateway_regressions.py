from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from integrations.polymarket.clob_v2_gateway import PolymarketGateway
from integrations.polymarket.models import MarketableOrderRequest


@pytest.mark.asyncio
async def test_legacy_market_order_uses_fetched_metadata_when_request_omits_it() -> None:
    book = SimpleNamespace(
        token_id="123",
        tick_size=Decimal("0.005"),
        min_order_size=Decimal("5"),
        neg_risk=True,
        bids=(SimpleNamespace(price=Decimal("0.40"), size=Decimal("20")),),
        asks=(SimpleNamespace(price=Decimal("0.42"), size=Decimal("10")),),
        timestamp=None,
    )
    public = SimpleNamespace(get_order_book=AsyncMock(return_value=book))
    client = SimpleNamespace(
        public_client=public,
        create_market_order=AsyncMock(return_value="signed-order"),
        post_signed_order=AsyncMock(
            return_value={"success": True, "orderID": "order-1", "status": "matched"}
        ),
    )
    gateway = PolymarketGateway(client)  # legacy/injected client branch

    result = await gateway.submit_marketable_order(
        MarketableOrderRequest(
            token_id="123",
            side="BUY",
            size=Decimal("5"),
            price_limit=Decimal("0.42"),
            condition_id="condition",
            tick_size=None,
            neg_risk=None,
            order_type="FOK",
        )
    )

    assert result.success is True
    client.create_market_order.assert_awaited_once_with(
        token_id="123",
        side="BUY",
        amount=Decimal("5"),
        max_price=Decimal("0.42"),
        tick_size="0.005",
        neg_risk=True,
        order_type="FOK",
    )


@pytest.mark.asyncio
async def test_sdk_order_without_exchange_average_does_not_claim_limit_price_is_average() -> None:
    created_at = __import__("datetime").datetime(
        2026,
        8,
        13,
        tzinfo=__import__("datetime").timezone.utc,
    )
    secure = SimpleNamespace(
        get_order=AsyncMock(
            return_value=SimpleNamespace(
                id="order-1",
                status="LIVE",
                side="BUY",
                price=Decimal("0.42"),
                original_size=Decimal("10"),
                size_matched=Decimal("2"),
                avg_price=None,
                average_price=None,
                created_at=created_at,
                model_dump=lambda **_kwargs: {},
            )
        )
    )
    client = SimpleNamespace(require_secure=lambda: secure)
    gateway = PolymarketGateway(client)

    state = await gateway.get_order("order-1")

    assert state.filled_size == Decimal("2")
    assert state.avg_fill_price == Decimal("0")
