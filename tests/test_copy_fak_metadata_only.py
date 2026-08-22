"""Regression coverage for metadata-only, leader-priced copy FAK orders."""

from __future__ import annotations

import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock

from domain.execution import ExecutionOrderType
from integrations.polymarket.models import VenueSubmitResult
from tests.test_book_quality_gate import _make_execution_service, _req


def test_copy_fak_uses_prefetched_metadata_without_reading_the_book() -> None:
    """Submit the full BUY cost at the copied price without a book snapshot."""

    service = _make_execution_service("0.69", "0.70")
    service._market_data_service.require_fresh_data = AsyncMock(
        side_effect=AssertionError("copy FAK execution must not read a book snapshot")
    )
    service.clob_gateway.get_execution_metadata = AsyncMock(
        side_effect=AssertionError("prefetched request metadata must be reused")
    )
    service.clob_gateway.submit_marketable_order = AsyncMock(
        return_value=VenueSubmitResult(
            success=True,
            order_id="ord-copy-fak",
            status="matched",
            raw={"size_matched": "7.2856", "avg_price": "0.70"},
        )
    )
    requested_cost = Decimal("5.0999590810009465")
    request = _req(
        amount=str(requested_cost),
        condition_id="0xcondition",
        order_type=ExecutionOrderType.FAK,
        price_limit=Decimal("0.70"),
        tick_size=Decimal("0.01"),
        min_order_size=Decimal("5"),
        neg_risk=False,
        recovery_metadata={
            "source": "copy_trade",
            "order_type": "FAK",
            "leader_price": "0.70",
            "price_limit": "0.70",
        },
    )

    result = asyncio.run(service.execute(request))

    assert result.accepted is True
    service._market_data_service.require_fresh_data.assert_not_awaited()
    service.clob_gateway.get_execution_metadata.assert_not_awaited()
    service.clob_gateway.submit_marketable_order.assert_awaited_once()
    submitted = service.clob_gateway.submit_marketable_order.await_args.args[0]
    assert submitted.order_type is ExecutionOrderType.FAK
    assert submitted.size == requested_cost
    assert submitted.price_limit == Decimal("0.70")


def test_copy_fak_rejects_prefetched_metadata_missing_the_dynamic_minimum() -> None:
    """Fail closed before the POST when copy metadata omits min order size."""

    service = _make_execution_service("0.69", "0.70")
    service._market_data_service.require_fresh_data = AsyncMock(
        side_effect=AssertionError("copy FAK execution must not read a book snapshot")
    )
    request = _req(
        amount="5.10",
        condition_id="0xcondition",
        order_type=ExecutionOrderType.FAK,
        price_limit=Decimal("0.70"),
        tick_size=Decimal("0.01"),
        min_order_size=None,
        neg_risk=False,
    )

    result = asyncio.run(service.execute(request))

    assert result.accepted is False
    assert result.status == "market_metadata_unavailable"
    assert "min_order_size" in str(result.error)
    service._market_data_service.require_fresh_data.assert_not_awaited()
    service.clob_gateway.submit_marketable_order.assert_not_awaited()


def test_copy_fak_rejects_buy_below_one_usdc_notional() -> None:
    """Fail closed when BUY shares clear the market minimum but spend is below $1."""

    service = _make_execution_service("0.09", "0.10")
    request = _req(
        amount="0.99",
        condition_id="0xcondition",
        order_type=ExecutionOrderType.FAK,
        price_limit=Decimal("0.10"),
        tick_size=Decimal("0.01"),
        min_order_size=Decimal("5"),
        neg_risk=False,
    )

    result = asyncio.run(service.execute(request))

    assert result.accepted is False
    assert result.status == "size_below_min"
    assert "$1" in str(result.error)
    service.clob_gateway.submit_marketable_order.assert_not_awaited()


def test_copy_fak_rejects_sell_below_one_usdc_notional() -> None:
    """Fail closed when SELL shares clear the market minimum but value is below $1."""

    service = _make_execution_service("0.10", "0.11")
    request = _req(
        amount="6",
        side="SELL",
        condition_id="0xcondition",
        order_type=ExecutionOrderType.FAK,
        price_limit=Decimal("0.10"),
        tick_size=Decimal("0.01"),
        min_order_size=Decimal("5"),
        neg_risk=False,
    )

    result = asyncio.run(service.execute(request))

    assert result.accepted is False
    assert result.status == "size_below_min"
    assert "$1" in str(result.error)
    service.clob_gateway.submit_marketable_order.assert_not_awaited()
