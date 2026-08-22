"""Regression tests for exact Polymarket order amount construction."""

from __future__ import annotations

from decimal import Decimal

import pytest

from integrations.polymarket.ccxt_compat.precision import (
    CcxtPrecisionError,
    install_ccxt_polymarket_precision_patch,
    polymarket_raw_amounts,
)
from integrations.polymarket.ccxt_compat.capabilities import PRECISION_PATCH_ID
from integrations.polymarket.ccxt_compat.tick_size import quantize_price_to_tick


@pytest.mark.parametrize("tick", ["0.1", "0.01", "0.005", "0.0025", "0.001", "0.0001"])
def test_arbitrary_supported_tick_sizes_produce_nonzero_amounts(tick: str) -> None:
    result = polymarket_raw_amounts("BUY", "10", "0.42", tick)
    assert int(result["makerAmount"]) > 0
    assert int(result["takerAmount"]) > 0


def test_price_quantization_is_side_conservative_and_exact() -> None:
    assert quantize_price_to_tick(
        price=Decimal("0.551"), tick_size=Decimal("0.01"), side="BUY"
    ) == Decimal("0.55")
    assert quantize_price_to_tick(
        price=Decimal("0.551"), tick_size=Decimal("0.01"), side="SELL"
    ) == Decimal("0.56")
    assert quantize_price_to_tick(
        price=Decimal("0.411"), tick_size=Decimal("0.0025"), side="BUY"
    ) == Decimal("0.4100")
    assert quantize_price_to_tick(
        price=Decimal("0.411"), tick_size=Decimal("0.0025"), side="SELL"
    ) == Decimal("0.4125")


@pytest.mark.parametrize(
    ("cost", "price", "tick_size"),
    [
        ("4.999932", "0.84", "0.01"),
        ("5.12", "0.415", "0.005"),
        ("5.12", "0.4125", "0.0025"),
    ],
)
def test_cost_sized_market_buy_crosses_selected_tick_without_reaching_next_tick(
    cost: str,
    price: str,
    tick_size: str,
) -> None:
    """Keep a cost BUY marketable without exposing the next price level."""

    result = polymarket_raw_amounts(
        "BUY",
        size=cost,
        price=price,
        tick_size=tick_size,
        cost=cost,
    )
    maker = Decimal(result["makerAmount"]) / Decimal("1000000")
    taker = Decimal(result["takerAmount"]) / Decimal("1000000")
    signed_price = maker / taker

    assert maker <= Decimal(cost)
    assert signed_price >= Decimal(price)
    assert signed_price < Decimal(price) + Decimal(tick_size)


def test_precision_patch_marker_identifies_marketable_cost_buy_semantics() -> None:
    """Expose the behavior change so stale exchange instances are repatched."""

    assert PRECISION_PATCH_ID == "pmcopy-arbitrary-tick-v2"


def test_zero_after_quantization_is_rejected() -> None:
    with pytest.raises(CcxtPrecisionError):
        polymarket_raw_amounts("SELL", "0.001", "0.0001", "0.0001")


def test_patch_is_instance_local_and_idempotent() -> None:
    class Exchange:
        def polymarket_order_raw_amounts(self, *_args):
            return {"makerAmount": "old", "takerAmount": "old"}

    first = Exchange()
    second = Exchange()
    original_second = second.polymarket_order_raw_amounts

    assert install_ccxt_polymarket_precision_patch(first) is first
    assert install_ccxt_polymarket_precision_patch(first) is first
    assert first.polymarket_order_raw_amounts("BUY", "10", "0.42", "0.005")["makerAmount"] != "old"
    assert second.polymarket_order_raw_amounts.__func__ is original_second.__func__


def test_signed_amounts_never_cross_caller_price_protection() -> None:
    buy = polymarket_raw_amounts("BUY", "7.13", "0.411", "0.0025")
    buy_price = Decimal(buy["makerAmount"]) / Decimal(buy["takerAmount"])
    assert buy_price <= Decimal("0.4100")

    sell = polymarket_raw_amounts("SELL", "7.13", "0.411", "0.0025")
    sell_price = Decimal(sell["takerAmount"]) / Decimal(sell["makerAmount"])
    assert sell_price >= Decimal("0.4125")
