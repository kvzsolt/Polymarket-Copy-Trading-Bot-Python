"""Contracts for the trader-price-based entry admission engine."""

from __future__ import annotations

from decimal import Decimal

import pytest

from services.execution.entry_admission import (
    EntryAdmissionDecision,
    EntryAdmissionEngine,
    EntryPolicy,
    EntryPriceBandPolicy,
    OffsetDirection,
    OffsetMode,
    TraderPriceOffsetPolicy,
)


def test_buy_with_default_policy_returns_explicit_fak_cap() -> None:
    decision = EntryAdmissionEngine(EntryPolicy()).evaluate(
        side="BUY",
        copied_trader_price=Decimal("0.45"),
    )
    assert isinstance(decision, EntryAdmissionDecision)
    assert decision.allowed is True
    assert decision.order_type == "FAK"
    assert decision.price_limit == Decimal("0.45")


def test_invalid_side_is_rejected() -> None:
    decision = EntryAdmissionEngine(EntryPolicy()).evaluate(
        side="HOLD",
        copied_trader_price=Decimal("0.45"),
    )
    assert decision.allowed is False
    assert decision.reason == "invalid_side"


def test_price_outside_band_is_rejected() -> None:
    policy = EntryPolicy(
        price_band=EntryPriceBandPolicy(
            enabled=True,
            minimum=Decimal("0.20"),
            maximum=Decimal("0.40"),
        ),
    )
    decision = EntryAdmissionEngine(policy).evaluate(
        side="BUY",
        copied_trader_price=Decimal("0.45"),
    )
    assert decision.allowed is False
    assert decision.reason == "copied_price_above_entry_band"


def test_percentage_offset_above_increases_buy_cap() -> None:
    policy = EntryPolicy(
        offset=TraderPriceOffsetPolicy(
            enabled=True,
            mode=OffsetMode.PERCENTAGE,
            direction=OffsetDirection.ABOVE,
            value=Decimal("5"),
        ),
    )
    decision = EntryAdmissionEngine(policy).evaluate(
        side="BUY",
        copied_trader_price=Decimal("0.40"),
    )
    assert decision.allowed is True
    assert decision.price_limit == Decimal("0.42")


def test_fixed_offset_below_decreases_buy_cap() -> None:
    policy = EntryPolicy(
        offset=TraderPriceOffsetPolicy(
            enabled=True,
            mode=OffsetMode.FIXED,
            direction=OffsetDirection.BELOW,
            value=Decimal("0.02"),
        ),
    )
    decision = EntryAdmissionEngine(policy).evaluate(
        side="BUY",
        copied_trader_price=Decimal("0.40"),
    )
    assert decision.allowed is True
    assert decision.price_limit == Decimal("0.38")


def test_marketable_direction_makes_buy_above_and_sell_below() -> None:
    policy = EntryPolicy(
        offset=TraderPriceOffsetPolicy(
            enabled=True,
            mode=OffsetMode.PERCENTAGE,
            direction=OffsetDirection.MARKETABLE,
            value=Decimal("5"),
        ),
    )
    buy = EntryAdmissionEngine(policy).evaluate(
        side="BUY",
        copied_trader_price=Decimal("0.40"),
    )
    sell = EntryAdmissionEngine(policy).evaluate(
        side="SELL",
        copied_trader_price=Decimal("0.40"),
    )
    assert buy.price_limit == Decimal("0.42")
    assert sell.price_limit == Decimal("0.38")


def test_derived_limit_out_of_range_is_rejected() -> None:
    policy = EntryPolicy(
        offset=TraderPriceOffsetPolicy(
            enabled=True,
            mode=OffsetMode.FIXED,
            direction=OffsetDirection.ABOVE,
            value=Decimal("0.9"),
        ),
    )
    decision = EntryAdmissionEngine(policy).evaluate(
        side="BUY",
        copied_trader_price=Decimal("0.5"),
    )
    assert decision.allowed is False
    assert decision.reason == "derived_price_limit_out_of_range"


def test_invalid_copied_price_is_rejected() -> None:
    decision = EntryAdmissionEngine(EntryPolicy()).evaluate(
        side="BUY",
        copied_trader_price=Decimal("1.5"),
    )
    assert decision.allowed is False
    assert decision.reason == "invalid_copied_trader_price"


def test_disabled_policy_still_returns_explicit_limit() -> None:
    decision = EntryAdmissionEngine(EntryPolicy(enabled=False)).evaluate(
        side="BUY",
        copied_trader_price=Decimal("0.45"),
    )
    assert decision.allowed is True
    assert decision.price_limit == Decimal("0.45")


def test_price_band_rejects_inverted_bounds() -> None:
    with pytest.raises(ValueError, match="entry price band"):
        EntryPriceBandPolicy(minimum=Decimal("0.8"), maximum=Decimal("0.2"))


def test_percentage_offset_rejects_over_100() -> None:
    with pytest.raises(ValueError, match="percentage trader price offset"):
        TraderPriceOffsetPolicy(mode=OffsetMode.PERCENTAGE, value=Decimal("150"))
