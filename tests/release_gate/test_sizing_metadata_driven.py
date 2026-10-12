"""Release-gate: metadata-driven sizing (Issue #75 item 6).

``services.order_sizing`` must compute the effective minimum as the larger of
the venue's dynamic share minimum and the $1 USDC taker floor. Tests:
* sub-min sizes are skipped when the cap can't cover the upsize;
* tick-rounded worst-case price is reflected in ``effective_price``;
* SELL sizing is bounded by held shares (the bot never invents shares);
* zero / negative price input is rejected.
"""

from __future__ import annotations

from decimal import Decimal

from pmcopy.services.order_sizing import size_buy_order, size_sell_order


def test_buy_below_min_rejected_when_cap_cannot_cover_upsize() -> None:
    """Reject when the cap cannot fund the venue's ten-share minimum."""

    decision = size_buy_order(
        live_price=0.50,
        base_notional_usdc=1.0,
        max_notional_usdc=1.5,
        market_min_size=10.0,
        tick_size=0.01,
        slippage_ratio=0.02,
    )
    assert decision.accept is False
    assert decision.reason == "buy_cap_blocks_upsize_to_venue_min"


def test_buy_rounds_up_to_dynamic_five_share_minimum() -> None:
    """Round a small BUY to five shares when risk headroom covers it."""

    decision = size_buy_order(
        live_price=Decimal("0.70"),
        base_notional_usdc=Decimal("0.50"),
        max_notional_usdc=Decimal("4.00"),
        market_min_size=Decimal("5"),
        tick_size=Decimal("0.01"),
        slippage_ratio=Decimal("0"),
    )

    assert decision.accept is True
    assert decision.upsized is True
    assert decision.effective_size == Decimal("5")
    assert decision.worst_notional == Decimal("3.50")


def test_buy_uses_one_usdc_floor_when_it_exceeds_share_minimum() -> None:
    """At a ten-cent price, round to ten shares so notional reaches $1."""

    decision = size_buy_order(
        live_price=Decimal("0.10"),
        base_notional_usdc=Decimal("0.50"),
        max_notional_usdc=Decimal("2.00"),
        market_min_size=Decimal("5"),
        tick_size=Decimal("0.01"),
        slippage_ratio=Decimal("0"),
    )

    assert decision.accept is True
    assert decision.upsized is True
    assert decision.effective_size == Decimal("10")
    assert decision.worst_notional == Decimal("1.00")


def test_buy_at_cap_uses_tick_rounded_worst_price() -> None:
    decision = size_buy_order(
        live_price=0.50,
        base_notional_usdc=10.0,
        max_notional_usdc=10.0,
        market_min_size=1.0,
        tick_size=0.01,
        slippage_ratio=0.02,
    )
    assert decision.accept is True
    assert decision.effective_price == Decimal("0.51")


def test_buy_rejects_zero_price() -> None:
    decision = size_buy_order(
        live_price=0.0,
        base_notional_usdc=10.0,
        max_notional_usdc=20.0,
        market_min_size=1.0,
        tick_size=0.01,
    )
    assert decision.accept is False
    assert decision.reason == "invalid_price"


def test_buy_rejects_zero_cap() -> None:
    decision = size_buy_order(
        live_price=0.50,
        base_notional_usdc=10.0,
        max_notional_usdc=0.0,
        market_min_size=1.0,
        tick_size=0.01,
    )
    assert decision.accept is False
    assert decision.reason == "zero_cap"


def test_sell_bounded_by_held_position() -> None:
    decision = size_sell_order(
        sell_tokens=100.0,
        held_size=5.0,
        live_price=0.50,
        market_min_size=1.0,
    )
    assert decision.accept is True
    assert decision.effective_size == Decimal("5")


def test_sell_below_min_rejected() -> None:
    """Skip a SELL when held shares cannot cover the venue minimum."""

    decision = size_sell_order(
        sell_tokens=0.5,
        held_size=0.5,
        live_price=0.50,
        market_min_size=10.0,
    )
    assert decision.accept is False
    assert decision.reason == "sell_held_below_venue_min"


# --- SELL parity with BUY: explicit min-size handling cases (issue-78) ---


def test_sell_below_min_upsized_when_held_allows() -> None:
    """Upsize a small SELL to the dynamic five-share venue minimum."""

    decision = size_sell_order(
        sell_tokens=1.0,
        held_size=12.0,
        live_price=0.99,
        market_min_size=5.0,
    )
    assert decision.accept is True
    assert decision.upsized is True
    assert decision.effective_size == Decimal("5")
    assert decision.reason == "upsized"


def test_sell_below_min_skipped_when_held_short() -> None:
    """Skip when held shares cannot cover the ten-share venue minimum."""

    decision = size_sell_order(
        sell_tokens=1.0,
        held_size=1.0,
        live_price=0.99,
        market_min_size=10.0,
    )
    assert decision.accept is False
    assert decision.reason == "sell_held_below_venue_min"


def test_sell_exactly_at_venue_min_accepted_unchanged() -> None:
    """Accept a SELL exactly at the dynamic minimum without an upsize."""

    decision = size_sell_order(
        sell_tokens=5.0,
        held_size=5.0,
        live_price=0.99,
        market_min_size=5.0,
    )
    assert decision.accept is True
    assert decision.upsized is False
    assert decision.effective_size == Decimal("5")


def test_sell_clamped_to_held_then_min_check() -> None:
    """Reject after the held clamp leaves fewer than ten shares."""

    decision = size_sell_order(
        sell_tokens=100.0,
        held_size=3.0,
        live_price=0.99,
        market_min_size=10.0,
    )
    assert decision.accept is False
    assert decision.reason == "sell_held_below_venue_min"


def test_sell_full_position_at_near_resolution_price() -> None:
    """Full-position sell at near-resolution price: held=size, price >= 0.99."""
    decision = size_sell_order(
        sell_tokens=42.5,
        held_size=42.5,
        live_price=0.995,
        market_min_size=5.0,
    )
    assert decision.accept is True
    assert decision.effective_size == Decimal("42.5")
