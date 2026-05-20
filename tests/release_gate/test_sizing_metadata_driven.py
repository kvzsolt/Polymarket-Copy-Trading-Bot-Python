"""Release-gate: metadata-driven sizing (Issue #75 item 6).

``services.order_sizing`` must always compute the effective minimum
from the **$1 USDC taker-minimum** (not the raw ``min_order_size``
which is for GTC-limit/maker orders). Tests:
* sub-min sizes are skipped when the cap can't cover the upsize;
* tick-rounded worst-case price is reflected in ``effective_price``;
* SELL sizing is bounded by held shares (the bot never invents shares);
* zero / negative price input is rejected.
"""
from __future__ import annotations

from decimal import Decimal

from services.order_sizing import size_buy_order, size_sell_order


def test_buy_below_min_rejected_when_cap_cannot_cover_upsize() -> None:
    """BUY too small to meet taker min ($1 notional) and cap covers upsize → accept."""
    # live_price=0.50 => taker_min = ceil(1/0.50) = 2 tokens
    # base_notional=1.0 => desired=2 tokens = exact min, worst_notional ~ 1.02
    # max_notional=1.5 >= 1.02 => accept
    decision = size_buy_order(
        live_price=0.50,
        base_notional_usdc=1.0,
        max_notional_usdc=1.5,
        market_min_size=10.0,  # ignored for taker orders
        tick_size=0.01,
        slippage_ratio=0.02,
    )
    assert decision.accept is True
    assert decision.upsized is False  # exactly at taker min


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
    """SELL below taker min where held shares can't cover upsize → skip."""
    # live_price=0.50 => taker_min=2 tokens
    # sell=0.5, held=0.5 < 2 => skip
    decision = size_sell_order(
        sell_tokens=0.5,
        held_size=0.5,
        live_price=0.50,
        market_min_size=10.0,  # ignored for taker orders
    )
    assert decision.accept is False
    assert decision.reason == "sell_held_below_venue_min"


# --- SELL parity with BUY: explicit min-size handling cases (issue-78) ---


def test_sell_below_min_upsized_when_held_allows() -> None:
    """SELL below taker min upsizes to taker min if held shares cover it."""
    # live_price=0.99 => taker_min = ceil(1/0.99) = 2 tokens
    decision = size_sell_order(
        sell_tokens=2.0,
        held_size=12.0,
        live_price=0.99,
        market_min_size=5.0,  # ignored
    )
    assert decision.accept is True
    assert decision.upsized is True
    assert decision.effective_size == Decimal("2")
    assert decision.reason == "upsized"


def test_sell_below_min_skipped_when_held_short() -> None:
    """SELL below taker min and held shares can't cover upsize → skip."""
    # live_price=0.99 => taker_min = ceil(1/0.99) = 2 tokens
    decision = size_sell_order(
        sell_tokens=1.0,
        held_size=2.0,
        live_price=0.99,
        market_min_size=10.0,  # ignored
    )
    assert decision.accept is False
    assert decision.reason == "sell_held_below_venue_min"


def test_sell_exactly_at_venue_min_accepted_unchanged() -> None:
    """SELL exactly at taker min is accepted without upsize."""
    # live_price=0.99 => taker_min = ceil(1/0.99) = 2 tokens
    decision = size_sell_order(
        sell_tokens=2.0,
        held_size=2.0,
        live_price=0.99,
        market_min_size=5.0,  # ignored
    )
    assert decision.accept is True
    assert decision.upsized is False
    assert decision.effective_size == Decimal("2")


def test_sell_clamped_to_held_then_min_check() -> None:
    """Requested > held → clamp to held; then min-size guard runs."""
    # live_price=0.99 => taker_min = ceil(1/0.99) = 2 tokens
    # sell=100 -> clamp to held=3; 3 >= 2 => accept (no upsize needed)
    decision = size_sell_order(
        sell_tokens=100.0,
        held_size=3.0,
        live_price=0.99,
        market_min_size=10.0,  # ignored
    )
    assert decision.accept is True
    assert decision.effective_size == Decimal("3")


def test_sell_full_position_at_near_one_price() -> None:
    """The exact path NearOneExitService takes: held=size, price >= 0.99."""
    decision = size_sell_order(
        sell_tokens=42.5,
        held_size=42.5,
        live_price=0.995,
        market_min_size=5.0,  # ignored
    )
    assert decision.accept is True
    assert decision.effective_size == Decimal("42.5")
