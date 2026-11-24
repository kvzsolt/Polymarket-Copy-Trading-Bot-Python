"""Regression: order_sizing accepts Decimal, SELL uses ROUND_FLOOR,
lot_size is configurable.

Three review findings:

  #4  Public API was float-typed end-to-end. Callers passing Decimal got
      a silent str()-round-trip and we wanted Decimal-first signatures.
  #6  Token size was always quantized to Decimal("0.0001") regardless
      of market lot size — markets with coarser lot_size (e.g. 0.005)
      would submit non-tick sizes to the venue.
  #7  SELL used ROUND_CEILING which rounds *up* — defensive direction
      for SELL is *down* so we never attempt to sell more than the
      caller requested, even if the clamp-to-held step keeps things
      safe today.

Tests below pin each contract.
"""

from __future__ import annotations

from decimal import Decimal

from pmcopy.services.trading.order_sizing import size_buy_order, size_sell_order


def test_size_buy_accepts_decimal_inputs():
    """Decimal arguments must work identically to float arguments."""
    dec = size_buy_order(
        live_price=Decimal("0.50"),
        base_notional_usdc=Decimal("10"),
        max_notional_usdc=Decimal("100"),
        market_min_size=Decimal("0"),
        tick_size=Decimal("0.01"),
        slippage_ratio=Decimal("0.02"),
    )
    flt = size_buy_order(
        live_price=0.50,
        base_notional_usdc=10.0,
        max_notional_usdc=100.0,
        market_min_size=0.0,
        tick_size=0.01,
        slippage_ratio=0.02,
    )
    assert dec.effective_size == flt.effective_size
    assert dec.effective_price == flt.effective_price
    assert dec.accept and flt.accept


def test_size_sell_uses_floor_rounding():
    """SELL of an oddly-precise size must round DOWN, never up."""
    # live_price=1.0 → taker_min = 1 token. Held = 1.00005 covers it.
    # Held has more precision than the lot grid. Request equals held.
    # FLOOR → 1.00005 → 1.0000 (sell on tick, slightly less than held).
    # CEILING (pre-fix) → 1.0001 → clamped to 1.00005 (off-tick).
    decision = size_sell_order(
        sell_tokens=Decimal("1.00005"),
        held_size=Decimal("1.00005"),
        live_price=Decimal("1.0"),
        market_min_size=Decimal("0"),
    )
    assert decision.accept, decision.reason
    # Must be at most the request and quantized to the lot grid (4dp default).
    assert decision.effective_size <= Decimal("1.00005")
    assert decision.effective_size == Decimal("1.0000"), (
        f"SELL must FLOOR to lot grid; got {decision.effective_size}"
    )


def test_size_buy_respects_custom_lot_size():
    """When the market lot_size is 0.005, sized output must align to 0.005."""
    decision = size_buy_order(
        live_price=Decimal("0.50"),
        base_notional_usdc=Decimal("10"),
        max_notional_usdc=Decimal("100"),
        market_min_size=Decimal("0"),
        tick_size=Decimal("0.01"),
        slippage_ratio=Decimal("0.02"),
        lot_size=Decimal("0.005"),
    )
    assert decision.accept
    # effective_size must be a multiple of 0.005 (no 0.0001 remainder).
    rem = decision.effective_size % Decimal("0.005")
    assert rem == Decimal("0"), (
        f"size must align to lot_size=0.005; got {decision.effective_size} (remainder {rem})"
    )


def test_size_sell_respects_custom_lot_size():
    decision = size_sell_order(
        sell_tokens=Decimal("3.123"),
        held_size=Decimal("10"),
        live_price=Decimal("0.50"),
        market_min_size=Decimal("0"),
        lot_size=Decimal("0.01"),
    )
    assert decision.accept
    assert decision.effective_size % Decimal("0.01") == Decimal("0")
    # FLOOR: 3.123 → 3.12 not 3.13
    assert decision.effective_size == Decimal("3.12"), decision.effective_size


def test_size_sell_floor_then_clamp_never_exceeds_held():
    """Floor + held-clamp interaction must never sell more than held."""
    # live_price 0.90 → taker_min ≈ 1.12 tokens; held=1.5 covers it.
    decision = size_sell_order(
        sell_tokens=Decimal("100"),
        held_size=Decimal("1.5"),
        live_price=Decimal("0.90"),
        market_min_size=Decimal("0"),
    )
    assert decision.accept, decision.reason
    assert decision.effective_size <= Decimal("1.5")
