"""Metadata-driven order sizing (Issue #69).

Centralises BUY and SELL effective-size computation so that:

* Venue min-size and tick-size are read from market metadata — never
  hardcoded.
* BUY effective notional uses the **worst-case protected price** (live
  price × (1 + slippage)), rounded up to the tick, so the downstream
  execution layer receives enough USDC to fill the desired shares even
  under adverse price movement.
* SELL effective size is bounded by the held position size — the bot
  never invents shares.
* Upsize logic (below venue min → round up to venue min) only activates
  when the position cap or held shares can cover the upsize; otherwise
  the trade is skipped.
* Every sizing decision emits a structured :class:`SizingDecision` so
  callers can trace why an order was accepted, upsized, or rejected.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from typing import Any

from utils.logger import get_logger

logger = get_logger("OrderSizing")

# Polymarket taker (FOK/IOC/marketable) minimum order notional in USDC.
_TAKER_MIN_NOTIONAL_USDC = Decimal("1")


@dataclass(frozen=True)
class SizingDecision:
    """Structured result of metadata-driven order sizing."""

    accept: bool
    effective_size: Decimal
    effective_price: Decimal
    venue_min_size: Decimal
    reason: str
    upsized: bool
    worst_notional: Decimal = Decimal("0")


def _taker_min_tokens(live_price: float) -> Decimal:
    """Compute minimum token count for a taker order given the $1 USDC floor."""
    if live_price <= 0:
        return Decimal("0")
    # Minimum token count = ceil($1 / live_price), quantized to 4 decimals.
    return (Decimal("1") / Decimal(str(live_price))).quantize(
        Decimal("0.0001"), rounding=ROUND_CEILING
    )


def size_buy_order(
    *,
    live_price: float,
    base_notional_usdc: float,
    max_notional_usdc: float,
    market_min_size: float,
    tick_size: float = 0.0,
    slippage_ratio: float = 0.02,
) -> SizingDecision:
    """Compute the effective BUY size and notional after venue constraints.

    Parameters
    ----------
    live_price:
        Current mid/ask price for the token.
    base_notional_usdc:
        Desired USDC notional before cap/clamp.
    max_notional_usdc:
        Hard cap on USDC notional (account cap, per-trade cap, etc.).
    market_min_size:
        **Ignored for taker orders.**  Kept for backward-compatible
        signatures; the actual floor is derived from the Polymarket
        taker-minimum of **$1 USDC notional**.
    tick_size:
        Tick size for price rounding (from market metadata).
    slippage_ratio:
        Slippage tolerance for worst-case protected price (e.g. 0.02 = 2%).

    Returns
    -------
    SizingDecision
        ``accept=True`` when the order should proceed; ``accept=False``
        when it must be skipped.  On acceptance, ``effective_size`` is the
        final token count and ``effective_price`` is the worst-case price
        rounded up to the tick.
    """
    d_market_min = _taker_min_tokens(live_price)
    d_tick_size = Decimal(str(tick_size)) if tick_size > 0 else Decimal("0")

    if live_price <= 0:
        return SizingDecision(False, Decimal("0"), Decimal("0"), d_market_min, "invalid_price", False, Decimal("0"))
    if base_notional_usdc <= 0:
        return SizingDecision(False, Decimal("0"), Decimal("0"), d_market_min, "zero_notional", False, Decimal("0"))
    if max_notional_usdc <= 0:
        return SizingDecision(False, Decimal("0"), Decimal("0"), d_market_min, "zero_cap", False, Decimal("0"))

    d_live_price = Decimal(str(live_price))
    d_slippage = Decimal(str(slippage_ratio))
    d_worst_price = d_live_price * (Decimal("1") + d_slippage)
    if d_tick_size > 0:
        d_worst_price = (d_worst_price / d_tick_size).to_integral_value(rounding=ROUND_CEILING) * d_tick_size

    d_max_notional = Decimal(str(max_notional_usdc))
    d_base_notional = Decimal(str(base_notional_usdc))

    # Desired size at live price
    d_desired_size = (d_base_notional / d_live_price).quantize(Decimal("0.0001"), rounding=ROUND_FLOOR)
    # Cap-clamp at max notional
    d_desired_notional = min(d_base_notional, d_max_notional)
    d_desired_size = min(d_desired_size, (d_max_notional / d_live_price).quantize(Decimal("0.0001"), rounding=ROUND_FLOOR))

    required_min_tokens = d_market_min
    minimum_executable_notional = d_market_min * d_worst_price if d_market_min > 0 else Decimal("0")

    upsized = False

    # Below venue minimum — try to upsize
    if d_market_min > 0 and d_desired_size < d_market_min:
        can_afford_upsize = minimum_executable_notional > 0 and d_max_notional >= minimum_executable_notional
        if not can_afford_upsize:
            return SizingDecision(
                False, Decimal("0"), Decimal("0"), d_market_min,
                "buy_cap_blocks_upsize_to_venue_min", False, Decimal("0"),
            )
        d_desired_size = required_min_tokens
        upsized = True

    # Round size up to 4 decimal places
    d_desired_size = d_desired_size.quantize(Decimal("0.0001"), rounding=ROUND_CEILING)

    # Worst-case notional check
    d_worst_notional = d_desired_size * d_worst_price
    if d_worst_notional > d_max_notional:
        # Try to floor to fit under cap
        d_floored = (d_max_notional / d_worst_price).quantize(Decimal("0.0001"), rounding=ROUND_FLOOR)
        if d_market_min > 0 and d_floored < d_market_min:
            return SizingDecision(
                False, Decimal("0"), Decimal("0"), d_market_min,
                "risk_cap_too_low_for_min_order", False, Decimal("0"),
            )
        d_desired_size = d_floored
        d_worst_notional = d_desired_size * d_worst_price

    reason = "upsized" if upsized else "ok"
    d_worst_notional = d_desired_size * d_worst_price
    return SizingDecision(True, d_desired_size, d_worst_price, d_market_min, reason, upsized, d_worst_notional)


def size_sell_order(
    *,
    sell_tokens: float,
    held_size: float,
    live_price: float,
    market_min_size: float,
) -> SizingDecision:
    """Compute the effective SELL size after venue constraints.

    Parameters
    ----------
    sell_tokens:
        Desired sell size in tokens.
    held_size:
        Currently held position size in tokens.
    live_price:
        Current mid/bid price.
    market_min_size:
        **Ignored for taker orders.**  Kept for backward-compatible
        signatures; the actual floor is derived from the Polymarket
        taker-minimum of **$1 USDC notional**.

    Returns
    -------
    SizingDecision
        ``accept=True`` when the order should proceed; ``accept=False``
        when it must be skipped.
    """
    d_market_min = _taker_min_tokens(live_price)
    d_live_price = Decimal(str(live_price)) if live_price > 0 else Decimal("0")

    if sell_tokens <= 0:
        return SizingDecision(False, Decimal("0"), d_live_price, d_market_min, "sell_size_zero", False, Decimal("0"))

    d_sell = Decimal(str(sell_tokens))
    d_held = Decimal(str(held_size))

    # Cannot sell more than held
    d_sell = min(d_sell, d_held)

    upsized = False
    # Below venue minimum — try to upsize to venue min if held shares allow
    if d_market_min >0 and d_sell < d_market_min:
        if d_held < d_market_min:
            return SizingDecision(
                False, Decimal("0"), d_live_price, d_market_min,
                "sell_held_below_venue_min", False, Decimal("0"),
            )
        d_sell = d_market_min
        upsized = True

    # Round up to 4 decimal places (venue wants ceiling for sells too)
    d_sell = d_sell.quantize(Decimal("0.0001"), rounding=ROUND_CEILING)

    # Re-clamp to held after ceiling
    d_sell = min(d_sell, d_held)

    # If after ceiling we're below venue min again, skip
    if d_market_min > 0 and d_sell < d_market_min:
        return SizingDecision(
            False, Decimal("0"), d_live_price, d_market_min,
            "sell_held_below_venue_min_after_rounding", False, Decimal("0"),
        )

    reason = "upsized" if upsized else "ok"
    d_sell_notional = d_sell * d_live_price
    return SizingDecision(True, d_sell, d_live_price, d_market_min, reason, upsized, d_sell_notional)