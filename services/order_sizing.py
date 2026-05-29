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
from typing import Union

from utils.logger import get_logger

logger = get_logger("OrderSizing")

# Polymarket taker (FOK/IOC/marketable) minimum order notional in USDC.
_TAKER_MIN_NOTIONAL_USDC = Decimal("1")

# Default token-lot grid when callers don't supply a market-metadata-driven
# value. 0.0001 = Polymarket's historical CTF lot. Markets with coarser lots
# must pass ``lot_size`` explicitly so the venue does not reject off-tick
# orders.
_DEFAULT_LOT_SIZE = Decimal("0.0001")

Number = Union[Decimal, float, int, str]


def _to_decimal(value: Number) -> Decimal:
    """Coerce *value* to Decimal losslessly via ``Decimal(str(x))`` for floats."""
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


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


def _taker_min_tokens(live_price: Number, lot_size: Decimal = _DEFAULT_LOT_SIZE) -> Decimal:
    """Compute minimum token count for a taker order given the $1 USDC floor.

    Quantizes UP to *lot_size* so the minimum is always venue-submittable.
    """
    d_price = _to_decimal(live_price)
    if d_price <= 0:
        return Decimal("0")
    raw = Decimal("1") / d_price
    # Round UP to the next lot grid so the minimum is at least $1 notional.
    return (raw / lot_size).to_integral_value(rounding=ROUND_CEILING) * lot_size


def size_buy_order(
    *,
    live_price: Number,
    base_notional_usdc: Number,
    max_notional_usdc: Number,
    market_min_size: Number,
    tick_size: Number = 0,
    slippage_ratio: Number = Decimal("0.02"),
    lot_size: Number | None = None,
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
    d_live_price = _to_decimal(live_price)
    d_base_notional = _to_decimal(base_notional_usdc)
    d_max_notional = _to_decimal(max_notional_usdc)
    d_tick_size = _to_decimal(tick_size) if _to_decimal(tick_size) > 0 else Decimal("0")
    d_slippage = _to_decimal(slippage_ratio)
    # Greptile P1: an illiquid-market metadata response can legally set
    # ``lot_size=0``; without this zero-guard, ``_quant_lot`` divides by
    # zero and the sizing path crashes. Fall back to ``_DEFAULT_LOT_SIZE``
    # so the order is still sized off the historical CTF lot.
    _raw_lot = _to_decimal(lot_size) if lot_size is not None else _DEFAULT_LOT_SIZE
    d_lot_size = _raw_lot if _raw_lot > 0 else _DEFAULT_LOT_SIZE

    d_market_min = _taker_min_tokens(d_live_price, d_lot_size)

    if d_live_price <= 0:
        return SizingDecision(False, Decimal("0"), Decimal("0"), d_market_min, "invalid_price", False, Decimal("0"))
    if d_base_notional <= 0:
        return SizingDecision(False, Decimal("0"), Decimal("0"), d_market_min, "zero_notional", False, Decimal("0"))
    if d_max_notional <= 0:
        return SizingDecision(False, Decimal("0"), Decimal("0"), d_market_min, "zero_cap", False, Decimal("0"))

    d_worst_price = d_live_price * (Decimal("1") + d_slippage)
    if d_tick_size > 0:
        d_worst_price = (d_worst_price / d_tick_size).to_integral_value(rounding=ROUND_CEILING) * d_tick_size

    def _quant_lot(value: Decimal, rounding) -> Decimal:
        """Quantize *value* to the lot grid via ``(value / lot) * lot``."""
        return (value / d_lot_size).to_integral_value(rounding=rounding) * d_lot_size

    # Desired size at live price — FLOOR so we never spend more than the
    # requested base notional.
    d_desired_size = _quant_lot(d_base_notional / d_live_price, ROUND_FLOOR)
    # Cap-clamp at max notional
    d_desired_size = min(
        d_desired_size,
        _quant_lot(d_max_notional / d_live_price, ROUND_FLOOR),
    )

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

    # Round size UP to lot grid so the upsized result reaches venue min.
    d_desired_size = _quant_lot(d_desired_size, ROUND_CEILING)

    # Worst-case notional check
    d_worst_notional = d_desired_size * d_worst_price
    if d_worst_notional > d_max_notional:
        # Try to floor to fit under cap
        d_floored = _quant_lot(d_max_notional / d_worst_price, ROUND_FLOOR)
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
    sell_tokens: Number,
    held_size: Number,
    live_price: Number,
    market_min_size: Number,
    lot_size: Number | None = None,
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
    lot_size:
        Token-lot grid from market metadata. Defaults to ``Decimal("0.0001")``
        (Polymarket CTF historical lot) when omitted; markets with a coarser
        lot must pass it explicitly so the venue does not reject off-tick.

    Returns
    -------
    SizingDecision
        ``accept=True`` when the order should proceed; ``accept=False``
        when it must be skipped.

    Notes
    -----
    SELL quantization uses ``ROUND_FLOOR``: we never round up a SELL,
    even if the held clamp would catch us. The defensive direction for
    SELL is to leave a tiny residual rather than risk attempting to
    submit more than the operator requested.
    """
    # Greptile P1: same zero-guard as the BUY path — ``lot_size=0`` from a
    # bad market-metadata response would otherwise divide by zero inside
    # ``_quant_lot`` and crash SELL sizing.
    _raw_lot = _to_decimal(lot_size) if lot_size is not None else _DEFAULT_LOT_SIZE
    d_lot_size = _raw_lot if _raw_lot > 0 else _DEFAULT_LOT_SIZE
    d_live_price = _to_decimal(live_price) if _to_decimal(live_price) > 0 else Decimal("0")
    d_market_min = _taker_min_tokens(d_live_price, d_lot_size)
    d_sell_in = _to_decimal(sell_tokens)

    if d_sell_in <= 0:
        return SizingDecision(False, Decimal("0"), d_live_price, d_market_min, "sell_size_zero", False, Decimal("0"))

    d_sell = d_sell_in
    d_held = _to_decimal(held_size)

    # Cannot sell more than held
    d_sell = min(d_sell, d_held)

    upsized = False
    # Below venue minimum — try to upsize to venue min if held shares allow
    if d_market_min > 0 and d_sell < d_market_min:
        if d_held < d_market_min:
            return SizingDecision(
                False, Decimal("0"), d_live_price, d_market_min,
                "sell_held_below_venue_min", False, Decimal("0"),
            )
        d_sell = d_market_min
        upsized = True

    # FLOOR to lot grid — conservative for SELL. Never round up, never sell
    # more than asked.
    d_sell = (d_sell / d_lot_size).to_integral_value(rounding=ROUND_FLOOR) * d_lot_size

    # Re-clamp to held — FLOOR may have produced a value still slightly above
    # held if held was off-grid. min() guarantees we never exceed held.
    d_sell = min(d_sell, d_held)

    # If FLOOR pushed us below venue min, skip — never submit a sub-min order.
    if d_market_min > 0 and d_sell < d_market_min:
        return SizingDecision(
            False, Decimal("0"), d_live_price, d_market_min,
            "sell_held_below_venue_min_after_rounding", False, Decimal("0"),
        )

    reason = "upsized" if upsized else "ok"
    d_sell_notional = d_sell * d_live_price
    return SizingDecision(True, d_sell, d_live_price, d_market_min, reason, upsized, d_sell_notional)