"""Book spread metrics for the spread quality gate."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_CEILING
from domain.units import BASIS_POINTS_PER_UNIT
from typing import Any, Mapping

from .types import BookSpreadMetrics, _SUPPORTED_BOOK_SPREAD_MODES


@dataclass(frozen=True)
class BookDepthMetrics:
    """Aggregate liquidity (in USDC notional) within a price band of the mid.

    ``band_pct`` is the half-band size relative to the mid (so a 5% band
    inspects asks/bids within ``mid * (1 +/- 0.05)``). When the band falls to
    the bottom of the book (price <= 0) the band is clamped at zero.
    """

    band_pct: Decimal
    band_low: Decimal
    band_high: Decimal
    bid_notional_usdc: Decimal
    ask_notional_usdc: Decimal
    bid_levels_in_band: int
    ask_levels_in_band: int

    def side_notional_usdc(self, side: str) -> Decimal:
        return self.ask_notional_usdc if side.upper() == "BUY" else self.bid_notional_usdc

    def side_levels(self, side: str) -> int:
        return self.ask_levels_in_band if side.upper() == "BUY" else self.bid_levels_in_band


def resolve_book_spread_mode(raw_mode: Any) -> str:
    mode = str(raw_mode or "absolute_bps").strip().lower()
    if mode in _SUPPORTED_BOOK_SPREAD_MODES:
        return mode
    return "absolute_bps"


def calculate_book_spread_metrics(
    *, best_bid: Decimal, best_ask: Decimal, tick_size: Decimal
) -> BookSpreadMetrics:
    absolute = best_ask - best_bid
    absolute_bps = int((absolute * BASIS_POINTS_PER_UNIT).to_integral_value(rounding=ROUND_CEILING))
    relative_bps: int | None = None
    if best_bid > 0:
        relative_bps = int(
            ((absolute / best_bid) * BASIS_POINTS_PER_UNIT).to_integral_value(
                rounding=ROUND_CEILING
            )
        )
    ticks: int | None = None
    if tick_size > 0:
        ticks = int((absolute / tick_size).to_integral_value(rounding=ROUND_CEILING))
    return BookSpreadMetrics(
        absolute=absolute,
        absolute_bps=absolute_bps,
        relative_bps=relative_bps,
        ticks=ticks,
    )


def _normalize_levels(levels: Any) -> dict[Decimal, Decimal]:
    """Coerce a book-side payload into ``{price: size}`` Decimal pairs.

    Accepts the same shapes that ``BookSnapshot`` already produces: a
    ``{Decimal: Decimal}`` dict (the canonical form), a ``{str: Any}`` dict
    (e.g. JSON payloads), or an iterable of ``(price, size)`` tuples. Invalid
    entries are silently skipped — depth is a soft signal, not an oracle.
    """
    if not levels:
        return {}
    out: dict[Decimal, Decimal] = {}
    if isinstance(levels, Mapping):
        items: Any = levels.items()
    else:
        items = levels
    for entry in items:
        if isinstance(entry, tuple) and len(entry) == 2:
            raw_price, raw_size = entry
        else:
            continue
        try:
            price = raw_price if isinstance(raw_price, Decimal) else Decimal(str(raw_price))
            size = raw_size if isinstance(raw_size, Decimal) else Decimal(str(raw_size))
        except Exception:
            continue
        if price <= 0 or size <= 0:
            continue
        out[price] = out.get(price, Decimal("0")) + size
    return out


def calculate_book_depth_metrics(
    *,
    bids: Any,
    asks: Any,
    best_bid: Decimal | None,
    best_ask: Decimal | None,
    band_pct: Decimal | float | int,
) -> BookDepthMetrics:
    """Aggregate notional liquidity (price * size) within ``band_pct`` of mid.

    A missing bid or ask only zeros that side; the other side is still summed
    so one-sided books retain useful diagnostic numbers for callers that want
    to log them. Callers that need to *reject* on a missing side should check
    ``best_bid``/``best_ask`` directly — this function only measures.
    """
    band = Decimal(str(band_pct)) / Decimal(100)
    if band < 0:
        band = Decimal(0)

    if best_bid is not None and best_ask is not None and best_bid > 0:
        mid = (best_bid + best_ask) / Decimal(2)
    elif best_ask is not None:
        mid = best_ask
    elif best_bid is not None:
        mid = best_bid
    else:
        mid = Decimal("0")

    if mid <= 0:
        return BookDepthMetrics(
            band_pct=band * Decimal(100),
            band_low=Decimal("0"),
            band_high=Decimal("0"),
            bid_notional_usdc=Decimal("0"),
            ask_notional_usdc=Decimal("0"),
            bid_levels_in_band=0,
            ask_levels_in_band=0,
        )

    band_low = max(Decimal("0"), mid * (Decimal(1) - band))
    band_high = mid * (Decimal(1) + band)

    norm_bids = _normalize_levels(bids)
    norm_asks = _normalize_levels(asks)

    bid_notional = Decimal("0")
    bid_levels = 0
    for price, size in norm_bids.items():
        if band_low <= price <= band_high:
            bid_notional += price * size
            bid_levels += 1

    ask_notional = Decimal("0")
    ask_levels = 0
    for price, size in norm_asks.items():
        if band_low <= price <= band_high:
            ask_notional += price * size
            ask_levels += 1

    return BookDepthMetrics(
        band_pct=band * Decimal(100),
        band_low=band_low,
        band_high=band_high,
        bid_notional_usdc=bid_notional,
        ask_notional_usdc=ask_notional,
        bid_levels_in_band=bid_levels,
        ask_levels_in_band=ask_levels,
    )
