"""Book spread metrics for the spread quality gate."""
from __future__ import annotations

from decimal import Decimal, ROUND_CEILING
from typing import Any

from .types import BookSpreadMetrics, _SUPPORTED_BOOK_SPREAD_MODES


def resolve_book_spread_mode(raw_mode: Any) -> str:
    mode = str(raw_mode or "absolute_bps").strip().lower()
    if mode in _SUPPORTED_BOOK_SPREAD_MODES:
        return mode
    return "absolute_bps"


def calculate_book_spread_metrics(
    *, best_bid: Decimal, best_ask: Decimal, tick_size: Decimal
) -> BookSpreadMetrics:
    absolute = best_ask - best_bid
    absolute_bps = int(
        (absolute * Decimal(10_000)).to_integral_value(rounding=ROUND_CEILING)
    )
    relative_bps: int | None = None
    if best_bid > 0:
        relative_bps = int(
            ((absolute / best_bid) * Decimal(10_000)).to_integral_value(
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
