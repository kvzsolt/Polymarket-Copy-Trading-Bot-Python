"""Legacy-named exact-decimal market-data helpers.

No client is constructed here. Runtime venue ownership belongs exclusively to
the CCXT composition under :mod:`integrations.polymarket`.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from typing import Any

from pmcopy.integrations.polymarket.models import PRICE_TOKEN_NOT_FOUND
from pmcopy.services.market_data_quality import (
    is_thin_or_empty_book as _is_thin_or_empty_book,
    top_of_book_from_levels as _top_of_book_from_levels,
)


@dataclass(frozen=True, slots=True)
class MarketMeta:
    """Execution metadata snapshot used by older deterministic test seams."""

    token_id: str
    tick_size: Decimal
    min_order_size: Decimal
    neg_risk: bool
    best_bid: Decimal | None
    best_ask: Decimal | None


def configure_from_appconfig(_config: Any) -> None:
    """Keep historical callers side-effect free during configuration load."""


def quantize_to_tick(
    price: Decimal,
    tick_size: Decimal,
    *,
    side: str | None = None,
) -> Decimal:
    """Quantize an execution limit conservatively using exact decimals."""
    if tick_size <= 0:
        return price
    rounding = ROUND_CEILING if str(side or "").upper() == "BUY" else ROUND_FLOOR
    steps = (price / tick_size).to_integral_value(rounding=rounding)
    return steps * tick_size


__all__ = [
    "MarketMeta",
    "PRICE_TOKEN_NOT_FOUND",
    "configure_from_appconfig",
    "quantize_to_tick",
    "_is_thin_or_empty_book",
    "_top_of_book_from_levels",
]
