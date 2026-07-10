"""Deprecated import bridge to the unified Polymarket SDK client factory.

Production code should import from ``integrations.polymarket.sdk_client``.
This module contains no venue protocol implementation and can be removed after
external callers have moved to the snake_case module path.
"""

from __future__ import annotations

from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from typing import Any

from polymarket import AsyncSecureClient as ClobClient

from integrations.polymarket.sdk_client import (
    MarketMeta,
    PolymarketClient,
    create_polymarket_client,
)
from integrations.polymarket.models import PRICE_TOKEN_NOT_FOUND
from services.market_data_quality import (
    is_thin_or_empty_book as _is_thin_or_empty_book,
    top_of_book_from_levels as _top_of_book_from_levels,
)

ClobClientWrapper = PolymarketClient
create_clob_client = create_polymarket_client


def configure_from_appconfig(_config: Any) -> None:
    """No-op retained while legacy composition code is removed.

    The unified SDK owns its environment and transport configuration; there is
    no process-wide client monkey patch to mutate.
    """


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
    "ClobClient",
    "ClobClientWrapper",
    "MarketMeta",
    "PRICE_TOKEN_NOT_FOUND",
    "configure_from_appconfig",
    "create_clob_client",
    "quantize_to_tick",
    "_is_thin_or_empty_book",
    "_top_of_book_from_levels",
]
