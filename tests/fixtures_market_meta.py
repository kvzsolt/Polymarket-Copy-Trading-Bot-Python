"""Test-only market metadata fixture replacing legacy ``createClobClient.MarketMeta``."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True, slots=True)
class MarketMeta:
    """Execution metadata snapshot used by deterministic unit test doubles."""

    token_id: str
    tick_size: Decimal
    min_order_size: Decimal
    neg_risk: bool
    best_bid: Decimal | None
    best_ask: Decimal | None
