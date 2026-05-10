"""Market domain types — canonical market metadata from the venue."""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal


@dataclass(frozen=True)
class MarketToken:
    """A single outcome token within a market."""

    token_id: str
    outcome: str
    price: Decimal = Decimal("0")


@dataclass(frozen=True)
class MarketInfo:
    """Canonical market metadata from the venue."""

    condition_id: str
    question_id: str | None = None
    tokens: list[MarketToken] | None = None
    min_order_size: Decimal = Decimal("0")
    tick_size: Decimal = Decimal("0.01")
    active: bool = True
    closed: bool = False
    archived: bool = False
