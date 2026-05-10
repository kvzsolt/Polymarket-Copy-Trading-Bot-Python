"""Venue model types for the CLOB gateway.

Canonical dataclasses that decouple the rest of the codebase from
py_clob_client_v2's own types.  All gateway methods return these types;
callers should never depend on raw SDK shapes directly.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal


@dataclass(frozen=True)
class VenueSubmitResult:
    """Result from submitting an order to the venue."""

    success: bool
    order_id: str | None = None
    status: str | None = None
    raw: dict | None = None
    error: str | None = None


@dataclass(frozen=True)
class VenueOrderState:
    """Current state of an order on the venue."""

    order_id: str
    status: str
    side: str | None = None
    price: Decimal | None = None
    size: Decimal | None = None
    filled_size: Decimal = Decimal("0")
    avg_fill_price: Decimal = Decimal("0")
    created_at: datetime | None = None
    raw: dict | None = None


@dataclass(frozen=True)
class VenueFill:
    """A single fill event from the venue."""

    fill_id: str | None = None
    order_id: str | None = None
    token_id: str | None = None
    side: str | None = None
    size: Decimal = Decimal("0")
    price: Decimal = Decimal("0")
    fee: Decimal = Decimal("0")
    timestamp: int | None = None
    raw: dict | None = None


@dataclass(frozen=True)
class VenueCancelResult:
    """Result from cancelling an order on the venue."""

    success: bool
    order_id: str | None = None
    status: str | None = None
    error: str | None = None


@dataclass(frozen=True)
class OrderBook:
    """Snapshotted order book for a token."""

    token_id: str
    bids: list[OrderBookLevel] = field(default_factory=list)
    asks: list[OrderBookLevel] = field(default_factory=list)
    best_bid: Decimal | None = None
    best_ask: Decimal | None = None
    spread_bps: int | None = None
    timestamp: datetime | None = None


@dataclass(frozen=True)
class OrderBookLevel:
    """A single price level in an order book."""

    price: Decimal
    size: Decimal


@dataclass(frozen=True)
class LimitOrderRequest:
    """Request to submit a limit order."""

    token_id: str
    side: str
    price: Decimal
    size: Decimal
    condition_id: str | None = None


@dataclass(frozen=True)
class MarketableOrderRequest:
    """Request to submit a marketable (FOK/IOC) order."""

    token_id: str
    side: str
    size: Decimal
    price_limit: Decimal | None = None
    condition_id: str | None = None
