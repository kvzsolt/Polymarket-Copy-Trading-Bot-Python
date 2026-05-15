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
    """Result from submitting an order to the venue.

    ``ambiguous_post_failure`` is True when the venue acknowledged the POST
    timed out after the order was already sent. Callers must persist a
    ``submitted_unconfirmed`` record and let reconciliation resolve the
    terminal state — a retry would risk duplicate fills.

    ``non_retryable`` is True when the venue rejection is final (invalid
    payload, auth, banned market, balance/allowance) and the request must not
    consume retry budget.
    """

    success: bool
    order_id: str | None = None
    status: str | None = None
    raw: dict | None = None
    error: str | None = None
    ambiguous_post_failure: bool = False
    non_retryable: bool = False


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
    """Snapshotted order book for a token.

    CLOB `/book` responses also carry execution metadata (`tick_size`,
    `min_order_size`, `neg_risk`). Keep it attached to the canonical book so
    REST-snapshot execution does not need a second metadata round-trip and,
    more importantly, does not drop tick metadata on the floor.
    """

    token_id: str
    bids: list[OrderBookLevel] = field(default_factory=list)
    asks: list[OrderBookLevel] = field(default_factory=list)
    best_bid: Decimal | None = None
    best_ask: Decimal | None = None
    spread_bps: int | None = None
    timestamp: datetime | None = None
    tick_size: Decimal | None = None
    min_order_size: Decimal | None = None
    neg_risk: bool = False


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
    """Request to submit a marketable (FOK/IOC) order.

    ``tick_size`` and ``neg_risk`` are optional pre-fetched market metadata.
    When the caller has already fetched metadata via ``MarketDataService``,
    passing them avoids a redundant venue round-trip inside the gateway. When
    ``None`` the gateway fetches metadata itself.
    """

    token_id: str
    side: str
    size: Decimal
    price_limit: Decimal | None = None
    condition_id: str | None = None
    tick_size: Decimal | None = None
    neg_risk: bool | None = None
