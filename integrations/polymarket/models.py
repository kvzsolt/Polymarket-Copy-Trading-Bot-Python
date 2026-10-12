"""Venue model types for the CLOB gateway.

Canonical dataclasses that decouple the rest of the codebase from the
unified Polymarket SDK's own types.  All gateway methods return these types;
callers should never depend on raw SDK shapes directly.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Literal

from domain.execution import ExecutionOrderType


class _PriceTokenNotFound:
    __slots__ = ()

    def __repr__(self) -> str:
        return "PRICE_TOKEN_NOT_FOUND"


PRICE_TOKEN_NOT_FOUND = _PriceTokenNotFound()


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
    effective_price: Decimal | None = None
    trade_ids: list[str] | None = None
    transactions_hashes: list[str] | None = None
    # The venue's own rejection text, scrubbed and bounded. ``error`` stays a
    # stable classification token that callers match on by equality, so the
    # detail that distinguishes two rejections sharing a token lives here.
    venue_reason: str | None = None


@dataclass(frozen=True)
class VenueOrderState:
    """Current state of an order on the venue.

    ``token_id`` is the normalized raw outcome token the order trades.  It
    is the adapter boundary identity: two outcomes of one condition (YES
    and NO) share every other descriptive field, so recovery matching may
    only treat two orders as the same intent when their tokens are equal.
    ``None`` means the venue payload carried no token identity — never a
    guessed mapping — and fail-closed consumers must reject such rows.
    """

    order_id: str
    status: str
    side: str | None = None
    price: Decimal | None = None
    size: Decimal | None = None
    filled_size: Decimal = Decimal("0")
    avg_fill_price: Decimal = Decimal("0")
    created_at: datetime | None = None
    token_id: str | None = None
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
    fee: Decimal | None = None
    timestamp: int | None = None
    status: str | None = None
    transaction_hash: str | None = None
    trader_side: str | None = None
    bucket_index: int | None = None
    raw: dict | None = None


@dataclass(frozen=True)
class VenueCancelResult:
    """Result from cancelling an order on the venue."""

    success: bool
    order_id: str | None = None
    status: str | None = None
    error: str | None = None


@dataclass(frozen=True, kw_only=True)
class OrderBook:
    """Snapshotted order book for a token.

    CLOB `/book` responses also carry execution metadata (`tick_size`,
    `min_order_size`, `neg_risk`). Keep it attached to the canonical book so
    REST-snapshot execution does not need a second metadata round-trip and,
    more importantly, does not drop tick metadata on the floor.
    """

    token_id: str
    tick_size: Decimal
    min_order_size: Decimal
    neg_risk: bool
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
    """Request to submit a marketable (FOK/IOC) order.

    ``tick_size`` and ``neg_risk`` preserve the caller's audit observation.
    The live CCXT adapter still refreshes all execution metadata immediately
    before signing; it never trusts stale caller values.
    """

    token_id: str
    side: str
    size: Decimal
    price_limit: Decimal | None = None
    condition_id: str | None = None
    tick_size: Decimal | None = None
    neg_risk: bool | None = None
    order_type: ExecutionOrderType | Literal["FOK"] | None = None
