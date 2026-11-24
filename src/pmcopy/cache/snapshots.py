"""Immutable market-data snapshots and per-token cache state dataclasses."""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any


def _env_int(name: str, default: int) -> int:
    """Read a non-negative int env override, falling back to ``default``."""
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        value = int(raw)
        return value if value > 0 else default
    except (TypeError, ValueError):
        return default


_logger = logging.getLogger("MarketStateStore")


# ---------------------------------------------------------------------------
# Snapshots returned to services
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class QuoteSnapshot:
    """Observed quote with continuity validity separate from observation age."""

    token_id: str
    best_bid: Decimal | None
    best_ask: Decimal | None
    last_trade_price: Decimal | None
    source: str
    monotonic_ts: float
    valid: bool = True

    def is_fresh(self, max_age_seconds: float) -> bool:
        """Return whether continuity and age both permit use of this observation."""
        return self.valid and 0 <= time.monotonic() - self.monotonic_ts <= max_age_seconds


@dataclass(frozen=True)
class BookSnapshot:
    token_id: str
    bids: dict[Decimal, Decimal]
    asks: dict[Decimal, Decimal]
    best_bid: Decimal | None
    best_ask: Decimal | None
    source: str
    monotonic_ts: float

    def is_fresh(self, max_age_seconds: float) -> bool:
        return time.monotonic() - self.monotonic_ts <= max_age_seconds


@dataclass(frozen=True)
class TickSnapshot:
    token_id: str
    tick_size: Decimal
    min_order_size: Decimal
    neg_risk: bool
    source: str
    monotonic_ts: float

    def is_fresh(self, max_age_seconds: float) -> bool:
        return time.monotonic() - self.monotonic_ts <= max_age_seconds


# ---------------------------------------------------------------------------
# Mutable internal state
# ---------------------------------------------------------------------------


@dataclass
class TokenMarketState:
    token_id: str

    condition_id: str | None = None

    bids: dict[Decimal, Decimal] = field(default_factory=dict)
    asks: dict[Decimal, Decimal] = field(default_factory=dict)

    best_bid: Decimal | None = None
    best_ask: Decimal | None = None
    last_trade_price: Decimal | None = None

    tick_size: Decimal | None = None
    min_order_size: Decimal | None = None
    neg_risk: bool | None = None

    active: bool = True
    resolved: bool = False

    # dirty=True means execution must not trade from this token until a fresh
    # authoritative book/tick state is available.
    dirty: bool = True
    stale: bool = True
    dirty_reason: str | None = "not_initialized"
    stale_reason: str | None = "not_initialized"

    # Aggregate ``source`` kept for back-compat with callers that just want a
    # last-touched hint. Component-specific sources are authoritative for
    # executable freshness checks. They are NOT promoted by tick-size changes
    # — a WSS ``tick_size_change`` event must not make a REST-only quote/book
    # appear WSS-fresh.
    source: str = "unknown"
    quote_source: str = "unknown"
    book_source: str = "unknown"
    tick_source: str = "unknown"
    metadata_source: str = "unknown"

    last_update_monotonic: float = 0.0
    last_book_monotonic: float = 0.0
    last_quote_monotonic: float = 0.0
    last_tick_monotonic: float = 0.0
    last_trade_monotonic: float = 0.0

    last_event_raw: dict[str, Any] | None = None

    def recompute_top_of_book(self) -> None:
        self.best_bid = max(self.bids) if self.bids else None
        self.best_ask = min(self.asks) if self.asks else None

    def mark_updated(self) -> None:
        self.last_update_monotonic = time.monotonic()




__all__ = [
    "BookSnapshot",
    "QuoteSnapshot",
    "TickSnapshot",
    "TokenMarketState",
    "_env_int",
]
