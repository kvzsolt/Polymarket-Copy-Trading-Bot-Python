"""Position/token-scoped market data cache with TTL and invalidation.

Phase 2 / issue #68 deliverable: replaces per-request REST polling for
quote/book/tick/meta data with a cache that is fed by WSS deltas and
invalidated on key events.

Invalidation triggers (per spec):
  - own fill
  - price change (WSS delta)
  - tick size change (WSS delta)
  - reconcile

All timestamps use ``time.monotonic()`` for jitter-free TTL comparisons.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from utils.logger import get_logger

logger = get_logger("PriceCache")


# ---------------------------------------------------------------------------
# Data snapshots
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class QuoteSnapshot:
    """Best-bid/ask snapshot for a single token."""
    best_bid: Decimal | None
    best_ask: Decimal | None
    midpoint: Decimal | None
    ts: float  # monotonic timestamp


@dataclass(frozen=True)
class TickSnapshot:
    """Tick-size and venue metadata snapshot for a single token."""
    tick_size: Decimal
    min_order_size: Decimal
    neg_risk: bool
    ts: float  # monotonic timestamp


@dataclass(frozen=True)
class BookLevel:
    """A single price level in the cached order book."""
    price: Decimal
    size: Decimal


@dataclass
class BookSnapshot:
    """Full order book snapshot for a single token.

    Mutable container so that delta application can add/remove levels
    in-place without reconstructing the entire snapshot on every event.
    """
    bids: dict[Decimal, Decimal] = field(default_factory=dict)
    asks: dict[Decimal, Decimal] = field(default_factory=dict)
    ts: float = 0.0

    def best_bid(self) -> Decimal | None:
        return max(self.bids.keys()) if self.bids else None

    def best_ask(self) -> Decimal | None:
        return min(self.asks.keys()) if self.asks else None


# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------

class PositionPriceCache:
    """Token-scoped cache for quote, book, tick/meta data.

    Quote TTL defaults to 5 s (matching the WSS heartbeat cadence).
    Book TTL defaults to 5 s.
    Tick TTL defaults to 300 s (tick sizes change rarely).

    All can be overridden via constructor for testing.
    """

    def __init__(
        self,
        *,
        quote_ttl: float = 5.0,
        book_ttl: float = 5.0,
        tick_ttl: float = 300.0,
    ) -> None:
        self._quotes: dict[str, QuoteSnapshot] = {}
        self._books: dict[str, BookSnapshot] = {}
        self._ticks: dict[str, TickSnapshot] = {}
        self._quote_ttl = quote_ttl
        self._book_ttl = book_ttl
        self._tick_ttl = tick_ttl

    # --- Reads ----------------------------------------------------------------

    def get_quote(self, token_id: str) -> QuoteSnapshot | None:
        """Return cached quote if fresh, else ``None``."""
        snap = self._quotes.get(token_id)
        if snap is None:
            return None
        if (time.monotonic() - snap.ts) > self._quote_ttl:
            del self._quotes[token_id]
            return None
        return snap

    def get_book(self, token_id: str) -> BookSnapshot | None:
        """Return cached book if fresh, else ``None``."""
        snap = self._books.get(token_id)
        if snap is None:
            return None
        if (time.monotonic() - snap.ts) > self._book_ttl:
            del self._books[token_id]
            return None
        return snap

    def get_tick(self, token_id: str) -> TickSnapshot | None:
        """Return cached tick/meta if fresh, else ``None``."""
        snap = self._ticks.get(token_id)
        if snap is None:
            return None
        if (time.monotonic() - snap.ts) > self._tick_ttl:
            del self._ticks[token_id]
            return None
        return snap

    # --- Writes (from WSS / REST bootstrap) -----------------------------------

    def set_quote(
        self,
        token_id: str,
        *,
        best_bid: Decimal | None,
        best_ask: Decimal | None,
    ) -> None:
        """Store/refresh quote from a WSS price event or REST snapshot."""
        mid: Decimal | None = None
        if best_bid is not None and best_ask is not None:
            mid = (best_bid + best_ask) / 2
        self._quotes[token_id] = QuoteSnapshot(
            best_bid=best_bid,
            best_ask=best_ask,
            midpoint=mid,
            ts=time.monotonic(),
        )

    def set_book(
        self,
        token_id: str,
        *,
        bids: dict[Decimal, Decimal],
        asks: dict[Decimal, Decimal],
    ) -> None:
        """Store/refresh full book from a REST snapshot."""
        self._books[token_id] = BookSnapshot(
            bids=dict(bids),
            asks=dict(asks),
            ts=time.monotonic(),
        )

    def set_tick(
        self,
        token_id: str,
        *,
        tick_size: Decimal,
        min_order_size: Decimal,
        neg_risk: bool,
    ) -> None:
        """Store/refresh tick metadata from a WSS event or REST snapshot."""
        self._ticks[token_id] = TickSnapshot(
            tick_size=tick_size,
            min_order_size=min_order_size,
            neg_risk=neg_risk,
            ts=time.monotonic(),
        )

    def apply_book_delta(
        self,
        token_id: str,
        side: str,
        price: Decimal,
        size: Decimal,
    ) -> None:
        """Apply a single book level delta from a WSS price_change event.

        When ``size`` is 0 the level is deleted from the book (per spec).
        When ``size`` > 0 the level is upserted.
        Creates a new ``BookSnapshot`` if none exists for the token.
        """
        book = self._books.get(token_id)
        if book is None:
            book = BookSnapshot(ts=time.monotonic())
            self._books[token_id] = book

        levels = book.bids if side.upper() == "BUY" else book.asks

        if size == Decimal("0"):
            levels.pop(price, None)
            logger.debug("Book level deleted: %s %s @ %s", token_id, side, price)
        else:
            levels[price] = size
            logger.debug("Book level upserted: %s %s @ %s x %s", token_id, side, price, size)

        book.ts = time.monotonic()

    # --- Invalidation triggers ------------------------------------------------

    def invalidate_on_fill(self, token_id: str) -> None:
        """Own fill: clear quote and book so execution path fetches fresh data."""
        self._quotes.pop(token_id, None)
        self._books.pop(token_id, None)
        try:
            from services.metrics import METRIC_CACHE_STALE_EVICTIONS, metrics

            metrics.inc_sync(METRIC_CACHE_STALE_EVICTIONS, labels={"reason": "fill"})
        except Exception:
            pass
        logger.debug("Cache invalidated (fill): quote+book for %s", token_id)

    def on_price_update(
        self,
        token_id: str,
        best_bid: Decimal | None,
        best_ask: Decimal | None,
    ) -> None:
        """WSS price-change event: refresh quote snapshot."""
        existing = self.get_quote(token_id)
        merged_bid = best_bid if best_bid is not None else (existing.best_bid if existing else None)
        merged_ask = best_ask if best_ask is not None else (existing.best_ask if existing else None)
        self.set_quote(token_id, best_bid=merged_bid, best_ask=merged_ask)

    def on_tick_size_change(
        self,
        token_id: str,
        new_tick_size: Decimal,
    ) -> None:
        """WSS tick-size change: update tick snapshot and clear stale quote/book."""
        existing = self._ticks.get(token_id)
        min_order = existing.min_order_size if existing else Decimal("0")
        neg_risk = existing.neg_risk if existing else False
        self._ticks[token_id] = TickSnapshot(
            tick_size=new_tick_size,
            min_order_size=min_order,
            neg_risk=neg_risk,
            ts=time.monotonic(),
        )
        self._quotes.pop(token_id, None)
        self._books.pop(token_id, None)
        logger.debug("Cache invalidated (tick_size_change): %s → %s", token_id, new_tick_size)

    def invalidate_on_reconcile(self, token_id: str) -> None:
        """Reconcile: clear quote, book, and tick for full refresh."""
        self._quotes.pop(token_id, None)
        self._books.pop(token_id, None)
        self._ticks.pop(token_id, None)
        try:
            from services.metrics import METRIC_CACHE_STALE_EVICTIONS, metrics

            metrics.inc_sync(
                METRIC_CACHE_STALE_EVICTIONS, labels={"reason": "reconcile"}
            )
        except Exception:
            pass
        logger.debug("Cache invalidated (reconcile): %s", token_id)

    def invalidate_all(self) -> None:
        """Full wipe — used before REST bootstrap on reconnect."""
        self._quotes.clear()
        self._books.clear()
        self._ticks.clear()
        try:
            from services.metrics import METRIC_CACHE_STALE_EVICTIONS, metrics

            metrics.inc_sync(
                METRIC_CACHE_STALE_EVICTIONS, labels={"reason": "reconnect"}
            )
        except Exception:
            pass
        logger.info("Cache fully invalidated (reconnect)")

    # --- Introspection --------------------------------------------------------

    def quote_count(self) -> int:
        return len(self._quotes)

    def book_count(self) -> int:
        return len(self._books)

    def tick_count(self) -> int:
        return len(self._ticks)

    def subscribed_tokens(self) -> set[str]:
        """Return all token IDs with any cached data."""
        return set(self._quotes.keys()) | set(self._books.keys()) | set(self._ticks.keys())
