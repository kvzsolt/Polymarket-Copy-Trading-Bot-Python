"""Position/token-scoped market data cache with TTL and invalidation.

Phase 2 deliverable: replaces per-request REST polling for quote/tick/meta
data with a cache that is fed by WSS deltas and invalidated on key events.

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


# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------

class PositionPriceCache:
    """Token-scoped cache for quote and tick/meta data.

    Quote TTL defaults to 5 s (matching the WSS heartbeat cadence).
    Tick TTL defaults to 300 s (tick sizes change rarely).

    Both can be overridden via constructor for testing.
    """

    def __init__(
        self,
        *,
        quote_ttl: float = 5.0,
        tick_ttl: float = 300.0,
    ) -> None:
        self._quotes: dict[str, QuoteSnapshot] = {}
        self._ticks: dict[str, TickSnapshot] = {}
        self._quote_ttl = quote_ttl
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

    # --- Invalidation triggers ------------------------------------------------

    def invalidate_on_fill(self, token_id: str) -> None:
        """Own fill: clear quote so execution path fetches fresh data."""
        removed = self._quotes.pop(token_id, None)
        if removed:
            logger.debug("Cache invalidated (fill): quote for %s", token_id)

    def on_price_update(
        self,
        token_id: str,
        best_bid: Decimal | None,
        best_ask: Decimal | None,
    ) -> None:
        """WSS price-change event: refresh quote snapshot."""
        existing = self._quotes.get(token_id)
        merged_bid = best_bid if best_bid is not None else (existing.best_bid if existing else None)
        merged_ask = best_ask if best_ask is not None else (existing.best_ask if existing else None)
        self.set_quote(token_id, best_bid=merged_bid, best_ask=merged_ask)

    def on_tick_size_change(
        self,
        token_id: str,
        new_tick_size: Decimal,
    ) -> None:
        """WSS tick-size change: update tick snapshot and clear stale quote."""
        existing = self._ticks.get(token_id)
        min_order = existing.min_order_size if existing else Decimal("0")
        neg_risk = existing.neg_risk if existing else False
        self._ticks[token_id] = TickSnapshot(
            tick_size=new_tick_size,
            min_order_size=min_order,
            neg_risk=neg_risk,
            ts=time.monotonic(),
        )
        # Quote is now stale because tick grid changed.
        self._quotes.pop(token_id, None)
        logger.debug("Cache invalidated (tick_size_change): %s → %s", token_id, new_tick_size)

    def invalidate_on_reconcile(self, token_id: str) -> None:
        """Reconcile: clear both quote and tick for full refresh."""
        self._quotes.pop(token_id, None)
        self._ticks.pop(token_id, None)
        logger.debug("Cache invalidated (reconcile): %s", token_id)

    def invalidate_all(self) -> None:
        """Full wipe — used before REST bootstrap on reconnect."""
        self._quotes.clear()
        self._ticks.clear()
        logger.info("Cache fully invalidated (reconnect)")

    # --- Introspection --------------------------------------------------------

    def quote_count(self) -> int:
        return len(self._quotes)

    def tick_count(self) -> int:
        return len(self._ticks)

    def subscribed_tokens(self) -> set[str]:
        """Return all token IDs with any cached data."""
        return set(self._quotes.keys()) | set(self._ticks.keys())
