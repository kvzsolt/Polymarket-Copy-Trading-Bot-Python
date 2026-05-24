"""Tests for cache.market_state_store — MarketStateStore.

Covers:
  - Cache reuse within freshness window
  - Invalidation marks stale/dirty
  - Invalidation on fill (via invalidate)
  - Invalidation on tick size change
  - Price update from WSS
  - Invalidation on reconcile (via invalidate)
  - Full invalidation for reconnect bootstrap
  - Tick data survives quote invalidation
"""
from __future__ import annotations

import time
from decimal import Decimal

import pytest

from cache.market_state_store import (
    MarketStateStore,
    QuoteSnapshot,
    TickSnapshot,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def cache() -> MarketStateStore:
    """Fresh MarketStateStore."""
    return MarketStateStore()


TOKEN_A = "0xaaa_token_yes"
TOKEN_B = "0xbbb_token_no"


# ---------------------------------------------------------------------------
# Cache reuse and freshness
# ---------------------------------------------------------------------------

class TestCacheReuse:
    """Verify cache serves data when fresh."""

    def test_cache_reuse_quote(self, cache: MarketStateStore) -> None:
        """Quote returned from cache when set."""
        cache.set_quote(TOKEN_A, best_bid=Decimal("0.55"), best_ask=Decimal("0.60"))

        quote = cache.get_quote(TOKEN_A)
        assert quote is not None
        assert quote.best_bid == Decimal("0.55")
        assert quote.best_ask == Decimal("0.60")

    def test_cache_reuse_tick(self, cache: MarketStateStore) -> None:
        """Tick data persists across quote invalidation."""
        cache.set_tick(
            TOKEN_A,
            tick_size=Decimal("0.01"),
            min_order_size=Decimal("5"),
            neg_risk=False,
        )
        cache.set_quote(TOKEN_A, best_bid=Decimal("0.50"), best_ask=Decimal("0.55"))

        # Invalidate quote
        cache.invalidate(TOKEN_A)

        # Quote gone, tick persists
        assert cache.get_quote(TOKEN_A) is None
        tick = cache.get_tick(TOKEN_A)
        assert tick is not None
        assert tick.tick_size == Decimal("0.01")
        assert tick.min_order_size == Decimal("5")

    def test_get_quote_missing_token(self, cache: MarketStateStore) -> None:
        """get_quote returns None for token not in cache."""
        assert cache.get_quote("nonexistent_token") is None

    def test_get_tick_missing_token(self, cache: MarketStateStore) -> None:
        """get_tick returns None for token not in cache."""
        assert cache.get_tick("nonexistent_token") is None


# ---------------------------------------------------------------------------
# Invalidation triggers
# ---------------------------------------------------------------------------

class TestInvalidation:
    """Verify invalidation triggers clear appropriate cache entries."""

    def test_invalidate_on_fill(self, cache: MarketStateStore) -> None:
        """Own fill clears quote and book for the affected token only."""
        cache.set_quote(TOKEN_A, best_bid=Decimal("0.55"), best_ask=Decimal("0.60"))
        cache.set_book(TOKEN_A, bids={Decimal("0.55"): Decimal("100")}, asks={Decimal("0.60"): Decimal("50")})
        cache.set_quote(TOKEN_B, best_bid=Decimal("0.40"), best_ask=Decimal("0.45"))

        cache.invalidate(TOKEN_A)

        assert cache.get_quote(TOKEN_A) is None
        assert cache.get_book(TOKEN_A) is None
        assert cache.get_quote(TOKEN_B) is not None  # unaffected

    def test_invalidate_on_fill_noop_for_missing(self, cache: MarketStateStore) -> None:
        """Invalidating a token not in cache does not raise."""
        cache.invalidate("nonexistent")  # should not raise

    def test_on_tick_size_change(self, cache: MarketStateStore) -> None:
        """Tick size change updates tick and marks dirty."""
        cache.set_tick(
            TOKEN_A,
            tick_size=Decimal("0.01"),
            min_order_size=Decimal("5"),
            neg_risk=False,
        )
        cache.set_quote(TOKEN_A, best_bid=Decimal("0.55"), best_ask=Decimal("0.60"))

        cache.on_tick_size_change(TOKEN_A, Decimal("0.001"))

        # Quote not immediately cleared, but state is dirty
        tick = cache.get_tick(TOKEN_A)
        assert tick is not None
        assert tick.tick_size == Decimal("0.001")
        # Preserved fields
        assert tick.min_order_size == Decimal("5")

    def test_on_price_update(self, cache: MarketStateStore) -> None:
        """WSS price change event refreshes quote snapshot."""
        cache.on_price_update(TOKEN_A, Decimal("0.60"), Decimal("0.65"))

        quote = cache.get_quote(TOKEN_A)
        assert quote is not None
        assert quote.best_bid == Decimal("0.60")
        assert quote.best_ask == Decimal("0.65")

    def test_invalidate_on_reconcile(self, cache: MarketStateStore) -> None:
        """Reconcile clears quote for the token (invalidate marks stale)."""
        cache.set_quote(TOKEN_A, best_bid=Decimal("0.55"), best_ask=Decimal("0.60"))
        cache.set_tick(
            TOKEN_A,
            tick_size=Decimal("0.01"),
            min_order_size=Decimal("5"),
            neg_risk=False,
        )

        cache.invalidate(TOKEN_A)

        assert cache.get_quote(TOKEN_A) is None

    def test_invalidate_all(self, cache: MarketStateStore) -> None:
        """Full wipe clears everything for reconnect bootstrap."""
        cache.set_quote(TOKEN_A, best_bid=Decimal("0.55"), best_ask=Decimal("0.60"))
        cache.set_quote(TOKEN_B, best_bid=Decimal("0.40"), best_ask=Decimal("0.45"))
        cache.set_book(TOKEN_A, bids={Decimal("0.55"): Decimal("100")}, asks={})
        cache.set_tick(
            TOKEN_A,
            tick_size=Decimal("0.01"),
            min_order_size=Decimal("5"),
            neg_risk=False,
        )

        cache.invalidate_all()

        assert cache.get_quote(TOKEN_A) is None
        assert cache.get_quote(TOKEN_B) is None
        assert cache.get_book(TOKEN_A) is None
        assert cache.get_tick(TOKEN_A) is None


# ---------------------------------------------------------------------------
# Data snapshot immutability
# ---------------------------------------------------------------------------

class TestSnapshots:
    """Verify dataclass snapshot integrity."""

    def test_quote_snapshot_frozen(self) -> None:
        snap = QuoteSnapshot(
            token_id="tok",
            best_bid=Decimal("0.50"),
            best_ask=Decimal("0.55"),
            last_trade_price=None,
            source="test",
            monotonic_ts=time.monotonic(),
        )
        with pytest.raises(AttributeError):
            snap.best_bid = Decimal("0.99")  # type: ignore[misc]

    def test_tick_snapshot_frozen(self) -> None:
        snap = TickSnapshot(
            token_id="tok",
            tick_size=Decimal("0.01"),
            min_order_size=Decimal("5"),
            neg_risk=False,
            source="test",
            monotonic_ts=time.monotonic(),
        )
        with pytest.raises(AttributeError):
            snap.tick_size = Decimal("0.001")  # type: ignore[misc]