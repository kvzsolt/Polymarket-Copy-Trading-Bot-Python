"""Tests for services.price_cache — Position-scoped market data cache.

Phase 2 test coverage:
  - Cache reuse within TTL
  - Cache expiry returns None
  - Invalidation on fill
  - Invalidation on tick size change
  - Price update from WSS
  - Invalidation on reconcile
  - Full invalidation for reconnect bootstrap
  - Tick data survives quote invalidation
"""

from __future__ import annotations

import time
from decimal import Decimal
from unittest.mock import patch

import pytest

from services.price_cache import PositionPriceCache, QuoteSnapshot, TickSnapshot


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def cache() -> PositionPriceCache:
    """Fresh cache with short TTLs for testing."""
    return PositionPriceCache(quote_ttl=5.0, tick_ttl=300.0)


TOKEN_A = "0xaaa_token_yes"
TOKEN_B = "0xbbb_token_no"


# ---------------------------------------------------------------------------
# Cache reuse and expiry
# ---------------------------------------------------------------------------

class TestCacheReuse:
    """Verify cache serves data within TTL and returns None when expired."""

    def test_cache_reuse_within_ttl(self, cache: PositionPriceCache) -> None:
        """Quote returned from cache when within TTL."""
        cache.set_quote(TOKEN_A, best_bid=Decimal("0.55"), best_ask=Decimal("0.60"))

        quote = cache.get_quote(TOKEN_A)
        assert quote is not None
        assert quote.best_bid == Decimal("0.55")
        assert quote.best_ask == Decimal("0.60")
        assert quote.midpoint == Decimal("0.575")

    def test_cache_expired_returns_none(self, cache: PositionPriceCache) -> None:
        """Quote returns None after TTL expires."""
        # Use a very short TTL
        short_cache = PositionPriceCache(quote_ttl=0.01, tick_ttl=0.01)
        short_cache.set_quote(TOKEN_A, best_bid=Decimal("0.50"), best_ask=Decimal("0.55"))

        time.sleep(0.02)
        assert short_cache.get_quote(TOKEN_A) is None

    def test_tick_cache_reuse(self, cache: PositionPriceCache) -> None:
        """Tick data persists across quote invalidations."""
        cache.set_tick(
            TOKEN_A,
            tick_size=Decimal("0.01"),
            min_order_size=Decimal("5"),
            neg_risk=False,
        )
        cache.set_quote(TOKEN_A, best_bid=Decimal("0.50"), best_ask=Decimal("0.55"))

        # Invalidate quote
        cache.invalidate_on_fill(TOKEN_A)

        # Quote gone, tick persists
        assert cache.get_quote(TOKEN_A) is None
        tick = cache.get_tick(TOKEN_A)
        assert tick is not None
        assert tick.tick_size == Decimal("0.01")
        assert tick.min_order_size == Decimal("5")

    def test_get_quote_missing_token(self, cache: PositionPriceCache) -> None:
        """get_quote returns None for token not in cache."""
        assert cache.get_quote("nonexistent_token") is None

    def test_get_tick_missing_token(self, cache: PositionPriceCache) -> None:
        """get_tick returns None for token not in cache."""
        assert cache.get_tick("nonexistent_token") is None

    def test_tick_expired_returns_none(self) -> None:
        """Tick returns None after tick TTL expires."""
        short_cache = PositionPriceCache(quote_ttl=0.01, tick_ttl=0.01)
        short_cache.set_tick(
            TOKEN_A,
            tick_size=Decimal("0.01"),
            min_order_size=Decimal("5"),
            neg_risk=False,
        )
        time.sleep(0.02)
        assert short_cache.get_tick(TOKEN_A) is None

    def test_midpoint_none_when_partial(self, cache: PositionPriceCache) -> None:
        """Midpoint is None when only bid or ask is set."""
        cache.set_quote(TOKEN_A, best_bid=Decimal("0.50"), best_ask=None)
        quote = cache.get_quote(TOKEN_A)
        assert quote is not None
        assert quote.midpoint is None

        cache.set_quote(TOKEN_B, best_bid=None, best_ask=Decimal("0.60"))
        quote = cache.get_quote(TOKEN_B)
        assert quote is not None
        assert quote.midpoint is None


# ---------------------------------------------------------------------------
# Invalidation triggers
# ---------------------------------------------------------------------------

class TestInvalidation:
    """Verify invalidation triggers clear appropriate cache entries."""

    def test_invalidate_on_fill(self, cache: PositionPriceCache) -> None:
        """Own fill clears quote for the affected token only."""
        cache.set_quote(TOKEN_A, best_bid=Decimal("0.55"), best_ask=Decimal("0.60"))
        cache.set_quote(TOKEN_B, best_bid=Decimal("0.40"), best_ask=Decimal("0.45"))

        cache.invalidate_on_fill(TOKEN_A)

        assert cache.get_quote(TOKEN_A) is None
        assert cache.get_quote(TOKEN_B) is not None  # unaffected

    def test_invalidate_on_fill_noop_for_missing(self, cache: PositionPriceCache) -> None:
        """Invalidating a token not in cache does not raise."""
        cache.invalidate_on_fill("nonexistent")  # should not raise

    def test_on_tick_size_change(self, cache: PositionPriceCache) -> None:
        """Tick size change updates tick and clears stale quote."""
        cache.set_tick(
            TOKEN_A,
            tick_size=Decimal("0.01"),
            min_order_size=Decimal("5"),
            neg_risk=False,
        )
        cache.set_quote(TOKEN_A, best_bid=Decimal("0.55"), best_ask=Decimal("0.60"))

        cache.on_tick_size_change(TOKEN_A, Decimal("0.001"))

        # Quote cleared
        assert cache.get_quote(TOKEN_A) is None
        # Tick updated
        tick = cache.get_tick(TOKEN_A)
        assert tick is not None
        assert tick.tick_size == Decimal("0.001")
        # Preserved fields
        assert tick.min_order_size == Decimal("5")

    def test_on_price_update(self, cache: PositionPriceCache) -> None:
        """WSS price change event refreshes quote snapshot."""
        cache.on_price_update(TOKEN_A, Decimal("0.60"), Decimal("0.65"))

        quote = cache.get_quote(TOKEN_A)
        assert quote is not None
        assert quote.best_bid == Decimal("0.60")
        assert quote.best_ask == Decimal("0.65")
        assert quote.midpoint == Decimal("0.625")

    def test_invalidate_on_reconcile(self, cache: PositionPriceCache) -> None:
        """Reconcile clears both quote and tick for the token."""
        cache.set_quote(TOKEN_A, best_bid=Decimal("0.55"), best_ask=Decimal("0.60"))
        cache.set_tick(
            TOKEN_A,
            tick_size=Decimal("0.01"),
            min_order_size=Decimal("5"),
            neg_risk=False,
        )

        cache.invalidate_on_reconcile(TOKEN_A)

        assert cache.get_quote(TOKEN_A) is None
        assert cache.get_tick(TOKEN_A) is None

    def test_invalidate_all(self, cache: PositionPriceCache) -> None:
        """Full wipe clears everything for reconnect bootstrap."""
        cache.set_quote(TOKEN_A, best_bid=Decimal("0.55"), best_ask=Decimal("0.60"))
        cache.set_quote(TOKEN_B, best_bid=Decimal("0.40"), best_ask=Decimal("0.45"))
        cache.set_tick(
            TOKEN_A,
            tick_size=Decimal("0.01"),
            min_order_size=Decimal("5"),
            neg_risk=False,
        )

        cache.invalidate_all()

        assert cache.get_quote(TOKEN_A) is None
        assert cache.get_quote(TOKEN_B) is None
        assert cache.get_tick(TOKEN_A) is None
        assert cache.quote_count() == 0
        assert cache.tick_count() == 0

    def test_on_tick_size_change_no_prior_tick(self, cache: PositionPriceCache) -> None:
        """Tick size change for token with no prior tick data uses defaults."""
        cache.on_tick_size_change(TOKEN_A, Decimal("0.005"))

        tick = cache.get_tick(TOKEN_A)
        assert tick is not None
        assert tick.tick_size == Decimal("0.005")
        assert tick.min_order_size == Decimal("0")
        assert tick.neg_risk is False


# ---------------------------------------------------------------------------
# Introspection
# ---------------------------------------------------------------------------

class TestIntrospection:
    """Verify cache introspection methods."""

    def test_subscribed_tokens(self, cache: PositionPriceCache) -> None:
        cache.set_quote(TOKEN_A, best_bid=Decimal("0.50"), best_ask=Decimal("0.55"))
        cache.set_tick(
            TOKEN_B,
            tick_size=Decimal("0.01"),
            min_order_size=Decimal("5"),
            neg_risk=False,
        )
        tokens = cache.subscribed_tokens()
        assert TOKEN_A in tokens
        assert TOKEN_B in tokens

    def test_counts(self, cache: PositionPriceCache) -> None:
        assert cache.quote_count() == 0
        assert cache.tick_count() == 0

        cache.set_quote(TOKEN_A, best_bid=Decimal("0.50"), best_ask=Decimal("0.55"))
        assert cache.quote_count() == 1

        cache.set_tick(
            TOKEN_A,
            tick_size=Decimal("0.01"),
            min_order_size=Decimal("5"),
            neg_risk=False,
        )
        assert cache.tick_count() == 1


# ---------------------------------------------------------------------------
# Data snapshot immutability
# ---------------------------------------------------------------------------

class TestSnapshots:
    """Verify dataclass snapshot integrity."""

    def test_quote_snapshot_frozen(self) -> None:
        snap = QuoteSnapshot(
            best_bid=Decimal("0.50"),
            best_ask=Decimal("0.55"),
            midpoint=Decimal("0.525"),
            ts=time.monotonic(),
        )
        with pytest.raises(AttributeError):
            snap.best_bid = Decimal("0.99")  # type: ignore[misc]

    def test_tick_snapshot_frozen(self) -> None:
        snap = TickSnapshot(
            tick_size=Decimal("0.01"),
            min_order_size=Decimal("5"),
            neg_risk=False,
            ts=time.monotonic(),
        )
        with pytest.raises(AttributeError):
            snap.tick_size = Decimal("0.001")  # type: ignore[misc]
