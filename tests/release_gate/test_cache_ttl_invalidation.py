"""Release-gate: market-data cache TTL + invalidation (Issue #75 item 3).

``services.price_cache.PositionPriceCache`` is the per-token freshness
boundary. The bot must fail-closed when its inputs are stale, which only
holds if:

* quote / book / tick TTLs expire correctly;
* own fills invalidate the affected token immediately;
* tick-size changes clear stale quote + book snapshots;
* reconciliation invalidates every snapshot for the affected token;
* a price_change event with ``size=0`` deletes the affected book level.
"""
from __future__ import annotations

from decimal import Decimal
from unittest.mock import patch

import pytest

from services.price_cache import PositionPriceCache


def test_quote_ttl_expires() -> None:
    cache = PositionPriceCache(quote_ttl=10.0)
    cache.set_quote("tok", best_bid=Decimal("0.50"), best_ask=Decimal("0.52"))
    assert cache.get_quote("tok") is not None
    with patch("services.price_cache.time.monotonic", return_value=10_000.0):
        cache.set_quote("tok", best_bid=Decimal("0.50"), best_ask=Decimal("0.52"))
    with patch("services.price_cache.time.monotonic", return_value=10_100.0):
        assert cache.get_quote("tok") is None


def test_invalidate_on_fill_clears_quote_and_book() -> None:
    cache = PositionPriceCache()
    cache.set_quote("tok", best_bid=Decimal("0.50"), best_ask=Decimal("0.52"))
    cache.set_book("tok", bids={Decimal("0.50"): Decimal("100")}, asks={})
    cache.invalidate_on_fill("tok")
    assert cache.get_quote("tok") is None
    assert cache.get_book("tok") is None


def test_tick_size_change_clears_quote_and_book() -> None:
    cache = PositionPriceCache()
    cache.set_quote("tok", best_bid=Decimal("0.50"), best_ask=Decimal("0.52"))
    cache.set_book("tok", bids={Decimal("0.50"): Decimal("100")}, asks={})
    cache.set_tick(
        "tok",
        tick_size=Decimal("0.01"),
        min_order_size=Decimal("1"),
        neg_risk=False,
    )
    cache.on_tick_size_change("tok", Decimal("0.001"))
    assert cache.get_quote("tok") is None
    assert cache.get_book("tok") is None
    tick = cache.get_tick("tok")
    assert tick is not None
    assert tick.tick_size == Decimal("0.001")


def test_invalidate_on_reconcile_clears_all_three() -> None:
    cache = PositionPriceCache()
    cache.set_quote("tok", best_bid=Decimal("0.5"), best_ask=Decimal("0.5"))
    cache.set_book("tok", bids={}, asks={})
    cache.set_tick(
        "tok",
        tick_size=Decimal("0.01"),
        min_order_size=Decimal("1"),
        neg_risk=False,
    )
    cache.invalidate_on_reconcile("tok")
    assert cache.get_quote("tok") is None
    assert cache.get_book("tok") is None
    assert cache.get_tick("tok") is None


def test_book_level_size_zero_deletes_level() -> None:
    cache = PositionPriceCache()
    cache.set_book(
        "tok",
        bids={Decimal("0.50"): Decimal("100"), Decimal("0.49"): Decimal("50")},
        asks={},
    )
    cache.apply_book_delta("tok", "BUY", Decimal("0.50"), Decimal("0"))
    book = cache.get_book("tok")
    assert book is not None
    assert Decimal("0.50") not in book.bids
    assert book.bids[Decimal("0.49")] == Decimal("50")
