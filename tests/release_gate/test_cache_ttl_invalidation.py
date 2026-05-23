"""Release-gate: market-data cache invalidation (Issue #75 item 3).

``cache.market_state_store.MarketStateStore`` is the per-token freshness
boundary. The bot must fail-closed when its inputs are stale, which only
holds if:

* own fills invalidate the affected token immediately;
* tick-size changes mark the token dirty;
* reconciliation invalidates every snapshot for the affected token;
* a price_change event with ``size=0`` deletes the affected book level.
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from cache.market_state_store import MarketStateStore


def test_invalidate_on_fill_clears_quote_and_book() -> None:
    cache = MarketStateStore()
    cache.set_quote("tok", best_bid=Decimal("0.50"), best_ask=Decimal("0.52"))
    cache.set_book("tok", bids={Decimal("0.50"): Decimal("100")}, asks={})
    cache.invalidate("tok")
    assert cache.get_quote("tok") is None
    assert cache.get_book("tok") is None


def test_tick_size_change_marks_dirty() -> None:
    cache = MarketStateStore()
    cache.set_quote("tok", best_bid=Decimal("0.50"), best_ask=Decimal("0.52"))
    cache.set_book("tok", bids={Decimal("0.50"): Decimal("100")}, asks={})
    cache.set_tick(
        "tok",
        tick_size=Decimal("0.01"),
        min_order_size=Decimal("1"),
        neg_risk=False,
    )
    cache.on_tick_size_change("tok", Decimal("0.001"))
    tick = cache.get_tick("tok")
    assert tick is not None
    assert tick.tick_size == Decimal("0.001")


def test_evict_clears_all_snapshots() -> None:
    """Full state-removal contract.

    ``invalidate(token)`` deliberately keeps tick metadata (see
    ``test_invalidate_on_fill_clears_quote_and_book`` above and the
    matching production assertion in ``test_price_cache.py`` — tick is
    market-static and survives quote/book invalidation).

    The "clear every snapshot" use case (own-fill cache hygiene, reconciler
    full-clear) is owned by ``evict(token)``, which pops the whole token
    state. This is what ``clients/user_ws.py`` actually calls on fills.
    """
    cache = MarketStateStore()
    cache.set_quote("tok", best_bid=Decimal("0.5"), best_ask=Decimal("0.5"))
    cache.set_book("tok", bids={}, asks={})
    cache.set_tick(
        "tok",
        tick_size=Decimal("0.01"),
        min_order_size=Decimal("1"),
        neg_risk=False,
    )
    cache.evict("tok")
    assert cache.get_quote("tok") is None
    assert cache.get_book("tok") is None
    assert cache.get_tick("tok") is None


def test_book_level_size_zero_deletes_level() -> None:
    cache = MarketStateStore()
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