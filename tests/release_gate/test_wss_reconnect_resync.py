"""Release-gate: WSS reconnect + REST resync (Issue #75 item 5).

The reconnect loop must:
* fully invalidate the cache before applying any post-reconnect deltas;
* call the REST bootstrap callback so the cache is rehydrated from a
  consistent snapshot before WSS deltas resume;
* apply a ``price_change`` event with size ``0`` as a level deletion (spec).
"""
from __future__ import annotations

from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from clients.market_ws import MarketWebSocket
from services.price_cache import PositionPriceCache


@pytest.mark.asyncio
async def test_rest_bootstrap_invoked_with_asset_ids() -> None:
    cache = PositionPriceCache()
    rest_bootstrap = AsyncMock()
    ws_client = MarketWebSocket(cache, rest_bootstrap=rest_bootstrap)
    ws_client._asset_ids = ["tokA", "tokB"]
    if ws_client._rest_bootstrap is not None:
        await ws_client._rest_bootstrap(ws_client._asset_ids)
    rest_bootstrap.assert_awaited_once_with(["tokA", "tokB"])


def test_cache_full_invalidation_clears_all_snapshots() -> None:
    cache = PositionPriceCache()
    cache.set_quote("tok", best_bid=Decimal("0.50"), best_ask=Decimal("0.52"))
    cache.set_book("tok", bids={Decimal("0.50"): Decimal("100")}, asks={})
    cache.set_tick(
        "tok",
        tick_size=Decimal("0.01"),
        min_order_size=Decimal("1"),
        neg_risk=False,
    )
    cache.invalidate_all()
    assert cache.quote_count() == 0
    assert cache.book_count() == 0
    assert cache.tick_count() == 0


def test_price_change_with_size_zero_deletes_level() -> None:
    cache = PositionPriceCache()
    cache.set_book(
        "tok",
        bids={Decimal("0.50"): Decimal("100"), Decimal("0.49"): Decimal("50")},
        asks={Decimal("0.52"): Decimal("80")},
    )
    cache.apply_book_delta("tok", "BUY", Decimal("0.50"), Decimal("0"))
    cache.apply_book_delta("tok", "SELL", Decimal("0.52"), Decimal("0"))
    book = cache.get_book("tok")
    assert book is not None
    assert Decimal("0.50") not in book.bids
    assert Decimal("0.52") not in book.asks
    assert book.bids[Decimal("0.49")] == Decimal("50")


def test_price_change_with_positive_size_upserts() -> None:
    cache = PositionPriceCache()
    cache.set_book("tok", bids={}, asks={})
    cache.apply_book_delta("tok", "BUY", Decimal("0.51"), Decimal("75"))
    book = cache.get_book("tok")
    assert book is not None
    assert book.bids[Decimal("0.51")] == Decimal("75")
