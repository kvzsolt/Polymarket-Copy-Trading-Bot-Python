"""Risk-mark sanity gate.

The near-max (and, when enabled, stop-loss) triggers act directly on the
best-bid risk mark. A crossed or absurdly wide book means the quote is
corrupt, stale, or the market is broken — using it as a mark can fire a
spurious protective SELL. The gate rejects such marks (no action this tick)
without caching them; one-sided books (no ask) stay accepted so thin markets
are not blinded.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

from pmcopy.services.risk.risk_monitor import (
    RiskPriceCache,
    _cache_market_store_risk_price,
)

_TOKEN = "7" * 70


def _quote(*, best_bid, best_ask, source="wss"):
    quote = MagicMock()
    quote.best_bid = best_bid
    quote.best_ask = best_ask
    quote.source = source
    quote.is_fresh.return_value = True
    return quote


def _context(quote, *, max_spread_bps=2000):
    store = MagicMock()
    store.get_quote.return_value = quote
    runtime = SimpleNamespace(market_store=store)
    return SimpleNamespace(
        market_data_runtime=runtime,
        config=SimpleNamespace(risk=SimpleNamespace(risk_mark_max_spread_bps=max_spread_bps)),
        extra={},
    )


def _mark(quote, **ctx_kwargs):
    cache = RiskPriceCache()
    ctx = _context(quote, **ctx_kwargs)
    price = _cache_market_store_risk_price(ctx, _TOKEN, cache, stale_after=30.0)
    return price, cache


def test_crossed_book_rejected():
    price, cache = _mark(_quote(best_bid="0.99", best_ask="0.40"))
    assert price is None
    assert cache.get_fresh(_TOKEN, stale_after=30.0) is None


def test_wide_spread_rejected():
    # bid 0.30 / ask 0.60 -> spread ~6666 bps of mid, above the 2000 cap.
    price, cache = _mark(_quote(best_bid="0.30", best_ask="0.60"))
    assert price is None
    assert cache.get_fresh(_TOKEN, stale_after=30.0) is None


def test_normal_spread_accepted_and_cached():
    price, cache = _mark(_quote(best_bid="0.33", best_ask="0.34"))
    assert price == Decimal("0.33")
    entry = cache.get_fresh(_TOKEN, stale_after=30.0)
    assert entry is not None


def test_one_sided_book_accepted():
    price, _cache = _mark(_quote(best_bid="0.95", best_ask=None))
    assert price == Decimal("0.95")


def test_spread_check_disabled_with_zero_cap():
    price, _cache = _mark(_quote(best_bid="0.30", best_ask="0.60"), max_spread_bps=0)
    assert price == Decimal("0.30")


def test_crossed_book_rejected_even_with_zero_cap():
    price, _cache = _mark(_quote(best_bid="0.99", best_ask="0.40"), max_spread_bps=0)
    assert price is None
