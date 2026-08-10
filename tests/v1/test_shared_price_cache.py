"""Contracts for the shared bounded market-price cache."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from services.copy_tasks.price_cache import MarketPriceCache, PriceQuote


_NOW = datetime(2026, 8, 9, 12, 0, tzinfo=UTC)


def _quote(
    token_id: str, *, bid: str = "0.40", ask: str = "0.42", source: str = "wss"
) -> PriceQuote:
    return PriceQuote(
        token_id=token_id,
        best_bid=Decimal(bid),
        best_ask=Decimal(ask),
        observed_at=_NOW,
        source=source,
    )


def test_put_and_get_returns_fresh_quote() -> None:
    cache = MarketPriceCache(max_entries=10)
    cache.put(_quote("t1"))
    quote = cache.get("t1", now=_NOW, max_age=timedelta(seconds=1))
    assert quote is not None
    assert quote.token_id == "t1"


def test_get_returns_none_when_stale() -> None:
    cache = MarketPriceCache(max_entries=10)
    cache.put(_quote("t1"))
    stale_now = _NOW + timedelta(seconds=5)
    assert cache.get("t1", now=stale_now, max_age=timedelta(seconds=1)) is None


def test_lru_eviction_drops_oldest_when_bound_exceeded() -> None:
    cache = MarketPriceCache(max_entries=2)
    cache.put(_quote("t1"))
    cache.put(_quote("t2"))
    cache.put(_quote("t3"))
    assert len(cache) == 2
    assert cache.get("t1", now=_NOW, max_age=timedelta(seconds=1)) is None
    assert cache.get("t3", now=_NOW, max_age=timedelta(seconds=1)) is not None


def test_crossed_quote_is_rejected() -> None:
    with pytest.raises(ValueError, match="crossed quote"):
        PriceQuote(
            token_id="t1",
            best_bid=Decimal("0.50"),
            best_ask=Decimal("0.40"),
            observed_at=_NOW,
            source="wss",
        )


def test_discard_removes_one_token() -> None:
    cache = MarketPriceCache(max_entries=10)
    cache.put(_quote("t1"))
    assert cache.discard("t1") is True
    assert cache.discard("t1") is False
    assert len(cache) == 0


def test_clear_drops_all_quotes() -> None:
    cache = MarketPriceCache(max_entries=10)
    cache.put(_quote("t1"))
    cache.put(_quote("t2"))
    cache.clear()
    assert len(cache) == 0


def test_price_quote_rejects_out_of_range_prices() -> None:
    with pytest.raises(ValueError, match="best_bid"):
        PriceQuote(
            token_id="t1",
            best_bid=Decimal("1.5"),
            best_ask=Decimal("1.6"),
            observed_at=_NOW,
            source="wss",
        )


def test_price_quote_rejects_naive_observed_at() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        PriceQuote(
            token_id="t1",
            best_bid=Decimal("0.4"),
            best_ask=Decimal("0.5"),
            observed_at=datetime(2026, 8, 9, 12, 0),
            source="wss",
        )


def test_get_rejects_naive_now() -> None:
    cache = MarketPriceCache(max_entries=10)
    with pytest.raises(ValueError, match="timezone-aware"):
        cache.get("t1", now=datetime(2026, 8, 9, 12, 0), max_age=timedelta(seconds=1))
