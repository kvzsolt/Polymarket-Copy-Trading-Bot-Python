"""Regression: PriceCache enforces an LRU upper bound.

Pre-fix the cache was a plain dict with no max_entries — a long-running
bot watching many markets accumulated entries indefinitely (silent
memory leak). MarketStateStore already enforced max_tokens; PriceCache
was the odd one out.

Post-fix: bounded OrderedDict with LRU eviction on insert + touch on
read so cold tokens drift to the oldest end.
"""
from __future__ import annotations

from decimal import Decimal

from cache.price_cache import DEFAULT_MAX_ENTRIES, PriceCache


def test_default_max_entries_is_bounded():
    cache = PriceCache()
    assert cache._max_entries == DEFAULT_MAX_ENTRIES


def test_lru_evicts_oldest_when_over_cap():
    cache = PriceCache(max_entries=3)
    cache.set_price("c", "t1", Decimal("0.5"))
    cache.set_price("c", "t2", Decimal("0.5"))
    cache.set_price("c", "t3", Decimal("0.5"))
    cache.set_price("c", "t4", Decimal("0.5"))  # forces eviction of t1

    assert cache.get_entry("c", "t1") is None, "oldest entry must be evicted"
    assert cache.get_entry("c", "t4") is not None


def test_read_touches_mru():
    cache = PriceCache(max_entries=3)
    cache.set_price("c", "t1", Decimal("0.5"))
    cache.set_price("c", "t2", Decimal("0.5"))
    cache.set_price("c", "t3", Decimal("0.5"))
    # Read t1 → MRU; next insert should evict t2 (now oldest), not t1.
    cache.get_price("c", "t1")
    cache.set_price("c", "t4", Decimal("0.5"))

    assert cache.get_entry("c", "t1") is not None, "touched t1 must survive"
    assert cache.get_entry("c", "t2") is None, "untouched t2 must be evicted"
