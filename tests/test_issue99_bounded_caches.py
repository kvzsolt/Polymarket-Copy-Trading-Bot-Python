"""Issue #99 — MarketStateStore and RiskPriceCache must be bounded.

A 24/7 bot watching many leader wallets across many Polymarket markets
accumulates token state indefinitely if these caches are unbounded.
Both stores now keep an OrderedDict bounded by ``max_tokens`` /
``max_size`` (configurable via env or constructor); oldest-touched
tokens evict first, active tokens stay because every WSS/REST touch
moves them to the MRU end.
"""
from __future__ import annotations

from decimal import Decimal

from cache.market_state_store import MarketStateStore
from services.riskMonitor import RiskPriceCache


class TestMarketStateStoreBounded:
    def test_eviction_kicks_in_at_max_tokens(self):
        store = MarketStateStore(max_tokens=3)
        store.ensure_state("t1")
        store.ensure_state("t2")
        store.ensure_state("t3")
        store.ensure_state("t4")  # exceeds cap → t1 evicted (oldest, untouched)

        assert store.get_state("t1") is None
        for tid in ("t2", "t3", "t4"):
            assert store.get_state(tid) is not None

    def test_touch_keeps_active_tokens(self):
        store = MarketStateStore(max_tokens=3)
        store.ensure_state("t1")
        store.ensure_state("t2")
        store.ensure_state("t3")
        # Touch t1 — it must move to MRU end so the next insert evicts t2.
        store.ensure_state("t1")
        store.ensure_state("t4")

        assert store.get_state("t2") is None  # cold token evicted
        assert store.get_state("t1") is not None  # active token survives


class TestRiskPriceCacheBounded:
    def test_eviction_kicks_in_at_max_size(self):
        cache = RiskPriceCache(max_size=2)
        cache.set("t1", price=Decimal("0.50"))
        cache.set("t2", price=Decimal("0.40"))
        cache.set("t3", price=Decimal("0.30"))  # exceeds → t1 evicted

        assert cache.get_fresh("t1", stale_after=1e9) is None
        assert cache.get_fresh("t2", stale_after=1e9) is not None
        assert cache.get_fresh("t3", stale_after=1e9) is not None

    def test_get_fresh_touch_keeps_active_entries(self):
        cache = RiskPriceCache(max_size=2)
        cache.set("t1", price=Decimal("0.50"))
        cache.set("t2", price=Decimal("0.40"))
        # Reading t1 keeps it warm; the next set must evict t2 instead.
        cache.get_fresh("t1", stale_after=1e9)
        cache.set("t3", price=Decimal("0.30"))

        assert cache.get_fresh("t1", stale_after=1e9) is not None
        assert cache.get_fresh("t2", stale_after=1e9) is None

    def test_default_max_size_is_generous(self):
        # Sanity: default cap is well above any sensible bot's working set.
        cache = RiskPriceCache()
        assert cache._max_size >= 1000
