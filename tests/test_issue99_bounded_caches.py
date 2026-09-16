"""Issue #99 — MarketStateStore and RiskPriceCache must be bounded.

A 24/7 bot watching many leader wallets across many Polymarket markets
accumulates token state indefinitely if these caches are unbounded.
Both stores now keep an OrderedDict bounded by ``max_tokens`` /
``max_size`` (configured through AppConfig or the constructor); oldest-touched
tokens evict first, active tokens stay because every WSS/REST touch
moves them to the MRU end.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from cache.market_state_store import MarketStateStore
from services.risk.risk_monitor import RiskPriceCache


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

    @pytest.mark.parametrize("bad", [0, -1, -1000])
    def test_explicit_non_positive_max_tokens_rejected(self, bad):
        """PR #105 review: max_tokens=0 evicts every insert; negative crashes
        the eviction loop. Reject explicit non-positive values rather than
        silently misbehaving."""
        with pytest.raises(ValueError):
            MarketStateStore(max_tokens=bad)


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


class TestRiskPriceCacheTypedConfig:
    """PR #105 review: typed AppConfig.risk.risk_price_cache_max_size is
    the production wiring path; cache-local environment parsing is gone."""

    def _load_config(self, max_size: int | None = None):
        from config.app_config import load_config

        overrides = {
            "database": {"dsn": "postgresql://test:test@localhost/test"},
            "wallet": {
                "funder_address": "0x" + "b" * 40,
                "polygon_rpc_url": "https://polygon-rpc.example.com",
            },
            "network": {"rpc_url": "https://polygon-rpc.example.com"},
        }
        if max_size is not None:
            overrides["risk"] = {"risk_price_cache_max_size": max_size}
        return load_config(overrides)

    def test_app_config_exposes_typed_field(self):
        config = self._load_config(777)
        assert config.risk.risk_price_cache_max_size == 777

    def test_app_config_falls_back_to_default_when_env_absent(self):
        config = self._load_config()
        assert config.risk.risk_price_cache_max_size == 5000

    def test_ensure_context_price_cache_uses_typed_cap(self):
        """A TradingContext carrying an AppConfig must construct the cache
        with the typed cap from the YAML settings graph."""
        from unittest.mock import MagicMock

        config = self._load_config(42)

        ctx = MagicMock()
        ctx.extra = {}
        ctx.config = config

        from services.risk.risk_monitor import _ensure_context_price_cache

        cache = _ensure_context_price_cache(ctx)
        assert cache._max_size == 42
