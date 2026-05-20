"""Phase 2 — PriceCache tests.

Covers:
  - set/get round trip with Decimal coercion
  - TTL eviction (lazy and explicit)
  - is_fresh
  - separate keys for (condition_id, token_id)
  - clear() wipes cache
  - get_or_fetch falls back to async callback on miss/expiry
  - get_or_fetch writes successful fetch result back into the cache
  - Decimal preserved end-to-end; no float drift
"""

from __future__ import annotations

import asyncio
import time
from decimal import Decimal

import pytest

from cache.price_cache import DEFAULT_TTL_SECONDS, PriceCache, PriceEntry


COND_A = "0xcond_a"
COND_B = "0xcond_b"
TOK_Y = "0xtok_yes"
TOK_N = "0xtok_no"


@pytest.fixture
def cache() -> PriceCache:
    return PriceCache(default_ttl_seconds=30.0)


class TestSetGet:
    def test_set_then_get_returns_decimal(self, cache: PriceCache) -> None:
        cache.set_price(COND_A, TOK_Y, Decimal("0.42"))
        got = cache.get_price(COND_A, TOK_Y)
        assert got == Decimal("0.42")
        assert isinstance(got, Decimal)

    def test_float_input_coerced_to_decimal(self, cache: PriceCache) -> None:
        cache.set_price(COND_A, TOK_Y, 0.5)  # float input
        got = cache.get_price(COND_A, TOK_Y)
        assert got == Decimal("0.5")
        assert isinstance(got, Decimal)

    def test_str_input_coerced_to_decimal(self, cache: PriceCache) -> None:
        cache.set_price(COND_A, TOK_Y, "0.55")
        assert cache.get_price(COND_A, TOK_Y) == Decimal("0.55")

    def test_miss_returns_none(self, cache: PriceCache) -> None:
        assert cache.get_price(COND_A, TOK_Y) is None

    def test_distinct_keys_isolated(self, cache: PriceCache) -> None:
        cache.set_price(COND_A, TOK_Y, Decimal("0.1"))
        cache.set_price(COND_A, TOK_N, Decimal("0.9"))
        cache.set_price(COND_B, TOK_Y, Decimal("0.7"))
        assert cache.get_price(COND_A, TOK_Y) == Decimal("0.1")
        assert cache.get_price(COND_A, TOK_N) == Decimal("0.9")
        assert cache.get_price(COND_B, TOK_Y) == Decimal("0.7")

    def test_set_overwrites(self, cache: PriceCache) -> None:
        cache.set_price(COND_A, TOK_Y, Decimal("0.1"))
        cache.set_price(COND_A, TOK_Y, Decimal("0.2"))
        assert cache.get_price(COND_A, TOK_Y) == Decimal("0.2")

    def test_empty_keys_rejected(self, cache: PriceCache) -> None:
        with pytest.raises(ValueError):
            cache.set_price("", TOK_Y, Decimal("0.5"))
        with pytest.raises(ValueError):
            cache.set_price(COND_A, "", Decimal("0.5"))


class TestTTL:
    def test_default_ttl_30s(self) -> None:
        c = PriceCache()
        assert c.default_ttl_seconds == DEFAULT_TTL_SECONDS == 30.0

    def test_zero_or_negative_ttl_rejected(self) -> None:
        with pytest.raises(ValueError):
            PriceCache(default_ttl_seconds=0)
        with pytest.raises(ValueError):
            PriceCache(default_ttl_seconds=-1)

    def test_entry_age_increases(self, cache: PriceCache) -> None:
        cache.set_price(COND_A, TOK_Y, Decimal("0.5"))
        entry = cache.get_entry(COND_A, TOK_Y)
        assert entry is not None
        time.sleep(0.01)
        assert entry.age_seconds() >= 0.01

    def test_expired_entry_returns_none(self, cache: PriceCache) -> None:
        # Backdate the entry past the TTL by writing with a stale monotonic_ts.
        stale_ts = time.monotonic() - 31.0
        cache.set_price(COND_A, TOK_Y, Decimal("0.5"), monotonic_ts=stale_ts)
        assert cache.get_price(COND_A, TOK_Y) is None

    def test_get_with_max_age_override(self, cache: PriceCache) -> None:
        stale_ts = time.monotonic() - 5.0
        cache.set_price(COND_A, TOK_Y, Decimal("0.5"), monotonic_ts=stale_ts)
        assert cache.get_price(COND_A, TOK_Y, max_age_seconds=10.0) == Decimal("0.5")
        assert cache.get_price(COND_A, TOK_Y, max_age_seconds=1.0) is None

    def test_lazy_eviction_removes_expired_entry(self, cache: PriceCache) -> None:
        stale_ts = time.monotonic() - 31.0
        cache.set_price(COND_A, TOK_Y, Decimal("0.5"), monotonic_ts=stale_ts)
        assert cache.size() == 1
        # First get sees expiry and evicts.
        assert cache.get_price(COND_A, TOK_Y) is None
        assert cache.size() == 0

    def test_evict_expired_batch(self, cache: PriceCache) -> None:
        fresh_ts = time.monotonic()
        stale_ts = fresh_ts - 31.0
        cache.set_price(COND_A, TOK_Y, Decimal("0.1"), monotonic_ts=stale_ts)
        cache.set_price(COND_A, TOK_N, Decimal("0.9"), monotonic_ts=fresh_ts)
        evicted = cache.evict_expired()
        assert evicted == 1
        assert cache.size() == 1
        assert cache.get_price(COND_A, TOK_N) == Decimal("0.9")


class TestIsFresh:
    def test_is_fresh_after_set(self, cache: PriceCache) -> None:
        cache.set_price(COND_A, TOK_Y, Decimal("0.5"))
        assert cache.is_fresh(COND_A, TOK_Y, max_age_seconds=30.0) is True

    def test_is_fresh_false_on_miss(self, cache: PriceCache) -> None:
        assert cache.is_fresh(COND_A, TOK_Y, max_age_seconds=30.0) is False

    def test_is_fresh_false_on_expired(self, cache: PriceCache) -> None:
        stale_ts = time.monotonic() - 100.0
        cache.set_price(COND_A, TOK_Y, Decimal("0.5"), monotonic_ts=stale_ts)
        assert cache.is_fresh(COND_A, TOK_Y, max_age_seconds=30.0) is False

    def test_is_fresh_uses_default_ttl_when_omitted(self, cache: PriceCache) -> None:
        cache.set_price(COND_A, TOK_Y, Decimal("0.5"))
        assert cache.is_fresh(COND_A, TOK_Y) is True


class TestInvalidationAndClear:
    def test_invalidate_removes_entry(self, cache: PriceCache) -> None:
        cache.set_price(COND_A, TOK_Y, Decimal("0.5"))
        cache.invalidate(COND_A, TOK_Y)
        assert cache.get_price(COND_A, TOK_Y) is None

    def test_clear_wipes_all_entries(self, cache: PriceCache) -> None:
        cache.set_price(COND_A, TOK_Y, Decimal("0.1"))
        cache.set_price(COND_B, TOK_N, Decimal("0.2"))
        cache.clear()
        assert cache.size() == 0
        assert cache.get_price(COND_A, TOK_Y) is None
        assert cache.get_price(COND_B, TOK_N) is None


class TestGetOrFetch:
    @pytest.mark.asyncio
    async def test_fetch_called_on_miss_and_result_cached(self, cache: PriceCache) -> None:
        calls: list[tuple[str, str]] = []

        async def gamma_fetch(cond: str, tok: str) -> Decimal | None:
            calls.append((cond, tok))
            return Decimal("0.73")

        got = await cache.get_or_fetch(COND_A, TOK_Y, gamma_fetch)
        assert got == Decimal("0.73")
        assert calls == [(COND_A, TOK_Y)]
        # Second call should hit cache, not the callback.
        got2 = await cache.get_or_fetch(COND_A, TOK_Y, gamma_fetch)
        assert got2 == Decimal("0.73")
        assert len(calls) == 1

    @pytest.mark.asyncio
    async def test_fetch_called_again_after_expiry(self, cache: PriceCache) -> None:
        async def gamma_fetch(cond: str, tok: str) -> Decimal | None:
            return Decimal("0.40")

        stale_ts = time.monotonic() - 31.0
        cache.set_price(COND_A, TOK_Y, Decimal("0.30"), monotonic_ts=stale_ts)
        got = await cache.get_or_fetch(COND_A, TOK_Y, gamma_fetch)
        assert got == Decimal("0.40")

    @pytest.mark.asyncio
    async def test_fetch_returns_none_preserves_miss(self, cache: PriceCache) -> None:
        async def empty_fetch(cond: str, tok: str) -> Decimal | None:
            return None

        got = await cache.get_or_fetch(COND_A, TOK_Y, empty_fetch)
        assert got is None
        assert cache.get_price(COND_A, TOK_Y) is None

    @pytest.mark.asyncio
    async def test_fetch_exception_propagates_no_partial_write(
        self, cache: PriceCache
    ) -> None:
        async def boom(cond: str, tok: str) -> Decimal | None:
            raise RuntimeError("upstream down")

        with pytest.raises(RuntimeError, match="upstream down"):
            await cache.get_or_fetch(COND_A, TOK_Y, boom)
        assert cache.get_price(COND_A, TOK_Y) is None


class TestThreadSafety:
    def test_concurrent_writes_dont_corrupt(self, cache: PriceCache) -> None:
        # Soft check — RLock guards mutation; many writes one read should
        # always return a valid Decimal from the set.
        import threading

        prices = [Decimal(f"0.{i:02d}") for i in range(1, 50)]
        threads: list[threading.Thread] = []

        def writer(p: Decimal) -> None:
            cache.set_price(COND_A, TOK_Y, p)

        for p in prices:
            t = threading.Thread(target=writer, args=(p,))
            threads.append(t)
            t.start()
        for t in threads:
            t.join()

        got = cache.get_price(COND_A, TOK_Y)
        assert got in prices
