"""Unit tests for MarketDataService.require_fresh_data.

Tests the service against a real MarketStateStore or controlled mocks to verify:
  - dirty market state raises StaleDataError
  - stale / unknown token raises StaleDataError
  - resolved market raises MarketDataNotReady
  - missing tick_size raises StaleDataError
  - missing min_order_size raises StaleDataError
  - valid fresh state returns correct MarketDataBundle
  - bundle quote/book/tick match cache contents
"""
from __future__ import annotations

import time
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from cache.market_state_store import BookSnapshot, MarketStateStore, QuoteSnapshot, TickSnapshot
from services.market_data_service import (
    MarketDataBundle,
    MarketDataNotReady,
    MarketDataPolicy,
    MarketDataService,
    MarketDataSource,
    StaleDataError,
)

TOKEN = "0xmarket_data_svc_test"


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _wss_policy() -> MarketDataPolicy:
    return MarketDataPolicy(
        max_quote_age_seconds=10.0,
        max_book_age_seconds=10.0,
        max_tick_age_seconds=300.0,
        require_wss_fresh=True,
        allow_rest_fallback=False,
    )


def _make_service(cache, *, policy: MarketDataPolicy | None = None) -> MarketDataService:
    gateway = MagicMock()
    return MarketDataService(cache, gateway, policy=policy or _wss_policy())


def _make_fresh_cache(token_id: str = TOKEN) -> MarketStateStore:
    """Return a MarketStateStore with valid WSS data for token_id."""
    cache = MarketStateStore()
    bid = Decimal("0.49")
    ask = Decimal("0.51")
    cache.replace_book_snapshot(
        token_id,
        {bid: Decimal("100")},
        {ask: Decimal("100")},
        {"source": "wss"},
    )
    cache.set_quote(token_id, best_bid=bid, best_ask=ask, source="wss")
    cache.set_tick(
        token_id,
        tick_size=Decimal("0.01"),
        min_order_size=Decimal("1"),
        neg_risk=False,
        source="wss",
    )
    cache.mark_fresh(token_id, source="wss")
    return cache


def _mock_cache(
    *,
    dirty: bool = False,
    resolved: bool = False,
    source: str = "wss",
    tick: TickSnapshot | None = None,
) -> MagicMock:
    """Build a mock cache with controlled snapshot values."""
    now = time.monotonic()
    bid = Decimal("0.49")
    ask = Decimal("0.51")
    cache = MagicMock()
    cache.is_dirty.return_value = dirty
    cache.is_resolved.return_value = resolved
    cache.source_of.return_value = source
    cache.get_quote.return_value = QuoteSnapshot(
        token_id=TOKEN,
        best_bid=bid,
        best_ask=ask,
        last_trade_price=None,
        source=source,
        monotonic_ts=now,
    )
    cache.get_book.return_value = BookSnapshot(
        token_id=TOKEN,
        bids={bid: Decimal("100")},
        asks={ask: Decimal("100")},
        best_bid=bid,
        best_ask=ask,
        source=source,
        monotonic_ts=now,
    )
    if tick is None:
        cache.get_tick.return_value = TickSnapshot(
            token_id=TOKEN,
            tick_size=Decimal("0.01"),
            min_order_size=Decimal("1"),
            neg_risk=False,
            source=source,
            monotonic_ts=now,
        )
    else:
        cache.get_tick.return_value = tick
    return cache


# ---------------------------------------------------------------------------
# Dirty state blocks execution
# ---------------------------------------------------------------------------


class TestDirtyMarketBlocks:
    @pytest.mark.asyncio
    async def test_dirty_token_raises_stale_error(self) -> None:
        cache = _make_fresh_cache()
        cache.mark_dirty(TOKEN, "test_dirty")
        svc = _make_service(cache)

        with pytest.raises(StaleDataError):
            await svc.require_fresh_data(TOKEN)

    @pytest.mark.asyncio
    async def test_dirty_flag_via_mock_raises_stale_error(self) -> None:
        cache = _mock_cache(dirty=True)
        svc = _make_service(cache)

        with pytest.raises(StaleDataError):
            await svc.require_fresh_data(TOKEN)


# ---------------------------------------------------------------------------
# Stale / unknown state blocks execution
# ---------------------------------------------------------------------------


class TestStaleMarketBlocks:
    @pytest.mark.asyncio
    async def test_unknown_token_raises_stale_error(self) -> None:
        """Unknown token is dirty=True in MarketStateStore; must raise StaleDataError."""
        cache = MarketStateStore()
        svc = _make_service(cache)

        with pytest.raises(StaleDataError):
            await svc.require_fresh_data(TOKEN)

    @pytest.mark.asyncio
    async def test_rest_source_blocked_when_wss_required(self) -> None:
        """require_wss_fresh=True rejects REST-sourced data without allow_rest_snapshot_for_execution."""
        cache = _mock_cache(source="rest_snapshot")
        svc = _make_service(
            cache,
            policy=MarketDataPolicy(
                require_wss_fresh=True,
                allow_rest_fallback=False,
                allow_rest_snapshot_for_execution=False,
            ),
        )

        with pytest.raises(StaleDataError):
            await svc.require_fresh_data(TOKEN)


# ---------------------------------------------------------------------------
# Resolved market blocks execution
# ---------------------------------------------------------------------------


class TestResolvedMarketBlocks:
    @pytest.mark.asyncio
    async def test_resolved_token_raises_market_not_ready(self) -> None:
        cache = _make_fresh_cache()
        cache.on_market_resolved(TOKEN, {"outcome": "yes"})
        svc = _make_service(cache)

        with pytest.raises(MarketDataNotReady):
            await svc.require_fresh_data(TOKEN)

    @pytest.mark.asyncio
    async def test_resolved_flag_via_mock_raises_market_not_ready(self) -> None:
        cache = _mock_cache(resolved=True)
        svc = _make_service(cache)

        with pytest.raises(MarketDataNotReady):
            await svc.require_fresh_data(TOKEN)


# ---------------------------------------------------------------------------
# Missing tick_size blocks execution
# ---------------------------------------------------------------------------


class TestMissingTickSizeBlocks:
    @pytest.mark.asyncio
    async def test_no_tick_in_cache_raises_stale_error(self) -> None:
        """Cache has no tick data → get_tick returns None → StaleDataError."""
        cache = _mock_cache(dirty=False)
        cache.get_tick.return_value = None
        svc = _make_service(cache)

        with pytest.raises(StaleDataError):
            await svc.require_fresh_data(TOKEN)

    @pytest.mark.asyncio
    async def test_zero_tick_size_raises_stale_error(self) -> None:
        """tick_size=0 is non-executable; MarketDataService must reject it."""
        now = time.monotonic()
        bad_tick = TickSnapshot(
            token_id=TOKEN,
            tick_size=Decimal("0"),
            min_order_size=Decimal("1"),
            neg_risk=False,
            source="wss",
            monotonic_ts=now,
        )
        cache = _mock_cache(tick=bad_tick)
        svc = _make_service(cache)

        with pytest.raises(StaleDataError):
            await svc.require_fresh_data(TOKEN)

    @pytest.mark.asyncio
    async def test_negative_tick_size_raises_stale_error(self) -> None:
        now = time.monotonic()
        bad_tick = TickSnapshot(
            token_id=TOKEN,
            tick_size=Decimal("-0.01"),
            min_order_size=Decimal("1"),
            neg_risk=False,
            source="wss",
            monotonic_ts=now,
        )
        cache = _mock_cache(tick=bad_tick)
        svc = _make_service(cache)

        with pytest.raises(StaleDataError):
            await svc.require_fresh_data(TOKEN)


# ---------------------------------------------------------------------------
# Missing min_order_size blocks execution
# ---------------------------------------------------------------------------


class TestMissingMinOrderSizeBlocks:
    @pytest.mark.asyncio
    async def test_zero_min_order_size_raises_stale_error(self) -> None:
        now = time.monotonic()
        bad_tick = TickSnapshot(
            token_id=TOKEN,
            tick_size=Decimal("0.01"),
            min_order_size=Decimal("0"),
            neg_risk=False,
            source="wss",
            monotonic_ts=now,
        )
        cache = _mock_cache(tick=bad_tick)
        svc = _make_service(cache)

        with pytest.raises(StaleDataError):
            await svc.require_fresh_data(TOKEN)

    @pytest.mark.asyncio
    async def test_negative_min_order_size_raises_stale_error(self) -> None:
        now = time.monotonic()
        bad_tick = TickSnapshot(
            token_id=TOKEN,
            tick_size=Decimal("0.01"),
            min_order_size=Decimal("-1"),
            neg_risk=False,
            source="wss",
            monotonic_ts=now,
        )
        cache = _mock_cache(tick=bad_tick)
        svc = _make_service(cache)

        with pytest.raises(StaleDataError):
            await svc.require_fresh_data(TOKEN)


# ---------------------------------------------------------------------------
# Valid state returns correct MarketDataBundle
# ---------------------------------------------------------------------------


class TestValidStateReturnsBundle:
    @pytest.mark.asyncio
    async def test_fresh_cache_returns_bundle(self) -> None:
        cache = _make_fresh_cache()
        svc = _make_service(cache)

        bundle = await svc.require_fresh_data(TOKEN)

        assert isinstance(bundle, MarketDataBundle)
        assert bundle.token_id == TOKEN

    @pytest.mark.asyncio
    async def test_bundle_source_is_wss(self) -> None:
        cache = _make_fresh_cache()
        svc = _make_service(cache)

        bundle = await svc.require_fresh_data(TOKEN)

        assert bundle.source == MarketDataSource.WSS

    @pytest.mark.asyncio
    async def test_bundle_quote_matches_cache(self) -> None:
        cache = _make_fresh_cache()
        svc = _make_service(cache)

        bundle = await svc.require_fresh_data(TOKEN)

        assert bundle.quote.best_bid == Decimal("0.49")
        assert bundle.quote.best_ask == Decimal("0.51")

    @pytest.mark.asyncio
    async def test_bundle_tick_matches_cache(self) -> None:
        cache = _make_fresh_cache()
        svc = _make_service(cache)

        bundle = await svc.require_fresh_data(TOKEN)

        assert bundle.tick.tick_size == Decimal("0.01")
        assert bundle.tick.min_order_size == Decimal("1")

    @pytest.mark.asyncio
    async def test_bundle_book_contains_bids_and_asks(self) -> None:
        cache = _make_fresh_cache()
        svc = _make_service(cache)

        bundle = await svc.require_fresh_data(TOKEN)

        assert bundle.book.bids
        assert bundle.book.asks
        assert bundle.book.best_bid == Decimal("0.49")
        assert bundle.book.best_ask == Decimal("0.51")

    @pytest.mark.asyncio
    async def test_rest_snapshot_allowed_when_policy_permits(self) -> None:
        """allow_rest_snapshot_for_execution=True lets REST source through."""
        cache = _mock_cache(source="rest_snapshot")
        policy = MarketDataPolicy(
            require_wss_fresh=True,
            allow_rest_fallback=False,
            allow_rest_snapshot_for_execution=True,
        )
        svc = _make_service(cache, policy=policy)

        bundle = await svc.require_fresh_data(TOKEN)

        assert bundle.source == MarketDataSource.REST_SNAPSHOT

class TestComponentSourceGating:
    """Per-component source split: tick-only WSS must not promote book to WSS."""

    @pytest.mark.asyncio
    async def test_tick_size_change_does_not_promote_quote_book_to_wss(self) -> None:
        cache = MarketStateStore()
        # REST-bootstrapped book + quote (not WSS-fresh for execution).
        cache.set_book(
            TOKEN,
            bids={Decimal("0.49"): Decimal("100")},
            asks={Decimal("0.51"): Decimal("100")},
            source="rest_snapshot",
        )
        cache.set_quote(
            TOKEN,
            best_bid=Decimal("0.49"),
            best_ask=Decimal("0.51"),
            source="rest_snapshot",
        )
        # A WSS tick_size_change event applies — but quote/book must remain
        # REST-sourced for the execution gate.
        cache.on_tick_size_change(TOKEN, Decimal("0.01"))
        cache.set_tick(
            TOKEN,
            tick_size=Decimal("0.01"),
            min_order_size=Decimal("1"),
            neg_risk=False,
            source="wss",
        )
        # Clear dirty (tick_size_change sets dirty=True); execution-gate logic
        # is what we're testing, not the dirty flag.
        cache.mark_fresh(TOKEN, source="rest_snapshot")
        # Re-set tick source after mark_fresh to keep tick "wss".
        cache.on_tick_size_change(TOKEN, Decimal("0.01"))
        # Re-clear dirty post tick_size_change.
        cache.mark_fresh(TOKEN, source="rest_snapshot")

        svc = _make_service(cache)

        with pytest.raises(StaleDataError):
            await svc.require_fresh_data(TOKEN)

    @pytest.mark.asyncio
    async def test_rest_metadata_with_wss_quote_book_is_executable(self) -> None:
        """quote/book WSS + REST tick is acceptable under default live policy."""
        cache = MarketStateStore()
        cache.replace_book_snapshot(
            TOKEN,
            {Decimal("0.49"): Decimal("100")},
            {Decimal("0.51"): Decimal("100")},
            {"source": "wss"},
        )
        cache.set_tick(
            TOKEN,
            tick_size=Decimal("0.01"),
            min_order_size=Decimal("1"),
            neg_risk=False,
            source="rest_snapshot",
        )

        svc = _make_service(cache)
        bundle = await svc.require_fresh_data(TOKEN)

        assert bundle.source == MarketDataSource.WSS
        assert bundle.tick.source == "rest_snapshot"
