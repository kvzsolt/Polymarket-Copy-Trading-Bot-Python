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
from unittest.mock import AsyncMock, MagicMock

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


# ---------------------------------------------------------------------------
# REST bootstrap policy
# ---------------------------------------------------------------------------


def _make_rest_bootstrap_cache(token_id: str = TOKEN, *, stale: bool = False) -> MarketStateStore:
    """Return a cache with rest_bootstrap-sourced quote + book (fresh unless stale=True)."""
    import time as _time

    cache = MarketStateStore()
    ts = _time.monotonic() - (10.0 if stale else 0.0)
    bid = Decimal("0.45")
    ask = Decimal("0.55")
    cache.replace_book_snapshot(
        token_id,
        {bid: Decimal("100")},
        {ask: Decimal("100")},
        {"source": "rest_bootstrap"},
    )
    cache.set_quote(token_id, best_bid=bid, best_ask=ask, source="rest_bootstrap")
    cache.set_tick(
        token_id,
        tick_size=Decimal("0.01"),
        min_order_size=Decimal("5"),
        neg_risk=False,
        source="rest_bootstrap",
    )
    if stale:
        # Backdate monotonic_ts on book/quote by patching the internal state.
        with cache._lock:
            state = cache._states[token_id]
            state.last_book_monotonic = ts
            state.last_quote_monotonic = ts
    return cache


class TestRestBootstrapPolicy:
    @pytest.mark.asyncio
    async def test_bootstrap_policy_fresh_returns_bundle(self) -> None:
        cache = _make_rest_bootstrap_cache()
        policy = MarketDataPolicy(
            max_quote_age_seconds=10.0,
            max_book_age_seconds=10.0,
            max_tick_age_seconds=300.0,
            require_wss_fresh=True,
            allow_rest_fallback=False,
            allow_rest_snapshot_for_execution=False,
            allow_rest_bootstrap_for_execution=True,
            rest_bootstrap_max_age_ms=5000,
        )
        svc = _make_service(cache, policy=policy)

        bundle = await svc.require_fresh_data(TOKEN)

        assert bundle is not None
        assert bundle.source == MarketDataSource.REST_BOOTSTRAP

    @pytest.mark.asyncio
    async def test_bootstrap_policy_stale_returns_none(self) -> None:
        cache = _make_rest_bootstrap_cache(stale=True)
        policy = MarketDataPolicy(
            max_quote_age_seconds=30.0,
            max_book_age_seconds=30.0,
            max_tick_age_seconds=300.0,
            require_wss_fresh=True,
            allow_rest_bootstrap_for_execution=True,
            rest_bootstrap_max_age_ms=50,  # 50ms — stale data is 10 seconds old
        )
        svc = _make_service(cache, policy=policy)

        with pytest.raises(StaleDataError):
            await svc.require_fresh_data(TOKEN)

    @pytest.mark.asyncio
    async def test_bootstrap_disabled_returns_none(self) -> None:
        cache = _make_rest_bootstrap_cache()
        policy = MarketDataPolicy(
            max_quote_age_seconds=10.0,
            max_book_age_seconds=10.0,
            max_tick_age_seconds=300.0,
            require_wss_fresh=True,
            allow_rest_bootstrap_for_execution=False,
        )
        svc = _make_service(cache, policy=policy)

        with pytest.raises(StaleDataError):
            await svc.require_fresh_data(TOKEN)

    @pytest.mark.asyncio
    async def test_wss_preferred_over_bootstrap(self) -> None:
        cache = _make_fresh_cache()
        policy = MarketDataPolicy(
            max_quote_age_seconds=10.0,
            max_book_age_seconds=10.0,
            max_tick_age_seconds=300.0,
            require_wss_fresh=True,
            allow_rest_bootstrap_for_execution=True,
            rest_bootstrap_max_age_ms=5000,
        )
        svc = _make_service(cache, policy=policy)

        bundle = await svc.require_fresh_data(TOKEN)

        assert bundle.source == MarketDataSource.WSS

    def test_rest_bootstrap_enum_value(self) -> None:
        assert MarketDataSource.REST_BOOTSTRAP == "rest_bootstrap"
        assert MarketDataSource.REST_BOOTSTRAP.value == "rest_bootstrap"

    @pytest.mark.asyncio
    async def test_bootstrap_past_age_cap_still_executable_when_wss_live(self) -> None:
        """REST bootstrap stays executable past its 500ms cap while WSS connection live.

        Polymarket WSS only sends deltas on changes. Quiet markets get no WSS
        updates for many seconds. After we bootstrap from REST and subscribe to
        WSS, the bootstrap data IS the current book until WSS delivers a delta —
        same liveness rule as for WSS-sourced data.
        """
        import time as _time

        cache = _make_rest_bootstrap_cache()
        # Backdate bootstrap so it's well past the 500ms cap.
        with cache._lock:
            state = cache._states[TOKEN]
            old = _time.monotonic() - 3.0
            state.last_book_monotonic = old
            state.last_quote_monotonic = old
        # WSS connection proven live via a recent INBOUND frame (pong). P1-d:
        # the self-sent outbound heartbeat alone does not prove liveness.
        cache.on_market_ws_pong(_time.monotonic())

        policy = MarketDataPolicy(
            max_quote_age_seconds=2.0,
            max_book_age_seconds=2.0,
            max_tick_age_seconds=300.0,
            require_wss_fresh=True,
            allow_rest_fallback=False,
            allow_rest_snapshot_for_execution=False,
            allow_rest_bootstrap_for_execution=True,
            rest_bootstrap_max_age_ms=500,
            wss_connection_max_silence_seconds=20.0,
        )
        svc = _make_service(cache, policy=policy)

        bundle = await svc.require_fresh_data(TOKEN)

        assert bundle.source == MarketDataSource.REST_BOOTSTRAP

    @pytest.mark.asyncio
    async def test_bootstrap_past_age_cap_rejected_when_wss_dead(self) -> None:
        """Without WSS liveness proof, the 500ms bootstrap cap still applies."""
        import time as _time

        cache = _make_rest_bootstrap_cache()
        with cache._lock:
            state = cache._states[TOKEN]
            old = _time.monotonic() - 3.0
            state.last_book_monotonic = old
            state.last_quote_monotonic = old
        # No heartbeat → WSS connection not proven live.

        policy = MarketDataPolicy(
            max_quote_age_seconds=10.0,
            max_book_age_seconds=10.0,
            max_tick_age_seconds=300.0,
            require_wss_fresh=True,
            allow_rest_bootstrap_for_execution=True,
            rest_bootstrap_max_age_ms=500,
            wss_connection_max_silence_seconds=20.0,
        )
        svc = _make_service(cache, policy=policy)

        with pytest.raises(StaleDataError):
            await svc.require_fresh_data(TOKEN)


class TestWssConnectionLivenessFreshness:
    """Option B: a quiet but connected WSS book stays executable.

    A WSS-sourced quote/book past its own age window is still the current
    state while the WSS connection is proven live (recent heartbeat/pong). A
    dead connection fails both the age and liveness paths and is rejected.
    """

    def _aged_wss_cache(self, age_seconds: float) -> MarketStateStore:
        cache = _make_fresh_cache()
        state = cache.ensure_state(TOKEN)
        old = time.monotonic() - age_seconds
        state.last_quote_monotonic = old
        state.last_book_monotonic = old
        state.last_update_monotonic = old
        return cache

    def _tight_policy(self, *, silence: float = 20.0) -> MarketDataPolicy:
        return MarketDataPolicy(
            max_quote_age_seconds=2.0,
            max_book_age_seconds=2.0,
            max_tick_age_seconds=300.0,
            require_wss_fresh=True,
            allow_rest_fallback=False,
            wss_connection_max_silence_seconds=silence,
        )

    @pytest.mark.asyncio
    async def test_quiet_wss_book_rejected_when_only_outbound_heartbeat(self) -> None:
        # P1-d: the self-sent outbound PING (heartbeat) must NOT prove
        # liveness. A silent / half-open feed keeps PINGing but receives
        # nothing inbound — it must fail the liveness path and be rejected.
        cache = self._aged_wss_cache(age_seconds=8.0)
        cache.on_market_ws_heartbeat(time.monotonic())
        svc = _make_service(cache, policy=self._tight_policy())

        with pytest.raises(StaleDataError):
            await svc.require_fresh_data(TOKEN)

    @pytest.mark.asyncio
    async def test_quiet_wss_book_executable_on_recent_pong(self) -> None:
        cache = self._aged_wss_cache(age_seconds=8.0)
        cache.on_market_ws_pong(time.monotonic())
        svc = _make_service(cache, policy=self._tight_policy())

        bundle = await svc.require_fresh_data(TOKEN)

        assert bundle.source == MarketDataSource.WSS

    @pytest.mark.asyncio
    async def test_aged_wss_book_rejected_when_no_heartbeat_seen(self) -> None:
        cache = self._aged_wss_cache(age_seconds=8.0)
        svc = _make_service(cache, policy=self._tight_policy())

        with pytest.raises(StaleDataError):
            await svc.require_fresh_data(TOKEN)

    @pytest.mark.asyncio
    async def test_aged_wss_book_rejected_when_heartbeat_stale(self) -> None:
        cache = self._aged_wss_cache(age_seconds=30.0)
        cache.on_market_ws_heartbeat(time.monotonic() - 25.0)
        svc = _make_service(cache, policy=self._tight_policy(silence=20.0))

        with pytest.raises(StaleDataError):
            await svc.require_fresh_data(TOKEN)

    @pytest.mark.asyncio
    async def test_fresh_wss_book_executable_without_heartbeat(self) -> None:
        cache = _make_fresh_cache()
        svc = _make_service(cache, policy=self._tight_policy())

        bundle = await svc.require_fresh_data(TOKEN)

        assert bundle.source == MarketDataSource.WSS

    @pytest.mark.asyncio
    async def test_liveness_path_disabled_when_silence_window_non_positive(self) -> None:
        cache = self._aged_wss_cache(age_seconds=8.0)
        cache.on_market_ws_heartbeat(time.monotonic())
        svc = _make_service(cache, policy=self._tight_policy(silence=0.0))

        with pytest.raises(StaleDataError):
            await svc.require_fresh_data(TOKEN)

    def test_store_connection_age_none_before_any_heartbeat(self) -> None:
        cache = MarketStateStore()
        assert cache.wss_connection_age_seconds() is None

    def test_store_connection_age_tracks_most_recent_signal(self) -> None:
        cache = MarketStateStore()
        cache.on_market_ws_heartbeat(time.monotonic() - 12.0)
        cache.on_market_ws_pong(time.monotonic() - 1.0)
        age = cache.wss_connection_age_seconds()
        assert age is not None and age < 3.0


# ---------------------------------------------------------------------------
# PR10.4 — require_execution_snapshot
# ---------------------------------------------------------------------------


import asyncio  # noqa: E402  (kept local to the PR10.4 section)
from types import SimpleNamespace  # noqa: E402


def _rest_policy(**overrides) -> MarketDataPolicy:
    return MarketDataPolicy(
        primary_source="rest_snapshot",
        require_wss_fresh=False,
        allow_rest_snapshot_for_execution=True,
        rest_snapshot_max_age_ms=overrides.pop("rest_snapshot_max_age_ms", 500),
        rest_snapshot_request_timeout_ms=overrides.pop("rest_snapshot_request_timeout_ms", 750),
        rest_snapshot_total_budget_ms=overrides.pop("rest_snapshot_total_budget_ms", 1000),
        rest_snapshot_max_spread_bps=overrides.pop("rest_snapshot_max_spread_bps", 300),
    )


def _book(bids: dict[Decimal, Decimal], asks: dict[Decimal, Decimal]):
    bid_levels = [SimpleNamespace(price=str(p), size=str(s)) for p, s in bids.items()]
    ask_levels = [SimpleNamespace(price=str(p), size=str(s)) for p, s in asks.items()]
    return SimpleNamespace(
        bids=bid_levels,
        asks=ask_levels,
        tick_size="0.01",
        min_order_size="5",
        neg_risk=False,
    )


def _gateway_returning(book) -> MagicMock:
    gw = MagicMock()

    async def _get(_token):
        return book

    async def _info(_token):
        return {"tick_size": "0.01", "min_order_size": "5", "neg_risk": False}

    gw.get_orderbook = _get
    gw.get_market_info = _info
    return gw


class TestRequireExecutionSnapshot:
    @pytest.mark.asyncio
    async def test_happy_path_returns_rest_snapshot_bundle(self) -> None:
        store = MarketStateStore()
        gw = _gateway_returning(
            _book({Decimal("0.495"): Decimal("100")}, {Decimal("0.505"): Decimal("100")})
        )
        svc = MarketDataService(store, gw, policy=_rest_policy())

        bundle = await svc.require_execution_snapshot(TOKEN, side="BUY")

        assert bundle.source == MarketDataSource.REST_SNAPSHOT
        assert bundle.quote.source == "rest_snapshot"
        assert bundle.book.source == "rest_snapshot"
        assert bundle.tick.source == "rest_snapshot"

    @pytest.mark.asyncio
    async def test_rest_snapshot_uses_canonical_book_tick_metadata(self) -> None:
        """Gateway OrderBook metadata should be enough; no extra meta fetch needed."""
        from integrations.polymarket.models import OrderBook, OrderBookLevel

        store = MarketStateStore()
        gw = MagicMock()
        gw.get_orderbook = AsyncMock(
            return_value=OrderBook(
                token_id=TOKEN,
                bids=[OrderBookLevel(price=Decimal("0.495"), size=Decimal("100"))],
                asks=[OrderBookLevel(price=Decimal("0.505"), size=Decimal("100"))],
                tick_size=Decimal("0.001"),
                min_order_size=Decimal("5"),
                neg_risk=False,
            )
        )
        gw.get_market_info = AsyncMock(
            side_effect=AssertionError("metadata fetch should not be needed")
        )
        svc = MarketDataService(store, gw, policy=_rest_policy())

        bundle = await svc.require_execution_snapshot(TOKEN, side="BUY")

        assert bundle.tick.tick_size == Decimal("0.001")
        assert bundle.tick.min_order_size == Decimal("5")
        gw.get_market_info.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_rest_snapshot_missing_neg_risk_fails_closed(self) -> None:
        store = MarketStateStore()
        book = _book(
            {Decimal("0.495"): Decimal("100")},
            {Decimal("0.505"): Decimal("100")},
        )
        del book.neg_risk
        gw = _gateway_returning(book)
        svc = MarketDataService(store, gw, policy=_rest_policy())

        with pytest.raises(StaleDataError, match="execution metadata"):
            await svc.require_execution_snapshot(TOKEN, side="BUY")

        assert store.get_tick(TOKEN) is None

    @pytest.mark.asyncio
    async def test_timeout_yields_market_data_timeout(self) -> None:
        store = MarketStateStore()
        gw = MagicMock()

        async def _slow(_token):
            raise asyncio.TimeoutError()

        gw.get_orderbook = _slow
        svc = MarketDataService(store, gw, policy=_rest_policy())

        with pytest.raises(StaleDataError) as exc:
            await svc.require_execution_snapshot(TOKEN, side="BUY")
        assert exc.value.skip_code == "market_data_timeout"

    @pytest.mark.asyncio
    async def test_http_error_yields_market_data_stale(self) -> None:
        store = MarketStateStore()
        gw = MagicMock()

        async def _boom(_token):
            raise RuntimeError("502 bad gateway")

        gw.get_orderbook = _boom
        svc = MarketDataService(store, gw, policy=_rest_policy())

        with pytest.raises(StaleDataError) as exc:
            await svc.require_execution_snapshot(TOKEN, side="BUY")
        assert exc.value.skip_code == "market_data_stale"

    @pytest.mark.asyncio
    async def test_buy_requires_ask_liquidity(self) -> None:
        store = MarketStateStore()
        gw = _gateway_returning(_book({Decimal("0.495"): Decimal("100")}, {}))
        svc = MarketDataService(store, gw, policy=_rest_policy())

        with pytest.raises(StaleDataError) as exc:
            await svc.require_execution_snapshot(TOKEN, side="BUY")
        assert exc.value.skip_code == "book_no_liquidity"

    @pytest.mark.asyncio
    async def test_sell_requires_bid_liquidity(self) -> None:
        store = MarketStateStore()
        gw = _gateway_returning(_book({}, {Decimal("0.505"): Decimal("100")}))
        svc = MarketDataService(store, gw, policy=_rest_policy())

        with pytest.raises(StaleDataError) as exc:
            await svc.require_execution_snapshot(TOKEN, side="SELL")
        assert exc.value.skip_code == "book_no_liquidity"

    @pytest.mark.asyncio
    async def test_empty_book_rejected(self) -> None:
        store = MarketStateStore()
        gw = _gateway_returning(_book({}, {}))
        svc = MarketDataService(store, gw, policy=_rest_policy())

        with pytest.raises(StaleDataError) as exc:
            await svc.require_execution_snapshot(TOKEN, side="BUY")
        assert exc.value.skip_code == "market_data_stale"

    @pytest.mark.asyncio
    async def test_wide_spread_rejects_rest_snapshot(self) -> None:
        """Post-fix the gate is FAIL CLOSED — wide spread above the cap
        must raise StaleDataError with skip_code='book_spread_wide'."""
        store = MarketStateStore()
        gw = _gateway_returning(
            _book(
                {Decimal("0.10"): Decimal("100")},
                {Decimal("0.90"): Decimal("100")},
            )
        )
        svc = MarketDataService(store, gw, policy=_rest_policy(rest_snapshot_max_spread_bps=300))

        with pytest.raises(StaleDataError) as exc:
            await svc.require_execution_snapshot(TOKEN, side="BUY")
        assert exc.value.skip_code == "book_spread_wide"

    @pytest.mark.asyncio
    async def test_spread_gate_disabled_still_allows_sell_with_bid_liquidity(self) -> None:
        store = MarketStateStore()
        gw = _gateway_returning(
            _book(
                {Decimal("0.06"): Decimal("100")},
                {Decimal("0.95"): Decimal("100")},
            )
        )
        svc = MarketDataService(store, gw, policy=_rest_policy(rest_snapshot_max_spread_bps=0))

        bundle = await svc.require_execution_snapshot(TOKEN, side="SELL")

        assert bundle.source == MarketDataSource.REST_SNAPSHOT
        assert bundle.quote.best_bid == Decimal("0.06")

    @pytest.mark.asyncio
    async def test_total_budget_exceeded(self) -> None:
        store = MarketStateStore()

        async def _slow_book(_token):
            await asyncio.sleep(0.02)
            return _book(
                {Decimal("0.495"): Decimal("100")},
                {Decimal("0.505"): Decimal("100")},
            )

        gw = MagicMock()
        gw.get_orderbook = _slow_book
        svc = MarketDataService(
            store,
            gw,
            policy=_rest_policy(
                rest_snapshot_request_timeout_ms=750,
                rest_snapshot_total_budget_ms=1,
            ),
        )

        with pytest.raises(StaleDataError) as exc:
            await svc.require_execution_snapshot(TOKEN, side="BUY")
        assert exc.value.skip_code == "market_data_timeout"

    @pytest.mark.asyncio
    async def test_resolved_token_raises_not_ready(self) -> None:
        store = MarketStateStore()
        store.on_market_resolved(TOKEN, {"asset_id": TOKEN})
        gw = _gateway_returning(
            _book({Decimal("0.495"): Decimal("100")}, {Decimal("0.505"): Decimal("100")})
        )
        svc = MarketDataService(store, gw, policy=_rest_policy())

        with pytest.raises(MarketDataNotReady) as exc:
            await svc.require_execution_snapshot(TOKEN, side="BUY")
        assert exc.value.skip_code == "market_resolved"

    @pytest.mark.asyncio
    async def test_bundle_components_never_marked_wss(self) -> None:
        store = MarketStateStore()
        gw = _gateway_returning(
            _book({Decimal("0.495"): Decimal("100")}, {Decimal("0.505"): Decimal("100")})
        )
        svc = MarketDataService(store, gw, policy=_rest_policy())

        bundle = await svc.require_execution_snapshot(TOKEN, side="BUY")
        assert bundle.quote.source != "wss"
        assert bundle.book.source != "wss"
        assert bundle.tick.source != "wss"

    @pytest.mark.asyncio
    async def test_require_fresh_data_routes_to_snapshot_when_side_provided(
        self,
    ) -> None:
        """REST primary mode + ``side`` → ``require_execution_snapshot`` path."""
        store = MarketStateStore()
        gw = _gateway_returning(
            _book({Decimal("0.495"): Decimal("100")}, {Decimal("0.505"): Decimal("100")})
        )
        svc = MarketDataService(store, gw, policy=_rest_policy())

        bundle = await svc.require_fresh_data(TOKEN, side="BUY")
        assert bundle.source == MarketDataSource.REST_SNAPSHOT


class TestRestSnapshotBudgetsMatchTheRoundTrip:
    """The freshness budget must be able to contain the fetch it measures.

    ``fetched_at`` anchors on ``fetch_start``, so the REST round trip itself
    counts against ``rest_snapshot_max_age_ms``. Live round trips measured
    515-571 ms, which the shipped 500 ms cap rejected outright — the fallback
    refused every snapshot it was meant to provide.
    """

    @staticmethod
    def _clock_and_gateway(elapsed_seconds: float, book) -> tuple[list[float], MagicMock]:
        """Return a monotonic-clock holder and a gateway that advances it.

        The clock is injected in place of the module's ``time`` reference so
        the test is deterministic and never sleeps.
        """
        now = [1000.0]

        async def _get(_token):
            now[0] += elapsed_seconds
            return book

        async def _info(_token):
            return {"tick_size": "0.01", "min_order_size": "5", "neg_risk": False}

        gw = MagicMock()
        gw.get_orderbook = _get
        gw.get_market_info = _info
        return now, gw

    @pytest.mark.asyncio
    async def test_seven_hundred_ms_round_trip_is_accepted_under_the_new_cap(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        import services.market_data_service as mds_mod

        store = MarketStateStore()
        now, gw = self._clock_and_gateway(
            0.7,
            _book({Decimal("0.495"): Decimal("100")}, {Decimal("0.505"): Decimal("100")}),
        )
        monkeypatch.setattr(mds_mod, "time", SimpleNamespace(monotonic=lambda: now[0]))
        svc = MarketDataService(
            store,
            gw,
            policy=_rest_policy(
                rest_snapshot_max_age_ms=1500,
                rest_snapshot_total_budget_ms=2000,
            ),
        )

        bundle = await svc.require_execution_snapshot(TOKEN, side="BUY")

        assert bundle.source == MarketDataSource.REST_SNAPSHOT

    @pytest.mark.asyncio
    async def test_seven_hundred_ms_round_trip_was_rejected_by_the_old_cap(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Characterizes the live failure the new budget fixes."""
        import services.market_data_service as mds_mod

        store = MarketStateStore()
        now, gw = self._clock_and_gateway(
            0.7,
            _book({Decimal("0.495"): Decimal("100")}, {Decimal("0.505"): Decimal("100")}),
        )
        monkeypatch.setattr(mds_mod, "time", SimpleNamespace(monotonic=lambda: now[0]))
        svc = MarketDataService(
            store,
            gw,
            policy=_rest_policy(rest_snapshot_max_age_ms=500),
        )

        with pytest.raises(StaleDataError) as exc:
            await svc.require_execution_snapshot(TOKEN, side="BUY")
        assert exc.value.skip_code == "market_data_stale"

    @pytest.mark.asyncio
    async def test_zero_spread_cap_never_rejects_a_wide_book(self) -> None:
        """0 disables the spread gate; the leader-price band owns price quality."""
        store = MarketStateStore()
        gw = _gateway_returning(
            _book(
                {Decimal("0.50"): Decimal("100")},
                {Decimal("1.00"): Decimal("100")},
            )
        )
        svc = MarketDataService(store, gw, policy=_rest_policy(rest_snapshot_max_spread_bps=0))

        bundle = await svc.require_execution_snapshot(TOKEN, side="BUY")

        assert bundle.source == MarketDataSource.REST_SNAPSHOT
        assert bundle.quote.best_ask == Decimal("1.00")


# ---------------------------------------------------------------------------
# REST_SNAPSHOT path in _bundle_from_cache (Fix 4)
# ---------------------------------------------------------------------------


class TestBundleFromCacheRestSnapshot:
    """_bundle_from_cache returns a REST_SNAPSHOT bundle when both components
    are rest_snapshot-sourced and within the millisecond age cap."""

    def _make_rest_snapshot_cache(self, *, stale_ms: float = 0.0) -> MarketStateStore:
        cache = MarketStateStore()
        ts = time.monotonic() - stale_ms / 1000.0
        cache.store_rest_snapshot(
            TOKEN,
            condition_id=None,
            bids={Decimal("0.84"): Decimal("100")},
            asks={Decimal("0.87"): Decimal("50")},
            tick_size=Decimal("0.01"),
            min_order_size=Decimal("5"),
            neg_risk=False,
            fetched_at_monotonic=ts,
        )
        return cache

    def _rest_snap_policy(self, **overrides) -> MarketDataPolicy:
        defaults = dict(
            primary_source="rest_snapshot",
            require_wss_fresh=True,
            allow_rest_snapshot_for_execution=True,
            rest_snapshot_max_age_ms=500,
            max_quote_age_seconds=10.0,
            max_book_age_seconds=10.0,
            max_tick_age_seconds=300.0,
        )
        defaults.update(overrides)
        return MarketDataPolicy(**defaults)

    @pytest.mark.asyncio
    async def test_fresh_rest_snapshot_returns_bundle(self) -> None:
        cache = self._make_rest_snapshot_cache(stale_ms=10.0)
        svc = _make_service(cache, policy=self._rest_snap_policy())

        bundle = await svc.require_fresh_data(TOKEN)

        assert bundle is not None
        assert bundle.source == MarketDataSource.REST_SNAPSHOT

    @pytest.mark.asyncio
    async def test_stale_rest_snapshot_raises(self) -> None:
        cache = self._make_rest_snapshot_cache(stale_ms=600.0)
        svc = _make_service(cache, policy=self._rest_snap_policy())

        with pytest.raises(StaleDataError):
            await svc.require_fresh_data(TOKEN)

    @pytest.mark.asyncio
    async def test_rest_snapshot_blocked_when_not_allowed(self) -> None:
        cache = self._make_rest_snapshot_cache(stale_ms=10.0)
        svc = _make_service(
            cache,
            policy=self._rest_snap_policy(allow_rest_snapshot_for_execution=False),
        )

        with pytest.raises(StaleDataError):
            await svc.require_fresh_data(TOKEN)

    @pytest.mark.asyncio
    async def test_wss_preferred_over_rest_snapshot(self) -> None:
        cache = self._make_rest_snapshot_cache(stale_ms=10.0)
        cache.replace_book_snapshot(
            TOKEN,
            {Decimal("0.85"): Decimal("100")},
            {Decimal("0.88"): Decimal("50")},
            {"source": "wss"},
        )
        cache.set_quote(TOKEN, best_bid=Decimal("0.85"), best_ask=Decimal("0.88"), source="wss")
        policy = self._rest_snap_policy()
        svc = _make_service(cache, policy=policy)

        bundle = await svc.require_fresh_data(TOKEN)

        assert bundle.source == MarketDataSource.WSS

    # ------------------------------------------------------------------
    # REST prefetch, WSS after subscription: side-scoped execution reads
    # ------------------------------------------------------------------
    # REST is the prefetch/bootstrap source; once the market WSS subscription
    # warms the cache, ``require_fresh_data(..., side=...)`` must prefer the
    # live WSS bundle instead of re-fetching a REST snapshot, and only fall
    # back to a fresh gated REST snapshot when the cache is not yet executable.

    @pytest.mark.asyncio
    async def test_require_fresh_data_side_prefers_wss_cache(self) -> None:
        cache = _make_fresh_cache(TOKEN)  # WSS-sourced quote+book+tick
        svc = _make_service(cache, policy=self._rest_snap_policy())
        svc.require_execution_snapshot = AsyncMock()  # must NOT be called

        bundle = await svc.require_fresh_data(TOKEN, side="BUY")

        assert bundle.source == MarketDataSource.WSS
        svc.require_execution_snapshot.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_require_fresh_data_side_uses_fresh_rest_snapshot_in_cache(self) -> None:
        # Bridge window: a still-fresh REST snapshot already seeded by the
        # prefetch is reused for a side-scoped read without a second REST
        # round-trip, until the first WSS delta upgrades the source.
        cache = self._make_rest_snapshot_cache(stale_ms=10.0)
        svc = _make_service(cache, policy=self._rest_snap_policy())
        svc.require_execution_snapshot = AsyncMock()

        bundle = await svc.require_fresh_data(TOKEN, side="BUY")

        assert bundle.source == MarketDataSource.REST_SNAPSHOT
        svc.require_execution_snapshot.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_require_fresh_data_side_falls_back_to_rest_when_cache_cold(self) -> None:
        cache = MarketStateStore()  # cold: no WSS, no REST snapshot yet
        svc = _make_service(cache, policy=self._rest_snap_policy())
        sentinel = object()
        svc.require_execution_snapshot = AsyncMock(return_value=sentinel)

        result = await svc.require_fresh_data(TOKEN, side="BUY")

        assert result is sentinel
        svc.require_execution_snapshot.assert_awaited_once()
        assert svc.require_execution_snapshot.await_args.kwargs.get("side") == "BUY"

    @pytest.mark.asyncio
    async def test_require_fresh_data_side_stale_rest_cache_falls_back_to_rest(self) -> None:
        # Stale REST snapshot in cache (past the ms age cap) and no WSS yet →
        # cache returns None → fresh gated REST fetch (fail-closed bridge).
        cache = self._make_rest_snapshot_cache(stale_ms=600.0)
        svc = _make_service(cache, policy=self._rest_snap_policy())
        sentinel = object()
        svc.require_execution_snapshot = AsyncMock(return_value=sentinel)

        result = await svc.require_fresh_data(TOKEN, side="BUY")

        assert result is sentinel
        svc.require_execution_snapshot.assert_awaited_once()
