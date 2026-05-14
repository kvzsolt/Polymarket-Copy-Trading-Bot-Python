"""Tests for orchestrators.market_data_runtime — MarketDataRuntime.

Covers:
  - start with no tokens stays not-ready
  - bootstrap calls gateway, sets tick, marks dirty on bad state
  - subscribe/unsubscribe token management
  - stop marks all tokens stale
  - status counts stale/dirty tokens
  - extract_token_ids_from_live_positions deduplicates
"""
from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from cache.market_state_store import MarketStateStore
from integrations.polymarket.models import OrderBook, OrderBookLevel
from orchestrators.market_data_runtime import (
    MarketDataRuntime,
    extract_token_ids_from_live_positions,
)
from services.healthServer import ReadinessTracker


TOKEN_A = "0xaaa"
TOKEN_B = "0xbbb"


def _make_gateway(
    *,
    tick_size: str = "0.01",
    min_order_size: str = "5",
    empty_book: bool = False,
    missing_tick: bool = False,
) -> AsyncMock:
    gw = AsyncMock()
    if empty_book:
        book = OrderBook(token_id=TOKEN_A, bids=[], asks=[], best_bid=None, best_ask=None)
    else:
        book = OrderBook(
            token_id=TOKEN_A,
            bids=[OrderBookLevel(price=Decimal("0.50"), size=Decimal("100"))],
            asks=[OrderBookLevel(price=Decimal("0.55"), size=Decimal("80"))],
            best_bid=Decimal("0.50"),
            best_ask=Decimal("0.55"),
        )
    gw.get_orderbook = AsyncMock(return_value=book)
    gw.get_market_info = AsyncMock(return_value={
        "tick_size": None if missing_tick else tick_size,
        "min_order_size": min_order_size,
        "neg_risk": False,
    })
    return gw


def _make_runtime(
    store: MarketStateStore | None = None,
    gateway=None,
    readiness: ReadinessTracker | None = None,
) -> MarketDataRuntime:
    return MarketDataRuntime(
        market_store=store or MarketStateStore(),
        gateway=gateway or _make_gateway(),
        readiness=readiness or ReadinessTracker(),
        config=SimpleNamespace(),
    )


# ---------------------------------------------------------------------------
# Start with no tokens
# ---------------------------------------------------------------------------

class TestStartNoTokens:
    @pytest.mark.asyncio
    async def test_start_empty_tokens_market_data_not_ready(self) -> None:
        readiness = ReadinessTracker()
        rt = _make_runtime(readiness=readiness)
        await rt.start([])

        snap = readiness.snapshot()
        assert snap.get("market_data_ready") is False


# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------

class TestBootstrap:
    @pytest.mark.asyncio
    async def test_bootstrap_calls_gateway_for_each_token(self) -> None:
        store = MarketStateStore()
        gw = _make_gateway()
        rt = _make_runtime(store=store, gateway=gw)
        await rt.bootstrap_tokens([TOKEN_A, TOKEN_B])

        assert gw.get_orderbook.await_count == 2
        assert gw.get_market_info.await_count == 2

    @pytest.mark.asyncio
    async def test_bootstrap_sets_tick_in_store(self) -> None:
        store = MarketStateStore()
        rt = _make_runtime(store=store, gateway=_make_gateway(tick_size="0.01"))
        await rt.bootstrap_tokens([TOKEN_A])

        tick = store.get_tick(TOKEN_A)
        assert tick is not None
        assert tick.tick_size == Decimal("0.01")

    @pytest.mark.asyncio
    async def test_bootstrap_empty_book_marks_dirty(self) -> None:
        store = MarketStateStore()
        rt = _make_runtime(store=store, gateway=_make_gateway(empty_book=True))
        await rt.bootstrap_tokens([TOKEN_A])

        assert store.is_dirty(TOKEN_A)

    @pytest.mark.asyncio
    async def test_bootstrap_missing_tick_marks_dirty(self) -> None:
        store = MarketStateStore()
        rt = _make_runtime(store=store, gateway=_make_gateway(missing_tick=True))
        await rt.bootstrap_tokens([TOKEN_A])

        assert store.is_dirty(TOKEN_A)

    @pytest.mark.asyncio
    async def test_bootstrap_gateway_exception_marks_dirty(self) -> None:
        store = MarketStateStore()
        gw = AsyncMock()
        gw.get_orderbook = AsyncMock(side_effect=RuntimeError("network"))
        rt = _make_runtime(store=store, gateway=gw)
        await rt.bootstrap_tokens([TOKEN_A])

        assert store.is_dirty(TOKEN_A)

    @pytest.mark.asyncio
    async def test_bootstrap_after_rest_token_stays_dirty_awaiting_wss(self) -> None:
        """After REST bootstrap, token must remain dirty until WSS provides fresh data."""
        store = MarketStateStore()
        rt = _make_runtime(store=store, gateway=_make_gateway())
        await rt.bootstrap_tokens([TOKEN_A])

        assert store.is_dirty(TOKEN_A)


# ---------------------------------------------------------------------------
# Subscribe / unsubscribe
# ---------------------------------------------------------------------------

class TestSubscribeTokens:
    @pytest.mark.asyncio
    async def test_subscribe_adds_tokens_to_set(self) -> None:
        rt = _make_runtime()
        await rt.subscribe_tokens([TOKEN_A])
        assert TOKEN_A in rt._subscribed_tokens

    @pytest.mark.asyncio
    async def test_subscribe_deduplicates(self) -> None:
        rt = _make_runtime()
        await rt.subscribe_tokens([TOKEN_A, TOKEN_A])
        assert len(rt._subscribed_tokens) == 1

    @pytest.mark.asyncio
    async def test_subscribe_no_duplicates_across_calls(self) -> None:
        rt = _make_runtime()
        await rt.subscribe_tokens([TOKEN_A])
        await rt.subscribe_tokens([TOKEN_A])
        assert len(rt._subscribed_tokens) == 1

    @pytest.mark.asyncio
    async def test_unsubscribe_removes_token(self) -> None:
        rt = _make_runtime()
        await rt.subscribe_tokens([TOKEN_A, TOKEN_B])
        await rt.unsubscribe_tokens([TOKEN_A])
        assert TOKEN_A not in rt._subscribed_tokens
        assert TOKEN_B in rt._subscribed_tokens

    @pytest.mark.asyncio
    async def test_unsubscribe_unknown_token_noop(self) -> None:
        rt = _make_runtime()
        await rt.unsubscribe_tokens(["0xunknown"])
        assert "0xunknown" not in rt._subscribed_tokens


# ---------------------------------------------------------------------------
# Stop
# ---------------------------------------------------------------------------

class TestStop:
    @pytest.mark.asyncio
    async def test_stop_marks_all_stale(self) -> None:
        store = MarketStateStore()
        store.replace_book_snapshot(
            TOKEN_A,
            bids={Decimal("0.5"): Decimal("10")},
            asks={Decimal("0.6"): Decimal("10")},
        )
        rt = _make_runtime(store=store)
        await rt.stop()

        assert store.is_stale(TOKEN_A)

    @pytest.mark.asyncio
    async def test_stop_sets_running_false(self) -> None:
        rt = _make_runtime()
        rt._status.running = True
        await rt.stop()
        assert rt._status.running is False

    @pytest.mark.asyncio
    async def test_stop_clears_ws_client(self) -> None:
        rt = _make_runtime()
        await rt.stop()
        assert rt._ws_client is None


# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------

class TestStatus:
    @pytest.mark.asyncio
    async def test_status_counts_stale_tokens(self) -> None:
        store = MarketStateStore()
        store.mark_stale(TOKEN_A, "test")
        rt = _make_runtime(store=store)
        rt._subscribed_tokens.add(TOKEN_A)

        status = rt.status()
        assert status.stale_token_count == 1

    @pytest.mark.asyncio
    async def test_status_counts_dirty_tokens(self) -> None:
        store = MarketStateStore()
        store.mark_dirty(TOKEN_A, "test")
        rt = _make_runtime(store=store)
        rt._subscribed_tokens.add(TOKEN_A)

        status = rt.status()
        assert status.dirty_token_count == 1

    @pytest.mark.asyncio
    async def test_status_subscribed_count(self) -> None:
        rt = _make_runtime()
        await rt.subscribe_tokens([TOKEN_A, TOKEN_B])

        status = rt.status()
        assert status.subscribed_token_count == 2


# ---------------------------------------------------------------------------
# extract_token_ids_from_live_positions
# ---------------------------------------------------------------------------

class TestReadinessFromWss:
    """``market_data_ready`` must reflect executable WSS state per token."""

    @pytest.mark.asyncio
    async def test_market_data_ready_true_after_wss_book_and_tick(self) -> None:
        readiness = ReadinessTracker()
        store = MarketStateStore()
        rt = _make_runtime(store=store, readiness=readiness)
        rt._subscribed_tokens.add(TOKEN_A)
        rt._status.running = True

        # WSS book snapshot — promotes both quote_source and book_source to wss.
        store.replace_book_snapshot(
            TOKEN_A,
            bids={Decimal("0.49"): Decimal("100")},
            asks={Decimal("0.51"): Decimal("100")},
            raw_event={"source": "wss"},
        )
        # Tick may legitimately be REST-derived.
        store.set_tick(
            TOKEN_A,
            tick_size=Decimal("0.01"),
            min_order_size=Decimal("5"),
            neg_risk=False,
            source="rest_snapshot",
        )

        await rt._on_market_state_change(TOKEN_A, "book")

        assert readiness.snapshot().get("market_data_ready") is True

    @pytest.mark.asyncio
    async def test_market_data_ready_false_when_dirty(self) -> None:
        readiness = ReadinessTracker()
        store = MarketStateStore()
        rt = _make_runtime(store=store, readiness=readiness)
        rt._subscribed_tokens.add(TOKEN_A)
        rt._status.running = True

        store.replace_book_snapshot(
            TOKEN_A,
            bids={Decimal("0.49"): Decimal("100")},
            asks={Decimal("0.51"): Decimal("100")},
            raw_event={"source": "wss"},
        )
        store.set_tick(
            TOKEN_A,
            tick_size=Decimal("0.01"),
            min_order_size=Decimal("5"),
            neg_risk=False,
        )
        store.mark_dirty(TOKEN_A, "test_dirty")

        await rt._on_market_state_change(TOKEN_A, "tick_size_change")

        assert readiness.snapshot().get("market_data_ready") is False

    @pytest.mark.asyncio
    async def test_market_data_ready_false_when_missing_tick(self) -> None:
        readiness = ReadinessTracker()
        store = MarketStateStore()
        rt = _make_runtime(store=store, readiness=readiness)
        rt._subscribed_tokens.add(TOKEN_A)
        rt._status.running = True

        # WSS book/quote but no tick.
        store.replace_book_snapshot(
            TOKEN_A,
            bids={Decimal("0.49"): Decimal("100")},
            asks={Decimal("0.51"): Decimal("100")},
            raw_event={"source": "wss"},
        )

        await rt._on_market_state_change(TOKEN_A, "book")

        assert readiness.snapshot().get("market_data_ready") is False

    @pytest.mark.asyncio
    async def test_market_data_ready_false_when_quote_only_rest(self) -> None:
        readiness = ReadinessTracker()
        store = MarketStateStore()
        rt = _make_runtime(store=store, readiness=readiness)
        rt._subscribed_tokens.add(TOKEN_A)
        rt._status.running = True

        # REST-only seeds — book/quote source stays "rest_snapshot".
        store.set_book(
            TOKEN_A,
            bids={Decimal("0.49"): Decimal("100")},
            asks={Decimal("0.51"): Decimal("100")},
            source="rest_snapshot",
        )
        store.set_tick(
            TOKEN_A,
            tick_size=Decimal("0.01"),
            min_order_size=Decimal("5"),
            neg_risk=False,
            source="rest_snapshot",
        )
        store.mark_dirty(TOKEN_A, "awaiting_wss_after_rest_bootstrap")

        await rt._on_market_state_change(TOKEN_A, "book")

        assert readiness.snapshot().get("market_data_ready") is False


class TestExtractTokenIds:
    def test_extracts_asset_field(self) -> None:
        positions = [{"asset": "0xabc"}, {"asset": "0xdef"}]
        result = extract_token_ids_from_live_positions(positions)
        assert result == ["0xabc", "0xdef"]

    def test_deduplicates(self) -> None:
        positions = [{"asset": "0xabc"}, {"asset": "0xabc"}]
        result = extract_token_ids_from_live_positions(positions)
        assert result == ["0xabc"]

    def test_skips_empty(self) -> None:
        positions = [{"asset": ""}, {"asset": None}, {}]
        result = extract_token_ids_from_live_positions(positions)
        assert result == []

    def test_extracts_clob_token_ids_list(self) -> None:
        positions = [{"clobTokenIds": ["0xaaa", "0xbbb"]}]
        result = extract_token_ids_from_live_positions(positions)
        assert "0xaaa" in result
        assert "0xbbb" in result

    def test_extracts_token_id_field(self) -> None:
        positions = [{"token_id": "0xfff"}]
        result = extract_token_ids_from_live_positions(positions)
        assert result == ["0xfff"]

    def test_empty_list_returns_empty(self) -> None:
        assert extract_token_ids_from_live_positions([]) == []


# ---------------------------------------------------------------------------
# ensure_token_ready_or_bootstrapped
# ---------------------------------------------------------------------------

def _make_runtime_with_config(
    store: MarketStateStore | None = None,
    gateway=None,
    *,
    enable: bool = True,
    max_age_ms: int = 500,
    request_timeout_ms: int = 750,
    total_budget_ms: int = 1000,
    max_spread_bps: int = 300,
    require_wss_connected: bool = False,
) -> MarketDataRuntime:
    cfg = SimpleNamespace(
        market_data=SimpleNamespace(
            enable_rest_bootstrap=enable,
            rest_bootstrap_max_age_ms=max_age_ms,
            rest_bootstrap_request_timeout_ms=request_timeout_ms,
            rest_bootstrap_total_budget_ms=total_budget_ms,
            rest_bootstrap_max_spread_bps=max_spread_bps,
            rest_bootstrap_require_wss_connected=require_wss_connected,
        )
    )
    return MarketDataRuntime(
        market_store=store or MarketStateStore(),
        gateway=gateway or _make_gateway(),
        readiness=ReadinessTracker(),
        config=cfg,
    )


def _make_good_book(token_id: str = TOKEN_A):
    from integrations.polymarket.models import OrderBook, OrderBookLevel
    return OrderBook(
        token_id=token_id,
        bids=[OrderBookLevel(price=Decimal("0.49"), size=Decimal("200"))],
        asks=[OrderBookLevel(price=Decimal("0.50"), size=Decimal("150"))],
        best_bid=Decimal("0.49"),
        best_ask=Decimal("0.50"),
    )


def _make_bootstrap_gateway(token_id: str = TOKEN_A) -> AsyncMock:
    gw = AsyncMock()
    gw.get_orderbook = AsyncMock(return_value=_make_good_book(token_id))
    gw.get_market_info = AsyncMock(return_value={
        "tick_size": "0.01",
        "min_order_size": "5",
        "neg_risk": False,
    })
    return gw


class TestEnsureTokenReadyOrBootstrapped:
    @pytest.mark.asyncio
    async def test_already_executable_returns_true_immediately(self) -> None:
        store = MarketStateStore()
        store.replace_book_snapshot(
            TOKEN_A,
            {Decimal("0.49"): Decimal("100")},
            {Decimal("0.51"): Decimal("100")},
            {"source": "wss"},
        )
        store.set_quote(TOKEN_A, best_bid=Decimal("0.49"), best_ask=Decimal("0.51"), source="wss")
        store.set_tick(TOKEN_A, tick_size=Decimal("0.01"), min_order_size=Decimal("5"), neg_risk=False, source="wss")
        gw = _make_bootstrap_gateway()
        rt = _make_runtime_with_config(store=store, gateway=gw)

        result = await rt.ensure_token_ready_or_bootstrapped(TOKEN_A)

        assert result is True
        gw.get_orderbook.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_new_token_subscribe_sent_and_bootstrap_fetched(self) -> None:
        store = MarketStateStore()
        gw = _make_bootstrap_gateway()
        rt = _make_runtime_with_config(store=store, gateway=gw)

        result = await rt.ensure_token_ready_or_bootstrapped(TOKEN_A)

        assert TOKEN_A in rt._subscribed_tokens
        gw.get_orderbook.assert_awaited_once()
        assert result is True

    @pytest.mark.asyncio
    async def test_token_subscribed_but_no_book_attempts_bootstrap(self) -> None:
        store = MarketStateStore()
        gw = _make_bootstrap_gateway()
        rt = _make_runtime_with_config(store=store, gateway=gw)
        rt._subscribed_tokens.add(TOKEN_A)

        result = await rt.ensure_token_ready_or_bootstrapped(TOKEN_A)

        gw.get_orderbook.assert_awaited_once()
        assert result is True

    @pytest.mark.asyncio
    async def test_bootstrap_writes_rest_bootstrap_book_source(self) -> None:
        store = MarketStateStore()
        gw = _make_bootstrap_gateway()
        rt = _make_runtime_with_config(store=store, gateway=gw)

        await rt.ensure_token_ready_or_bootstrapped(TOKEN_A)

        assert store.book_source_of(TOKEN_A) == "rest_bootstrap"

    @pytest.mark.asyncio
    async def test_bootstrap_writes_rest_bootstrap_quote_source(self) -> None:
        store = MarketStateStore()
        gw = _make_bootstrap_gateway()
        rt = _make_runtime_with_config(store=store, gateway=gw)

        await rt.ensure_token_ready_or_bootstrapped(TOKEN_A)

        assert store.quote_source_of(TOKEN_A) == "rest_bootstrap"

    @pytest.mark.asyncio
    async def test_bootstrap_does_not_write_wss_source(self) -> None:
        store = MarketStateStore()
        gw = _make_bootstrap_gateway()
        rt = _make_runtime_with_config(store=store, gateway=gw)

        await rt.ensure_token_ready_or_bootstrapped(TOKEN_A)

        assert store.book_source_of(TOKEN_A) != "wss"
        assert store.quote_source_of(TOKEN_A) != "wss"

    @pytest.mark.asyncio
    async def test_bootstrap_accepted_when_fresh_and_spread_good(self) -> None:
        store = MarketStateStore()
        gw = _make_bootstrap_gateway()
        rt = _make_runtime_with_config(store=store, gateway=gw, max_age_ms=2000)

        result = await rt.ensure_token_ready_or_bootstrapped(TOKEN_A)

        assert result is True

    @pytest.mark.asyncio
    async def test_bootstrap_rejected_when_stale(self) -> None:
        import asyncio as _asyncio

        async def _slow_book(token_id):
            await _asyncio.sleep(0.05)
            return _make_good_book(token_id)

        gw = AsyncMock()
        gw.get_orderbook = _slow_book
        gw.get_market_info = AsyncMock(return_value={"tick_size": "0.01", "min_order_size": "5"})

        rt = _make_runtime_with_config(gateway=gw, max_age_ms=1)

        result = await rt.ensure_token_ready_or_bootstrapped(TOKEN_A)

        assert result is False

    @pytest.mark.asyncio
    async def test_bootstrap_rejected_when_request_times_out(self) -> None:
        import asyncio as _asyncio

        async def _timeout_book(token_id):
            await _asyncio.sleep(10)
            return _make_good_book(token_id)

        gw = AsyncMock()
        gw.get_orderbook = _timeout_book
        gw.get_market_info = AsyncMock(return_value={})

        rt = _make_runtime_with_config(gateway=gw, request_timeout_ms=50)

        result = await rt.ensure_token_ready_or_bootstrapped(TOKEN_A)

        assert result is False

    @pytest.mark.asyncio
    async def test_bootstrap_rejected_when_budget_exceeded_before_fetch(self) -> None:
        gw = _make_bootstrap_gateway()
        rt = _make_runtime_with_config(gateway=gw, total_budget_ms=0)

        result = await rt.ensure_token_ready_or_bootstrapped(TOKEN_A)

        assert result is False

    @pytest.mark.asyncio
    async def test_bootstrap_rejected_when_spread_too_wide(self) -> None:
        from integrations.polymarket.models import OrderBook, OrderBookLevel

        wide_book = OrderBook(
            token_id=TOKEN_A,
            bids=[OrderBookLevel(price=Decimal("0.10"), size=Decimal("100"))],
            asks=[OrderBookLevel(price=Decimal("0.90"), size=Decimal("100"))],
            best_bid=Decimal("0.10"),
            best_ask=Decimal("0.90"),
        )
        gw = AsyncMock()
        gw.get_orderbook = AsyncMock(return_value=wide_book)
        gw.get_market_info = AsyncMock(return_value={"tick_size": "0.01", "min_order_size": "5"})

        rt = _make_runtime_with_config(gateway=gw, max_spread_bps=100)

        result = await rt.ensure_token_ready_or_bootstrapped(TOKEN_A)

        assert result is False

    @pytest.mark.asyncio
    async def test_bootstrap_rejected_when_wss_required_and_disconnected(self) -> None:
        gw = _make_bootstrap_gateway()
        rt = _make_runtime_with_config(gateway=gw, require_wss_connected=True)

        result = await rt.ensure_token_ready_or_bootstrapped(TOKEN_A)

        assert result is False

    @pytest.mark.asyncio
    async def test_wss_book_event_overrides_rest_bootstrap_source(self) -> None:
        store = MarketStateStore()
        gw = _make_bootstrap_gateway()
        rt = _make_runtime_with_config(store=store, gateway=gw)

        await rt.ensure_token_ready_or_bootstrapped(TOKEN_A)
        assert store.book_source_of(TOKEN_A) == "rest_bootstrap"

        store.replace_book_snapshot(
            TOKEN_A,
            {Decimal("0.50"): Decimal("50")},
            {Decimal("0.52"): Decimal("50")},
            {"source": "wss"},
        )

        assert store.book_source_of(TOKEN_A) == "wss"

    @pytest.mark.asyncio
    async def test_apply_book_delta_after_bootstrap_sets_wss_source(self) -> None:
        store = MarketStateStore()
        gw = _make_bootstrap_gateway()
        rt = _make_runtime_with_config(store=store, gateway=gw)

        await rt.ensure_token_ready_or_bootstrapped(TOKEN_A)
        assert store.book_source_of(TOKEN_A) == "rest_bootstrap"

        store.apply_book_delta(TOKEN_A, "buy", Decimal("0.50"), Decimal("10"))

        assert store.book_source_of(TOKEN_A) == "wss"

    @pytest.mark.asyncio
    async def test_bootstrap_rejected_when_empty_book(self) -> None:
        from integrations.polymarket.models import OrderBook

        empty_book = OrderBook(token_id=TOKEN_A, bids=[], asks=[], best_bid=None, best_ask=None)
        gw = AsyncMock()
        gw.get_orderbook = AsyncMock(return_value=empty_book)
        gw.get_market_info = AsyncMock(return_value={})

        rt = _make_runtime_with_config(gateway=gw)

        result = await rt.ensure_token_ready_or_bootstrapped(TOKEN_A)

        assert result is False

    @pytest.mark.asyncio
    async def test_bootstrap_uses_defaults_when_no_market_data_config(self) -> None:
        """Missing market_data config must not raise; defaults are applied."""
        store = MarketStateStore()
        gw = _make_bootstrap_gateway()
        rt = MarketDataRuntime(
            market_store=store,
            gateway=gw,
            readiness=ReadinessTracker(),
            config=SimpleNamespace(),
        )

        # Default require_wss_connected=True with WSS not started → bootstrap
        # is gated, returns False (not an error). Just verify no crash.
        result = await rt.ensure_token_ready_or_bootstrapped(TOKEN_A)

        assert isinstance(result, bool)
