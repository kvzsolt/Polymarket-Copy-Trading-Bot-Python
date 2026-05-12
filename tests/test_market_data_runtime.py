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
