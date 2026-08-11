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
from integrations.polymarket.errors import VenueOrderbookNotFoundError
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
        book = OrderBook(
            token_id=TOKEN_A,
            tick_size=Decimal("0.01"),
            min_order_size=Decimal("5"),
            neg_risk=False,
            bids=[],
            asks=[],
            best_bid=None,
            best_ask=None,
        )
    else:
        book = OrderBook(
            token_id=TOKEN_A,
            tick_size=Decimal("0.01"),
            min_order_size=Decimal("5"),
            neg_risk=False,
            bids=[OrderBookLevel(price=Decimal("0.50"), size=Decimal("100"))],
            asks=[OrderBookLevel(price=Decimal("0.55"), size=Decimal("80"))],
            best_bid=Decimal("0.50"),
            best_ask=Decimal("0.55"),
        )
    gw.get_orderbook = AsyncMock(return_value=book)
    gw.get_market_info = AsyncMock(
        return_value={
            "tick_size": None if missing_tick else tick_size,
            "min_order_size": min_order_size,
            "neg_risk": False,
        }
    )
    return gw


def _make_runtime(
    store: MarketStateStore | None = None,
    gateway=None,
    readiness: ReadinessTracker | None = None,
    quote_sink=None,
) -> MarketDataRuntime:
    return MarketDataRuntime(
        market_store=store or MarketStateStore(),
        gateway=gateway or _make_gateway(),
        readiness=readiness or ReadinessTracker(),
        config=SimpleNamespace(),
        quote_sink=quote_sink,
    )


class RecordingQuoteSink:
    """Capture authoritative two-sided quote projections."""

    def __init__(self) -> None:
        self.quotes: list[tuple[str, Decimal, Decimal, str]] = []
        self.discarded: list[str] = []

    def record(
        self,
        *,
        token_id: str,
        best_bid: Decimal,
        best_ask: Decimal,
        source: str,
    ) -> None:
        """Record one normalized quote."""

        self.quotes.append((token_id, best_bid, best_ask, source))

    def discard(self, token_id: str) -> None:
        """Record one resolved-token eviction."""

        self.discarded.append(token_id)


# ---------------------------------------------------------------------------
# Start with no tokens
# ---------------------------------------------------------------------------


class _FakeMarketWebSocket:
    """Stand-in for MarketWebSocket that never touches the network."""

    def __init__(self, *args, on_connected=None, **kwargs) -> None:
        self._on_connected = on_connected
        self.connected_with: list[str] | None = None
        self.subscribed: list[str] = []

    async def connect(self, asset_ids: list[str]) -> None:
        self.connected_with = list(asset_ids)
        if self._on_connected is not None:
            await self._on_connected()

    async def subscribe(self, asset_ids: list[str]) -> None:
        self.subscribed.extend(asset_ids)

    async def unsubscribe(self, asset_ids: list[str]) -> None:
        for a in asset_ids:
            if a in self.subscribed:
                self.subscribed.remove(a)

    async def stop(self) -> None:
        return None


class TestStartNoTokens:
    @pytest.mark.asyncio
    async def test_start_empty_tokens_market_data_not_ready(self, monkeypatch) -> None:
        monkeypatch.setattr(
            "orchestrators.market_data_runtime.MarketWebSocket",
            _FakeMarketWebSocket,
        )
        readiness = ReadinessTracker()
        rt = _make_runtime(readiness=readiness)
        await rt.start([])

        snap = readiness.snapshot()
        assert snap.get("market_data_ready") is False
        await rt.stop()

    @pytest.mark.asyncio
    async def test_start_empty_tokens_still_opens_wss_connection(self, monkeypatch) -> None:
        """Regression: empty initial token set must NOT skip WSS startup.

        Trade detection is HTTP-polled, so the runtime routinely starts with
        zero tokens. If start() returns early, _ws_client/_ws_task never exist
        and every dynamically-added token is permanently un-bootstrappable.
        """
        monkeypatch.setattr(
            "orchestrators.market_data_runtime.MarketWebSocket",
            _FakeMarketWebSocket,
        )
        rt = _make_runtime()
        await rt.start([])

        assert rt._ws_client is not None
        assert rt._ws_task is not None
        assert rt._status.running is True
        await rt.stop()

    @pytest.mark.asyncio
    async def test_start_empty_then_ensure_token_subscribes_via_ws_client(
        self, monkeypatch
    ) -> None:
        """After an empty start, a new token must reach the live WS client."""
        monkeypatch.setattr(
            "orchestrators.market_data_runtime.MarketWebSocket",
            _FakeMarketWebSocket,
        )
        gw = _make_bootstrap_gateway()
        rt = _make_runtime_with_config(gateway=gw)
        await rt.start([])
        # let the fake connect() callback run
        import asyncio as _asyncio

        await _asyncio.sleep(0)

        result = await rt.ensure_token_ready_or_bootstrapped(TOKEN_A)

        assert TOKEN_A in rt._subscribed_tokens
        assert TOKEN_A in rt._ws_client.subscribed
        assert result is True
        await rt.stop()


class TestWssConnectionState:
    @pytest.mark.asyncio
    async def test_is_wss_down_true_before_running(self) -> None:
        rt = _make_runtime()
        assert rt.is_wss_down() is True

    @pytest.mark.asyncio
    async def test_is_wss_down_true_when_running_but_not_connected(self) -> None:
        rt = _make_runtime()
        rt._status.running = True
        rt._wss_connected = False
        assert rt.is_wss_down() is True

    @pytest.mark.asyncio
    async def test_is_wss_down_false_when_connected_no_message_yet(self) -> None:
        """A connected stream with no message yet is NOT down (connecting race)."""
        rt = _make_runtime()
        rt._status.running = True
        rt._wss_connected = True
        rt._last_wss_message_at = 0.0
        assert rt.is_wss_down() is False

    @pytest.mark.asyncio
    async def test_is_wss_down_true_when_connected_but_silent(self) -> None:
        import time as _time

        rt = _make_runtime()
        rt._status.running = True
        rt._wss_connected = True
        rt._last_wss_message_at = _time.monotonic() - 999.0
        assert rt.is_wss_down(threshold_seconds=15.0) is True

    @pytest.mark.asyncio
    async def test_on_wss_connected_sets_connected_flag(self) -> None:
        rt = _make_runtime()
        await rt._on_wss_connected()
        assert rt._wss_connected is True

    @pytest.mark.asyncio
    async def test_on_wss_disconnected_clears_connected_flag(self) -> None:
        rt = _make_runtime()
        rt._wss_connected = True
        await rt._on_wss_disconnected()
        assert rt._wss_connected is False

    @pytest.mark.asyncio
    async def test_await_wss_connected_returns_immediately_when_connected(self) -> None:
        rt = _make_runtime()
        rt._wss_connected = True
        assert await rt._await_wss_connected(timeout_seconds=1.0) is True

    @pytest.mark.asyncio
    async def test_await_wss_connected_gives_up_when_not_running(self) -> None:
        rt = _make_runtime()
        rt._status.running = False
        rt._wss_connected = False
        assert await rt._await_wss_connected(timeout_seconds=1.0) is False

    @pytest.mark.asyncio
    async def test_await_wss_connected_waits_then_succeeds(self) -> None:
        import asyncio as _asyncio

        rt = _make_runtime()
        rt._status.running = True
        rt._wss_connected = False

        async def _connect_soon() -> None:
            await _asyncio.sleep(0.06)
            rt._wss_connected = True

        task = _asyncio.create_task(_connect_soon())
        result = await rt._await_wss_connected(timeout_seconds=1.0)
        await task
        assert result is True


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
    async def test_bootstrap_orderbook_not_found_marks_resolved_not_failed(self) -> None:
        store = MarketStateStore()
        gw = AsyncMock()
        gw.get_orderbook = AsyncMock(side_effect=VenueOrderbookNotFoundError("no orderbook"))
        gw.get_market_info = AsyncMock()
        rt = _make_runtime(store=store, gateway=gw)
        await rt.bootstrap_tokens([TOKEN_A])

        assert store.is_resolved(TOKEN_A)
        assert store.get_book(TOKEN_A) is None
        assert rt.status().last_error is None
        gw.get_market_info.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_bootstrap_after_rest_token_immediately_executable(self) -> None:
        """PR10.4: REST bootstrap is the primary execution source.

        A healthy REST bootstrap must leave the token clean and not gated on
        a follow-up WSS event — execution proceeds against the REST book.
        """
        store = MarketStateStore()
        rt = _make_runtime(store=store, gateway=_make_gateway())
        await rt.bootstrap_tokens([TOKEN_A])

        assert not store.is_dirty(TOKEN_A)
        assert not store.is_stale(TOKEN_A)
        assert store.book_source_of(TOKEN_A) == "rest_snapshot"
        assert store.quote_source_of(TOKEN_A) == "rest_snapshot"


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
    async def test_wss_state_change_updates_shared_copy_task_quote_sink(self) -> None:
        """The v1 shared cache receives the same authoritative WSS quote."""

        store = MarketStateStore()
        sink = RecordingQuoteSink()
        runtime = _make_runtime(store=store, quote_sink=sink)
        store.set_book(
            TOKEN_A,
            bids={Decimal("0.49"): Decimal("100")},
            asks={Decimal("0.51"): Decimal("100")},
            source="wss",
        )

        await runtime._on_market_state_change(TOKEN_A, "book")
        await runtime._on_market_state_change(TOKEN_A, "market_resolved")

        assert sink.quotes == [(TOKEN_A, Decimal("0.49"), Decimal("0.51"), "wss")]
        assert sink.discarded == [TOKEN_A]

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
        tick_size=Decimal("0.01"),
        min_order_size=Decimal("5"),
        neg_risk=False,
        bids=[OrderBookLevel(price=Decimal("0.49"), size=Decimal("200"))],
        asks=[OrderBookLevel(price=Decimal("0.50"), size=Decimal("150"))],
        best_bid=Decimal("0.49"),
        best_ask=Decimal("0.50"),
    )


def _make_bootstrap_gateway(token_id: str = TOKEN_A) -> AsyncMock:
    gw = AsyncMock()
    gw.get_orderbook = AsyncMock(return_value=_make_good_book(token_id))
    gw.get_market_info = AsyncMock(
        return_value={
            "tick_size": "0.01",
            "min_order_size": "5",
            "neg_risk": False,
        }
    )
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
        store.set_tick(
            TOKEN_A,
            tick_size=Decimal("0.01"),
            min_order_size=Decimal("5"),
            neg_risk=False,
            source="wss",
        )
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
    async def test_bootstrap_rejects_wide_spread(self) -> None:
        from integrations.polymarket.models import OrderBook, OrderBookLevel

        wide_book = OrderBook(
            token_id=TOKEN_A,
            tick_size=Decimal("0.01"),
            min_order_size=Decimal("5"),
            neg_risk=False,
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

        empty_book = OrderBook(
            token_id=TOKEN_A,
            tick_size=Decimal("0.01"),
            min_order_size=Decimal("5"),
            neg_risk=False,
            bids=[],
            asks=[],
            best_bid=None,
            best_ask=None,
        )
        gw = AsyncMock()
        gw.get_orderbook = AsyncMock(return_value=empty_book)
        gw.get_market_info = AsyncMock(return_value={})

        rt = _make_runtime_with_config(gateway=gw)

        result = await rt.ensure_token_ready_or_bootstrapped(TOKEN_A)

        assert result is False

    @pytest.mark.asyncio
    async def test_bootstrap_rejected_when_orderbook_not_found_marks_resolved(self) -> None:
        store = MarketStateStore()
        gw = AsyncMock()
        gw.get_orderbook = AsyncMock(side_effect=VenueOrderbookNotFoundError("no orderbook"))
        gw.get_market_info = AsyncMock(return_value={})
        rt = _make_runtime_with_config(store=store, gateway=gw)

        result = await rt.ensure_token_ready_or_bootstrapped(
            TOKEN_A,
            condition_id="0xcond",
        )

        assert result is False
        assert store.is_resolved(TOKEN_A)
        assert store.get_state(TOKEN_A).condition_id == "0xcond"
        gw.get_market_info.assert_not_awaited()

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
