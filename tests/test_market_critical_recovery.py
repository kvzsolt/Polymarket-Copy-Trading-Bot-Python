"""Critical market failures cannot publish a fresh quote or recover book continuity."""

from decimal import Decimal
import asyncio

import pytest

from pmcopy.cache.market_state_store import MarketStateStore
from pmcopy.clients.ws.market_ws import MarketWebSocket
from tests.test_book_update_atomicity import seeded_store


def test_failed_snapshot_hook_does_not_use_an_alias_or_publish_quote(caplog) -> None:
    class FailingStore(MarketStateStore):
        def replace_book_snapshot(self, *args, **kwargs):
            raise ValueError("SYNTHETIC_SNAPSHOT_SECRET")

        def on_book_snapshot(self, *args, **kwargs):
            pytest.fail("A failed authoritative update must not be retried through an alias")

    store = FailingStore()
    MarketStateStore.replace_book_snapshot(
        store, "token", {Decimal(".4"): Decimal(10)}, {Decimal(".6"): Decimal(10)}
    )
    MarketWebSocket(store)._handle_book(
        {
            "asset_id": "token",
            "bids": [{"price": ".5", "size": "10"}],
            "asks": [{"price": ".7", "size": "10"}],
        }
    )
    state = store.get_state("token")
    assert state is not None and state.stale
    assert state.best_bid == Decimal(".4")
    assert "SYNTHETIC_SNAPSHOT_SECRET" not in caplog.text


@pytest.mark.parametrize("bad", ["NaN", "Infinity", "-0.1", "1.1", True, "broken"])
def test_invalid_top_of_book_is_atomic_and_invalidates(bad: object) -> None:
    store = seeded_store()
    MarketWebSocket(store)._handle_best_bid_ask(
        {"asset_id": "token", "best_bid": ".45", "best_ask": bad}
    )
    state = store.get_state("token")
    assert state is not None and state.stale
    assert state.best_bid == Decimal(".4")
    assert state.best_ask == Decimal(".6")


@pytest.mark.parametrize("operation", ["tick_event", "metadata_refresh"])
def test_metadata_cannot_restore_a_disconnected_depth_book(operation: str) -> None:
    store = seeded_store()
    store.mark_stale("token", "disconnected")
    if operation == "tick_event":
        store.on_tick_size_change("token", Decimal(".005"))
    else:
        store.set_tick(
            "token", tick_size=Decimal(".005"), min_order_size=Decimal(5), neg_risk=False
        )
    store.apply_book_delta("token", "BUY", Decimal(".45"), Decimal(1))
    state = store.get_state("token")
    assert state is not None and state.stale


@pytest.mark.parametrize("reason", ["stale", "dirty", "resolved"])
def test_invalidated_quote_is_not_fresh_for_marks_or_shared_projection(reason: str) -> None:
    store = seeded_store()
    if reason == "stale":
        store.mark_stale("token", "disconnected")
    elif reason == "dirty":
        store.mark_dirty("token", "invalid_frame")
    else:
        store.mark_resolved("token")
    quote = store.get_quote("token")
    assert quote is None or not quote.is_fresh(100)


def test_missing_token_in_critical_snapshot_invalidates_cached_state() -> None:
    store = seeded_store()
    MarketWebSocket(store)._handle_book({"bids": [], "asks": []})
    state = store.get_state("token")
    assert state is not None and state.stale


@pytest.mark.asyncio
@pytest.mark.parametrize("event", ["disconnected", "book_invalidated"])
async def test_shared_task_quote_is_discarded_after_continuity_failure(event: str) -> None:
    from tests.test_market_data_runtime import RecordingQuoteSink, _make_runtime

    store = seeded_store()
    sink = RecordingQuoteSink()
    runtime = _make_runtime(store=store, quote_sink=sink)
    runtime._subscribed_tokens.add("token")
    await runtime._on_market_state_change("token", "book")
    assert len(sink.quotes) == 1
    if event == "disconnected":
        await runtime._on_wss_disconnected()
    else:
        store.mark_stale("token", "invalid_frame")
        await runtime._on_market_state_change("token", event)
    assert sink.discarded == ["token"]
    assert len(sink.quotes) == 1


@pytest.mark.asyncio
async def test_malformed_socket_book_notifies_real_shared_quote_projection() -> None:
    from tests.test_market_data_runtime import RecordingQuoteSink, _make_runtime

    store = seeded_store()
    sink = RecordingQuoteSink()
    runtime = _make_runtime(store=store, quote_sink=sink)
    await runtime._on_market_state_change("token", "book")
    socket = MarketWebSocket(store, on_state_change=runtime._on_market_state_change)
    socket._handle_book({"asset_id": "token", "bids": [{"price": "bad", "size": "1"}]})
    await asyncio.gather(*tuple(socket._tasks))
    assert sink.discarded == ["token"]


@pytest.mark.asyncio
@pytest.mark.parametrize("event_type", ["tick_size_change", "last_trade_price", "new_market"])
async def test_non_quote_message_cannot_renew_shared_executable_quote(event_type: str) -> None:
    from tests.test_market_data_runtime import RecordingQuoteSink, _make_runtime

    sink = RecordingQuoteSink()
    runtime = _make_runtime(store=seeded_store(), quote_sink=sink)
    await runtime._on_market_state_change("token", "book")
    await runtime._on_market_state_change("token", event_type)
    assert len(sink.quotes) == 1
