"""Invalid critical book data must not publish a partially updated fresh book."""

from decimal import Decimal

import pytest

from pmcopy.cache.market_state_store import MarketStateStore
from pmcopy.clients.ws.market_ws import MarketWebSocket


def seeded_store() -> MarketStateStore:
    store = MarketStateStore()
    store.replace_book_snapshot("token", {Decimal(".4"): Decimal(10)}, {Decimal(".6"): Decimal(10)})
    return store


def test_invalid_ask_snapshot_does_not_replace_valid_bid_half() -> None:
    store = seeded_store()
    with pytest.raises(ValueError):
        store.replace_book_snapshot(
            "token", {Decimal(".5"): Decimal(20)}, {Decimal(".7"): Decimal("NaN")}
        )
    state = store.get_state("token")
    assert state.bids == {Decimal(".4"): Decimal(10)}
    assert state.stale


@pytest.mark.parametrize(
    "price,shares", [("NaN", "1"), (".5", "Infinity"), ("1.1", "1"), (".5", "-1")]
)
def test_invalid_delta_invalidates_without_changing_book(price: str, shares: str) -> None:
    store = seeded_store()
    with pytest.raises(ValueError):
        store.apply_book_delta("token", "BUY", Decimal(price), Decimal(shares))
    state = store.get_state("token")
    assert state.bids == {Decimal(".4"): Decimal(10)}
    assert state.stale


def test_delta_cannot_restore_a_disconnected_book() -> None:
    store = seeded_store()
    store.invalidate("token")
    store.apply_book_delta("token", "BUY", Decimal(".45"), Decimal(1))
    assert store.get_state("token").stale
    store.replace_book_snapshot("token", {Decimal(".4"): Decimal(10)}, {Decimal(".6"): Decimal(10)})
    assert not store.get_state("token").stale


def test_quote_then_delta_cannot_revalidate_a_disconnected_depth_book():
    store = seeded_store()
    store.invalidate("token")
    store.on_price_update("token", Decimal(".45"), Decimal(".6"))
    store.apply_book_delta("token", "BUY", Decimal(".45"), Decimal(1))
    assert store.get_state("token").stale


def test_malformed_critical_batch_does_not_publish_a_valid_prefix():
    store = seeded_store()
    socket = MarketWebSocket(store)
    socket._handle_price_change(
        {
            "price_changes": [
                {"asset_id": "token", "side": "BUY", "price": ".5", "size": "20"},
                {"asset_id": "token", "side": "SELL", "price": ".7", "size": "invalid"},
            ]
        }
    )
    state = store.get_state("token")
    assert state.bids == {Decimal(".4"): Decimal(10)}
    assert state.stale


def test_malformed_snapshot_cannot_silently_drop_invalid_levels():
    store = seeded_store()
    socket = MarketWebSocket(store)
    socket._handle_book(
        {
            "asset_id": "token",
            "bids": [{"price": ".5", "size": "20"}],
            "asks": [{"price": ".6", "size": "10"}, {"price": ".7", "size": "invalid"}],
        }
    )
    assert store.get_state("token").bids == {Decimal(".4"): Decimal(10)}
    assert store.get_state("token").stale


def test_unattributable_invalid_delta_invalidates_all_subscribed_books():
    store = seeded_store()
    socket = MarketWebSocket(store)
    socket._handle_price_change({"price_changes": [{"side": "BUY", "price": ".5", "size": "1"}]})
    assert store.get_state("token").stale


def test_failed_atomic_cache_update_never_falls_back_to_partial_mutation(caplog):
    class FailingStore(MarketStateStore):
        def apply_book_deltas(self, changes):
            raise ValueError("synthetic-private-marker")

    store = FailingStore()
    store.replace_book_snapshot("token", {Decimal(".4"): Decimal(10)}, {Decimal(".6"): Decimal(10)})
    socket = MarketWebSocket(store)
    socket._handle_price_change(
        {
            "price_changes": [
                {"asset_id": "token", "side": "BUY", "price": ".5", "size": "20"},
            ]
        }
    )
    assert store.get_state("token").bids == {Decimal(".4"): Decimal(10)}
    assert store.get_state("token").stale
    assert "synthetic-private-marker" not in caplog.text
