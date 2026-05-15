"""PR10.4 — MarketStateStore REST-snapshot invariants.

The store is now shared between REST snapshots and WSS deltas. Component-level
sources (``quote_source``/``book_source``/``tick_source``/``metadata_source``)
are authoritative for execution gating, so the new setters must:

  * stamp the correct ``source`` on the components they touch,
  * not promote unrelated components,
  * honour explicit ``fetched_at_monotonic`` timestamps,
  * log a one-shot override line when a WSS event replaces REST snapshot state.

``mark_fresh`` is intentionally restricted: it no longer rewrites component
sources, so a quiet WSS touch cannot disguise a REST-only book as WSS-fresh.

``get_debug_state`` normalises field names — both branches return
``quote_age_ms`` / ``book_age_ms``.
"""

from __future__ import annotations

import logging
import time
from decimal import Decimal

import pytest

from cache.market_state_store import MarketStateStore


TOKEN = "tok-pr10-4"


def _bids() -> dict[Decimal, Decimal]:
    return {Decimal("0.49"): Decimal("100"), Decimal("0.48"): Decimal("50")}


def _asks() -> dict[Decimal, Decimal]:
    return {Decimal("0.51"): Decimal("100"), Decimal("0.52"): Decimal("50")}


# ---------------------------------------------------------------------------
# set_book / set_quote / set_tick
# ---------------------------------------------------------------------------


def test_set_book_writes_rest_snapshot_source_and_derives_quote() -> None:
    store = MarketStateStore()
    store.set_book(
        TOKEN,
        bids=_bids(),
        asks=_asks(),
        source="rest_snapshot",
    )

    assert store.book_source_of(TOKEN) == "rest_snapshot"
    assert store.quote_source_of(TOKEN) == "rest_snapshot"

    quote = store.get_quote(TOKEN)
    book = store.get_book(TOKEN)
    assert quote is not None and book is not None
    assert quote.source == "rest_snapshot"
    assert book.source == "rest_snapshot"
    assert quote.best_bid == Decimal("0.49")
    assert quote.best_ask == Decimal("0.51")


def test_set_book_honours_fetched_at_monotonic() -> None:
    store = MarketStateStore()
    pinned = time.monotonic() - 0.25
    store.set_book(
        TOKEN,
        bids=_bids(),
        asks=_asks(),
        source="rest_snapshot",
        fetched_at_monotonic=pinned,
    )

    state = store.get_state(TOKEN)
    assert state is not None
    assert state.last_book_monotonic == pytest.approx(pinned)
    assert state.last_quote_monotonic == pytest.approx(pinned)


def test_set_quote_only_touches_quote_source() -> None:
    store = MarketStateStore()
    store.set_book(TOKEN, bids=_bids(), asks=_asks(), source="rest_snapshot")
    # Simulate a later top-of-book-only update from a different writer.
    store.set_quote(
        TOKEN,
        best_bid=Decimal("0.495"),
        best_ask=Decimal("0.505"),
        source="rest_snapshot_quote_refresh",
    )

    assert store.book_source_of(TOKEN) == "rest_snapshot"
    assert store.quote_source_of(TOKEN) == "rest_snapshot_quote_refresh"


def test_set_tick_only_touches_tick_and_metadata_sources() -> None:
    store = MarketStateStore()
    store.set_book(TOKEN, bids=_bids(), asks=_asks(), source="rest_snapshot")
    store.set_tick(
        TOKEN,
        tick_size=Decimal("0.01"),
        min_order_size=Decimal("5"),
        source="rest_snapshot",
    )

    assert store.tick_source_of(TOKEN) == "rest_snapshot"
    assert store.metadata_source_of(TOKEN) == "rest_snapshot"
    # Quote/book components are unchanged.
    assert store.quote_source_of(TOKEN) == "rest_snapshot"
    assert store.book_source_of(TOKEN) == "rest_snapshot"


# ---------------------------------------------------------------------------
# store_rest_snapshot helper
# ---------------------------------------------------------------------------


def test_store_rest_snapshot_writes_full_executable_state() -> None:
    store = MarketStateStore()
    fetched = time.monotonic()
    debug = store.store_rest_snapshot(
        TOKEN,
        condition_id="cond-1",
        bids=_bids(),
        asks=_asks(),
        tick_size=Decimal("0.01"),
        min_order_size=Decimal("5"),
        neg_risk=False,
        fetched_at_monotonic=fetched,
    )

    assert debug["has_book"] is True
    assert debug["has_quote"] is True
    assert debug["bid_count"] > 0
    assert debug["ask_count"] > 0
    assert debug["book_source"] == "rest_snapshot"
    assert debug["quote_source"] == "rest_snapshot"
    assert debug["tick_source"] == "rest_snapshot"
    assert debug["metadata_source"] == "rest_snapshot"
    # No follow-up WSS event needed — clean and not stale.
    assert debug["dirty"] is False
    assert debug["stale"] is False


def test_store_rest_snapshot_skips_partial_tick() -> None:
    store = MarketStateStore()
    store.store_rest_snapshot(
        TOKEN,
        condition_id=None,
        bids=_bids(),
        asks=_asks(),
        tick_size=None,
        min_order_size=None,
        neg_risk=False,
        fetched_at_monotonic=time.monotonic(),
    )

    # Tick wasn't written because both fields were missing.
    assert store.get_tick(TOKEN) is None
    # Book/quote still went through.
    assert store.get_book(TOKEN) is not None
    assert store.get_quote(TOKEN) is not None


# ---------------------------------------------------------------------------
# WSS override of an existing REST snapshot
# ---------------------------------------------------------------------------


def test_wss_book_event_overrides_rest_snapshot_and_logs(caplog) -> None:
    store = MarketStateStore()
    store.set_book(TOKEN, bids=_bids(), asks=_asks(), source="rest_snapshot")

    caplog.set_level(logging.INFO, logger="MarketStateStore")
    # WSS book event: replace_book_snapshot with the WSS source default.
    store.replace_book_snapshot(
        TOKEN,
        bids=_bids(),
        asks=_asks(),
        raw_event={"source": "wss"},
    )

    assert store.book_source_of(TOKEN) == "wss"
    assert store.quote_source_of(TOKEN) == "wss"
    assert any(
        "market_data_rest_snapshot_wss_override" in rec.getMessage()
        for rec in caplog.records
    )


def test_wss_book_delta_after_rest_snapshot_promotes_and_logs(caplog) -> None:
    store = MarketStateStore()
    store.set_book(TOKEN, bids=_bids(), asks=_asks(), source="rest_snapshot")

    caplog.set_level(logging.INFO, logger="MarketStateStore")
    store.apply_book_delta(TOKEN, "BUY", Decimal("0.495"), Decimal("75"))

    assert store.book_source_of(TOKEN) == "wss"
    assert store.quote_source_of(TOKEN) == "wss"
    assert any(
        "market_data_rest_snapshot_wss_override" in rec.getMessage()
        for rec in caplog.records
    )


# ---------------------------------------------------------------------------
# mark_fresh no longer promotes component sources
# ---------------------------------------------------------------------------


def test_mark_fresh_does_not_promote_component_sources() -> None:
    store = MarketStateStore()
    store.store_rest_snapshot(
        TOKEN,
        condition_id=None,
        bids=_bids(),
        asks=_asks(),
        tick_size=Decimal("0.01"),
        min_order_size=Decimal("5"),
        neg_risk=False,
        fetched_at_monotonic=time.monotonic(),
    )

    store.mark_fresh(TOKEN, source="wss")

    # Aggregate source label moves; component sources do NOT.
    assert store.source_of(TOKEN) == "wss"
    assert store.quote_source_of(TOKEN) == "rest_snapshot"
    assert store.book_source_of(TOKEN) == "rest_snapshot"
    assert store.tick_source_of(TOKEN) == "rest_snapshot"
    assert store.metadata_source_of(TOKEN) == "rest_snapshot"


# ---------------------------------------------------------------------------
# get_debug_state field-name normalisation
# ---------------------------------------------------------------------------


def test_get_debug_state_unknown_token_has_consistent_zero_state() -> None:
    store = MarketStateStore()
    debug = store.get_debug_state("never-seen")

    assert debug["has_book"] is False
    assert debug["has_quote"] is False
    assert debug["bid_count"] == 0
    assert debug["ask_count"] == 0
    assert debug["quote_age_ms"] is None
    assert debug["book_age_ms"] is None
    # Source fields are present but None for unknown tokens.
    assert debug["quote_source"] is None
    assert debug["book_source"] is None


def test_get_debug_state_known_token_reports_ms_ages() -> None:
    store = MarketStateStore()
    store.set_book(TOKEN, bids=_bids(), asks=_asks(), source="rest_snapshot")
    debug = store.get_debug_state(TOKEN)

    assert "quote_age_ms" in debug
    assert "book_age_ms" in debug
    assert debug["quote_age_ms"] is not None
    assert debug["book_age_ms"] is not None
    # Legacy field names are no longer present in either branch.
    assert "quote_age" not in debug
    assert "book_age" not in debug
