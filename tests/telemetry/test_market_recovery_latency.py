"""Reconnect timing ends at complete usable books, not socket connection."""

from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from pmcopy.cache.market_state_store import BookSnapshot, QuoteSnapshot, TickSnapshot
from pmcopy.orchestrators.market_data_runtime import MarketDataRuntime
from pmcopy.services.healthServer import ReadinessTracker
from pmcopy.services.metrics import MetricsRegistry, TELEMETRY_METRIC_SCHEMAS
from pmcopy.services.telemetry.latency import PipelineLatency
from tests.telemetry.test_pipeline_latency import ReceiptClock


@pytest.mark.asyncio
async def test_recovery_waits_for_each_fresh_book_and_never_counts_metadata_as_recovery() -> None:
    clock = ReceiptClock()
    registry = MetricsRegistry(schemas=TELEMETRY_METRIC_SCHEMAS)
    store = MagicMock()
    store.is_resolved.return_value = False
    store.is_stale.return_value = False
    store.is_dirty.return_value = False
    quotes = {
        token: QuoteSnapshot(token, Decimal(".4"), Decimal(".6"), None, "wss", 0)
        for token in ("a", "b")
    }
    books = {
        token: BookSnapshot(token, {}, {}, Decimal(".4"), Decimal(".6"), "wss", 0)
        for token in quotes
    }
    store.get_quote.side_effect = quotes.get
    store.get_book.side_effect = books.get
    store.get_tick.return_value = TickSnapshot("a", Decimal(".005"), Decimal(5), False, "rest", 1)
    runtime = MarketDataRuntime(
        market_store=store,
        gateway=MagicMock(),
        readiness=ReadinessTracker(),
        config=SimpleNamespace(
            market_data=SimpleNamespace(
                executable_quote_max_age_seconds=0.5,
                executable_book_max_age_seconds=0.5,
            )
        ),
        latency=PipelineLatency(clock=clock, metrics=registry),
        monotonic=lambda: clock.nanoseconds / 1_000_000_000,
    )
    runtime._subscribed_tokens.update(quotes)
    runtime._status.running = True
    await runtime._on_wss_disconnected()
    clock.nanoseconds += 100_000_000
    await runtime._on_wss_connected()
    await runtime._on_market_state_change("a", "tick_size_change")
    assert not registry.histograms
    assert not runtime._token_executable("a")
    quotes["a"] = replace(quotes["a"], monotonic_ts=1.1)
    books["a"] = replace(books["a"], monotonic_ts=1.1)
    await runtime._on_market_state_change("a", "book")
    assert not registry.histograms
    clock.nanoseconds += 100_000_000
    quotes["b"] = replace(quotes["b"], monotonic_ts=1.2)
    books["b"] = replace(books["b"], monotonic_ts=1.2)
    await runtime._on_market_state_change("b", "book")
    await runtime._on_market_state_change("b", "book")
    metric = registry.histograms['telemetry_latency_seconds{phase="market_reconnect_recovery"}']
    assert metric.count == 1
    assert metric.total == 0.2
