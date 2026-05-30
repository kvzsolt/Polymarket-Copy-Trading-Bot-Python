from __future__ import annotations

from types import SimpleNamespace

import pytest

from services.discovery.polymarket_history_backfill import PolymarketHistoryBackfill


class _Client:
    def __init__(self):
        self.calls = []

    async def fetch_activity_page(self, **kwargs):
        self.calls.append(kwargs)
        if len(self.calls) == 1:
            return SimpleNamespace(rows=[{"timestamp": "1000"}, {"timestamp": "900"}])
        return SimpleNamespace(rows=[])


class _Ingestion:
    def __init__(self):
        self.rows = []

    async def ingest_activity_batch(self, rows):
        self.rows.extend(rows)
        return len(rows), 0


@pytest.mark.asyncio
async def test_backfill_uses_time_windows_and_ingests_rows(monkeypatch):
    monkeypatch.setattr("services.discovery.polymarket_history_backfill.time.time", lambda: 10_000)
    client = _Client()
    ingestion = _Ingestion()
    config = SimpleNamespace(
        discovery=SimpleNamespace(
            polymarket_history_lookback_days=1,
            polymarket_history_window_days=1,
            polymarket_activity_max_offset=500,
        )
    )

    result = await PolymarketHistoryBackfill(
        client=client,
        ingestion=ingestion,
        config=config,
    ).backfill_wallet("0x" + "a" * 40)

    assert result.inserted == 2
    assert result.fetched == 2
    assert result.errors == 0
    assert result.earliest_ts == 900
    assert client.calls[0]["start_ts"] == 10_000 - 86400
    assert client.calls[0]["end_ts"] == 10_000


class _FailingClient:
    def __init__(self):
        self.calls = []

    async def fetch_activity_page(self, **kwargs):
        self.calls.append(kwargs)
        if len(self.calls) == 1:
            return SimpleNamespace(rows=[{"timestamp": str(i)} for i in range(500)])
        raise RuntimeError("page failed")


class _ZeroClient:
    async def fetch_activity_page(self, **kwargs):
        return SimpleNamespace(rows=[])


@pytest.mark.asyncio
async def test_history_backfill_page_error_returns_partial_result(monkeypatch):
    monkeypatch.setattr(
        "services.discovery.polymarket_history_backfill.time.time",
        lambda: 10_000,
    )
    result = await PolymarketHistoryBackfill(
        client=_FailingClient(),
        ingestion=_Ingestion(),
        config=SimpleNamespace(
            discovery=SimpleNamespace(
                polymarket_history_lookback_days=1,
                polymarket_history_window_days=1,
                polymarket_activity_max_offset=1_000,
            )
        ),
    ).backfill_wallet("0x" + "b" * 40)

    assert result.inserted == 500
    assert result.fetched == 500
    assert result.errors == 1
    assert result.earliest_ts == 0
    assert result.latest_ts == 499


@pytest.mark.asyncio
async def test_history_backfill_zero_rows_does_not_throw(monkeypatch):
    monkeypatch.setattr(
        "services.discovery.polymarket_history_backfill.time.time",
        lambda: 10_000,
    )

    result = await PolymarketHistoryBackfill(
        client=_ZeroClient(),
        ingestion=_Ingestion(),
        config=SimpleNamespace(
            discovery=SimpleNamespace(
                polymarket_history_lookback_days=1,
                polymarket_history_window_days=1,
                polymarket_activity_max_offset=500,
            )
        ),
    ).backfill_wallet("0x" + "c" * 40)

    assert result.inserted == 0
    assert result.skipped == 0
    assert result.fetched == 0
    assert result.errors == 0
    assert result.earliest_ts is None
    assert result.latest_ts is None
