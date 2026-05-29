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
    assert result.earliest_ts == 900
    assert client.calls[0]["start_ts"] == 10_000 - 86400
    assert client.calls[0]["end_ts"] == 10_000

