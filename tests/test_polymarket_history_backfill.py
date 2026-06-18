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


class _FullUntilCapClient:
    def __init__(self, *, full_offsets: set[int] | None = None):
        self.calls = []
        self.full_offsets = full_offsets or set()

    async def fetch_activity_page(self, **kwargs):
        self.calls.append(kwargs)
        offset = int(kwargs["offset"])
        if offset in self.full_offsets:
            return SimpleNamespace(rows=[{"timestamp": str(offset + i)} for i in range(500)])
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


@pytest.mark.asyncio
async def test_history_backfill_never_requests_offset_above_3000(monkeypatch):
    monkeypatch.setattr(
        "services.discovery.polymarket_history_backfill.time.time",
        lambda: 100_000,
    )
    client = _FullUntilCapClient(full_offsets={0, 500, 1000, 1500, 2000, 2500, 3000})

    await PolymarketHistoryBackfill(
        client=client,
        ingestion=_Ingestion(),
        config=SimpleNamespace(
            discovery=SimpleNamespace(
                polymarket_history_lookback_days=1,
                polymarket_history_window_days=1,
                polymarket_history_min_window_seconds=200_000,
                polymarket_activity_max_offset=10_000,
            )
        ),
    ).backfill_wallet("0x" + "d" * 40)

    assert max(call["offset"] for call in client.calls) == 3000


@pytest.mark.asyncio
async def test_history_backfill_splits_saturated_windows(monkeypatch):
    monkeypatch.setattr(
        "services.discovery.polymarket_history_backfill.time.time",
        lambda: 100_000,
    )

    class _SplitClient:
        def __init__(self):
            self.calls = []

        async def fetch_activity_page(self, **kwargs):
            self.calls.append(kwargs)
            if kwargs["start_ts"] == 100_000 - 86400 and kwargs["end_ts"] == 100_000:
                return SimpleNamespace(rows=[{"timestamp": "1"} for _ in range(500)])
            if kwargs["offset"] == 0:
                return SimpleNamespace(rows=[{"timestamp": str(kwargs["start_ts"])}])
            return SimpleNamespace(rows=[])

    client = _SplitClient()
    result = await PolymarketHistoryBackfill(
        client=client,
        ingestion=_Ingestion(),
        config=SimpleNamespace(
            discovery=SimpleNamespace(
                polymarket_history_lookback_days=1,
                polymarket_history_window_days=1,
                polymarket_history_min_window_seconds=1,
                polymarket_activity_max_offset=0,
            )
        ),
    ).backfill_wallet("0x" + "e" * 40)

    assert result.fetched == 2
    assert any(call["end_ts"] < 100_000 for call in client.calls)
    assert any(call["start_ts"] > 100_000 - 86400 for call in client.calls)


@pytest.mark.asyncio
async def test_history_backfill_min_dense_window_keeps_partial(monkeypatch):
    monkeypatch.setattr(
        "services.discovery.polymarket_history_backfill.time.time",
        lambda: 100_000,
    )
    client = _FullUntilCapClient(full_offsets={0})

    result = await PolymarketHistoryBackfill(
        client=client,
        ingestion=_Ingestion(),
        config=SimpleNamespace(
            discovery=SimpleNamespace(
                polymarket_history_lookback_days=1,
                polymarket_history_window_days=1,
                polymarket_history_min_window_seconds=200_000,
                polymarket_activity_max_offset=0,
            )
        ),
    ).backfill_wallet("0x" + "f" * 40)

    assert result.fetched == 500
    assert result.inserted == 500
    assert max(call["offset"] for call in client.calls) == 0
