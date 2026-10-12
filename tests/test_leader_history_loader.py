"""Tests for ``services/strategy_profile/leader_history_loader.py`` (PR3)."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest

from pmcopy.services.strategy_profile.leader_history_loader import LeaderHistoryLoader, LoadedHistory


def _mk_activity_rows(n: int) -> list[dict]:
    return [{"id": i, "side": "BUY", "timestamp": 1700000000000 + i} for i in range(n)]


@pytest.mark.asyncio
async def test_loader_fetches_all_pages(monkeypatch: pytest.MonkeyPatch) -> None:
    loader = LeaderHistoryLoader(page_limit=2, max_offset=10, lookback_days=30)

    # Patch the activity client's fetch_activity_page to return 2 rows then 1.
    activity_pages = [_mk_activity_rows(2), _mk_activity_rows(1)]
    fetch_activity = AsyncMock(side_effect=[_ActivityPage(rows=page) for page in activity_pages])
    monkeypatch.setattr(loader._activity_client, "fetch_activity_page", fetch_activity)

    # Patch _fetch_json_list for /trades and /closed-positions.
    async def fake_json_list(path: str, params: dict) -> list[dict]:
        # Yield briefly so SonarCloud S7503 sees the function uses an
        # async feature (the body is otherwise sync-only dispatch).
        await asyncio.sleep(0)
        if path == "/trades":
            return [{"id": 1}]
        if path == "/closed-positions":
            return [{"id": 2, "pnl": "1.0"}]
        return []

    monkeypatch.setattr(loader, "_fetch_json_list", fake_json_list)

    history = await loader.load("0x" + "ab" * 20)
    assert isinstance(history, LoadedHistory)
    assert history.wallet == "0x" + "ab" * 20
    # Activity: 2 + 1 = 3 rows (second page < page_limit stops the walk).
    assert len(history.activity) == 3
    assert len(history.trades) == 1
    assert len(history.closed_positions) == 1


@pytest.mark.asyncio
async def test_loader_empty_wallet_returns_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    loader = LeaderHistoryLoader()
    history = await loader.load("")
    assert history.wallet == ""
    assert history.activity == []


@pytest.mark.asyncio
async def test_loader_stops_at_max_offset(monkeypatch: pytest.MonkeyPatch) -> None:
    loader = LeaderHistoryLoader(page_limit=2, max_offset=2, lookback_days=30)
    # Always return a full page so the walk would be unbounded without max_offset.
    fetch_activity = AsyncMock(return_value=_ActivityPage(rows=_mk_activity_rows(2)))
    monkeypatch.setattr(loader._activity_client, "fetch_activity_page", fetch_activity)
    monkeypatch.setattr(loader, "_fetch_json_list", AsyncMock(return_value=[]))

    history = await loader.load("0xabc")
    # max_offset=2, page_limit=2 -> offsets 0 and 2 -> 2 pages -> 4 rows.
    assert len(history.activity) == 4


@pytest.mark.asyncio
async def test_fetch_json_list_swallows_aiohttp_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """The real ``_fetch_json_list`` catches aiohttp.ClientError and returns []."""
    import aiohttp

    loader = LeaderHistoryLoader(page_limit=2, max_offset=2, lookback_days=30)

    class _BoomSession:
        async def __aenter__(self) -> "_BoomSession":
            return self

        async def __aexit__(self, *exc) -> None:
            return None

        def get(self, *args, **kwargs):
            raise aiohttp.ClientError("boom")

    monkeypatch.setattr(aiohttp, "ClientSession", lambda *a, **kw: _BoomSession())
    result = await loader._fetch_json_list("/trades", {"user": "0xabc"})
    assert result == []


@pytest.mark.asyncio
async def test_loader_trades_endpoint_empty_response_returns_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When /trades and /closed-positions return empty payloads, the loader
    returns empty lists for those sections (no crash)."""
    loader = LeaderHistoryLoader(page_limit=2, max_offset=2, lookback_days=30)
    monkeypatch.setattr(
        loader._activity_client,
        "fetch_activity_page",
        AsyncMock(return_value=_ActivityPage(rows=[])),
    )
    monkeypatch.setattr(loader, "_fetch_json_list", AsyncMock(return_value=[]))
    history = await loader.load("0xabc")
    assert history.trades == []
    assert history.closed_positions == []
    assert history.activity == []


class _ActivityPage:
    def __init__(self, rows: list[dict]) -> None:
        self.rows = rows
