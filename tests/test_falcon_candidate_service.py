from __future__ import annotations

from types import SimpleNamespace

import pytest

from services.discovery.falcon_candidate_service import (
    FALCON_LEADERBOARD_AGENT_ID,
    FalconCandidateService,
)


class _Client:
    def __init__(self):
        self.calls = []

    async def query(self, **kwargs):
        self.calls.append(kwargs)
        if kwargs["offset"] == 0:
            return SimpleNamespace(
                has_more=True,
                results=[
                    {
                        "proxy_wallet": "0x" + "a" * 40,
                        "h_score": "80",
                        "roi_pct_15d": "25",
                        "win_rate_pct_15d": "60",
                        "sharpe_ratio_15d": "2",
                        "total_pnl_15d": "125",
                        "total_trades_15d": "42",
                        "markets_traded_15d": "9",
                        "trajectory": "stable",
                    }
                ],
            )
        return SimpleNamespace(has_more=False, results=[])


def _config(**overrides):
    data = dict(
        falcon_min_win_rate_15d=0.45,
        falcon_max_win_rate_15d=0.90,
        falcon_min_roi_15d=0,
        falcon_min_trades_15d=30,
        falcon_max_trades_15d=750,
        falcon_min_pnl_15d=100,
        falcon_page_size=100,
        falcon_max_pages=5,
        falcon_sort_modes=("h_score",),
        falcon_dedupe_candidates=True,
        falcon_max_candidates=250,
    )
    data.update(overrides)
    return SimpleNamespace(
        discovery=SimpleNamespace(**data)
    )


@pytest.mark.asyncio
async def test_falcon_candidate_service_paginates_and_parses_variants():
    client = _Client()
    service = FalconCandidateService(client, _config())

    candidates = await service.fetch_candidates()

    assert len(candidates) == 1
    assert candidates[0].wallet == "0x" + "a" * 40
    assert candidates[0].h_score == 80
    assert client.calls[0]["agent_id"] == FALCON_LEADERBOARD_AGENT_ID
    assert client.calls[1]["offset"] == 100


@pytest.mark.asyncio
async def test_falcon_candidate_service_calls_multiple_sort_modes():
    class _MultiClient:
        def __init__(self):
            self.calls = []

        async def query(self, **kwargs):
            self.calls.append(kwargs)
            wallet = "0x" + kwargs["params"]["sort_by"][0] * 40
            return SimpleNamespace(
                has_more=False,
                results=[
                    {
                        "wallet": wallet[:42],
                        "h_score": "80",
                        "roi_15d": "20",
                        "win_rate_15d": "55",
                        "sharpe_15d": "1",
                        "total_pnl_15d": "100",
                        "total_trades_15d": "30",
                        "markets_traded_15d": "5",
                    }
                ],
            )

    client = _MultiClient()
    service = FalconCandidateService(
        client,
        _config(falcon_sort_modes=("h_score", "pnl", "sharpe")),
    )

    await service.fetch_candidates()

    assert [call["params"]["sort_by"] for call in client.calls] == [
        "h_score",
        "pnl",
        "sharpe",
    ]


@pytest.mark.asyncio
async def test_falcon_candidate_service_queries_all_modes_before_truncating():
    class _BreadthClient:
        def __init__(self):
            self.calls = []

        async def query(self, **kwargs):
            self.calls.append(kwargs)
            sort_by = kwargs["params"]["sort_by"]
            if sort_by == "h_score":
                rows = [
                    {
                        "wallet": "0x" + "a" * 40,
                        "h_score": "90",
                        "roi_15d": "20",
                        "win_rate_15d": "55",
                        "sharpe_15d": "1",
                        "total_pnl_15d": "100",
                        "total_trades_15d": "30",
                        "markets_traded_15d": "5",
                    },
                    {
                        "wallet": "0x" + "b" * 40,
                        "h_score": "89",
                        "roi_15d": "20",
                        "win_rate_15d": "55",
                        "sharpe_15d": "1",
                        "total_pnl_15d": "100",
                        "total_trades_15d": "30",
                        "markets_traded_15d": "5",
                    },
                ]
            else:
                rows = [
                    {
                        "wallet": "0x" + "c" * 40,
                        "h_score": "70",
                        "roi_15d": "20",
                        "win_rate_15d": "55",
                        "sharpe_15d": "1",
                        "total_pnl_15d": "100",
                        "total_trades_15d": "30",
                        "markets_traded_15d": "5",
                    }
                ]
            return SimpleNamespace(has_more=False, results=rows)

    client = _BreadthClient()
    service = FalconCandidateService(
        client,
        _config(
            falcon_sort_modes=("h_score", "pnl"),
            falcon_max_candidates=2,
        ),
    )

    candidates = await service.fetch_candidates()

    assert [call["params"]["sort_by"] for call in client.calls] == ["h_score", "pnl"]
    assert [candidate.wallet for candidate in candidates] == [
        "0x" + "a" * 40,
        "0x" + "c" * 40,
    ]


@pytest.mark.asyncio
async def test_falcon_candidate_service_dedupe_merges_sort_metadata():
    class _DedupeClient:
        async def query(self, **kwargs):
            return SimpleNamespace(
                has_more=False,
                results=[
                    {
                        "proxyWallet": "0x" + "b" * 40,
                        "h_score": "80",
                        "roi_pct_15d": "25",
                        "win_rate_pct_15d": "60",
                        "sharpe_ratio_15d": "2",
                        "total_pnl_15d": "125",
                        "total_trades_15d": "42",
                        "markets_traded_15d": "9",
                        "leaderboard_rank": "7",
                        "tier": "gold",
                        "total_volume_15d": "12345.5",
                    }
                ],
            )

    service = FalconCandidateService(
        _DedupeClient(),
        _config(falcon_sort_modes=("h_score", "pnl")),
    )

    candidates = await service.fetch_candidates()

    assert len(candidates) == 1
    assert candidates[0].wallet == "0x" + "b" * 40
    assert candidates[0].leaderboard_rank == 7
    assert candidates[0].tier == "gold"
    assert candidates[0].total_volume_15d == pytest.approx(12345.5)
    assert candidates[0].first_seen_sort_by == "h_score"
    assert candidates[0].all_seen_sort_modes == ("h_score", "pnl")


@pytest.mark.asyncio
async def test_falcon_candidate_service_partial_sort_failure_continues():
    class _PartialClient:
        def __init__(self):
            self.calls = []

        async def query(self, **kwargs):
            self.calls.append(kwargs)
            if kwargs["params"]["sort_by"] == "pnl":
                raise RuntimeError("falcon down")
            return SimpleNamespace(
                has_more=False,
                results=[
                    {
                        "address": "0x" + "c" * 40,
                        "h_score": "75",
                        "roi_15d": "10",
                        "win_rate_15d": "55",
                        "sharpe_15d": "1",
                        "pnl_15d": "100",
                        "total_trades_15d": "31",
                        "markets_traded_15d": "4",
                    }
                ],
            )

    client = _PartialClient()
    service = FalconCandidateService(
        client,
        _config(falcon_sort_modes=("pnl", "h_score")),
    )

    candidates = await service.fetch_candidates()

    assert len(candidates) == 1
    assert candidates[0].wallet == "0x" + "c" * 40
    assert [call["params"]["sort_by"] for call in client.calls] == ["pnl", "h_score"]
