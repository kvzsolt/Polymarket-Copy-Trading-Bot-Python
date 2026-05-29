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


def _config():
    return SimpleNamespace(
        discovery=SimpleNamespace(
            falcon_min_win_rate_15d=0.45,
            falcon_max_win_rate_15d=0.90,
            falcon_min_roi_15d=0,
            falcon_min_trades_15d=30,
            falcon_max_trades_15d=750,
            falcon_min_pnl_15d=100,
            falcon_page_size=100,
            falcon_max_pages=5,
        )
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

