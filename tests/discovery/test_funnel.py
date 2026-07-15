from __future__ import annotations

import time
from typing import Any

import pytest

from config.app_config import DiscoveryConfig
from integrations.falcon.client import Agent, FalconPage
from services.discovery.funnel import DiscoveryFunnel
from services.discovery.models import Candidate
from services.discovery.scoring import CompositeScorer

GOOD = "0x" + "1" * 40
BOT = "0x" + "2" * 40
DRAWDOWN = "0x" + "3" * 40
STALE = "0x" + "4" * 40


class FakeFalcon:
    async def query(
        self,
        *,
        agent_id: int,
        params: dict[str, str],
        limit: int = 100,
        offset: int = 0,
    ) -> FalconPage:
        assert all(isinstance(value, str) for value in params.values())
        if agent_id == Agent.WALLET360:
            wallet = params["proxy_wallet"]
            row = {
                "win_rate": 0.60,
                "max_drawdown_pct": 10,
                "market_diversity": 5,
                "bet_size_cv": 1.0,
                "bot_flag": wallet == BOT,
            }
            if wallet == DRAWDOWN:
                row["max_drawdown_pct"] = 90
            return FalconPage((row,), 1)
        if agent_id == Agent.MARKET_INSIGHTS:
            return FalconPage(
                (
                    {
                        "volume_24h": 100_000,
                        "liquidity_percentile": 90,
                        "top1_wallet_pct": 10,
                        "unique_traders_7d": 100,
                    },
                ),
                1,
            )
        raise AssertionError(f"unexpected query agent {agent_id}")

    async def paginate(
        self,
        *,
        agent_id: int,
        params: dict[str, str],
        page_size: int,
        max_pages: int,
    ):
        assert all(isinstance(value, str) for value in params.values())
        if agent_id == Agent.HSCORE:
            for index, wallet in enumerate((GOOD, BOT, DRAWDOWN, STALE), 1):
                yield {
                    "proxy_wallet": wallet,
                    "h_score": 80,
                    "roi_15d": 30,
                    "win_rate_15d": 0.60,
                    "sharpe_15d": 2,
                    "total_volume_15d": 25_000,
                    "total_trades_15d": 100,
                    "leaderboard_rank": index,
                }
            return
        if agent_id == Agent.TRADES and params["side"] == "BUY":
            wallet = params["wallet_proxy"]
            timestamp = time.time() - (72 * 3600 if wallet == STALE else 60)
            for index in range(3):
                yield {
                    "trade_id": f"{wallet}-{index}",
                    "timestamp": timestamp - index,
                    "condition_id": f"condition-{index}",
                    "side": "BUY",
                }


class EmptyWallet360Falcon(FakeFalcon):
    async def query(
        self,
        *,
        agent_id: int,
        params: dict[str, str],
        limit: int = 100,
        offset: int = 0,
    ) -> FalconPage:
        if agent_id == Agent.WALLET360:
            return FalconPage((), 0)
        return await super().query(
            agent_id=agent_id,
            params=params,
            limit=limit,
            offset=offset,
        )


class MemoryRepo:
    def __init__(self) -> None:
        self.persisted: list[Candidate] = []
        self.completed: dict[str, Any] = {}

    async def begin_cycle(self, config_hash: str) -> str:
        return "cycle"

    async def persist_cycle(self, cycle_id, candidates, config_hash) -> None:
        self.persisted = candidates

    async def complete_cycle(self, cycle_id, **values) -> None:
        self.completed = values

    async def score_history(self, wallet: str, *, since_ts: float):
        return []

    async def latest_candidates(self):
        return []

    async def persist_rescore(self, candidates, config_hash):
        return None


@pytest.mark.asyncio
async def test_funnel_keeps_good_wallet_and_audits_every_rejection() -> None:
    config = DiscoveryConfig.model_validate(
        {
            "shortlist": {"sort_modes": ["h_score"]},
            "longitudinal": {"enabled": True},
        }
    )
    repository = MemoryRepo()
    funnel = DiscoveryFunnel(
        client=FakeFalcon(),  # type: ignore[arg-type]
        config=config,
        scorer=CompositeScorer(),
        repository=repository,
    )
    kept = await funnel.run()
    assert [candidate.wallet for candidate in kept] == [GOOD]
    reasons = {
        candidate.wallet: candidate.rejected_reason
        for candidate in repository.persisted
    }
    assert reasons == {
        GOOD: None,
        BOT: "bot_flagged",
        DRAWDOWN: "drawdown",
        STALE: "stale_activity",
    }
    assert kept[0].base_score > 0
    assert kept[0].longitudinal_score is not None
    assert repository.completed["candidate_count"] == 4
    assert repository.completed["kept_count"] == 1


@pytest.mark.asyncio
async def test_upstream_evaluation_outage_fails_cycle_instead_of_mass_demoting() -> None:
    config = DiscoveryConfig.model_validate(
        {
            "shortlist": {"sort_modes": ["h_score"]},
            "scheduler": {"max_evaluation_error_ratio": 0.25},
        }
    )
    repository = MemoryRepo()
    funnel = DiscoveryFunnel(
        client=EmptyWallet360Falcon(),  # type: ignore[arg-type]
        config=config,
        scorer=CompositeScorer(),
        repository=repository,
    )

    with pytest.raises(RuntimeError, match="error ratio"):
        await funnel.run()

    assert repository.completed["status"] == "failed"
    assert len(repository.persisted) == 4
    assert all(
        candidate.rejected_reason == "evaluation_error:ValueError"
        for candidate in repository.persisted
    )
