"""LeaderRegistry no longer promotes discovery tiers into runtime monitors."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from orchestrators.leader_registry import LeaderRegistry


WALLET_A = "0x" + "a" * 40
WALLET_B = "0x" + "b" * 40


def _mock_config() -> SimpleNamespace:
    return SimpleNamespace(
        discovery=SimpleNamespace(
            enable_trader_discovery=True,
            min_leader_positions=5,
            min_active_positions=1,
            discovery_promote_to_active=True,
        ),
        leader=SimpleNamespace(
            auto_discover_min_score=0.40,
            auto_discover_top_n=10,
            tier_promote_to_paper=0.40,
            tier_promote_to_live=0.55,
            tier_demote_to_paper=0.35,
            tier_demote_to_watchlist=0.25,
            tier_min_paper_seconds=86400,
        ),
    )


class _Profile:
    def __init__(self, wallet: str, *, score: float):
        self.wallet = wallet
        self.composite_score = score
        self.total_positions = 50
        self.active_positions = 3


class _Discovery:
    async def discover_candidates(self):
        return [_Profile(WALLET_A, score=0.80), _Profile(WALLET_B, score=0.60)]


class _MockLeadersRepo:
    def __init__(self):
        self.records: list = []

    async def upsert_candidates(self, records):
        self.records.extend(records)


@pytest.mark.asyncio
async def test_discovery_candidates_stay_shadow_and_inactive():
    repo = _MockLeadersRepo()
    registry = LeaderRegistry(
        discovery_service=_Discovery(),
        leaders_repo=repo,
        config=_mock_config(),
    )

    state = await registry.refresh([])

    assert state.active_monitored == []
    assert state.mode == "no_configured_leaders"
    assert [c.follow_tier for c in state.discovered] == ["shadow", "shadow"]
    assert [c.active for c in state.discovered] == [False, False]
    assert [r.active for r in repo.records] == [False, False]

