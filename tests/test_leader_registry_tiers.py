"""Tests for permission-ladder integration in LeaderRegistry."""
from __future__ import annotations

import time
from types import SimpleNamespace

import pytest

from orchestrators.leader_registry import LeaderRegistry
from storage.sqlite.leaders_repo import LeaderTierRecord


WALLET_A = "0x" + "a" * 40
WALLET_B = "0x" + "b" * 40


def _mock_config(**overrides) -> SimpleNamespace:
    """Minimal AppConfig-shaped object for LeaderRegistry tests."""
    discovery = dict(
        enable_trader_discovery=True,
        min_leader_positions=5,
        min_active_positions=1,
        discovery_promote_to_active=True,
    )
    leader = dict(
        auto_discover_min_score=0.40,
        auto_discover_top_n=10,
        tier_promote_to_paper=0.40,
        tier_promote_to_live=0.55,
        tier_demote_to_paper=0.35,
        tier_demote_to_watchlist=0.25,
        tier_min_paper_seconds=86400,
    )
    for key, value in overrides.items():
        if key in discovery:
            discovery[key] = value
        elif key in leader:
            leader[key] = value
        else:
            raise KeyError(f"Unknown override for test config: {key}")
    return SimpleNamespace(
        discovery=SimpleNamespace(**discovery),
        leader=SimpleNamespace(**leader),
    )


class _Profile:
    def __init__(self, wallet: str, *, score: float, total_positions: int, active_positions: int):
        self.wallet = wallet
        self.composite_score = score
        self.total_positions = total_positions
        self.active_positions = active_positions


class _Discovery:
    def __init__(self, profiles):
        self._profiles = profiles

    async def discover_candidates(self):
        return list(self._profiles)


class _MockLeadersRepo:
    def __init__(self):
        self.records: list = []
        self._tiers: dict[str, tuple[str, float | None]] = {}

    def upsert_candidates(self, records):
        self.records.extend(records)

    def get_tier(self, wallet):
        tier, changed_at = self._tiers.get(wallet.lower(), ("watchlist", None))
        return LeaderTierRecord(wallet=wallet.lower(), follow_tier=tier, tier_changed_at=changed_at)

    def update_tier(self, wallet, follow_tier, tier_changed_at):
        self._tiers[wallet.lower()] = (follow_tier, tier_changed_at)


@pytest.mark.asyncio
async def test_new_leader_promotes_from_watchlist_if_score_qualifies():
    repo = _MockLeadersRepo()
    registry = LeaderRegistry(
        discovery_service=_Discovery([
            _Profile(WALLET_A, score=0.80, total_positions=50, active_positions=3),
        ]),
        leaders_repo=repo,
        config=_mock_config(),
    )
    state = await registry.refresh([])
    assert len(state.discovered) == 1
    assert state.discovered[0].follow_tier == "paper_follow"


@pytest.mark.asyncio
async def test_leader_promotes_to_paper_after_threshold():
    repo = _MockLeadersRepo()
    repo._tiers[WALLET_A] = ("watchlist", time.time() - 100_000)

    registry = LeaderRegistry(
        discovery_service=_Discovery([
            _Profile(WALLET_A, score=0.50, total_positions=50, active_positions=3),
        ]),
        leaders_repo=repo,
        config=_mock_config(),
    )
    state = await registry.refresh([])
    assert state.discovered[0].follow_tier == "paper_follow"


@pytest.mark.asyncio
async def test_leader_promotes_to_live_after_time_gate():
    repo = _MockLeadersRepo()
    repo._tiers[WALLET_A] = ("paper_follow", time.time() - 100_000)

    registry = LeaderRegistry(
        discovery_service=_Discovery([
            _Profile(WALLET_A, score=0.60, total_positions=50, active_positions=3),
        ]),
        leaders_repo=repo,
        config=_mock_config(tier_promote_to_live=0.55, tier_min_paper_seconds=86400),
    )
    state = await registry.refresh([])
    assert state.discovered[0].follow_tier == "live_follow"


@pytest.mark.asyncio
async def test_leader_does_not_promote_to_live_without_time_gate():
    repo = _MockLeadersRepo()
    repo._tiers[WALLET_A] = ("paper_follow", time.time() - 100)

    registry = LeaderRegistry(
        discovery_service=_Discovery([
            _Profile(WALLET_A, score=0.60, total_positions=50, active_positions=3),
        ]),
        leaders_repo=repo,
        config=_mock_config(),
    )
    state = await registry.refresh([])
    assert state.discovered[0].follow_tier == "paper_follow"


@pytest.mark.asyncio
async def test_leader_demotes_to_watchlist_when_below_min_score():
    repo = _MockLeadersRepo()
    repo._tiers[WALLET_A] = ("live_follow", time.time() - 200_000)

    registry = LeaderRegistry(
        discovery_service=_Discovery([
            _Profile(WALLET_A, score=0.30, total_positions=50, active_positions=3),
        ]),
        leaders_repo=repo,
        config=_mock_config(),
    )
    state = await registry.refresh([])
    assert state.discovered[0].follow_tier == "watchlist"


@pytest.mark.asyncio
async def test_inactive_leader_gets_watchlist():
    repo = _MockLeadersRepo()
    repo._tiers[WALLET_A] = ("paper_follow", time.time() - 200_000)

    registry = LeaderRegistry(
        discovery_service=_Discovery([
            _Profile(WALLET_A, score=0.80, total_positions=50, active_positions=0),
        ]),
        leaders_repo=repo,
        config=_mock_config(),
    )
    state = await registry.refresh([])
    assert state.discovered[0].follow_tier == "watchlist"


@pytest.mark.asyncio
async def test_multiple_leaders_independent_tiers():
    repo = _MockLeadersRepo()
    repo._tiers[WALLET_A] = ("paper_follow", time.time() - 200_000)
    repo._tiers[WALLET_B] = ("watchlist", time.time() - 200_000)

    registry = LeaderRegistry(
        discovery_service=_Discovery([
            _Profile(WALLET_A, score=0.60, total_positions=50, active_positions=3),
            _Profile(WALLET_B, score=0.50, total_positions=50, active_positions=3),
        ]),
        leaders_repo=repo,
        config=_mock_config(),
    )
    state = await registry.refresh([])
    tiers = {c.leader_id: c.follow_tier for c in state.discovered}
    assert tiers[WALLET_A] == "live_follow"
    assert tiers[WALLET_B] == "paper_follow"


@pytest.mark.asyncio
async def test_tier_state_persisted():
    repo = _MockLeadersRepo()
    repo._tiers[WALLET_A] = ("watchlist", time.time() - 100_000)

    registry = LeaderRegistry(
        discovery_service=_Discovery([
            _Profile(WALLET_A, score=0.50, total_positions=50, active_positions=3),
        ]),
        leaders_repo=repo,
        config=_mock_config(),
    )
    await registry.refresh([])
    tier, changed_at = repo._tiers[WALLET_A]
    assert tier == "paper_follow"
    assert changed_at is not None
