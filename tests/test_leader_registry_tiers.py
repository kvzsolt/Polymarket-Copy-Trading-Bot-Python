"""Tests for permission-ladder integration in LeaderRegistry."""
from __future__ import annotations

import time
from unittest.mock import patch

import pytest

from orchestrators.leader_registry import LeaderCandidate, LeaderRegistry
from storage.sqlite.leaders_repo import LeaderTierRecord


WALLET_A = "0x" + "a" * 40
WALLET_B = "0x" + "b" * 40


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
    """A freshly discovered leader with score >= promote_to_paper gets paper_follow."""
    repo = _MockLeadersRepo()
    registry = LeaderRegistry(
        discovery_service=_Discovery([
            _Profile(WALLET_A, score=0.80, total_positions=50, active_positions=3),
        ]),
        leaders_repo=repo,
    )
    state = await registry.refresh([])
    assert len(state.discovered) == 1
    assert state.discovered[0].follow_tier == "paper_follow"


@pytest.mark.asyncio
async def test_leader_promotes_to_paper_after_threshold():
    """A leader already at watchlist with score >= 0.40 promotes to paper_follow."""
    repo = _MockLeadersRepo()
    # Pre-set: was in watchlist for a while
    repo._tiers[WALLET_A] = ("watchlist", time.time() - 100_000)

    registry = LeaderRegistry(
        discovery_service=_Discovery([
            _Profile(WALLET_A, score=0.50, total_positions=50, active_positions=3),
        ]),
        leaders_repo=repo,
    )
    state = await registry.refresh([])
    assert state.discovered[0].follow_tier == "paper_follow"


@pytest.mark.asyncio
async def test_leader_promotes_to_live_after_time_gate():
    """A leader at paper_follow with score >= 0.55 and enough time promotes to live."""
    repo = _MockLeadersRepo()
    # Pre-set: was in paper_follow for >24h
    repo._tiers[WALLET_A] = ("paper_follow", time.time() - 100_000)

    registry = LeaderRegistry(
        discovery_service=_Discovery([
            _Profile(WALLET_A, score=0.60, total_positions=50, active_positions=3),
        ]),
        leaders_repo=repo,
    )
    with patch.dict("orchestrators.leader_registry.ENV", {
        "TIER_PROMOTE_TO_LIVE": "0.55",
        "TIER_MIN_PAPER_SECONDS": "86400",
    }, clear=False):
        state = await registry.refresh([])
    assert state.discovered[0].follow_tier == "live_follow"


@pytest.mark.asyncio
async def test_leader_does_not_promote_to_live_without_time_gate():
    """A leader at paper_follow with high score but <24h stays at paper."""
    repo = _MockLeadersRepo()
    repo._tiers[WALLET_A] = ("paper_follow", time.time() - 100)  # only 100 seconds

    registry = LeaderRegistry(
        discovery_service=_Discovery([
            _Profile(WALLET_A, score=0.60, total_positions=50, active_positions=3),
        ]),
        leaders_repo=repo,
    )
    state = await registry.refresh([])
    assert state.discovered[0].follow_tier == "paper_follow"


@pytest.mark.asyncio
async def test_leader_demotes_to_watchlist_when_below_min_score():
    """A leader at live_follow with score below min_score filter gets watchlist (no coverage)."""
    repo = _MockLeadersRepo()
    repo._tiers[WALLET_A] = ("live_follow", time.time() - 200_000)

    registry = LeaderRegistry(
        discovery_service=_Discovery([
            _Profile(WALLET_A, score=0.30, total_positions=50, active_positions=3),
        ]),
        leaders_repo=repo,
    )
    state = await registry.refresh([])
    # score 0.30 < min_score 0.45 → insufficient coverage → forced to watchlist
    assert state.discovered[0].follow_tier == "watchlist"


@pytest.mark.asyncio
async def test_inactive_leader_gets_watchlist():
    """A leader failing activity filter gets watchlist tier."""
    repo = _MockLeadersRepo()
    repo._tiers[WALLET_A] = ("paper_follow", time.time() - 200_000)

    registry = LeaderRegistry(
        discovery_service=_Discovery([
            _Profile(WALLET_A, score=0.80, total_positions=50, active_positions=0),
        ]),
        leaders_repo=repo,
    )
    state = await registry.refresh([])
    # Inactive → has_sufficient_coverage=False → forced to watchlist
    assert state.discovered[0].follow_tier == "watchlist"


@pytest.mark.asyncio
async def test_multiple_leaders_independent_tiers():
    """Each leader gets independently evaluated tiers."""
    repo = _MockLeadersRepo()
    repo._tiers[WALLET_A] = ("paper_follow", time.time() - 200_000)
    repo._tiers[WALLET_B] = ("watchlist", time.time() - 200_000)

    registry = LeaderRegistry(
        discovery_service=_Discovery([
            _Profile(WALLET_A, score=0.60, total_positions=50, active_positions=3),
            _Profile(WALLET_B, score=0.50, total_positions=50, active_positions=3),
        ]),
        leaders_repo=repo,
    )
    state = await registry.refresh([])
    tiers = {c.leader_id: c.follow_tier for c in state.discovered}
    assert tiers[WALLET_A] == "live_follow"
    assert tiers[WALLET_B] == "paper_follow"


@pytest.mark.asyncio
async def test_tier_state_persisted():
    """Tier changes are persisted via update_tier."""
    repo = _MockLeadersRepo()
    repo._tiers[WALLET_A] = ("watchlist", time.time() - 100_000)

    registry = LeaderRegistry(
        discovery_service=_Discovery([
            _Profile(WALLET_A, score=0.50, total_positions=50, active_positions=3),
        ]),
        leaders_repo=repo,
    )
    await registry.refresh([])
    # Check tier was updated in the mock repo
    tier, changed_at = repo._tiers[WALLET_A]
    assert tier == "paper_follow"
    assert changed_at is not None
