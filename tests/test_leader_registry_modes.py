from __future__ import annotations

import asyncio

import pytest

from index import _wait_for_active_leaders
from orchestrators.leader_registry import LeaderRegistry
from services.metrics import metrics
from unittest.mock import patch


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


class _LeadersRepo:
    def __init__(self):
        self.records = []
        self._tiers: dict[str, tuple[str, float | None]] = {}

    def upsert_candidates(self, records):
        self.records.extend(records)

    def get_tier(self, wallet):
        from storage.sqlite.leaders_repo import LeaderTierRecord
        tier, changed_at = self._tiers.get(wallet.lower(), ("watchlist", None))
        return LeaderTierRecord(wallet=wallet.lower(), follow_tier=tier, tier_changed_at=changed_at)

    def update_tier(self, wallet, follow_tier, tier_changed_at):
        self._tiers[wallet.lower()] = (follow_tier, tier_changed_at)


@pytest.mark.asyncio
async def test_discovery_only_mode_emits_no_leader_signal(capsys):
    await metrics.clear()

    registry = LeaderRegistry(
        discovery_service=_Discovery(
            [
                _Profile(
                    "0x1111111111111111111111111111111111111111",
                    score=0.1,
                    total_positions=2,
                    active_positions=0,
                )
            ]
        ),
        leaders_repo=_LeadersRepo(),
    )

    state = await registry.refresh([])

    assert state.mode == "discovery_only"
    assert state.active_monitored == []
    captured = capsys.readouterr()
    assert "no_leaders_monitored" in captured.out

    _counters, gauges = await metrics.snapshot()
    assert gauges.get("monitored_leaders") == 0


@pytest.mark.asyncio
async def test_discovery_only_wait_loop_stays_alive_without_leaders():
    registry = LeaderRegistry(discovery_service=_Discovery([]), leaders_repo=_LeadersRepo())
    task = asyncio.create_task(_wait_for_active_leaders(registry, [], refresh_interval=1))
    await asyncio.sleep(0.1)
    assert not task.done()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_discovery_only_mode_promotes_top_ranked_leaders_when_auto_enabled():
    registry = LeaderRegistry(
        discovery_service=_Discovery(
            [
                _Profile(
                    "0x1111111111111111111111111111111111111111",
                    score=0.91,
                    total_positions=120,
                    active_positions=5,
                ),
                _Profile(
                    "0x2222222222222222222222222222222222222222",
                    score=0.87,
                    total_positions=95,
                    active_positions=4,
                ),
            ]
        ),
        leaders_repo=_LeadersRepo(),
    )

    with patch.dict(
        "orchestrators.leader_registry.ENV",
        {
            "DISCOVERY_PROMOTE_TO_ACTIVE": True,
            "AUTO_DISCOVER_TOP_N": 1,
            "AUTO_DISCOVER_MIN_SCORE": 0.45,
            "MIN_LEADER_POSITIONS": 20,
            "MIN_ACTIVE_POSITIONS": 1,
        },
        clear=False,
    ):
        state = await registry.refresh([])

    assert state.mode == "discovery_only"
    assert state.active_monitored == ["0x1111111111111111111111111111111111111111"]
