from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from app import _wait_for_active_leaders
from orchestrators.leader_registry import LeaderRegistry
from services.metrics import metrics


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

    async def upsert_candidates(self, records):
        self.records.extend(records)

    async def get_tier(self, wallet):
        from storage.postgres.leaders_repo import LeaderTierRecord

        tier, changed_at = self._tiers.get(wallet.lower(), ("watchlist", None))
        return LeaderTierRecord(wallet=wallet.lower(), follow_tier=tier, tier_changed_at=changed_at)

    async def update_tier(self, wallet, follow_tier, tier_changed_at):
        self._tiers[wallet.lower()] = (follow_tier, tier_changed_at)


def _mock_config(*, promote_to_active: bool = False, top_n: int = 5, leader_auto: bool = False):
    return SimpleNamespace(
        discovery=SimpleNamespace(
            enable_trader_discovery=True,
            min_leader_positions=20,
            min_active_positions=1,
            discovery_promote_to_active=promote_to_active,
        ),
        leader=SimpleNamespace(
            leader_auto=leader_auto,
            auto_discover_top_n=top_n,
            auto_discover_min_score=0.45,
            tier_promote_to_paper=0.40,
            tier_promote_to_live=0.55,
            tier_demote_to_paper=0.35,
            tier_demote_to_watchlist=0.25,
            tier_min_paper_seconds=86400,
        ),
    )


@pytest.mark.asyncio
async def test_discovery_only_mode_emits_no_leader_signal(capsys):
    metrics.clear()

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
        config=_mock_config(promote_to_active=False),
    )

    state = await registry.refresh([])

    assert state.mode == "discovery_only"
    assert state.active_monitored == []
    captured = capsys.readouterr()
    assert "no_leaders_monitored" in captured.out

    _counters, gauges = metrics.snapshot()
    assert gauges.get("monitored_leaders") == 0


@pytest.mark.asyncio
async def test_discovery_only_wait_loop_stays_alive_without_leaders():
    config = _mock_config(promote_to_active=False, leader_auto=False)
    registry = LeaderRegistry(discovery_service=_Discovery([]), leaders_repo=_LeadersRepo(), config=config)
    task = asyncio.create_task(
        _wait_for_active_leaders(registry, [], refresh_interval=1, config=config)
    )
    await asyncio.sleep(0.1)
    assert not task.done()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_discovery_only_mode_promotes_top_ranked_leaders_when_auto_enabled():
    mock_config = _mock_config(promote_to_active=True, top_n=1, leader_auto=True)
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
        config=mock_config,
    )

    state = await registry.refresh([])

    assert state.mode == "discovery_only"
    assert state.active_monitored == ["0x1111111111111111111111111111111111111111"]
