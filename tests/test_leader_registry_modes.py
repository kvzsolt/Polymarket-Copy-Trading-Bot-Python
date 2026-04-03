from __future__ import annotations

import asyncio

import pytest

from index import _wait_for_active_leaders
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

    def upsert_candidates(self, records):
        self.records.extend(records)


@pytest.mark.asyncio
async def test_discovery_only_mode_emits_no_leader_signal(capsys):
    async with metrics._lock:
        metrics.counters.clear()
        metrics.gauges.clear()

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
