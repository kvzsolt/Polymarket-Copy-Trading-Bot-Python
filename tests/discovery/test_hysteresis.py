from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace

import pytest

from config.app_config import DiscoveryConfig
from orchestrators.leader_registry import LeaderRegistry, apply_hysteresis
from services.discovery.models import Candidate

WALLET = "0x" + "a" * 40


class MemoryLeadersRepo:
    def __init__(self) -> None:
        self.records = {}

    async def list_roster(self):
        return list(self.records.values())

    async def save_roster(self, records):
        self.records.update({record.wallet: record for record in records})

    async def list_active(self, limit=None, config_hash=None):
        rows = sorted(
            (
                row
                for row in self.records.values()
                if row.tier == "active"
                and (config_hash is None or row.config_hash == config_hash)
            ),
            key=lambda row: -row.score,
        )
        return [row.wallet for row in rows[:limit]]


class Service:
    config_hash = "hash"

    async def run(self):
        raise AssertionError("manual leaders must not start discovery")


def _candidate(score: float) -> Candidate:
    return Candidate(
        wallet=WALLET,
        hscore={},
        base_score=score,
        longitudinal_score=score,
        score=score,
    )


def test_pure_hysteresis_respects_gap_dwell_and_cycles() -> None:
    config = DiscoveryConfig().hysteresis
    assert apply_hysteresis("watchlist", 0.70, 0, 2, config) == "watchlist"
    assert apply_hysteresis("watchlist", 0.70, 0, 3, config) == "active"
    assert apply_hysteresis("active", 0.40, 1, 3, config) == "active"
    assert (
        apply_hysteresis("active", 0.40, config.min_dwell_seconds, 3, config)
        == "watchlist"
    )


@pytest.mark.asyncio
async def test_registry_requires_consecutive_cycles_and_prevents_churn() -> None:
    discovery = DiscoveryConfig.model_validate(
        {
            "hysteresis": {
                "min_tenure_cycles": 3,
                "min_dwell_seconds": 100,
            }
        }
    )
    repository = MemoryLeadersRepo()
    registry = LeaderRegistry(
        discovery_service=Service(),
        leaders_repo=repository,  # type: ignore[arg-type]
        config=SimpleNamespace(discovery=discovery),  # type: ignore[arg-type]
    )
    for _ in range(2):
        await registry.reconcile_candidates([_candidate(0.80)])
        assert repository.records[WALLET].tier == "watchlist"
    await registry.reconcile_candidates([_candidate(0.80)])
    assert repository.records[WALLET].tier == "active"

    # Three bad observations inside dwell do not flip the active tier.
    for _ in range(3):
        await registry.reconcile_candidates([_candidate(0.10)])
    assert repository.records[WALLET].tier == "active"

    repository.records[WALLET] = replace(
        repository.records[WALLET], tier_changed_at=0
    )
    await registry.reconcile_candidates([_candidate(0.10)])
    assert repository.records[WALLET].tier == "watchlist"


@pytest.mark.asyncio
async def test_registry_uses_manual_leaders_without_discovery_or_roster() -> None:
    discovery = DiscoveryConfig.model_validate({"api": {"token": "secret"}})
    repository = MemoryLeadersRepo()
    registry = LeaderRegistry(
        discovery_service=Service(),
        leaders_repo=repository,  # type: ignore[arg-type]
        config=SimpleNamespace(discovery=discovery),  # type: ignore[arg-type]
    )

    state = await registry.refresh([WALLET])

    assert state.configured == [WALLET]
    assert state.active_monitored == [WALLET]
    assert state.discovered == []
    assert state.mode == "configured"
