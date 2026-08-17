"""Deterministic tests for discovery roster hysteresis and degradation."""

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
                if row.tier == "active" and (config_hash is None or row.config_hash == config_hash)
            ),
            key=lambda row: -row.score,
        )
        return [row.wallet for row in rows[:limit]]


class Service:
    config_hash = "hash"

    async def run(self):
        raise AssertionError("manual leaders must not start discovery")


class RescoreService(Service):
    """Stands in for the discovery service's cached rescore.

    ``FalconDiscoveryService.rescore`` re-ranks the last accepted roster
    from stored snapshots without any external call, so it observes nothing
    new about a wallet.
    """

    def __init__(self, score: float = 0.80) -> None:
        self.score = score
        self.rescore_calls = 0

    async def rescore(self):
        self.rescore_calls += 1
        return [_candidate(self.score)]


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
    assert apply_hysteresis("active", 0.40, config.min_dwell_seconds, 3, config) == "watchlist"


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

    repository.records[WALLET] = replace(repository.records[WALLET], tier_changed_at=0)
    await registry.reconcile_candidates([_candidate(0.10)])
    assert repository.records[WALLET].tier == "watchlist"


def _tenure_registry(service, repository):
    discovery = DiscoveryConfig.model_validate(
        {"hysteresis": {"min_tenure_cycles": 3, "min_dwell_seconds": 100}}
    )
    return LeaderRegistry(
        discovery_service=service,
        leaders_repo=repository,  # type: ignore[arg-type]
        config=SimpleNamespace(discovery=discovery),  # type: ignore[arg-type]
    )


@pytest.mark.asyncio
async def test_cached_rescore_does_not_advance_promotion_tenure() -> None:
    """Only fresh evaluation cycles may count toward promotion tenure.

    The rescore schedule fires far more often than rediscovery, so counting
    cached re-ranks as cycles would let a wallet reach ``min_tenure_cycles``
    and start being copied without ever being freshly re-evaluated.
    """
    service = RescoreService()
    repository = MemoryLeadersRepo()
    registry = _tenure_registry(service, repository)

    for _ in range(2):
        await registry.reconcile_candidates([_candidate(0.80)])
    assert repository.records[WALLET].tier == "watchlist"
    assert repository.records[WALLET].promote_streak == 2

    for _ in range(5):
        await registry.rescore([])

    assert service.rescore_calls == 5
    assert repository.records[WALLET].tier == "watchlist"
    assert repository.records[WALLET].promote_streak == 2
    assert repository.records[WALLET].cycles_in_tier == 1

    # A genuine evaluation cycle still completes the tenure.
    await registry.reconcile_candidates([_candidate(0.80)])
    assert repository.records[WALLET].tier == "active"


@pytest.mark.asyncio
async def test_cached_rescore_does_not_advance_demotion_tenure() -> None:
    service = RescoreService(score=0.10)
    repository = MemoryLeadersRepo()
    registry = _tenure_registry(service, repository)

    for _ in range(3):
        await registry.reconcile_candidates([_candidate(0.80)])
    assert repository.records[WALLET].tier == "active"
    repository.records[WALLET] = replace(
        repository.records[WALLET],
        tier_changed_at=0,
        demote_streak=3,
    )

    for _ in range(5):
        await registry.rescore([])

    assert repository.records[WALLET].tier == "active"
    assert repository.records[WALLET].demote_streak == 3

    # Fresh cycles remain able to demote a leader that has gone bad.
    repository.records[WALLET] = replace(repository.records[WALLET], demote_streak=0)
    for _ in range(3):
        await registry.reconcile_candidates([_candidate(0.10)])
    assert repository.records[WALLET].tier == "watchlist"


@pytest.mark.asyncio
async def test_cached_rescore_still_refreshes_score_and_ranking() -> None:
    """Freezing tenure must not freeze the score the roster ranks on."""
    service = RescoreService(score=0.42)
    repository = MemoryLeadersRepo()
    registry = _tenure_registry(service, repository)

    await registry.reconcile_candidates([_candidate(0.80)])
    assert repository.records[WALLET].score == 0.80

    await registry.rescore([])

    assert repository.records[WALLET].score == 0.42
    assert repository.records[WALLET].longitudinal_score == 0.42


# ---------------------------------------------------------------------------
# Degraded cycles must not demote wallets they never looked at.
#
# A partial H-Score sort-mode failure still returns usable candidates, so the
# discovery service completes the cycle and reports ``last_cycle_degraded``.
# signal every active leader missing from the partial set scores 0.0 and grows
# a demote streak — so a persistent one-mode outage would demote exactly the
# leaders that mode surfaces.
# ---------------------------------------------------------------------------

OTHER = "0x" + "b" * 40

# A scoring-config change moves the hash; roster rows stay pinned to the hash
# they were last scored under.
OLD_HASH = "hash-before-config-change"
NEW_HASH = "hash-after-config-change"


class DiscoveryService:
    """Stands in for ``FalconDiscoveryService``: a run() plus its degraded flag."""

    def __init__(
        self,
        candidates: list[Candidate],
        *,
        degraded: bool = False,
        config_hash: str = "hash",
    ) -> None:
        self._candidates = candidates
        self.last_cycle_degraded = degraded
        self.config_hash = config_hash

    async def run(self) -> list[Candidate]:
        return list(self._candidates)


def _discovery_registry(service, repository) -> LeaderRegistry:
    discovery = DiscoveryConfig.model_validate(
        {
            "api": {"token": "secret"},
            "scheduler": {"enabled": True},
            "hysteresis": {"min_tenure_cycles": 1, "min_dwell_seconds": 0},
        }
    )
    return LeaderRegistry(
        discovery_service=service,
        leaders_repo=repository,  # type: ignore[arg-type]
        config=SimpleNamespace(discovery=discovery),  # type: ignore[arg-type]
    )


async def _seed_active_leader(
    repository: MemoryLeadersRepo,
    wallet: str,
    *,
    config_hash: str = "hash",
) -> None:
    service = DiscoveryService([_candidate_for(wallet, 0.90)], config_hash=config_hash)
    await _discovery_registry(service, repository).refresh([])
    assert repository.records[wallet].tier == "active"


def _candidate_for(wallet: str, score: float) -> Candidate:
    return Candidate(
        wallet=wallet,
        hscore={},
        base_score=score,
        longitudinal_score=score,
        score=score,
    )


@pytest.mark.asyncio
async def test_degraded_cycle_does_not_demote_an_absent_wallet() -> None:
    """A one-sort-mode outage must not look like evidence a leader went bad."""
    repository = MemoryLeadersRepo()
    await _seed_active_leader(repository, WALLET)
    seeded = repository.records[WALLET]

    # OTHER is present; WALLET is missing purely because a sort mode failed.
    service = DiscoveryService([_candidate_for(OTHER, 0.95)], degraded=True)
    registry = _discovery_registry(service, repository)

    for _ in range(5):
        await registry.refresh([])

    record = repository.records[WALLET]
    assert record.tier == "active"
    assert record.demote_streak == 0
    # Its score must not be rewritten to 0.0 — that is what drives demotion.
    assert record.score == seeded.score
    assert record.cycles_in_tier == seeded.cycles_in_tier
    # The wallet the degraded cycle *did* see still updates normally.
    assert repository.records[OTHER].score == 0.95


@pytest.mark.asyncio
async def test_degraded_cycle_keeps_the_old_config_hash_on_frozen_rows() -> None:
    """A frozen row must keep the hash its score was actually scored under.

    ``list_active`` filters on ``config_hash`` so that a scoring-config change
    invalidates every roster row until a cycle recomputes it. A degraded cycle
    recomputes nothing for a wallet it never observed, so stamping the current
    hash onto the carried-forward row would re-qualify, under the new config,
    a score that only the old config ever justified.
    """
    repository = MemoryLeadersRepo()
    await _seed_active_leader(repository, WALLET, config_hash=OLD_HASH)
    seeded = repository.records[WALLET]
    assert seeded.config_hash == OLD_HASH

    # The config changes, and the first cycle under the new hash is degraded
    # and never observes WALLET.
    service = DiscoveryService([_candidate_for(OTHER, 0.95)], degraded=True, config_hash=NEW_HASH)
    registry = _discovery_registry(service, repository)

    state = await registry.refresh([])

    # The row is carried forward unchanged — old hash included.
    assert repository.records[WALLET] == seeded

    # So its stale score stays ineligible under the new hash...
    assert await repository.list_active(config_hash=NEW_HASH) == [OTHER]
    assert WALLET not in state.active_monitored
    # ...while the wallet this cycle did observe is scored under it.
    assert repository.records[OTHER].config_hash == NEW_HASH


@pytest.mark.asyncio
async def test_healthy_cycle_still_demotes_a_wallet_absent_from_the_shortlist() -> None:
    """Preservation: suppression is scoped to degraded cycles only.

    If absence stopped demoting on healthy cycles too, a leader that genuinely
    stopped qualifying would stay active forever.
    """
    repository = MemoryLeadersRepo()
    await _seed_active_leader(repository, WALLET)

    # Same absence, but the cycle is complete — so absence is real evidence.
    service = DiscoveryService([_candidate_for(OTHER, 0.95)], degraded=False)
    registry = _discovery_registry(service, repository)

    await registry.refresh([])

    record = repository.records[WALLET]
    assert record.score == 0.0
    assert record.tier == "watchlist"


@pytest.mark.asyncio
async def test_degraded_cycle_still_promotes_the_candidates_it_did_see() -> None:
    """Suppressing absence-based demotion must not freeze the whole roster."""
    repository = MemoryLeadersRepo()
    service = DiscoveryService([_candidate_for(OTHER, 0.95)], degraded=True)
    registry = _discovery_registry(service, repository)

    await registry.refresh([])

    assert repository.records[OTHER].tier == "active"
    assert repository.records[OTHER].score == 0.95


@pytest.mark.asyncio
async def test_recovered_cycle_resumes_demoting_absent_wallets() -> None:
    """Suppression must not outlive the degraded cycle that caused it."""
    repository = MemoryLeadersRepo()
    await _seed_active_leader(repository, WALLET)

    service = DiscoveryService([_candidate_for(OTHER, 0.95)], degraded=True)
    registry = _discovery_registry(service, repository)
    await registry.refresh([])
    assert repository.records[WALLET].tier == "active"

    # Upstream recovers: the same absence is now real evidence again.
    service.last_cycle_degraded = False
    await registry.refresh([])

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
