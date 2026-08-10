"""RED contracts for the Heisenberg-backed live discovery adapter.

The adapter bridges the new ``HeisenbergCandidateService.discover(funds)``
interface (returns ``DiscoveryResult``) to the live ``LeaderRegistry``
contract (``run() -> list[Candidate]``, ``rescore() -> list[Candidate]``,
``config_hash``, ``last_cycle_degraded``) and the existing
``DiscoveryRepo`` persistence, without a DB migration.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest

from services.discovery.discovery_models import (
    DiscoveryEvidence,
    DiscoveryResult,
    RankedTrader,
    RejectedTrader,
)
from services.discovery.heisenberg_discovery_service import (
    HeisenbergDiscoveryService,
)
from services.discovery.models import Candidate


GOOD = "0x" + "1" * 40
WHALE = "0x" + "2" * 40


def _config() -> Any:
    from config.app_config import DiscoveryConfig

    class _C:
        discovery = DiscoveryConfig.model_validate({})

    return _C()


def _ranked(wallet: str, score: Decimal) -> RankedTrader:
    return RankedTrader(
        wallet=wallet,
        score=score,
        source="heisenberg",
        h_score=Decimal("80"),
        bankroll_fit=Any,  # type: ignore[arg-type]  # structural only
        evidence=DiscoveryEvidence(recent_trade_count=12),
        components={"h_score": score},
        metrics={"recommended_stake_usdc": "10"},
    )


class FakeService:
    def __init__(self, result: DiscoveryResult) -> None:
        self._result = result
        self.discover_calls: list[Decimal] = []

    async def discover(self, funds: Decimal) -> DiscoveryResult:
        self.discover_calls.append(Decimal(str(funds)))
        return self._result


class FakeRepo:
    """Minimal DiscoveryRepository stand-in mirroring the real protocol."""

    def __init__(self) -> None:
        self.cycles: list[str] = []
        self.persisted: list[tuple[str, list[Candidate], str]] = []
        self.completed: list[tuple[str, dict[str, Any]]] = []
        self.rescored: list[tuple[list[Candidate], str]] = []
        self.stored: list[Candidate] = []
        self._config_hash = "test-hash"

    async def begin_cycle(self, config_hash: str) -> str:
        self.cycles.append(config_hash)
        return "cycle-1"

    async def persist_cycle(
        self,
        cycle_id: str,
        candidates: list[Candidate],
        config_hash: str,
    ) -> None:
        self.persisted.append((cycle_id, list(candidates), config_hash))
        self.stored = [c for c in candidates if c.rejected_reason is None]

    async def complete_cycle(
        self,
        cycle_id: str,
        *,
        status: str,
        candidate_count: int,
        kept_count: int,
        error: str | None = None,
    ) -> None:
        self.completed.append(
            (
                cycle_id,
                dict(
                    status=status,
                    candidate_count=candidate_count,
                    kept_count=kept_count,
                    error=error,
                ),
            )
        )

    async def score_history(self, wallet: str, *, since_ts: float):
        return []

    async def latest_candidates(self, *, config_hash: str | None = None):
        return list(self.stored)

    async def persist_rescore(self, candidates: list[Candidate], config_hash: str) -> None:
        self.rescored.append((list(candidates), config_hash))


def _config() -> Any:
    from config.app_config import DiscoveryConfig

    class _C:
        discovery = DiscoveryConfig.model_validate({})

    return _C()


@pytest.mark.asyncio
async def test_run_reads_funds_discovers_persists_and_returns_candidates() -> None:
    result = DiscoveryResult(
        ranked=(
            RankedTrader(
                wallet=GOOD,
                score=Decimal("0.82"),
                source="heisenberg",
                h_score=Decimal("82"),
                bankroll_fit=Any,  # type: ignore[arg-type]
                evidence=DiscoveryEvidence(recent_trade_count=12),
                components={"h_score": Decimal("0.82")},
                metrics={"recommended_stake_usdc": "10"},
            ),
        ),
        rejected=(
            RejectedTrader(wallet=WHALE, reasons=("bankroll_incompatible",), source="heisenberg"),
        ),
        source="heisenberg",
    )
    service = FakeService(result)
    repo = FakeRepo()
    funds_seen: list[Decimal] = []

    async def get_funds() -> Decimal | None:
        funds_seen.append(Decimal("500"))
        return Decimal("500")

    adapter = HeisenbergDiscoveryService(
        service=service,  # type: ignore[arg-type]
        repository=repo,
        config=_config(),
        get_funds=get_funds,
    )

    candidates = await adapter.run()

    expected_hash = adapter.config_hash
    assert funds_seen == [Decimal("500")]
    assert service.discover_calls == [Decimal("500")]
    assert repo.cycles == [expected_hash]
    assert repo.completed and repo.completed[0][1]["status"] == "completed"
    assert len(candidates) == 1
    assert candidates[0].wallet == GOOD
    assert candidates[0].score == pytest.approx(0.82)
    assert candidates[0].rejected_reason is None
    assert isinstance(candidates[0], Candidate)
    # Rejected wallet is persisted too (for diagnosis) but not returned.
    assert len(repo.persisted[0][1]) == 2
    rejected_stored = {c.wallet: c for c in repo.persisted[0][1]}
    assert rejected_stored[WHALE].rejected_reason is not None
    assert adapter.last_cycle_degraded is False


@pytest.mark.asyncio
async def test_run_fails_closed_when_funds_unavailable() -> None:
    service = FakeService(DiscoveryResult(ranked=(), rejected=(), source="heisenberg"))
    repo = FakeRepo()

    async def get_funds() -> Decimal | None:
        return None

    adapter = HeisenbergDiscoveryService(
        service=service,  # type: ignore[arg-type]
        repository=repo,
        config=_config(),
        get_funds=get_funds,
    )

    candidates = await adapter.run()
    assert candidates == []
    assert service.discover_calls == []
    assert adapter.last_cycle_degraded is True
    assert repo.completed and repo.completed[0][1]["status"] == "failed"


@pytest.mark.asyncio
async def test_run_fails_closed_when_funds_zero_or_negative() -> None:
    service = FakeService(DiscoveryResult(ranked=(), rejected=(), source="heisenberg"))
    repo = FakeRepo()

    async def get_funds() -> Decimal | None:
        return Decimal("0")

    adapter = HeisenbergDiscoveryService(
        service=service,  # type: ignore[arg-type]
        repository=repo,
        config=_config(),
        get_funds=get_funds,
    )

    assert await adapter.run() == []
    assert adapter.last_cycle_degraded is True


@pytest.mark.asyncio
async def test_run_marks_cycle_degraded_on_transport_outage() -> None:
    class TransportDown:
        async def discover(self, funds: Decimal) -> DiscoveryResult:
            raise RuntimeError("heisenberg unavailable")

    repo = FakeRepo()

    async def get_funds() -> Decimal | None:
        return Decimal("500")

    adapter = HeisenbergDiscoveryService(
        service=TransportDown(),  # type: ignore[arg-type]
        repository=repo,
        config=_config(),
        get_funds=get_funds,
    )

    with pytest.raises(RuntimeError):
        await adapter.run()
    assert adapter.last_cycle_degraded is True
    assert repo.completed and repo.completed[0][1]["status"] == "failed"


@pytest.mark.asyncio
async def test_rescore_reranks_stored_candidates_without_an_api_call() -> None:
    stored = [
        Candidate(wallet=GOOD, hscore={}, base_score=0.5, score=0.5),
        Candidate(wallet=WHALE, hscore={}, base_score=0.7, score=0.7),
    ]
    repo = FakeRepo()
    repo.stored = list(stored)

    adapter = HeisenbergDiscoveryService(
        service=FakeService(DiscoveryResult(ranked=(), rejected=(), source="heisenberg")),  # type: ignore[arg-type]
        repository=repo,
        config=_config(),
        get_funds=lambda: None,  # type: ignore[arg-type]
    )

    rescored = await adapter.rescore()

    assert [c.wallet for c in rescored] == [WHALE, GOOD]  # sorted by score desc
    assert repo.rescored and repo.rescored[0][1] == adapter.config_hash
    # No discover call made during rescore.
    assert adapter._service.discover_calls == []  # type: ignore[attr-defined]


def test_config_hash_exposed_for_roster_scoping() -> None:
    adapter = HeisenbergDiscoveryService(
        service=FakeService(DiscoveryResult(ranked=(), rejected=(), source="heisenberg")),  # type: ignore[arg-type]
        repository=FakeRepo(),
        config=_config(),
        get_funds=lambda: None,  # type: ignore[arg-type]
    )
    assert isinstance(adapter.config_hash, str) and adapter.config_hash
