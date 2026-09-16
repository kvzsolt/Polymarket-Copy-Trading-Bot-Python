"""RED contracts for the Falcon-backed live discovery adapter.

The adapter bridges the new ``FalconCandidateService.discover(funds)``
interface (returns ``DiscoveryResult``) to the live ``LeaderRegistry``
contract (``run() -> list[Candidate]``, ``rescore() -> list[Candidate]``,
``config_hash``, ``last_cycle_degraded``) and the existing
``DiscoveryRepo`` persistence, without a DB migration.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

import pytest

from services.discovery.discovery_models import (
    DiscoveryEvidence,
    DiscoveryResult,
    RankedTrader,
    RejectedTrader,
)
from services.discovery.falcon_discovery_service import (
    FalconDiscoveryUnavailable,
    FalconDiscoveryService,
)
from services.discovery.models import Candidate


GOOD = "0x" + "1" * 40
WHALE = "0x" + "2" * 40
SYBIL = "0x" + "3" * 40


def _config() -> Any:
    from config.app_config import DiscoveryConfig

    class _C:
        discovery = DiscoveryConfig.model_validate({})

    return _C()


def _ranked(wallet: str, score: Decimal) -> RankedTrader:
    return RankedTrader(
        wallet=wallet,
        score=score,
        source="falcon",
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
                source="falcon",
                h_score=Decimal("82"),
                bankroll_fit=Any,  # type: ignore[arg-type]
                evidence=DiscoveryEvidence(recent_trade_count=12),
                components={"h_score": Decimal("0.82")},
                metrics={"recommended_stake_usdc": "10"},
            ),
        ),
        rejected=(
            RejectedTrader(wallet=WHALE, reasons=("bankroll_incompatible",), source="falcon"),
        ),
        source="falcon",
    )
    service = FakeService(result)
    repo = FakeRepo()
    funds_seen: list[Decimal] = []

    async def get_funds() -> Decimal | None:
        funds_seen.append(Decimal("500"))
        return Decimal("500")

    adapter = FalconDiscoveryService(
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
    service = FakeService(DiscoveryResult(ranked=(), rejected=(), source="falcon"))
    repo = FakeRepo()

    async def get_funds() -> Decimal | None:
        return None

    adapter = FalconDiscoveryService(
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
    service = FakeService(DiscoveryResult(ranked=(), rejected=(), source="falcon"))
    repo = FakeRepo()

    async def get_funds() -> Decimal | None:
        return Decimal("0")

    adapter = FalconDiscoveryService(
        service=service,  # type: ignore[arg-type]
        repository=repo,
        config=_config(),
        get_funds=get_funds,
    )

    assert await adapter.run() == []
    assert adapter.last_cycle_degraded is True


@pytest.mark.asyncio
async def test_run_fails_closed_with_specific_error_when_provider_is_disabled() -> None:
    """Missing provider composition must not surface as an AttributeError."""
    repo = FakeRepo()

    async def get_funds() -> Decimal | None:
        return Decimal("500")

    adapter = FalconDiscoveryService(
        service=None,
        repository=repo,
        config=_config(),
        get_funds=get_funds,
    )

    with pytest.raises(FalconDiscoveryUnavailable, match="not enabled"):
        await adapter.run()
    assert adapter.last_cycle_degraded is True
    assert repo.completed and repo.completed[0][1]["status"] == "failed"


@pytest.mark.asyncio
async def test_run_marks_cycle_degraded_on_transport_outage() -> None:
    class TransportDown:
        async def discover(self, funds: Decimal) -> DiscoveryResult:
            raise RuntimeError("falcon unavailable")

    repo = FakeRepo()

    async def get_funds() -> Decimal | None:
        return Decimal("500")

    adapter = FalconDiscoveryService(
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

    adapter = FalconDiscoveryService(
        service=FakeService(DiscoveryResult(ranked=(), rejected=(), source="falcon")),  # type: ignore[arg-type]
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
    adapter = FalconDiscoveryService(
        service=FakeService(DiscoveryResult(ranked=(), rejected=(), source="falcon")),  # type: ignore[arg-type]
        repository=FakeRepo(),
        config=_config(),
        get_funds=lambda: None,  # type: ignore[arg-type]
    )
    assert isinstance(adapter.config_hash, str) and adapter.config_hash


@pytest.mark.asyncio
async def test_run_exposes_last_cycle_summary_with_rejection_histogram(
    caplog: pytest.LogCaptureFixture,
) -> None:
    result = DiscoveryResult(
        ranked=(_ranked(GOOD, Decimal("0.82")),),
        rejected=(
            RejectedTrader(
                wallet=WHALE,
                reasons=("bankroll_incompatible", "hyperactive_trader"),
                source="falcon",
            ),
            RejectedTrader(wallet=SYBIL, reasons=("hyperactive_trader",), source="falcon"),
        ),
        source="falcon",
    )
    repo = FakeRepo()

    async def get_funds() -> Decimal | None:
        return Decimal("500")

    adapter = FalconDiscoveryService(
        service=FakeService(result),  # type: ignore[arg-type]
        repository=repo,
        config=_config(),
        get_funds=get_funds,
        logger_instance=logging.getLogger("test.FalconDiscoveryService"),
    )

    with caplog.at_level(logging.INFO, logger="test.FalconDiscoveryService"):
        await adapter.run()

    summary = adapter.last_cycle_summary
    assert summary is not None
    assert summary.accepted == 1
    assert summary.rejected == 2
    assert dict(summary.rejection_reasons) == {
        "bankroll_incompatible": 1,
        "hyperactive_trader": 2,
    }
    records = [item for item in caplog.records if item.message == "discovery_cycle_summary"]
    assert records and records[0].accepted_count == 1
    assert records[0].rejected_count == 2


@pytest.mark.asyncio
async def test_last_cycle_summary_is_reset_before_each_cycle() -> None:
    repo = FakeRepo()

    async def no_funds() -> Decimal | None:
        return None

    adapter = FalconDiscoveryService(
        service=FakeService(DiscoveryResult(ranked=(), rejected=(), source="falcon")),  # type: ignore[arg-type]
        repository=repo,
        config=_config(),
        get_funds=no_funds,
    )

    await adapter.run()

    assert adapter.last_cycle_summary is None


class TargetedFakeService:
    """Candidate-service stand-in that records the targeted wallet rows."""

    def __init__(self, result: DiscoveryResult) -> None:
        self._result = result
        self.calls: list[tuple[Decimal, Any]] = []

    async def discover(self, funds: Decimal, *, wallets: Any = None) -> DiscoveryResult:
        self.calls.append((Decimal(str(funds)), wallets))
        return self._result


def _stored(wallet: str, h_score: str, score: float) -> Candidate:
    return Candidate(
        wallet=wallet,
        hscore={"h_score": h_score, "source": "falcon"},
        base_score=score,
        score=score,
    )


@pytest.mark.asyncio
async def test_persisted_wallets_reads_the_stored_accepted_roster() -> None:
    repo = FakeRepo()
    repo.stored = [_stored(GOOD, "82", 0.9), _stored(WHALE, "88", 0.7)]

    async def funds() -> Decimal:
        return Decimal("500")

    adapter = FalconDiscoveryService(
        service=FakeService(DiscoveryResult(ranked=(), rejected=(), source="falcon")),  # type: ignore[arg-type]
        repository=repo,
        config=_config(),
        get_funds=funds,
    )

    assert await adapter.persisted_wallets() == [GOOD, WHALE]


@pytest.mark.asyncio
async def test_targeted_run_passes_stored_h_scores_and_persists_a_normal_cycle(
    caplog: pytest.LogCaptureFixture,
) -> None:
    repo = FakeRepo()
    repo.stored = [_stored(GOOD, "82", 0.9), _stored(WHALE, "88", 0.7)]
    service = TargetedFakeService(
        DiscoveryResult(ranked=(_ranked(GOOD, Decimal("0.9")),), rejected=(), source="falcon")
    )

    async def funds() -> Decimal:
        return Decimal("500")

    adapter = FalconDiscoveryService(
        service=service,  # type: ignore[arg-type]
        repository=repo,
        config=_config(),
        get_funds=funds,
        logger_instance=logging.getLogger("test.FalconDiscoveryService"),
    )

    with caplog.at_level(logging.INFO, logger="test.FalconDiscoveryService"):
        accepted = await adapter.run(wallets=[GOOD])

    assert [item.wallet for item in accepted] == [GOOD]
    assert len(service.calls) == 1
    funds_used, rows = service.calls[0]
    assert funds_used == Decimal("500")
    assert [dict(row) for row in rows] == [{"wallet": GOOD, "h_score": "82"}]
    assert repo.cycles == [adapter.config_hash]
    assert repo.persisted and repo.persisted[0][0] == "cycle-1"
    assert repo.completed[0][1]["status"] == "completed"
    records = [item for item in caplog.records if item.message == "discovery_cycle_summary"]
    assert records and records[0].discovery_cycle_mode == "targeted"


@pytest.mark.asyncio
async def test_sweep_run_reports_the_sweep_cycle_mode(
    caplog: pytest.LogCaptureFixture,
) -> None:
    repo = FakeRepo()
    service = FakeService(
        DiscoveryResult(ranked=(_ranked(GOOD, Decimal("0.9")),), rejected=(), source="falcon")
    )

    async def funds() -> Decimal:
        return Decimal("500")

    adapter = FalconDiscoveryService(
        service=service,  # type: ignore[arg-type]
        repository=repo,
        config=_config(),
        get_funds=funds,
        logger_instance=logging.getLogger("test.FalconDiscoveryService"),
    )

    with caplog.at_level(logging.INFO, logger="test.FalconDiscoveryService"):
        await adapter.run()

    records = [item for item in caplog.records if item.message == "discovery_cycle_summary"]
    assert records and records[0].discovery_cycle_mode == "sweep"


async def test_targeted_checks_recover_each_wallet_score_across_other_account_cycles():
    from unittest.mock import AsyncMock

    repo = FakeRepo()
    repo.candidate_hscores = AsyncMock(return_value={GOOD: Decimal("82")})
    service = TargetedFakeService(
        DiscoveryResult(ranked=(_ranked(GOOD, Decimal(".9")),), rejected=(), source="falcon")
    )
    adapter = FalconDiscoveryService(
        service=service,
        repository=repo,
        config=_config(),
        get_funds=AsyncMock(return_value=Decimal("100")),
    )
    await adapter.run(wallets=[GOOD])
    assert service.calls[0][1] == [{"wallet": GOOD, "h_score": Decimal("82")}]
    assert adapter.last_evaluations[0].wallet == GOOD
    assert adapter.last_cycle_id == "cycle-1"
