"""Preserve Falcon's non-negative drawdown metric without guessing its scale."""

from decimal import Decimal

import pytest

from pmcopy.integrations.falcon.models import Wallet360Snapshot
from pmcopy.services.discovery.falcon_candidate_service import (
    FalconCandidateService,
    FalconDiscoveryPolicy,
)
from tests.test_falcon_candidate_service import GOOD, NOW, OneWalletFalcon


@pytest.mark.parametrize("raw", ["0", "0.1003", "1", "3.7464", "6.9473", "455.7477"])
def test_drawdown_preserves_documented_and_premium_values(raw: str) -> None:
    """Provider examples exceed one; accepting them must never shrink the risk value."""
    snapshot = Wallet360Snapshot.from_mapping({"max_drawdown": raw})

    assert snapshot.max_drawdown == Decimal(raw)


def test_missing_drawdown_stays_missing() -> None:
    """Missing data does not become an observed zero drawdown."""
    assert Wallet360Snapshot.from_mapping({}).max_drawdown is None
    assert Wallet360Snapshot.from_mapping({"max_drawdown": None}).max_drawdown is None


@pytest.mark.parametrize("raw", [True, "NaN", "Infinity", "-Infinity", "-0.01", "secret", {}])
def test_invalid_drawdown_is_rejected_without_provider_text(raw: object) -> None:
    """Unbounded positive magnitude does not admit malformed or negative risk."""
    with pytest.raises(ValueError) as captured:
        Wallet360Snapshot.from_mapping({"max_drawdown": raw})

    assert "secret" not in str(captured.value)


@pytest.mark.asyncio
@pytest.mark.parametrize("raw", ["3.7464", "455.7477"])
async def test_large_drawdown_is_rejected_when_the_risk_gate_is_enabled(raw: str) -> None:
    """A valid high provider metric still exceeds every supported gate threshold."""
    service = FalconCandidateService(
        OneWalletFalcon(overrides={None: {"max_drawdown": raw}}),
        policy=FalconDiscoveryPolicy(drawdown_gate_enabled=True, max_drawdown=Decimal("1")),
        now=lambda: NOW,
    )

    result = await service.discover(Decimal("500"))

    assert result.best is None
    assert result.rejected[0].wallet == GOOD
    assert "drawdown_above_threshold" in result.rejected[0].reasons


@pytest.mark.asyncio
async def test_large_drawdowns_keep_the_existing_maximum_scoring_penalty() -> None:
    """Disabling hard rejection keeps the full penalty, without rescaling or wraparound."""
    scores = []
    for raw in ("0", "1", "3.7464", "455.7477"):
        service = FalconCandidateService(
            OneWalletFalcon(overrides={None: {"max_drawdown": raw}}),
            policy=FalconDiscoveryPolicy(drawdown_gate_enabled=False),
            now=lambda: NOW,
        )

        result = await service.discover(Decimal("500"))

        assert result.best is not None
        assert result.best.metrics["max_drawdown"] == raw
        assert result.best.evidence.source_errors == ()
        scores.append(result.best.score)

    assert scores[0] > scores[1]
    assert scores[1] == scores[2] == scores[3]
