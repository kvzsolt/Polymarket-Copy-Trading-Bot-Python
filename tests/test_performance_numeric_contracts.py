"""Exercise explicit performance units at the real scoring/provider boundaries."""

from decimal import Decimal

import pytest

from pmcopy.integrations.falcon.models import LeaderboardEntry, Market360Snapshot, Wallet360Snapshot
from pmcopy.services.trading.leader_scoring_service import LeaderScoringService
from pmcopy.domain.performance import PerformanceUnits
from tests.discovery.test_runtime_scoring import _config


@pytest.mark.parametrize("raw", ["0", "0.5", "1", "1.01", "2", "50", "100"])
def test_runtime_win_rate_is_always_percentage_points(raw: str) -> None:
    result = LeaderScoringService(config=_config()).score_leader(
        "leader", analytics={"win_rate": Decimal(raw)}
    )
    assert result.details["runtime_win_rate"] == float(raw)
    expected = (0.15 * float(Decimal(raw) / 100) + 0.1 * 0.5) / 0.3
    assert result.runtime_score == pytest.approx(expected)


def test_missing_runtime_evidence_stays_missing() -> None:
    result = LeaderScoringService(config=_config()).score_leader("leader")
    assert result.details["runtime_win_rate"] is None
    assert result.details["runtime_pnl"] is None


@pytest.mark.parametrize("raw", [True, "NaN", "Infinity", "-1", "100.01", "secret-invalid"])
def test_invalid_runtime_win_rate_fails_at_boundary(raw) -> None:
    with pytest.raises(ValueError) as error:
        LeaderScoringService(config=_config()).score_leader("leader", analytics={"win_rate": raw})
    assert "secret-invalid" not in str(error.value)


@pytest.mark.parametrize("raw", ["-0.5", "0", "0.5", "1", "1.01", "2", "100"])
def test_wallet_roi_always_uses_percentage_points(raw: str) -> None:
    snapshot = Wallet360Snapshot.from_mapping({"roi": raw})
    assert snapshot.roi == Decimal(raw) / 100


@pytest.mark.parametrize("raw", ["0", "0.005", "0.5", "1"])
def test_wallet_fraction_fields_do_not_use_roi_conversion(raw: str) -> None:
    fields = ("win_rate", "market_concentration_ratio", "statistical_confidence", "max_drawdown")
    snapshot = Wallet360Snapshot.from_mapping(dict.fromkeys(fields, raw))
    for field in fields:
        assert getattr(snapshot, field) == Decimal(raw)


@pytest.mark.parametrize("raw", ["0", "0.5", "1", "1.01", "100"])
def test_market_wallet_percentage_points_have_explicit_units(raw: str) -> None:
    snapshot = Market360Snapshot.from_mapping({"top1_wallet_pct": raw})
    assert snapshot.top1_wallet_fraction == Decimal(raw) / 100


@pytest.mark.parametrize("raw", [True, "NaN", "Infinity", "-0.1", "1.01", "invalid-secret"])
def test_invalid_provider_fraction_is_not_missing_or_rescaled(raw) -> None:
    with pytest.raises(ValueError) as error:
        Wallet360Snapshot.from_mapping({"win_rate": raw})
    assert "invalid-secret" not in str(error.value)


@pytest.mark.parametrize("raw", [None, True, "NaN", "Infinity", "invalid-secret"])
def test_missing_or_invalid_provider_score_is_not_observed_zero(raw) -> None:
    with pytest.raises(ValueError):
        LeaderboardEntry.from_mapping({"wallet": "0xabc", "h_score": raw})


def test_zero_provider_score_is_an_observation() -> None:
    assert LeaderboardEntry.from_mapping({"h_score": "0"}).h_score == 0


def test_provider_missing_and_zero_are_distinct() -> None:
    assert Wallet360Snapshot.from_mapping({}).win_rate is None
    assert Wallet360Snapshot.from_mapping({"win_rate": "0"}).win_rate == 0


@pytest.mark.parametrize("raw", ["0", "0.5", "1", "1.01", "2", "50", "100"])
def test_exact_percentage_conversion(raw: str) -> None:
    assert PerformanceUnits.win_rate_percent_to_fraction(Decimal(raw)) == Decimal(raw) / 100


def test_falcon_realized_corroboration_preserves_zero_win_rate() -> None:
    from pmcopy.services.discovery.falcon_candidate_service import FalconCandidateService

    zero = FalconCandidateService._realized_pnl_corroboration(
        [{"pnl": "0", "invested": "100", "wins": 0, "losses": 10}]
    )
    small = FalconCandidateService._realized_pnl_corroboration(
        [{"pnl": "0", "invested": "100", "wins": 1, "losses": 99}]
    )
    assert zero <= small


@pytest.mark.asyncio
async def test_discovery_score_does_not_reward_zero_over_small_win_rate() -> None:
    from pmcopy.services.discovery.falcon_candidate_service import FalconCandidateService
    from tests.test_falcon_candidate_service import NOW, OneWalletFalcon

    results = []
    for win_rate in ("0", "0.001"):
        client = OneWalletFalcon(overrides={None: {"win_rate": win_rate}})
        result = await FalconCandidateService(client, now=lambda: NOW).discover(Decimal("1000"))
        assert len(result.ranked) == 1
        results.append(result.ranked[0].score)
    assert results[0] <= results[1]
