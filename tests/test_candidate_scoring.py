from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pytest

from services.discovery.candidate_classifier import MarketClass
from services.discovery.candidate_scoring import CandidateScoringService, RecentState
from services.discovery.copyability_replay import CopyabilityReplayResult


def _config(
    *,
    min_score: float = 0.45,
    require_recent: bool = True,
    score_version: str = "leader_score_v2",
):
    return SimpleNamespace(
        discovery=SimpleNamespace(
            discovery_min_copyable_grouped_signal_ratio=0.75,
            discovery_min_copyable_notional_ratio=0.60,
            discovery_require_recent_state_for_live=require_recent,
            discovery_require_7d_non_negative=True,
            discovery_min_7d_pnl_usdc=0,
            discovery_min_3d_pnl_usdc=-50,
            discovery_crypto_fast_reject_signal_ratio=0.70,
            discovery_crypto_fast_reject_notional_ratio=0.70,
            discovery_crypto_fast_min_non_crypto_signals=5,
            leader_score_version=score_version,
            discovery_volume_target_15d=10000,
            discovery_min_volume_15d=0,
            discovery_rank_target=100,
            discovery_risk_penalty_enabled=True,
        ),
        leader=SimpleNamespace(auto_discover_min_score=min_score),
    )


def _candidate(**overrides):
    data = {
        "wallet": "0x" + "a" * 40,
        "h_score": 80,
        "roi_15d": 25,
        "win_rate_15d": 60,
        "sharpe_15d": 2,
        "pnl_15d": 125,
        "total_trades_15d": 42,
        "markets_traded_15d": 9,
        "total_volume_15d": 10000,
        "leaderboard_rank": 10,
        "tier": "gold",
        "source_agent_id": 584,
        "source_sort_by": "h_score",
        "source_rank": 10,
        "all_seen_sort_modes": ("h_score",),
        "trajectory": "stable",
        "raw": {"source": "test"},
    }
    data.update(overrides)
    return SimpleNamespace(**data)


def _replay():
    return CopyabilityReplayResult(
        wallet="0x" + "a" * 40,
        total_signals=1,
        meaningful_signals=1,
        copyable_signals=1,
        total_leader_notional=Decimal("10"),
        copyable_leader_notional=Decimal("10"),
        target_notional_sum=Decimal("10"),
        copyable_target_notional_sum=Decimal("10"),
        copyable_signal_ratio=1.0,
        copyable_notional_ratio=1.0,
        max_concurrent_open_positions=1,
        estimated_required_daily_budget=Decimal("10"),
        details={
            "balance_usdc": "100",
            "config_hash": "hash",
            "evaluation_context": {"balance_usdc": "100"},
        },
    )


def _signal():
    return SimpleNamespace(total_usdc_size=Decimal("10"))


def _sports_class():
    return MarketClass(
        family="sports_esports",
        is_crypto=False,
        is_fast_crypto=False,
        is_sports_esports=True,
        is_politics_macro=False,
    )


@pytest.mark.asyncio
async def test_low_score_becomes_shadow_not_live():
    evaluation = await CandidateScoringService(_config(min_score=0.99)).evaluate(
        candidate=_candidate(),
        signals=[_signal()],
        market_classes=[_sports_class()],
        replay=_replay(),
        recent_state=RecentState(pnl_7d=Decimal("1"), pnl_3d=Decimal("0")),
    )

    assert evaluation.status == "shadow"
    assert evaluation.rejection_reason == "below_live_score"
    assert evaluation.eligible_live is False
    assert evaluation.eligible_shadow is True


@pytest.mark.asyncio
async def test_no_shadow_row_has_eligible_live_true():
    evaluation = await CandidateScoringService(_config(min_score=0.99)).evaluate(
        candidate=_candidate(),
        signals=[_signal()],
        market_classes=[_sports_class()],
        replay=_replay(),
        recent_state=RecentState(pnl_7d=Decimal("1"), pnl_3d=Decimal("0")),
    )

    assert not (evaluation.status == "shadow" and evaluation.eligible_live)


@pytest.mark.asyncio
async def test_recent_state_unavailable_becomes_shadow_when_required():
    evaluation = await CandidateScoringService(_config(require_recent=True)).evaluate(
        candidate=_candidate(),
        signals=[_signal()],
        market_classes=[_sports_class()],
        replay=_replay(),
        recent_state=RecentState(available=False),
    )

    assert evaluation.status == "shadow"
    assert evaluation.rejection_reason == "recent_state_unavailable"
    assert evaluation.eligible_live is False
    assert evaluation.eligible_shadow is True


@pytest.mark.asyncio
async def test_score_v2_remains_bounded():
    evaluation = await CandidateScoringService(_config()).evaluate(
        candidate=_candidate(),
        signals=[_signal()],
        market_classes=[_sports_class()],
        replay=_replay(),
        recent_state=RecentState(pnl_7d=Decimal("1"), pnl_3d=Decimal("0")),
    )

    assert 0 <= evaluation.final_score <= 1
    assert evaluation.score_version == "leader_score_v2"
    assert evaluation.score_breakdown_json["version"] == "leader_score_v2"


@pytest.mark.asyncio
async def test_score_v2_volume_improves_score_when_other_inputs_equal():
    low_volume = await CandidateScoringService(_config()).evaluate(
        candidate=_candidate(total_volume_15d=0),
        signals=[_signal()],
        market_classes=[_sports_class()],
        replay=_replay(),
        recent_state=RecentState(pnl_7d=Decimal("1"), pnl_3d=Decimal("0")),
    )
    high_volume = await CandidateScoringService(_config()).evaluate(
        candidate=_candidate(total_volume_15d=10000),
        signals=[_signal()],
        market_classes=[_sports_class()],
        replay=_replay(),
        recent_state=RecentState(pnl_7d=Decimal("1"), pnl_3d=Decimal("0")),
    )

    assert high_volume.final_score > low_volume.final_score


@pytest.mark.asyncio
async def test_score_v2_bad_rank_lowers_rank_quality():
    good = await CandidateScoringService(_config()).evaluate(
        candidate=_candidate(leaderboard_rank=5),
        signals=[_signal()],
        market_classes=[_sports_class()],
        replay=_replay(),
        recent_state=RecentState(pnl_7d=Decimal("1"), pnl_3d=Decimal("0")),
    )
    bad = await CandidateScoringService(_config()).evaluate(
        candidate=_candidate(leaderboard_rank=200),
        signals=[_signal()],
        market_classes=[_sports_class()],
        replay=_replay(),
        recent_state=RecentState(pnl_7d=Decimal("1"), pnl_3d=Decimal("0")),
    )

    assert good.score_breakdown_json["rank_quality"] > bad.score_breakdown_json["rank_quality"]


@pytest.mark.asyncio
async def test_score_v2_missing_optional_falcon_fields_does_not_crash():
    candidate = _candidate()
    for name in (
        "total_volume_15d",
        "leaderboard_rank",
        "tier",
        "source_agent_id",
        "source_sort_by",
        "source_rank",
        "all_seen_sort_modes",
    ):
        delattr(candidate, name)

    evaluation = await CandidateScoringService(_config()).evaluate(
        candidate=candidate,
        signals=[_signal()],
        market_classes=[_sports_class()],
        replay=_replay(),
        recent_state=RecentState(pnl_7d=Decimal("1"), pnl_3d=Decimal("0")),
    )

    assert 0 <= evaluation.final_score <= 1


@pytest.mark.asyncio
async def test_score_v1_compatibility_formula():
    evaluation = await CandidateScoringService(
        _config(score_version="leader_score_v1")
    ).evaluate(
        candidate=_candidate(),
        signals=[_signal()],
        market_classes=[_sports_class()],
        replay=_replay(),
        recent_state=RecentState(pnl_7d=Decimal("1"), pnl_3d=Decimal("0")),
    )

    expected = (
        0.50 * evaluation.falcon_quality_score
        + 0.30 * evaluation.copyability_score
        + 0.15 * evaluation.recent_state_score
        + 0.05 * evaluation.market_fit_score
    )
    assert evaluation.score_version == "leader_score_v1"
    assert evaluation.final_score == pytest.approx(expected)
