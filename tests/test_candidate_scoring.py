from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pytest

from services.discovery.candidate_classifier import MarketClass
from services.discovery.candidate_scoring import CandidateScoringService, RecentState
from services.discovery.copyability_replay import CopyabilityReplayResult


def _config(*, min_score: float = 0.45, require_recent: bool = True):
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
