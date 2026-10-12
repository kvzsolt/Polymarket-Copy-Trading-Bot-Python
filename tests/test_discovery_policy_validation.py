"""Validation tests for fail-closed Falcon discovery policy settings."""

from __future__ import annotations

from decimal import Decimal

import pytest

from pmcopy.config.app_config import load_config
from pmcopy.services.discovery.falcon_candidate_service import FalconDiscoveryPolicy


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"wallet360_windows": ()}, "wallet360_windows"),
        ({"wallet360_windows": (2,)}, "wallet360_windows"),
        (
            {"wallet360_windows": (1, 3), "required_wallet360_windows": 3},
            "required_wallet360_windows",
        ),
        ({"recent_trade_lookback_hours": 0}, "recent_trade_lookback_hours"),
        ({"max_last_trade_age_hours": 0}, "max_last_trade_age_hours"),
        ({"min_recent_trades": -1}, "min_recent_trades"),
        ({"max_recent_trades": 0}, "max_recent_trades"),
        (
            {"min_recent_trades": 20, "max_recent_trades": 10},
            "min_recent_trades",
        ),
        ({"min_short_trade_share": Decimal("1.01")}, "min_short_trade_share"),
        ({"min_h_score": Decimal("101")}, "min_h_score"),
        ({"min_liquidity_percentile": Decimal("-1")}, "min_liquidity_percentile"),
        ({"max_market_concentration": Decimal("NaN")}, "max_market_concentration"),
        (
            {
                "leaderboard_min_win_rate_15d": Decimal("0.9"),
                "leaderboard_max_win_rate_15d": Decimal("0.5"),
            },
            "leaderboard_min_win_rate_15d",
        ),
        (
            {
                "leaderboard_min_trades_15d": 50,
                "leaderboard_max_trades_15d": 10,
            },
            "leaderboard_min_trades_15d",
        ),
        ({"toxic_market_flags": ("bogus_flag",)}, "toxic_market_flags"),
        ({"toxic_market_flags": ()}, "toxic_market_flags"),
        ({"activity_window_hours": 0}, "activity_window_hours"),
        (
            {"activity_window_hours": 72, "recent_trade_lookback_hours": 48},
            "activity_window_hours",
        ),
        ({"min_activity_window_trades": -1}, "min_activity_window_trades"),
        ({"max_similar_wallets": -1}, "max_similar_wallets"),
        ({"max_trades_per_day": Decimal("-1")}, "max_trades_per_day"),
        ({"min_avg_trade_size_usdc": Decimal("-1")}, "min_avg_trade_size_usdc"),
        (
            {"max_excluded_category_trade_share": Decimal("1.5")},
            "max_excluded_category_trade_share",
        ),
        ({"excluded_categories": ("   ",)}, "excluded_categories"),
        ({"specialist_min_dominant_share": Decimal("1.5")}, "specialist_min_dominant_share"),
        ({"specialist_min_win_rate": Decimal("1.5")}, "specialist_min_win_rate"),
        ({"specialist_bonus_weight": Decimal("-0.1")}, "specialist_bonus_weight"),
        ({"leaderboard_sort_modes": ("bogus_mode",)}, "leaderboard_sort_modes"),
        ({"leaderboard_sort_modes": ()}, "leaderboard_sort_modes"),
    ],
)
def test_discovery_policy_rejects_invalid_or_inconsistent_thresholds(
    kwargs: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        FalconDiscoveryPolicy(**kwargs)


def test_policy_defaults_reflect_loosened_gates() -> None:
    policy = FalconDiscoveryPolicy()

    assert policy.toxic_market_flags == ("liquidity_risk", "volume_collapse", "whale_control")
    assert policy.max_toxic_market_share == Decimal("0.50")
    assert policy.activity_window_hours == 24
    assert policy.min_activity_window_trades == 1
    assert policy.max_last_trade_age_hours == 36
    assert policy.min_short_trade_count == 0
    assert policy.min_short_trade_share == Decimal("0")
    assert policy.drawdown_gate_enabled is False
    assert policy.min_h_score == Decimal("60")
    assert policy.leaderboard_min_trades_15d == 15
    assert policy.leaderboard_max_trades_15d == 3000
    assert policy.reject_perfect_timing_flag is True
    assert policy.reject_suspicious_win_rate_flag is True
    assert policy.max_similar_wallets == 2
    assert policy.max_trades_per_day == Decimal("300")
    assert policy.min_avg_trade_size_usdc == Decimal("5")
    assert policy.excluded_categories == ("crypto",)
    assert policy.max_excluded_category_trade_share == Decimal("0.50")
    assert policy.specialist_bonus_enabled is True
    assert policy.specialist_min_dominant_share == Decimal("0.80")
    assert policy.specialist_min_win_rate == Decimal("0.57")
    assert policy.specialist_bonus_weight == Decimal("0.10")
    assert not hasattr(policy, "min_today_trades")


def test_policy_normalizes_flag_and_category_case() -> None:
    policy = FalconDiscoveryPolicy(
        toxic_market_flags=(" Whale_Control ", "SQUEEZE"),
        excluded_categories=(" Crypto ", "POLITICS"),
    )

    assert policy.toxic_market_flags == ("whale_control", "squeeze")
    assert policy.excluded_categories == ("crypto", "politics")


def test_policy_shortlists_five_hundred_candidates_across_sort_modes() -> None:
    policy = FalconDiscoveryPolicy()

    assert policy.candidate_limit == 500
    assert policy.leaderboard_sort_modes == (
        "h_score",
        "roi",
        "win_rate",
        "pnl",
        "sharpe",
        "trades",
    )


def test_policy_normalizes_and_deduplicates_sort_modes() -> None:
    policy = FalconDiscoveryPolicy(leaderboard_sort_modes=(" H_Score ", "ROI", "h_score"))

    assert policy.leaderboard_sort_modes == ("h_score", "roi")


def test_packaged_profile_caps_the_startup_sweep_at_fifty_candidates() -> None:
    """The shipped discovery profile bounds one full sweep to 50 wallets."""
    from pmcopy.services.discovery.falcon_candidate_service import PROVIDER_LEADERBOARD_ROW_CAP
    from pmcopy.services.discovery.policy_config import discovery_policy_from_config

    config = load_config(
        {
            "database": {"dsn": "postgresql://test:test@localhost/test"},
            "wallet": {
                "funder_address": "0x" + "1" * 40,
                "private_key": "0x" + "a" * 64,
            },
        }
    )
    policy = discovery_policy_from_config(config)

    assert policy.candidate_limit == 50
    assert min(policy.candidate_limit, PROVIDER_LEADERBOARD_ROW_CAP) == 50
