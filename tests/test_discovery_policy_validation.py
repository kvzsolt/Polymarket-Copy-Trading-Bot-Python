"""Validation tests for fail-closed Falcon discovery policy settings."""

from __future__ import annotations

from decimal import Decimal

import pytest

from services.discovery.falcon_candidate_service import FalconDiscoveryPolicy


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
    ],
)
def test_discovery_policy_rejects_invalid_or_inconsistent_thresholds(
    kwargs: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        FalconDiscoveryPolicy(**kwargs)
