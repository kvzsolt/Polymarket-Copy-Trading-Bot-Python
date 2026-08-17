"""Tests for exact-decimal follower bankroll compatibility policy."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from integrations.falcon.models import Market360Snapshot, TradeSnapshot
from services.discovery.bankroll_compatibility import (
    BankrollCompatibilityPolicy,
    evaluate_bankroll_compatibility,
)


def trade(notional: str, condition: str = "c1") -> TradeSnapshot:
    price = Decimal("0.5")
    return TradeSnapshot(
        trade_id=f"{condition}-{notional}",
        wallet="0xabc",
        condition_id=condition,
        market_slug=None,
        side="BUY",
        price=price,
        size=Decimal(notional) / price,
        timestamp=datetime(2026, 7, 30, tzinfo=timezone.utc),
        outcome="Yes",
        transaction_hash=None,
        raw={},
    )


def quality(percentile: str = "80") -> Market360Snapshot:
    return Market360Snapshot.from_mapping(
        {
            "condition_id": "c1",
            "liquidity_percentile": percentile,
            "current_volume_24h": "100000",
            "top1_wallet_pct": "10",
            "top10_wallet_pct": "40",
            "market_active": True,
            "market_closed": False,
            "volume_trend": "Normal",
        }
    )


def test_small_bankroll_prefers_copyable_micro_trader_over_whale() -> None:
    policy = BankrollCompatibilityPolicy(min_order_usdc=Decimal("1"))
    micro = evaluate_bankroll_compatibility(
        funds=Decimal("50"),
        recent_trades=[trade("4"), trade("5"), trade("6")],
        market_quality={"c1": quality()},
        avg_position_size=Decimal("5"),
        policy=policy,
    )
    whale = evaluate_bankroll_compatibility(
        funds=Decimal("50"),
        recent_trades=[trade("8000"), trade("10000"), trade("12000")],
        market_quality={"c1": quality()},
        avg_position_size=Decimal("10000"),
        policy=policy,
    )

    assert micro.fit_score > whale.fit_score
    assert micro.copyable_trade_share == Decimal("1")
    assert whale.recommended_copy_ratio < policy.min_practical_copy_ratio
    assert "copy_ratio_below_practical_floor" in whale.reasons


def test_large_bankroll_can_make_whale_strategy_copyable() -> None:
    result = evaluate_bankroll_compatibility(
        funds=Decimal("5000"),
        recent_trades=[trade("8000"), trade("10000"), trade("12000")],
        market_quality={"c1": quality("95")},
        avg_position_size=Decimal("10000"),
    )

    assert result.copyable_trade_share == Decimal("1")
    assert result.fit_score >= Decimal("0.70")
    assert Decimal("1") <= result.recommended_stake_usdc <= result.max_trade_budget_usdc


def test_no_recent_trades_fails_closed() -> None:
    result = evaluate_bankroll_compatibility(
        funds=Decimal("100"),
        recent_trades=[],
        market_quality={},
        avg_position_size=None,
    )

    assert result.fit_score == Decimal("0")
    assert result.copyable_trade_share == Decimal("0")
    assert result.reasons == ("no_recent_trades",)


def test_visible_depth_caps_copyable_share() -> None:
    result = evaluate_bankroll_compatibility(
        funds=Decimal("1000"),
        recent_trades=[trade("100"), trade("120"), trade("140")],
        market_quality={"c1": quality()},
        avg_position_size=Decimal("120"),
        visible_capacity_usdc={"c1": Decimal("2")},
    )

    assert result.copyable_trade_share < Decimal("1")
    assert "insufficient_visible_capacity" in result.reasons


def test_wallet360_average_position_size_changes_bankroll_fit_even_for_same_trades() -> None:
    trades = [trade("20"), trade("25"), trade("30")]
    small_positions = evaluate_bankroll_compatibility(
        funds=Decimal("100"),
        recent_trades=trades,
        market_quality={"c1": quality()},
        avg_position_size=Decimal("25"),
    )
    oversized_positions = evaluate_bankroll_compatibility(
        funds=Decimal("100"),
        recent_trades=trades,
        market_quality={"c1": quality()},
        avg_position_size=Decimal("100000"),
    )

    assert small_positions.viable is True
    assert oversized_positions.viable is False
    assert small_positions.position_scale_ratio > oversized_positions.position_scale_ratio
    assert "position_scale_below_practical_floor" in oversized_positions.reasons


def test_non_positive_trade_does_not_shift_capacity_lookup_for_valid_trades() -> None:
    result = evaluate_bankroll_compatibility(
        funds=Decimal("1000"),
        recent_trades=[
            trade("0", "discarded"),
            trade("100", "c1"),
            trade("100", "c2"),
        ],
        market_quality={"c1": quality(), "c2": quality()},
        avg_position_size=Decimal("100"),
        visible_capacity_usdc={
            "discarded": Decimal("0"),
            "c1": Decimal("1000"),
            "c2": Decimal("1000"),
        },
    )

    assert result.copyable_trade_share == Decimal("1")
    assert "insufficient_visible_capacity" not in result.reasons


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"max_copy_ratio": Decimal("0")}, "max_copy_ratio"),
        (
            {
                "min_practical_copy_ratio": Decimal("0.5"),
                "max_copy_ratio": Decimal("0.1"),
            },
            "min_practical_copy_ratio",
        ),
        ({"max_position_fraction": Decimal("1.01")}, "max_position_fraction"),
        ({"target_trade_fraction": Decimal("0")}, "target_trade_fraction"),
        ({"min_copyable_trade_share": Decimal("1.01")}, "min_copyable_trade_share"),
        ({"min_fit_score": Decimal("NaN")}, "min_fit_score"),
        ({"visible_capacity_utilization": Decimal("1.01")}, "visible_capacity_utilization"),
    ],
)
def test_bankroll_policy_rejects_unsafe_or_non_finite_bounds(
    kwargs: dict[str, Decimal],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        BankrollCompatibilityPolicy(**kwargs)
