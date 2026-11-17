"""Tests for WP-D polymorphic BUY sizing strategies across both active contracts."""

from __future__ import annotations

from decimal import Decimal

import pytest

from pmcopy.services.copy_sizing_v5.domain import CopySizingRequest
from pmcopy.services.copy_sizing_v5.engine import CopySizingEngine, _parse_tier_multiplier
from pmcopy.services.copy_sizing_v5.policy import CopySizingPolicy
from pmcopy.services.copy_sizing_v5.strategies import (
    DEFAULT_BUY_INTENT_STRATEGIES,
    BuyIntentStrategy,
    ExactMirrorBuyIntentStrategy,
    FixedBuyIntentStrategy,
)
from pmcopy.services.copyStrategy import (
    LegacyNotionalStrategy,
    calculate_copy_notional,
    normalize_strategy,
    parse_tiered_multipliers,
)


class TestV5BuyIntentStrategyHierarchy:
    """All 7 V5 modes implement BuyIntentStrategy while sharing engine caps."""

    def test_all_seven_modes_registered_as_buy_intent_strategies(self) -> None:
        expected_modes = {
            "fixed",
            "exact_mirror",
            "proportional",
            "adaptive",
            "tiered",
            "balance_weighted",
            "portfolio_weighted",
        }
        assert set(DEFAULT_BUY_INTENT_STRATEGIES) == expected_modes
        for mode, strategy in DEFAULT_BUY_INTENT_STRATEGIES.items():
            assert isinstance(strategy, BuyIntentStrategy)
            assert strategy.mode == mode

    def test_fixed_and_exact_mirror_requested_notional_helpers(self) -> None:
        fixed_policy = CopySizingPolicy(
            mode="fixed",
            fixed_amount=Decimal("6.50"),
            buy_min_amount=Decimal("5.0"),
            buy_max_amount=Decimal("10.0"),
        )
        assert (
            FixedBuyIntentStrategy().requested_notional(
                policy=fixed_policy, leader_trade_notional=Decimal("100")
            )
            == Decimal("6.50")
        )
        assert (
            ExactMirrorBuyIntentStrategy().requested_notional(
                policy=fixed_policy, leader_trade_notional=Decimal("7.25")
            )
            == Decimal("7.25")
        )

    def test_parse_tier_multiplier_threshold_step_semantics(self) -> None:
        spec = "100:0.75,500:1.0,1000:1.25"
        assert _parse_tier_multiplier(spec, Decimal("50")) == Decimal("0.75")
        assert _parse_tier_multiplier(spec, Decimal("100")) == Decimal("0.75")
        assert _parse_tier_multiplier(spec, Decimal("499.99")) == Decimal("0.75")
        assert _parse_tier_multiplier(spec, Decimal("500")) == Decimal("1.0")
        assert _parse_tier_multiplier(spec, Decimal("999.99")) == Decimal("1.0")
        assert _parse_tier_multiplier(spec, Decimal("1000")) == Decimal("1.25")
        assert _parse_tier_multiplier("", Decimal("250")) == Decimal("1")

    def test_tiered_and_adaptive_modes_preserve_configured_pct_and_caps(self) -> None:
        tiered_policy = CopySizingPolicy(
            mode="tiered",
            percentage=Decimal("10"),
            tiered_trade_multipliers="100:0.5,500:1.5",
            buy_min_amount=Decimal("2.0"),
            buy_max_amount=Decimal("100.0"),
            max_position_pct=Decimal("0.50"),
            proportional_balance_cap_enabled=False,
        )
        decision = CopySizingEngine(tiered_policy).size(
            CopySizingRequest(
                leader_wallet="0xleader",
                leader_trade_notional=Decimal("600"),
                follower_pusd_balance=Decimal("500"),
            )
        )
        assert decision.allowed is True
        assert decision.configured_percentage == Decimal("15.0")
        assert decision.final_notional == Decimal("90.0")

        adaptive_policy = CopySizingPolicy(
            mode="adaptive",
            adaptive_basis="performance",
            adaptive_min_percent=Decimal("2.0"),
            adaptive_max_percent=Decimal("10.0"),
            buy_min_amount=Decimal("1.0"),
            buy_max_amount=Decimal("50.0"),
            max_position_pct=Decimal("0.50"),
            proportional_balance_cap_enabled=False,
        )
        perf_decision = CopySizingEngine(adaptive_policy).size(
            CopySizingRequest(
                leader_wallet="0xleader",
                leader_trade_notional=Decimal("100"),
                follower_pusd_balance=Decimal("500"),
                adaptive_signal=Decimal("0.5"),
            )
        )
        assert perf_decision.allowed is True
        assert perf_decision.configured_percentage == Decimal("6.0")
        assert perf_decision.final_notional == Decimal("6.0")

    def test_sell_side_always_rejected_by_v5_engine(self) -> None:
        engine = CopySizingEngine(CopySizingPolicy())
        decision = engine.size(
            CopySizingRequest(
                leader_wallet="0xleader",
                leader_trade_notional=Decimal("50"),
                follower_pusd_balance=Decimal("200"),
                side="SELL",
            )
        )
        assert decision.allowed is False
        assert decision.reason == "sell_sizing_remains_existing_exit_path"


class TestLegacyAccountCopyStrategyContract:
    """copyStrategy preserves permissive normalization, range tiers, and min uplift."""

    def test_unknown_mode_normalizes_to_proportional_unlike_v5(self) -> None:
        assert normalize_strategy("exact_mirror") == "proportional"
        assert normalize_strategy("percentage") == "proportional"
        with pytest.raises(ValueError):
            CopySizingPolicy(mode="unknown_mode")

    def test_fixed_adaptive_proportional_and_sub_minimum_uplift(self) -> None:
        assert issubclass(type, object) and LegacyNotionalStrategy is not None
        res_prop = calculate_copy_notional(
            strategy="proportional",
            leader_notional_usdc=Decimal("10"),
            copy_percentage=Decimal("5"),
            fixed_amount=Decimal("3"),
            min_order_usdc=Decimal("2"),
            max_order_usdc=Decimal("50"),
        )
        assert res_prop.base_notional_usdc == Decimal("0.50")
        assert res_prop.final_notional_usdc == Decimal("2")
        assert res_prop.below_minimum is True

        tiers = parse_tiered_multipliers("0-100:0.5,100+:1.5")
        res_tiered = calculate_copy_notional(
            strategy="fixed",
            leader_notional_usdc=Decimal("200"),
            copy_percentage=Decimal("5"),
            fixed_amount=Decimal("10"),
            min_order_usdc=Decimal("2"),
            max_order_usdc=Decimal("12"),
            tiered_multipliers=tiers,
        )
        assert res_tiered.multiplier == Decimal("1.5")
        assert res_tiered.final_notional_usdc == Decimal("12")
        assert res_tiered.capped_by_max is True

        res_adapt = calculate_copy_notional(
            strategy="adaptive",
            leader_notional_usdc=Decimal("100"),
            copy_percentage=Decimal("10"),
            fixed_amount=Decimal("0"),
            min_order_usdc=Decimal("1"),
            max_order_usdc=Decimal("100"),
            adaptive_basis="performance",
            performance_win_rate=Decimal("60"),
        )
        # 0.5 + 0.6 * 1.5 = 1.4x -> 100 * 0.10 * 1.4 = 14
        assert res_adapt.final_notional_usdc == Decimal("14.0")
