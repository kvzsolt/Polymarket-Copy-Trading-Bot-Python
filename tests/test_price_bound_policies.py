"""Tests for consolidated domain tick-grid and price-bound policies (WP-H)."""

from __future__ import annotations

from decimal import Decimal

import pytest

from pmcopy.domain.price_bound import (
    HARD_BOUND_TICK_POLICY,
    MARKETABLE_TICK_POLICY,
    HardBoundTickPolicy,
    HardPriceBound,
    InvalidTickSize,
    MarketableTickPolicy,
    TickPricePolicy,
    TickRounding,
    TickSize,
    quantize_price_to_tick,
)
from pmcopy.integrations.polymarket.ccxt_compat import tick_size as ccxt_tick_size


class TestDomainTickPolicyIdentityAndArbitraryTicks:
    """Domain price_bound is canonical; ccxt_compat re-exports the exact types."""

    def test_ccxt_compat_reexports_domain_symbols(self) -> None:
        assert ccxt_tick_size.InvalidTickSize is InvalidTickSize
        assert ccxt_tick_size.TickRounding is TickRounding
        assert ccxt_tick_size.TickSize is TickSize
        assert ccxt_tick_size.quantize_price_to_tick is quantize_price_to_tick

    def test_policies_are_tick_price_policy_subclasses(self) -> None:
        assert isinstance(HARD_BOUND_TICK_POLICY, TickPricePolicy)
        assert isinstance(MARKETABLE_TICK_POLICY, TickPricePolicy)
        assert HardBoundTickPolicy.policy_name == "hard_bound"
        assert MarketableTickPolicy.policy_name == "marketable"

    @pytest.mark.parametrize(
        ("tick", "price", "expected_buy_hard", "expected_buy_mkt", "expected_sell_hard", "expected_sell_mkt"),
        [
            (
                Decimal("0.005"),
                Decimal("0.413"),
                Decimal("0.410"),
                Decimal("0.415"),
                Decimal("0.415"),
                Decimal("0.410"),
            ),
            (
                Decimal("0.0025"),
                Decimal("0.4138"),
                Decimal("0.4125"),
                Decimal("0.4150"),
                Decimal("0.4150"),
                Decimal("0.4125"),
            ),
        ],
    )
    def test_arbitrary_ticks_preserve_opposite_rounding_semantics(
        self,
        tick: Decimal,
        price: Decimal,
        expected_buy_hard: Decimal,
        expected_buy_mkt: Decimal,
        expected_sell_hard: Decimal,
        expected_sell_mkt: Decimal,
    ) -> None:
        assert HardPriceBound.on_tick(price, tick, "BUY") == expected_buy_hard
        assert MARKETABLE_TICK_POLICY.quantize(price=price, tick_size=tick, side="BUY") == expected_buy_mkt
        assert HardPriceBound.on_tick(price, tick, "SELL") == expected_sell_hard
        assert MARKETABLE_TICK_POLICY.quantize(price=price, tick_size=tick, side="SELL") == expected_sell_mkt

    def test_copy_fak_limit_honors_max_share_price_and_rejects_zero_tick(self) -> None:
        snapped = HARD_BOUND_TICK_POLICY.snap_copy_fak_limit(
            price_limit=Decimal("0.884"),
            tick_size=Decimal("0.005"),
            side="BUY",
            max_share_price=Decimal("0.853"),
        )
        assert snapped == Decimal("0.850")
        with pytest.raises(ValueError):
            HARD_BOUND_TICK_POLICY.snap_copy_fak_limit(
                price_limit=Decimal("0.50"),
                tick_size=Decimal("0"),
                side="BUY",
            )

    def test_marketable_policy_preserves_non_positive_tick_passthrough_and_caller_cap(self) -> None:
        assert (
            MARKETABLE_TICK_POLICY.quantize(
                price=Decimal("0.513"), tick_size=Decimal("0"), side="BUY"
            )
            == Decimal("0.513")
        )
        assert (
            MARKETABLE_TICK_POLICY.apply_caller_protection(
                calculated_price=Decimal("0.520"),
                caller_limit=Decimal("0.514"),
                tick_size=Decimal("0.005"),
                side="BUY",
            )
            == Decimal("0.510")
        )
        assert (
            MARKETABLE_TICK_POLICY.apply_caller_protection(
                calculated_price=Decimal("0.480"),
                caller_limit=Decimal("0.486"),
                tick_size=Decimal("0.005"),
                side="SELL",
            )
            == Decimal("0.490")
        )
