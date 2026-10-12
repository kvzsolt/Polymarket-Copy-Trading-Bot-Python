"""Contracts for V5 policy compatibility with the branch SizingConfig."""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pytest

from pmcopy.services.copy_sizing_v5.policy import CopySizingPolicy, copy_sizing_policy_from_config


def test_branch_sizing_config_maps_user_requested_fields_and_v5_defaults() -> None:
    source = SimpleNamespace(
        mode="proportional",
        percentage=4.0,
        fixed_amount=3.0,
        trade_multiplier=1.0,
        tiered_trade_multipliers="",
        adaptive_basis="trade_size",
        adaptive_min_percent=0.0,
        adaptive_max_percent=0.0,
        adaptive_threshold_usdc=500.0,
        buy_min_amount=5.0,
        buy_max_amount=8.0,
        position_threshold=0,
        max_position_pct=0.10,
        enable_trust_size_multipliers=False,
        trust_size_multiplier_min=0.5,
        trust_size_multiplier_max=1.25,
    )
    policy = copy_sizing_policy_from_config(source)
    assert policy.mode == "proportional"
    assert policy.percentage == Decimal("4.0")
    assert policy.copyable_balance_percent == Decimal("75")
    assert policy.buy_min_amount == Decimal("5.0")
    assert policy.buy_max_amount == Decimal("8.0")


def test_copyable_balance_percent_rejects_out_of_range_values() -> None:
    with pytest.raises(ValueError, match="copyable_balance_percent"):
        CopySizingPolicy(copyable_balance_percent=Decimal("101"))


def test_fixed_mode_rejects_default_fixed_below_minimum_at_policy_boundary() -> None:
    with pytest.raises(ValueError, match="fixed_amount"):
        CopySizingPolicy(
            mode="fixed",
            fixed_amount=Decimal("3"),
            buy_min_amount=Decimal("5"),
        )
