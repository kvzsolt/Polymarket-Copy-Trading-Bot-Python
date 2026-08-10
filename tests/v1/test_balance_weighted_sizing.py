"""Contracts for Kreo-compatible balance-weighted sizing."""

from __future__ import annotations

from decimal import Decimal

from services.copy_sizing_v5.domain import CopySizingRequest
from services.copy_sizing_v5.engine import CopySizingEngine
from services.copy_sizing_v5.policy import CopySizingPolicy


def test_balance_weighted_applies_leader_available_balance_fraction_to_follower_budget() -> None:
    engine = CopySizingEngine(
        CopySizingPolicy(
            mode="balance_weighted",
            copyable_balance_percent=Decimal("75"),
            portfolio_weight_multiplier=Decimal("0.5"),
            buy_min_amount=Decimal("1"),
            buy_max_amount=Decimal("1000"),
            max_position_pct=Decimal("1"),
        )
    )
    decision = engine.size(
        CopySizingRequest(
            leader_wallet="0x1",
            leader_trade_notional=Decimal("100"),
            leader_pusd_balance=Decimal("1000"),
            follower_pusd_balance=Decimal("500"),
        )
    )
    # Leader used 10% of available balance; follower copyable budget is 375;
    # multiplier 0.5 targets 18.75.
    assert decision.allowed is True
    assert decision.final_notional == Decimal("18.750")
    assert decision.effective_leader_balance_percent == Decimal("10")
