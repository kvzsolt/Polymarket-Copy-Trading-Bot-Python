"""Contracts for balance-adjusted proportional percentage sizing."""

from __future__ import annotations

from decimal import Decimal

from pmcopy.services.copy_sizing_v5.domain import CopySizingRequest
from pmcopy.services.copy_sizing_v5.engine import CopySizingEngine
from pmcopy.services.copy_sizing_v5.policy import CopySizingPolicy


def test_balance_adjusted_proportional_percentage_caps_configured_percentage() -> None:
    policy = CopySizingPolicy(
        mode="proportional",
        percentage=Decimal("5"),
        copyable_balance_percent=Decimal("75"),
        buy_min_amount=Decimal("0.01"),
        buy_max_amount=Decimal("0"),
        max_position_pct=Decimal("1"),
    )
    decision = CopySizingEngine(policy).size(
        CopySizingRequest(
            leader_wallet="0x" + "1" * 40,
            leader_trade_notional=Decimal("200"),
            leader_pusd_balance=Decimal("10000"),
            follower_pusd_balance=Decimal("500"),
        )
    )

    assert decision.allowed is True
    assert decision.balance_ratio_percent == Decimal("5")
    assert decision.balance_adjusted_copy_percent == Decimal("3.75")
    assert decision.effective_configured_percentage == Decimal("3.75")
    assert decision.final_notional == Decimal("7.50")
    assert decision.effective_follower_balance_percent == Decimal("1.50")
    assert decision.effective_leader_trade_percent == Decimal("3.75")


def test_smaller_configured_percentage_stays_smaller_than_balance_cap() -> None:
    policy = CopySizingPolicy(
        mode="proportional",
        percentage=Decimal("2"),
        copyable_balance_percent=Decimal("75"),
        buy_min_amount=Decimal("0.01"),
        buy_max_amount=Decimal("0"),
        max_position_pct=Decimal("1"),
    )
    decision = CopySizingEngine(policy).size(
        CopySizingRequest(
            leader_wallet="0x" + "2" * 40,
            leader_trade_notional=Decimal("200"),
            leader_pusd_balance=Decimal("10000"),
            follower_pusd_balance=Decimal("500"),
        )
    )
    assert decision.effective_configured_percentage == Decimal("2")
    assert decision.final_notional == Decimal("4")


def test_invalid_external_numeric_payload_is_audited_as_rejection() -> None:
    policy = CopySizingPolicy(
        mode="proportional",
        percentage=Decimal("5"),
        copyable_balance_percent=Decimal("75"),
        buy_min_amount=Decimal("1"),
        buy_max_amount=Decimal("0"),
        max_position_pct=Decimal("1"),
    )

    decision = CopySizingEngine(policy).size(
        CopySizingRequest(
            leader_wallet="0x" + "b" * 40,
            leader_trade_notional="not-a-number",  # type: ignore[arg-type]
            leader_pusd_balance=Decimal("10000"),
            follower_pusd_balance=Decimal("500"),
        )
    )

    assert decision.allowed is False
    assert decision.reason == "leader_trade_notional must be decimal-like"
    assert decision.final_notional == Decimal("0")
