"""Contracts for the four canonical V5 copy-sizing modes."""

from __future__ import annotations

from decimal import Decimal

import pytest

from services.copy_sizing_v5.domain import CopySizingRequest
from services.copy_sizing_v5.engine import CopySizingEngine
from services.copy_sizing_v5.policy import CopySizingPolicy


@pytest.mark.parametrize(
    ("mode", "kwargs", "request_kwargs", "expected"),
    [
        (
            "fixed",
            {"fixed_amount": Decimal("20")},
            {"leader_trade_notional": Decimal("500")},
            Decimal("20"),
        ),
        (
            "exact_mirror",
            {},
            {"leader_trade_notional": Decimal("50")},
            Decimal("50"),
        ),
        (
            "percentage_based",
            {"percentage": Decimal("20")},
            {"leader_trade_notional": Decimal("200")},
            Decimal("40"),
        ),
        (
            "portfolio_weighted",
            {"portfolio_weight_multiplier": Decimal("0.5")},
            {
                "leader_trade_notional": Decimal("10"),
                "leader_market_weight": Decimal("0.30"),
                "follower_market_exposure": Decimal("10"),
            },
            Decimal("46.25"),
        ),
    ],
)
def test_all_four_canonical_modes(
    mode: str,
    kwargs: dict[str, Decimal],
    request_kwargs: dict[str, Decimal],
    expected: Decimal,
) -> None:
    policy = CopySizingPolicy(
        mode=mode,
        copyable_balance_percent=Decimal("75"),
        buy_min_amount=Decimal("0.01"),
        buy_max_amount=Decimal("0"),
        max_position_pct=Decimal("1"),
        proportional_balance_cap_enabled=False,
        **kwargs,
    )
    request = CopySizingRequest(
        leader_wallet="0x" + "3" * 40,
        leader_pusd_balance=Decimal("10000"),
        follower_pusd_balance=Decimal("500"),
        **request_kwargs,
    )
    decision = CopySizingEngine(policy).size(request)
    assert decision.allowed is True
    assert decision.final_notional == expected
    assert decision.effective_follower_balance_percent == expected / Decimal("500") * Decimal("100")


def test_exact_mirror_is_exact_or_skipped_not_silently_downsized() -> None:
    policy = CopySizingPolicy(
        mode="exact_mirror",
        copyable_balance_percent=Decimal("75"),
        buy_min_amount=Decimal("1"),
        buy_max_amount=Decimal("0"),
        max_position_pct=Decimal("1"),
    )
    decision = CopySizingEngine(policy).size(
        CopySizingRequest(
            leader_wallet="0x" + "4" * 40,
            leader_trade_notional=Decimal("500"),
            leader_pusd_balance=Decimal("10000"),
            follower_pusd_balance=Decimal("500"),
        )
    )
    assert decision.allowed is False
    assert decision.reason == "strict_mode_exceeds_available_budget"
    assert decision.final_notional == Decimal("0")


def test_fixed_amount_is_fixed_or_skipped_not_silently_downsized() -> None:
    policy = CopySizingPolicy(
        mode="fixed",
        fixed_amount=Decimal("400"),
        copyable_balance_percent=Decimal("75"),
        buy_min_amount=Decimal("1"),
        buy_max_amount=Decimal("0"),
        max_position_pct=Decimal("1"),
    )
    decision = CopySizingEngine(policy).size(
        CopySizingRequest(
            leader_wallet="0x" + "5" * 40,
            leader_trade_notional=Decimal("1000"),
            leader_pusd_balance=Decimal("10000"),
            follower_pusd_balance=Decimal("500"),
        )
    )
    assert decision.allowed is False
    assert decision.reason == "strict_mode_exceeds_available_budget"


def test_portfolio_weighted_missing_weight_fails_closed_without_exception() -> None:
    policy = CopySizingPolicy(
        mode="portfolio_weighted",
        copyable_balance_percent=Decimal("75"),
        buy_min_amount=Decimal("1"),
        buy_max_amount=Decimal("0"),
        max_position_pct=Decimal("1"),
    )

    decision = CopySizingEngine(policy).size(
        CopySizingRequest(
            leader_wallet="0x" + "9" * 40,
            leader_trade_notional=Decimal("50"),
            leader_pusd_balance=Decimal("10000"),
            follower_pusd_balance=Decimal("500"),
        )
    )

    assert decision.allowed is False
    assert decision.reason == (
        "invalid_sizing_input:portfolio_weighted mode requires "
        "leader market weight or portfolio notionals"
    )
    assert decision.final_notional == Decimal("0")