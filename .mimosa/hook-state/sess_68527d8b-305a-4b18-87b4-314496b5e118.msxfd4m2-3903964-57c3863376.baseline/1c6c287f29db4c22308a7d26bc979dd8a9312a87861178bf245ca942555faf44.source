"""Contracts for the Kreo-compatible source-trade reaction engine."""

from __future__ import annotations

from decimal import Decimal

from services.copy_tasks.domain import (
    CopySideFilter,
    ReactionPolicy,
    SourceTrade,
)
from services.copy_tasks.reaction import CopyReactionEngine, ReactionAction


def _trade(action: str = "BUY", outcome: str = "YES") -> SourceTrade:
    return SourceTrade(
        leader_wallet="0x" + "11" * 20,
        market_id="cond-1",
        outcome_label=outcome,
        action=action,
        price=Decimal("0.42"),
        notional=Decimal("10"),
        shares=Decimal("23.81"),
    )


def test_follow_buy_emits_buy_reaction() -> None:
    decision = CopyReactionEngine().decide(ReactionPolicy(), _trade("BUY"))

    assert decision.action is ReactionAction.BUY
    assert decision.outcome_label == "YES"
    assert decision.reason == "follow_buy"


def test_disabled_reaction_skips() -> None:
    policy = ReactionPolicy(enabled=False)
    decision = CopyReactionEngine().decide(policy, _trade("BUY"))

    assert decision.action is ReactionAction.SKIP


def test_buy_on_sell_emits_buy_on_opposite_outcome() -> None:
    policy = ReactionPolicy(copy_sells=False, buy_on_sells=True)
    decision = CopyReactionEngine().decide(policy, _trade("SELL", "NO"))

    assert decision.action is ReactionAction.BUY
    assert decision.outcome_label == "NO"


def test_copy_sells_emits_sell_reaction() -> None:
    policy = ReactionPolicy()
    decision = CopyReactionEngine().decide(policy, _trade("SELL"))

    assert decision.action is ReactionAction.SELL


def test_yes_only_filter_skips_no_outcome() -> None:
    policy = ReactionPolicy(copy_side=CopySideFilter.YES_ONLY)
    decision = CopyReactionEngine().decide(policy, _trade("BUY", "NO"))

    assert decision.action is ReactionAction.SKIP
    assert decision.reason == "copy_side_filtered"


def test_counter_trade_swaps_outcome() -> None:
    policy = ReactionPolicy(counter_trade=True)
    decision = CopyReactionEngine().decide(policy, _trade("BUY", "YES"))

    assert decision.action is ReactionAction.BUY
    assert decision.outcome_label == "NO"


def test_counter_trade_rejects_non_binary_outcome() -> None:
    import pytest

    policy = ReactionPolicy(counter_trade=True)
    with pytest.raises(ValueError, match="counter trade"):
        CopyReactionEngine().decide(policy, _trade("BUY", "MAYBE"))
