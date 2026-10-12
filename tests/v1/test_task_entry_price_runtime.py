"""Contracts for task-specific copied-price execution protection."""

from __future__ import annotations

from decimal import Decimal

from pmcopy.services.copy_tasks.domain import (
    CopyTask,
    PriceOffsetDirection,
    PriceOffsetMode,
    TaskPricePolicy,
)
from pmcopy.services.copy_tasks.entry_price import CopyTaskEntryPriceService


def task(price: TaskPricePolicy) -> CopyTask:
    """Return one deterministic task with the supplied price policy."""

    return CopyTask(
        task_id="task-price",
        name="Price task",
        leader_wallet="0x1111111111111111111111111111111111111111",
        follower_account="account-a",
        price=price,
    )


def test_market_mode_uses_max_slippage_as_marketability_bound() -> None:
    service = CopyTaskEntryPriceService()
    policy = TaskPricePolicy(
        offset_mode=PriceOffsetMode.DISABLED,
        max_slippage_percent=Decimal("5"),
    )

    buy = service.evaluate(
        task=task(policy),
        follower_action="BUY",
        source_price=Decimal("0.50"),
    )
    sell = service.evaluate(
        task=task(policy),
        follower_action="SELL",
        source_price=Decimal("0.50"),
    )

    assert buy.allowed is True
    assert buy.order_type == "FAK"
    assert buy.price_limit == Decimal("0.5250")
    assert sell.allowed is True
    assert sell.price_limit == Decimal("0.4750")


def test_price_match_fixed_direction_is_literal_above_or_below() -> None:
    service = CopyTaskEntryPriceService()
    above = task(
        TaskPricePolicy(
            offset_mode=PriceOffsetMode.FIXED,
            offset_direction=PriceOffsetDirection.ABOVE,
            offset=Decimal("0.02"),
            max_slippage_percent=Decimal("5"),
        )
    )
    below = task(
        TaskPricePolicy(
            offset_mode=PriceOffsetMode.FIXED,
            offset_direction=PriceOffsetDirection.BELOW,
            offset=Decimal("0.02"),
            max_slippage_percent=Decimal("5"),
        )
    )

    assert service.evaluate(
        task=above,
        follower_action="SELL",
        source_price=Decimal("0.50"),
    ).price_limit == Decimal("0.52")
    assert service.evaluate(
        task=below,
        follower_action="BUY",
        source_price=Decimal("0.50"),
    ).price_limit == Decimal("0.48")


def test_explicit_offset_cannot_exceed_task_slippage_ceiling() -> None:
    service = CopyTaskEntryPriceService()
    policy = TaskPricePolicy(
        offset_mode=PriceOffsetMode.PERCENTAGE,
        offset_direction=PriceOffsetDirection.ABOVE,
        offset=Decimal("7"),
        max_slippage_percent=Decimal("5"),
    )

    decision = service.evaluate(
        task=task(policy),
        follower_action="BUY",
        source_price=Decimal("0.50"),
    )

    assert decision.allowed is False
    assert decision.reason == "task_price_offset_exceeds_slippage"
    assert decision.price_limit is None


def test_counter_trade_requires_authoritative_opposite_outcome_price() -> None:
    service = CopyTaskEntryPriceService()
    candidate = task(TaskPricePolicy())

    missing = service.evaluate(
        task=candidate,
        follower_action="BUY",
        source_price=Decimal("0.70"),
        counter_trade=True,
    )
    resolved = service.evaluate(
        task=candidate,
        follower_action="BUY",
        source_price=Decimal("0.70"),
        execution_reference_price=Decimal("0.31"),
        counter_trade=True,
    )

    assert missing.allowed is False
    assert missing.reason == "counter_trade_execution_price_missing"
    assert resolved.allowed is True
    assert resolved.reference_price == Decimal("0.31")
    assert resolved.price_limit == Decimal("0.3255")


def test_disabled_price_policy_keeps_explicit_source_bound() -> None:
    service = CopyTaskEntryPriceService()
    candidate = task(TaskPricePolicy(enabled=False))

    decision = service.evaluate(
        task=candidate,
        follower_action="BUY",
        source_price=Decimal("0.44"),
    )

    assert decision.allowed is True
    assert decision.reason == "task_price_policy_disabled"
    assert decision.price_limit == Decimal("0.44")


def test_invalid_follower_action_is_rejected() -> None:
    decision = CopyTaskEntryPriceService().evaluate(
        task=task(TaskPricePolicy()),
        follower_action="HOLD",
        source_price=Decimal("0.45"),
    )
    assert decision.allowed is False
    assert decision.reason == "invalid_follower_action"


def test_invalid_source_price_is_rejected() -> None:
    decision = CopyTaskEntryPriceService().evaluate(
        task=task(TaskPricePolicy()),
        follower_action="BUY",
        source_price=Decimal("1.5"),
    )
    assert decision.allowed is False
    assert decision.reason == "invalid_source_price"
