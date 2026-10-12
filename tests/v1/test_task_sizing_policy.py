"""Per-task sizing policy and engine contracts."""

from __future__ import annotations

from decimal import Decimal

import pytest

from pmcopy.services.copy_sizing_v5.domain import CopySizingRequest
from pmcopy.services.copy_sizing_v5.policy import CopySizingPolicy
from pmcopy.services.copy_tasks.domain import CopyTask, TaskSizingMode, TaskSizingPolicy
from pmcopy.services.copy_tasks.sizing import CopyTaskSizingService


LEADER = "0x1111111111111111111111111111111111111111"


def task(policy: TaskSizingPolicy) -> CopyTask:
    """Return one task with explicit sizing policy."""

    return CopyTask(
        task_id="task-a",
        name="Task",
        leader_wallet=LEADER,
        follower_account="account-a",
        sizing=policy,
    )


def request(*, leader_trade: str = "100") -> CopySizingRequest:
    """Return one follower sizing request."""

    return CopySizingRequest(
        leader_wallet=LEADER,
        leader_trade_notional=Decimal(leader_trade),
        follower_pusd_balance=Decimal("500"),
        leader_pusd_balance=Decimal("1000"),
    )


def test_sizing_mode_compatibility_alias_populates_typed_policy() -> None:
    value = CopyTask(
        task_id="task-a",
        name="Task",
        leader_wallet=LEADER,
        follower_account="account-a",
        sizing_mode="balance_weighted",
    )

    assert value.sizing.mode is TaskSizingMode.BALANCE_WEIGHTED
    assert value.sizing_mode == "balance_weighted"


def test_explicit_sizing_policy_becomes_canonical_mode() -> None:
    value = task(
        TaskSizingPolicy(
            mode=TaskSizingMode.PORTFOLIO_WEIGHTED,
            weighted_multiplier=Decimal("0.5"),
        )
    )

    assert value.sizing_mode == "portfolio_weighted"
    assert value.sizing.weighted_multiplier == Decimal("0.5")


def test_invalid_task_sizing_values_fail_at_boundary() -> None:
    with pytest.raises(ValueError):
        TaskSizingPolicy(copyable_balance_percent=Decimal("101"))
    with pytest.raises(ValueError):
        TaskSizingPolicy(
            mode=TaskSizingMode.PROPORTIONAL,
            percentage=Decimal("0"),
        )
    with pytest.raises(ValueError):
        TaskSizingPolicy(
            buy_min_amount=Decimal("10"),
            buy_max_amount=Decimal("5"),
        )


def test_fixed_task_amount_ignores_calculated_mode_min_max_bounds() -> None:
    service = CopyTaskSizingService(
        base_policy=CopySizingPolicy(
            buy_min_amount=Decimal("5"),
            buy_max_amount=Decimal("8"),
        )
    )
    value = task(
        TaskSizingPolicy(
            mode=TaskSizingMode.FIXED,
            fixed_amount=Decimal("3"),
            buy_min_amount=Decimal("5"),
            buy_max_amount=Decimal("8"),
        )
    )

    decision = service.size(value, request())

    assert decision.allowed is True
    assert decision.mode == "fixed"
    assert decision.final_notional == Decimal("3")


def test_percentage_task_uses_task_rate_and_shared_balance_caps() -> None:
    service = CopyTaskSizingService(base_policy=CopySizingPolicy())
    value = task(
        TaskSizingPolicy(
            mode=TaskSizingMode.PROPORTIONAL,
            percentage=Decimal("20"),
            buy_min_amount=Decimal("1"),
            buy_max_amount=Decimal("100"),
        )
    )

    decision = service.size(value, request(leader_trade="100"))

    assert decision.allowed is True
    assert decision.configured_percentage == Decimal("20")
    assert decision.final_notional == Decimal("20")


def test_disabled_task_sizing_returns_auditable_rejection() -> None:
    service = CopyTaskSizingService(base_policy=CopySizingPolicy())
    value = task(TaskSizingPolicy(enabled=False))

    decision = service.size(value, request())

    assert decision.allowed is False
    assert decision.reason == "task_sizing_disabled"
