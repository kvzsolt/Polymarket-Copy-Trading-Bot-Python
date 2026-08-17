"""Contracts for immutable v1 copy-task configuration."""

from __future__ import annotations

from datetime import time
from decimal import Decimal

import pytest

from services.copy_tasks.domain import (
    CopySideFilter,
    CopyTask,
    CopyTaskStatus,
    MonitoringMode,
    PriceOffsetMode,
    ReactionPolicy,
    TaskPricePolicy,
    TaskSchedule,
    TaskSpendPolicy,
    TaskSizingMode,
    TaskSizingPolicy,
    TaskTriggerPolicy,
)


def test_copy_task_is_immutable_and_validates_decimal_limits() -> None:
    task = CopyTask(
        task_id="task-1",
        name="leader one",
        leader_wallet="0x1111111111111111111111111111111111111111",
        follower_account="deposit-wallet",
        status=CopyTaskStatus.ACTIVE,
        monitoring_mode=MonitoringMode.NORMAL,
        trigger=TaskTriggerPolicy(min_notional=Decimal("5"), max_notional=Decimal("500")),
        spend=TaskSpendPolicy(
            max_total=Decimal("1000"),
            max_daily=Decimal("100"),
            max_per_market=Decimal("50"),
        ),
        price=TaskPricePolicy(
            min_price=Decimal("0.20"),
            max_price=Decimal("0.80"),
            offset_mode=PriceOffsetMode.PERCENTAGE,
            offset=Decimal("2"),
        ),
        reaction=ReactionPolicy(copy_side=CopySideFilter.BOTH),
        schedule=TaskSchedule(
            enabled=True,
            timezone_name="Europe/Budapest",
            start=time(8, 0),
            end=time(22, 0),
        ),
    )

    assert task.enabled is True
    assert task.trigger.min_notional == Decimal("5")
    with pytest.raises((AttributeError, TypeError)):
        task.name = "mutated"  # type: ignore[misc]


def test_copy_task_rejects_invalid_wallet_and_inverted_price_band() -> None:
    with pytest.raises(ValueError, match="leader_wallet"):
        CopyTask(task_id="task", name="bad", leader_wallet="nope", follower_account="x")

    with pytest.raises(ValueError, match="price band"):
        TaskPricePolicy(min_price=Decimal("0.8"), max_price=Decimal("0.2"))


def test_sizing_mode_aliases_normalize_to_canonical_enum() -> None:
    task = CopyTask(
        task_id="task-alias",
        name="alias",
        leader_wallet="0x" + "11" * 20,
        follower_account="acct",
        sizing_mode="percentage",
    )
    assert task.sizing_mode == "proportional"
    assert task.sizing.mode is TaskSizingMode.PROPORTIONAL


def test_sizing_policy_rejects_zero_copyable_balance_percent() -> None:
    with pytest.raises(ValueError, match="copyable_balance_percent"):
        TaskSizingPolicy(copyable_balance_percent=Decimal("0"))


def test_sizing_policy_rejects_inverted_buy_bounds() -> None:
    with pytest.raises(ValueError, match="buy_max_amount"):
        TaskSizingPolicy(buy_min_amount=Decimal("10"), buy_max_amount=Decimal("5"))


def test_fixed_sizing_requires_positive_fixed_amount() -> None:
    with pytest.raises(ValueError, match="fixed_amount"):
        TaskSizingPolicy(mode=TaskSizingMode.FIXED, fixed_amount=Decimal("0"))


def test_schedule_admits_overnight_window() -> None:
    from datetime import UTC, datetime

    schedule = TaskSchedule(
        enabled=True,
        timezone_name="UTC",
        start=time(22, 0),
        end=time(6, 0),
    )
    assert schedule.admits_entry(datetime(2026, 8, 9, 23, 0, tzinfo=UTC)) is True
    assert schedule.admits_entry(datetime(2026, 8, 9, 7, 0, tzinfo=UTC)) is False


def test_schedule_rejects_unknown_timezone() -> None:
    with pytest.raises(ValueError, match="timezone"):
        TaskSchedule(enabled=True, timezone_name="Mars/Olympus")


def test_schedule_requires_timezone_aware_now() -> None:
    from datetime import datetime

    schedule = TaskSchedule(enabled=True, timezone_name="UTC")
    with pytest.raises(ValueError, match="timezone-aware"):
        schedule.admits_entry(datetime(2026, 8, 9, 12, 0))


def test_spend_policy_rejects_inverted_max_total() -> None:
    with pytest.raises(ValueError, match="max_notional"):
        TaskTriggerPolicy(min_notional=Decimal("100"), max_notional=Decimal("50"))
