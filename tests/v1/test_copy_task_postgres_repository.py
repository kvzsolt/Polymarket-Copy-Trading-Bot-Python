"""Contract tests for durable copy-task persistence."""

from __future__ import annotations

import json
from datetime import time
from decimal import Decimal

import pytest

from pmcopy.services.copy_tasks.domain import (
    CopyTask,
    CopyTaskStatus,
    MonitoringMode,
    PriceOffsetDirection,
    PriceOffsetMode,
    ReactionPolicy,
    TaskMarketFilter,
    TaskPricePolicy,
    TaskSchedule,
    TaskSizingMode,
    TaskSizingPolicy,
    TaskSpendPolicy,
    TaskTriggerPolicy,
)
from pmcopy.services.copy_tasks.service import CopyTaskConflict, CopyTaskNotFound
from pmcopy.storage.postgres.copy_task_repository import CopyTaskCodec, PostgresCopyTaskRepository


class FakePool:
    """Minimal asyncpg-like fake with deterministic scripted results."""

    def __init__(self) -> None:
        self.fetchval_results: list[object] = []
        self.fetchrow_results: list[object] = []
        self.fetch_results: list[list[object]] = []
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    async def fetchval(self, sql: str, *args: object) -> object:
        self.calls.append((sql, args))
        return self.fetchval_results.pop(0)

    async def fetchrow(self, sql: str, *args: object) -> object:
        self.calls.append((sql, args))
        return self.fetchrow_results.pop(0)

    async def fetch(self, sql: str, *args: object) -> list[object]:
        self.calls.append((sql, args))
        return self.fetch_results.pop(0)


def task(*, version: int = 1, status: CopyTaskStatus = CopyTaskStatus.ACTIVE) -> CopyTask:
    """Return a task exercising all persisted nested configuration."""

    return CopyTask(
        task_id="task-a",
        name="Primary",
        leader_wallet="0x1111111111111111111111111111111111111111",
        follower_account="account-a",
        status=status,
        monitoring_mode=MonitoringMode.PRIORITY,
        trigger=TaskTriggerPolicy(min_notional=Decimal("5"), max_notional=Decimal("900")),
        spend=TaskSpendPolicy(
            max_total=Decimal("1000"),
            max_daily=Decimal("100"),
            max_per_market=Decimal("30"),
            one_entry_per_market=True,
        ),
        price=TaskPricePolicy(
            min_price=Decimal("0.10"),
            max_price=Decimal("0.91"),
            offset_mode=PriceOffsetMode.PERCENTAGE,
            offset_direction=PriceOffsetDirection.ABOVE,
            offset=Decimal("2.5"),
            max_slippage_percent=Decimal("3"),
        ),
        reaction=ReactionPolicy(counter_trade=True, copy_merges=True),
        market_filter=TaskMarketFilter(
            include_keywords=("weather",),
            exclude_keywords=("politics",),
            categories=("climate & weather",),
            max_days_until_close=Decimal("4"),
        ),
        schedule=TaskSchedule(
            enabled=True,
            timezone_name="Europe/Budapest",
            start=time(8, 30),
            end=time(22, 0),
        ),
        sizing_mode="balance_weighted",
        version=version,
        notifications_muted=True,
    )


def row_from_task(value: CopyTask) -> dict[str, object]:
    """Return the database shape used by the adapter."""

    return {
        "task_id": value.task_id,
        "name": value.name,
        "leader_wallet": value.leader_wallet,
        "follower_account": value.follower_account,
        "status": value.status.value,
        "monitoring_mode": value.monitoring_mode.value,
        "sizing_mode": value.sizing_mode,
        "notifications_muted": value.notifications_muted,
        "version": value.version,
        "configuration": {
            "trigger": {"enabled": True, "min_notional": "5", "max_notional": "900"},
            "spend": {
                "enabled": True,
                "max_total": "1000",
                "max_daily": "100",
                "max_per_market": "30",
                "one_entry_per_market": True,
                "global_one_entry_per_market": False,
                "one_entry_per_sports_type": False,
            },
            "price": {
                "enabled": True,
                "min_price": "0.10",
                "max_price": "0.91",
                "offset_mode": "percentage",
                "offset_direction": "above",
                "offset": "2.5",
                "max_slippage_percent": "3",
            },
            "reaction": {
                "enabled": True,
                "follow_buys": True,
                "copy_sells": True,
                "buy_on_sells": False,
                "counter_trade": True,
                "copy_side": "both",
                "copy_merges": True,
                "copy_splits": False,
                "copy_conversions": False,
            },
            "market_filter": {
                "enabled": True,
                "include_keywords": ["weather"],
                "exclude_keywords": ["politics"],
                "categories": ["climate & weather"],
                "tokens": [],
                "timeframes": [],
                "min_liquidity_usdc": "0",
                "min_volume_24h_usdc": "0",
                "max_days_until_close": "4",
                "allow_expired_unresolved": False,
            },
            "schedule": {
                "enabled": True,
                "timezone_name": "Europe/Budapest",
                "start": "08:30:00",
                "end": "22:00:00",
            },
            "sizing": {
                "enabled": True,
                "mode": "balance_weighted",
                "percentage": "4",
                "fixed_amount": "3",
                "trade_multiplier": "1",
                "weighted_multiplier": "1",
                "buy_min_amount": "5",
                "buy_max_amount": "8",
                "copyable_balance_percent": "75",
                "proportional_balance_cap_enabled": True,
                "require_leader_balance": True,
                "strict_fixed_amount": True,
                "strict_exact_mirror": True,
            },
        },
    }


@pytest.mark.asyncio
async def test_round_trips_nested_task_configuration_without_float_conversion() -> None:
    pool = FakePool()
    original = task()
    pool.fetchval_results = [original.task_id]
    pool.fetchrow_results = [row_from_task(original)]
    repository = PostgresCopyTaskRepository(pool)

    await repository.create(original)
    restored = await repository.get(original.task_id)

    assert restored == original
    insert_args = pool.calls[0][1]
    configuration = insert_args[9]
    assert isinstance(configuration, str)
    assert '"2.5"' in configuration
    assert "2.5" in configuration


@pytest.mark.asyncio
async def test_create_raises_conflict_when_insert_does_not_return_identity() -> None:
    pool = FakePool()
    pool.fetchval_results = [None]
    repository = PostgresCopyTaskRepository(pool)

    with pytest.raises(CopyTaskConflict):
        await repository.create(task())


@pytest.mark.asyncio
async def test_save_uses_optimistic_version_and_classifies_missing_task() -> None:
    pool = FakePool()
    pool.fetchval_results = [None]
    pool.fetchrow_results = [None]
    repository = PostgresCopyTaskRepository(pool)

    with pytest.raises(CopyTaskNotFound):
        await repository.save(task(version=2), expected_version=1)


@pytest.mark.asyncio
async def test_save_classifies_version_conflict() -> None:
    pool = FakePool()
    pool.fetchval_results = [None]
    current = task(version=4)
    pool.fetchrow_results = [row_from_task(current)]
    repository = PostgresCopyTaskRepository(pool)

    with pytest.raises(CopyTaskConflict):
        await repository.save(task(version=2), expected_version=1)


@pytest.mark.asyncio
async def test_list_active_excludes_soft_deleted_rows_and_is_stably_ordered() -> None:
    pool = FakePool()
    first = task()
    second = CopyTask(
        task_id="task-b",
        name="Second",
        leader_wallet="0x2222222222222222222222222222222222222222",
        follower_account="account-a",
    )
    pool.fetch_results = [[row_from_task(first), row_from_task(second)]]
    repository = PostgresCopyTaskRepository(pool)

    result = await repository.list_active()

    assert [item.task_id for item in result] == ["task-a", "task-b"]
    assert "status <> 'deleted'" in pool.calls[0][0]
    assert "ORDER BY task_id" in pool.calls[0][0]


@pytest.mark.asyncio
async def test_round_trip_persists_full_task_sizing_policy() -> None:
    pool = FakePool()
    original = CopyTask(
        task_id="task-sized",
        name="Sized",
        leader_wallet="0x3333333333333333333333333333333333333333",
        follower_account="account-a",
        sizing=TaskSizingPolicy(
            mode=TaskSizingMode.PERCENTAGE,
            percentage=Decimal("12.5"),
            weighted_multiplier=Decimal("0.75"),
            buy_min_amount=Decimal("2"),
            buy_max_amount=Decimal("44"),
            copyable_balance_percent=Decimal("70"),
        ),
    )

    codec = CopyTaskCodec()
    configuration = json.loads(codec.encode_configuration(original))
    pool.fetchval_results = [original.task_id]
    pool.fetchrow_results = [
        {
            "task_id": original.task_id,
            "name": original.name,
            "leader_wallet": original.leader_wallet,
            "follower_account": original.follower_account,
            "status": original.status.value,
            "monitoring_mode": original.monitoring_mode.value,
            "sizing_mode": original.sizing_mode,
            "notifications_muted": False,
            "version": 1,
            "configuration": configuration,
        }
    ]
    repository = PostgresCopyTaskRepository(pool)

    await repository.create(original)
    restored = await repository.get(original.task_id)

    assert restored == original
    assert configuration["sizing"]["percentage"] == "12.5"
    assert configuration["sizing"]["copyable_balance_percent"] == "70"
