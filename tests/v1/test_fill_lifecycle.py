"""Entry-fill accounting and protective-exit lock lifecycle contracts."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from services.copy_tasks.lifecycle import CopyTaskFillLifecycleService
from services.copy_tasks.reentry_lock import (
    MarketReentryKey,
    MarketReentryLockResult,
    ProtectiveExitFill,
    ProtectiveExitReason,
)


class BudgetSpy:
    """Record entry fill commits."""

    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    async def commit(self, **kwargs: object) -> bool:
        self.calls.append(kwargs)
        return True


class LockSpy:
    """Record protective exit evidence."""

    def __init__(self) -> None:
        self.calls: list[ProtectiveExitFill] = []

    async def record_protective_exit(
        self,
        fill: ProtectiveExitFill,
    ) -> MarketReentryLockResult:
        self.calls.append(fill)
        return MarketReentryLockResult(True, True, "market_locked_after_protective_exit")


def exit_fill() -> ProtectiveExitFill:
    """Return authoritative terminal take-profit evidence."""

    return ProtectiveExitFill(
        key=MarketReentryKey(
            follower_account="account-a",
            task_id="task-a",
            leader_wallet="0x1111111111111111111111111111111111111111",
            market_id="market-a",
        ),
        reason=ProtectiveExitReason.TAKE_PROFIT,
        authoritative=True,
        filled_shares=Decimal("10"),
        remaining_protected_shares=Decimal("0"),
        occurred_at=datetime(2026, 8, 8, 12, tzinfo=UTC),
        fill_id="exit-fill-a",
    )


@pytest.mark.asyncio
async def test_entry_fill_commits_reserved_task_budget_exactly_once() -> None:
    budgets = BudgetSpy()
    locks = LockSpy()
    service = CopyTaskFillLifecycleService(
        task_budgets=budgets,
        reentry_locks=locks,
    )
    occurred_at = datetime(2026, 8, 8, 12, tzinfo=UTC)

    result = await service.record_entry_fill(
        reservation_id="intent-a",
        fill_id="buy-fill-a",
        filled_notional=Decimal("7.5"),
        occurred_at=occurred_at,
    )

    assert result is True
    assert budgets.calls == [
        {
            "reservation_id": "intent-a",
            "fill_id": "buy-fill-a",
            "filled_notional": Decimal("7.5"),
            "occurred_at": occurred_at,
        }
    ]
    assert locks.calls == []


@pytest.mark.asyncio
async def test_authoritative_terminal_protective_fill_creates_reentry_lock() -> None:
    budgets = BudgetSpy()
    locks = LockSpy()
    service = CopyTaskFillLifecycleService(
        task_budgets=budgets,
        reentry_locks=locks,
    )

    result = await service.record_protective_exit(exit_fill())

    assert result.locked is True
    assert result.created is True
    assert locks.calls == [exit_fill()]
    assert budgets.calls == []


@pytest.mark.asyncio
async def test_disabled_lifecycle_has_no_budget_or_lock_side_effect() -> None:
    budgets = BudgetSpy()
    locks = LockSpy()
    service = CopyTaskFillLifecycleService(
        task_budgets=budgets,
        reentry_locks=locks,
        enabled=False,
    )

    entry = await service.record_entry_fill(
        reservation_id="intent-a",
        fill_id="buy-fill-a",
        filled_notional=Decimal("7.5"),
        occurred_at=datetime(2026, 8, 8, 12, tzinfo=UTC),
    )
    protective = await service.record_protective_exit(exit_fill())

    assert entry is False
    assert protective == MarketReentryLockResult(
        locked=False,
        created=False,
        reason="copy_task_fill_lifecycle_disabled",
    )
    assert budgets.calls == []
    assert locks.calls == []