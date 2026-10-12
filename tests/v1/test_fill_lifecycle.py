"""Entry-fill accounting and protective-exit lock lifecycle contracts."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from pmcopy.services.copy_tasks.lifecycle import (
    CopyTaskFillLifecycleService,
    TaskBudgetLifecycleAction,
    TaskExecutionDisposition,
    TaskExecutionOutcome,
)
from pmcopy.services.copy_tasks.lots import DurableRiskLot, RiskLotStatus
from pmcopy.services.copy_tasks.protective_exits import FillRiskLot, ProtectiveRiskPolicy
from pmcopy.services.copy_tasks.reentry_lock import (
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

    async def retain(self, reservation_id: str) -> bool:
        self.calls.append({"retain": reservation_id})
        return True

    async def release(self, reservation_id: str) -> bool:
        self.calls.append({"release": reservation_id})
        return True

    async def record_fill(self, **kwargs: object) -> object:
        self.calls.append(kwargs)
        return object()

    async def settle(self, reservation_id: str, *, terminal_fill_id: str) -> bool:
        """Record terminal settlement of existing partial evidence."""

        self.calls.append(
            {
                "settle_reservation_id": reservation_id,
                "terminal_fill_id": terminal_fill_id,
            }
        )
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


class LotSpy:
    """Record durable entry-fill lot creation."""

    def __init__(self) -> None:
        self.calls: list[DurableRiskLot] = []

    async def create_from_entry_fill(self, lot: DurableRiskLot) -> bool:
        """Record one lot aggregate."""

        self.calls.append(lot)
        return True


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


def entry_lot() -> DurableRiskLot:
    """Return one durable lot created from the entry fill."""

    opened_at = datetime(2026, 8, 8, 12, tzinfo=UTC)
    return DurableRiskLot(
        follower_account="account-a",
        leader_wallet="0x1111111111111111111111111111111111111111",
        entry_fill_id="buy-fill-a",
        lot=FillRiskLot(
            lot_id="buy-fill-a",
            task_id="task-a",
            market_id="market-a",
            outcome_id="token-a",
            entry_price=Decimal("0.5"),
            original_shares=Decimal("15"),
            remaining_shares=Decimal("15"),
            opened_at=opened_at,
            peak_price=Decimal("0.5"),
        ),
        policy=ProtectiveRiskPolicy(stop_loss_percent=Decimal("10")),
        status=RiskLotStatus.OPEN,
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
async def test_existing_partial_entry_budget_can_be_marked_terminal_without_new_notional() -> None:
    """Settle a retained partial fill without fabricating another ledger row."""

    budgets = BudgetSpy()
    service = CopyTaskFillLifecycleService(
        task_budgets=budgets,
        reentry_locks=LockSpy(),
    )

    changed = await service.settle_entry_budget(
        reservation_id="intent-a",
        terminal_fill_id="order-a:5",
    )

    assert changed is True
    assert budgets.calls == [
        {
            "settle_reservation_id": "intent-a",
            "terminal_fill_id": "order-a:5",
        }
    ]


@pytest.mark.asyncio
async def test_entry_fill_creates_restart_safe_lot_from_same_fill_identity() -> None:
    """Persist protective state whenever authoritative BUY fill evidence arrives."""

    budgets = BudgetSpy()
    lots = LotSpy()
    service = CopyTaskFillLifecycleService(
        task_budgets=budgets,
        reentry_locks=LockSpy(),
        risk_lots=lots,
    )
    lot = entry_lot()

    result = await service.record_entry_fill(
        reservation_id="intent-a",
        fill_id="buy-fill-a",
        filled_notional=Decimal("7.5"),
        occurred_at=lot.lot.opened_at,
        risk_lot=lot,
    )

    assert result is True
    assert lots.calls == [lot]


@pytest.mark.asyncio
async def test_entry_fill_rejects_lot_with_different_fill_identity() -> None:
    """Prevent task accounting and protective lots from referring to different fills."""

    service = CopyTaskFillLifecycleService(
        task_budgets=BudgetSpy(),
        reentry_locks=LockSpy(),
        risk_lots=LotSpy(),
    )

    with pytest.raises(ValueError, match="entry_fill_id"):
        await service.record_entry_fill(
            reservation_id="intent-a",
            fill_id="different-fill",
            filled_notional=Decimal("7.5"),
            occurred_at=entry_lot().lot.opened_at,
            risk_lot=entry_lot(),
        )


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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "disposition",
    [
        TaskExecutionDisposition.SUBMITTED_UNCONFIRMED,
        TaskExecutionDisposition.ACCEPTED_PENDING,
    ],
)
async def test_ambiguous_or_accepted_execution_retains_budget(
    disposition: TaskExecutionDisposition,
) -> None:
    """Never release budget while venue settlement remains uncertain."""

    budgets = BudgetSpy()
    service = CopyTaskFillLifecycleService(task_budgets=budgets, reentry_locks=LockSpy())

    result = await service.apply_execution_outcome(
        reservation_id="reservation-a",
        outcome=TaskExecutionOutcome(disposition=disposition),
    )

    assert result.action is TaskBudgetLifecycleAction.RETAINED
    assert budgets.calls == [{"retain": "reservation-a"}]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "disposition",
    [
        TaskExecutionDisposition.PRE_POST_REJECTED,
        TaskExecutionDisposition.TERMINAL_NO_FILL,
        TaskExecutionDisposition.CONFIRMED_ABSENT,
    ],
)
async def test_definitive_no_order_or_no_fill_releases_budget(
    disposition: TaskExecutionDisposition,
) -> None:
    """Release only after definitive evidence that no spend remains pending."""

    budgets = BudgetSpy()
    service = CopyTaskFillLifecycleService(task_budgets=budgets, reentry_locks=LockSpy())

    result = await service.apply_execution_outcome(
        reservation_id="reservation-a",
        outcome=TaskExecutionOutcome(disposition=disposition),
    )

    assert result.action is TaskBudgetLifecycleAction.RELEASED
    assert budgets.calls == [{"release": "reservation-a"}]


@pytest.mark.asyncio
async def test_partial_fill_is_recorded_without_terminal_release() -> None:
    """Forward incremental fill identity and terminality without inference."""

    budgets = BudgetSpy()
    service = CopyTaskFillLifecycleService(task_budgets=budgets, reentry_locks=LockSpy())
    occurred_at = datetime(2026, 8, 8, 12, tzinfo=UTC)

    result = await service.apply_execution_outcome(
        reservation_id="reservation-a",
        outcome=TaskExecutionOutcome(
            disposition=TaskExecutionDisposition.PARTIAL_FILL,
            fill_id="fill-a",
            filled_notional=Decimal("2.5"),
            occurred_at=occurred_at,
        ),
    )

    assert result.action is TaskBudgetLifecycleAction.FILL_RECORDED
    assert budgets.calls == [
        {
            "reservation_id": "reservation-a",
            "fill_id": "fill-a",
            "filled_notional": Decimal("2.5"),
            "occurred_at": occurred_at,
            "terminal": False,
        }
    ]
