"""Application lifecycle for entry fills and protective-exit evidence.

The service coordinates task budget commits and protective-exit lock creation
through narrow injected ports. It performs no venue or storage I/O directly
and never imports CCXT, the SDK, PostgreSQL, or HTTP clients.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Protocol

from services.copy_tasks.lots import DurableRiskLot
from services.copy_tasks.reentry_lock import (
    MarketReentryLockResult,
    ProtectiveExitFill,
)


class TaskBudgetCommitter(Protocol):
    """Small port for committing follower BUY fills to task spend."""

    async def commit(
        self,
        *,
        reservation_id: str,
        fill_id: str,
        filled_notional: Decimal,
        occurred_at: datetime,
    ) -> bool:
        """Commit one authoritative BUY fill idempotently."""

        ...

    async def retain(self, reservation_id: str) -> bool:
        """Keep one accepted or ambiguous reservation non-expiring."""

        ...

    async def release(self, reservation_id: str) -> bool:
        """Release one definitively unspent reservation."""

        ...

    async def record_fill(
        self,
        *,
        reservation_id: str,
        fill_id: str,
        filled_notional: Decimal,
        occurred_at: datetime,
        terminal: bool,
    ) -> object:
        """Append one authoritative partial or terminal fill."""

        ...

    async def settle(
        self,
        reservation_id: str,
        *,
        terminal_fill_id: str,
    ) -> bool:
        """Mark existing partial evidence terminal without new notional."""

        ...


class ProtectiveExitRecorder(Protocol):
    """Small port for terminal protective-exit re-entry locks."""

    async def record_protective_exit(
        self,
        fill: ProtectiveExitFill,
    ) -> MarketReentryLockResult:
        """Record protective fill evidence."""

        ...


class EntryRiskLotRecorder(Protocol):
    """Small port for durable per-fill protective state."""

    async def create_from_entry_fill(self, lot: DurableRiskLot) -> bool:
        """Create one restart-safe lot exactly once."""

        ...


class TaskExecutionDisposition(StrEnum):
    """Typed execution evidence controlling task-budget settlement."""

    PRE_POST_REJECTED = "pre_post_rejected"
    TERMINAL_NO_FILL = "terminal_no_fill"
    SUBMITTED_UNCONFIRMED = "submitted_unconfirmed"
    ACCEPTED_PENDING = "accepted_pending"
    PARTIAL_FILL = "partial_fill"
    TERMINAL_FILL = "terminal_fill"
    CONFIRMED_ABSENT = "confirmed_absent"


class TaskBudgetLifecycleAction(StrEnum):
    """Auditable task-budget mutation selected from execution evidence."""

    RELEASED = "released"
    RETAINED = "retained"
    FILL_RECORDED = "fill_recorded"
    DISABLED = "disabled"


@dataclass(frozen=True, slots=True)
class TaskExecutionOutcome:
    """Venue-neutral authoritative outcome for one downstream submission."""

    disposition: TaskExecutionDisposition
    fill_id: str | None = None
    filled_notional: Decimal | None = None
    occurred_at: datetime | None = None
    reason: str | None = None

    def __post_init__(self) -> None:
        fill_dispositions = {
            TaskExecutionDisposition.PARTIAL_FILL,
            TaskExecutionDisposition.TERMINAL_FILL,
        }
        if self.disposition in fill_dispositions:
            if not str(self.fill_id or "").strip():
                raise ValueError("fill_id is required for a fill outcome")
            try:
                notional = Decimal(str(self.filled_notional))
            except (InvalidOperation, TypeError, ValueError) as exc:
                raise ValueError("filled_notional must be decimal-like") from exc
            if not notional.is_finite() or notional <= 0:
                raise ValueError("filled_notional must be finite and positive")
            if self.occurred_at is None or self.occurred_at.tzinfo is None:
                raise ValueError("occurred_at must be timezone-aware for a fill outcome")
            object.__setattr__(self, "fill_id", str(self.fill_id).strip())
            object.__setattr__(self, "filled_notional", notional)
            return
        if any(
            value is not None for value in (self.fill_id, self.filled_notional, self.occurred_at)
        ):
            raise ValueError("non-fill execution outcomes cannot carry fill evidence")


@dataclass(frozen=True, slots=True)
class TaskBudgetLifecycleResult:
    """Result of applying typed execution evidence to task accounting."""

    action: TaskBudgetLifecycleAction
    changed: bool
    repository_result: object | None = None


class CopyTaskFillLifecycleService:
    """Coordinate task accounting without importing venue or storage adapters.

    Attributes:
        task_budgets: Narrow port that commits follower BUY fills to task spend.
        reentry_locks: Narrow port that records terminal protective-exit locks.
        enabled: When False, both record paths short-circuit without side
            effects so the runtime can disable v1 lifecycle without unwiring
            collaborators.
    """

    def __init__(
        self,
        *,
        task_budgets: TaskBudgetCommitter,
        reentry_locks: ProtectiveExitRecorder,
        risk_lots: EntryRiskLotRecorder | None = None,
        enabled: bool = True,
    ) -> None:
        self._task_budgets = task_budgets
        self._reentry_locks = reentry_locks
        self._risk_lots = risk_lots
        self._enabled = bool(enabled)

    async def record_entry_fill(
        self,
        *,
        reservation_id: str,
        fill_id: str,
        filled_notional: Decimal,
        occurred_at: datetime,
        risk_lot: DurableRiskLot | None = None,
    ) -> bool:
        """Commit an authoritative follower BUY fill to the task ledger.

        Args:
            reservation_id: Task budget reservation identity.
            fill_id: Authoritative fill identity used for idempotency.
            filled_notional: Filled pUSD notional.
            occurred_at: Timezone-aware fill timestamp.

        Returns:
            ``True`` when a new ledger row was committed, ``False`` when the
            fill identity was already recorded or the lifecycle is disabled.
        """

        if not self._enabled:
            return False
        if risk_lot is not None:
            if risk_lot.entry_fill_id != str(fill_id):
                raise ValueError("risk lot entry_fill_id must equal fill_id")
            risk_lots = self._risk_lots
            if risk_lots is None:
                raise RuntimeError("risk lot service is required for entry lot persistence")
        else:
            risk_lots = None
        committed = await self._task_budgets.commit(
            reservation_id=reservation_id,
            fill_id=fill_id,
            filled_notional=filled_notional,
            occurred_at=occurred_at,
        )
        if risk_lot is not None and risk_lots is not None:
            await risk_lots.create_from_entry_fill(risk_lot)
        return committed

    async def apply_execution_outcome(
        self,
        *,
        reservation_id: str,
        outcome: TaskExecutionOutcome,
    ) -> TaskBudgetLifecycleResult:
        """Apply typed execution evidence without guessing from exceptions.

        Args:
            reservation_id: Durable task-budget reservation identity.
            outcome: Explicit venue-neutral submission or fill evidence.

        Returns:
            Auditable action and whether repository state changed.
        """

        identity = str(reservation_id or "").strip()
        if not identity:
            raise ValueError("reservation_id is required")
        if not self._enabled:
            return TaskBudgetLifecycleResult(
                action=TaskBudgetLifecycleAction.DISABLED,
                changed=False,
            )
        if outcome.disposition in {
            TaskExecutionDisposition.PRE_POST_REJECTED,
            TaskExecutionDisposition.TERMINAL_NO_FILL,
            TaskExecutionDisposition.CONFIRMED_ABSENT,
        }:
            changed = await self._task_budgets.release(identity)
            return TaskBudgetLifecycleResult(
                action=TaskBudgetLifecycleAction.RELEASED,
                changed=changed,
            )
        if outcome.disposition in {
            TaskExecutionDisposition.SUBMITTED_UNCONFIRMED,
            TaskExecutionDisposition.ACCEPTED_PENDING,
        }:
            changed = await self._task_budgets.retain(identity)
            return TaskBudgetLifecycleResult(
                action=TaskBudgetLifecycleAction.RETAINED,
                changed=changed,
            )
        repository_result = await self._task_budgets.record_fill(
            reservation_id=identity,
            fill_id=str(outcome.fill_id),
            filled_notional=Decimal(str(outcome.filled_notional)),
            occurred_at=outcome.occurred_at,  # type: ignore[arg-type]
            terminal=outcome.disposition is TaskExecutionDisposition.TERMINAL_FILL,
        )
        return TaskBudgetLifecycleResult(
            action=TaskBudgetLifecycleAction.FILL_RECORDED,
            changed=bool(getattr(repository_result, "created", True)),
            repository_result=repository_result,
        )

    async def settle_entry_budget(
        self,
        *,
        reservation_id: str,
        terminal_fill_id: str,
    ) -> bool:
        """Settle a retained partial entry without creating another fill."""

        if not self._enabled:
            return False
        return await self._task_budgets.settle(
            reservation_id,
            terminal_fill_id=terminal_fill_id,
        )

    async def record_protective_exit(
        self,
        fill: ProtectiveExitFill,
    ) -> MarketReentryLockResult:
        """Create a durable lock from authoritative terminal exit evidence.

        Args:
            fill: Protective-exit fill evidence.

        Returns:
            Idempotent lock result. When the lifecycle is disabled, returns a
            disabled result without invoking the lock recorder.
        """

        if not self._enabled:
            return MarketReentryLockResult(
                locked=False,
                created=False,
                reason="copy_task_fill_lifecycle_disabled",
            )
        return await self._reentry_locks.record_protective_exit(fill)


__all__ = [
    "CopyTaskFillLifecycleService",
    "EntryRiskLotRecorder",
    "ProtectiveExitRecorder",
    "TaskBudgetLifecycleAction",
    "TaskBudgetLifecycleResult",
    "TaskBudgetCommitter",
    "TaskExecutionDisposition",
    "TaskExecutionOutcome",
]
