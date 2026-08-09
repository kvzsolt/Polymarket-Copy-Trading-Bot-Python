"""Application lifecycle for entry fills and protective-exit evidence.

The service coordinates task budget commits and protective-exit lock creation
through narrow injected ports. It performs no venue or storage I/O directly
and never imports CCXT, the SDK, PostgreSQL, or HTTP clients.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Protocol

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


class ProtectiveExitRecorder(Protocol):
    """Small port for terminal protective-exit re-entry locks."""

    async def record_protective_exit(
        self,
        fill: ProtectiveExitFill,
    ) -> MarketReentryLockResult:
        """Record protective fill evidence."""


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
        enabled: bool = True,
    ) -> None:
        self._task_budgets = task_budgets
        self._reentry_locks = reentry_locks
        self._enabled = bool(enabled)

    async def record_entry_fill(
        self,
        *,
        reservation_id: str,
        fill_id: str,
        filled_notional: Decimal,
        occurred_at: datetime,
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
        return await self._task_budgets.commit(
            reservation_id=reservation_id,
            fill_id=fill_id,
            filled_notional=filled_notional,
            occurred_at=occurred_at,
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
    "ProtectiveExitRecorder",
    "TaskBudgetCommitter",
]