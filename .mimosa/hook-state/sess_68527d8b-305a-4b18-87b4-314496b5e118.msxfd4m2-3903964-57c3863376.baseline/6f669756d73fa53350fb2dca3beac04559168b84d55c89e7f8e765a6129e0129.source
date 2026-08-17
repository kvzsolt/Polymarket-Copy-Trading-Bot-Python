"""Bridge authoritative order reconciliation into task-budget lifecycle state.

The service is venue-neutral. It translates cumulative order evidence into
typed task outcomes and delegates persistence to ``CopyTaskFillLifecycleService``.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from decimal import Decimal
from typing import Protocol

from services.copy_tasks.lifecycle import (
    TaskExecutionDisposition,
    TaskExecutionOutcome,
)


class Clock(Protocol):
    """Supply deterministic timezone-aware settlement time."""

    def now(self) -> datetime:
        """Return the current instant."""

        ...


class SystemClock:
    """Production UTC settlement clock."""

    def now(self) -> datetime:
        """Return the current UTC instant."""

        return datetime.now(UTC)


class TaskBudgetLifecycle(Protocol):
    """Narrow task-budget mutation surface used by reconciliation."""

    async def apply_execution_outcome(
        self,
        *,
        reservation_id: str,
        outcome: TaskExecutionOutcome,
    ) -> object:
        """Apply typed fill or no-fill evidence."""

        ...

    async def settle_entry_budget(
        self,
        *,
        reservation_id: str,
        terminal_fill_id: str,
    ) -> bool:
        """Settle an existing partial fill without adding notional."""

        ...


class OrderReader(Protocol):
    """Read persisted sanitized order request metadata."""

    async def get_by_order_id(self, order_id: str) -> Mapping[str, object] | None:
        """Return one order or ``None`` when unavailable."""

        ...


class TaskBudgetReconciliationService:
    """Settle retained task reservations from authoritative order evidence."""

    def __init__(
        self,
        *,
        lifecycle: TaskBudgetLifecycle,
        order_reader: OrderReader,
        clock: Clock | None = None,
    ) -> None:
        self._lifecycle = lifecycle
        self._order_reader = order_reader
        self._clock = clock or SystemClock()

    async def record_terminal_no_fill(
        self,
        *,
        record: Mapping[str, object],
        side: str,
        order_id: str,
    ) -> bool:
        """Release a retained BUY reservation after definitive no-fill evidence."""

        reservation_id = await self._reservation_id(record, side=side, order_id=order_id)
        if reservation_id is None:
            return False
        await self._lifecycle.apply_execution_outcome(
            reservation_id=reservation_id,
            outcome=TaskExecutionOutcome(TaskExecutionDisposition.TERMINAL_NO_FILL),
        )
        return True

    async def record_fill(
        self,
        *,
        record: Mapping[str, object],
        side: str,
        order_id: str,
        cumulative_matched: Decimal,
        delta_matched: Decimal,
        average_price: Decimal,
        terminal: bool,
    ) -> bool:
        """Append one replay-safe partial or terminal BUY fill notional."""

        reservation_id = await self._reservation_id(record, side=side, order_id=order_id)
        if reservation_id is None:
            return False
        cumulative = self._positive(cumulative_matched, name="cumulative_matched")
        delta = self._positive(delta_matched, name="delta_matched")
        price = self._positive(average_price, name="average_price")
        now = self._clock.now()
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("task budget reconciliation clock must be timezone-aware")
        await self._lifecycle.apply_execution_outcome(
            reservation_id=reservation_id,
            outcome=TaskExecutionOutcome(
                TaskExecutionDisposition.TERMINAL_FILL
                if terminal
                else TaskExecutionDisposition.PARTIAL_FILL,
                fill_id=self._fill_id(order_id, cumulative),
                filled_notional=delta * price,
                occurred_at=now.astimezone(UTC),
            ),
        )
        return True

    async def settle_existing_fill(
        self,
        *,
        record: Mapping[str, object],
        side: str,
        order_id: str,
        cumulative_matched: Decimal,
    ) -> bool:
        """Mark prior partial evidence terminal without fabricating a new fill."""

        reservation_id = await self._reservation_id(record, side=side, order_id=order_id)
        if reservation_id is None:
            return False
        cumulative = self._positive(cumulative_matched, name="cumulative_matched")
        return await self._lifecycle.settle_entry_budget(
            reservation_id=reservation_id,
            terminal_fill_id=self._fill_id(order_id, cumulative),
        )

    async def _reservation_id(
        self,
        record: Mapping[str, object],
        *,
        side: str,
        order_id: str,
    ) -> str | None:
        if str(side).strip().upper() != "BUY":
            return None
        identity = self._from_record(record)
        if identity:
            return identity
        persisted = await self._order_reader.get_by_order_id(str(order_id))
        return self._from_record(persisted or {}) or None

    @staticmethod
    def _from_record(record: Mapping[str, object]) -> str:
        request = record.get("request")
        if not isinstance(request, Mapping):
            return ""
        recovery = request.get("recovery_metadata")
        if not isinstance(recovery, Mapping):
            return ""
        return str(recovery.get("task_budget_reservation_id") or "").strip()

    @staticmethod
    def _fill_id(order_id: str, cumulative_matched: Decimal) -> str:
        identity = str(order_id).strip()
        if not identity:
            raise ValueError("order_id is required")
        return f"{identity}:{format(cumulative_matched.normalize(), 'f')}"

    @staticmethod
    def _positive(value: Decimal, *, name: str) -> Decimal:
        result = Decimal(str(value))
        if not result.is_finite() or result <= 0:
            raise ValueError(f"{name} must be finite and positive")
        return result


__all__ = ["TaskBudgetReconciliationService"]
