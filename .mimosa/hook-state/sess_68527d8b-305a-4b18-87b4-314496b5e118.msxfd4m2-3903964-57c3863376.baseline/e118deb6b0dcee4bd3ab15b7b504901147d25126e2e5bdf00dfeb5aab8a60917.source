"""Execution result types — outcome of executing an OrderIntent through the coordinator."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from domain.order_lifecycle import OrderLifecycleStatus


@dataclass(frozen=True)
class ExecutionResult:
    """Outcome of executing an OrderIntent through the coordinator."""

    order_id: str
    status: OrderLifecycleStatus
    venue_order_id: str | None = None
    filled_amount: Decimal = Decimal("0")
    avg_price: Decimal = Decimal("0")
    error: str | None = None

    @property
    def is_filled(self) -> bool:
        return self.status == OrderLifecycleStatus.FILLED

    @property
    def is_terminal(self) -> bool:
        return self.status in {
            OrderLifecycleStatus.FILLED,
            OrderLifecycleStatus.CANCELLED,
            OrderLifecycleStatus.EXPIRED,
            OrderLifecycleStatus.REJECTED,
            OrderLifecycleStatus.TERMINAL_FAILED,
        }

    @classmethod
    def filled(
        cls,
        order_id: str,
        venue_order_id: str | None = None,
        filled_amount: Decimal = Decimal("0"),
        avg_price: Decimal = Decimal("0"),
    ) -> ExecutionResult:
        return cls(
            order_id=order_id,
            status=OrderLifecycleStatus.FILLED,
            venue_order_id=venue_order_id,
            filled_amount=filled_amount,
            avg_price=avg_price,
        )

    @classmethod
    def unconfirmed(
        cls,
        order_id: str,
        status: OrderLifecycleStatus,
        venue_order_id: str | None = None,
    ) -> ExecutionResult:
        return cls(
            order_id=order_id,
            status=status,
            venue_order_id=venue_order_id,
        )

    @classmethod
    def retryable_failed(cls, order_id: str, error: str) -> ExecutionResult:
        return cls(
            order_id=order_id,
            status=OrderLifecycleStatus.RETRYABLE_FAILED,
            error=error,
        )

    @classmethod
    def terminal_failed(cls, order_id: str, error: str) -> ExecutionResult:
        return cls(
            order_id=order_id,
            status=OrderLifecycleStatus.TERMINAL_FAILED,
            error=error,
        )
