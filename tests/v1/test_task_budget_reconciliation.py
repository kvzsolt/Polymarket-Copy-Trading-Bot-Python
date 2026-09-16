"""Task-budget settlement contracts for canonical order reconciliation."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from services.copy_tasks.lifecycle import (
    TaskExecutionDisposition,
    TaskExecutionOutcome,
)
from services.copy_tasks.reconciliation import TaskBudgetReconciliationService
from services.execution.pending_order_reconciliation import (
    PendingOrderReconciliationQueryService,
)
from services.execution.reconciliation_service import ReconciliationService

_NOW = datetime(2026, 8, 11, 16, tzinfo=UTC)


class FixedClock:
    """Return one deterministic settlement instant."""

    def now(self) -> datetime:
        """Return the fixed UTC time."""

        return _NOW


class LifecycleRecorder:
    """Capture typed task-budget lifecycle evidence."""

    def __init__(self) -> None:
        self.outcomes: list[tuple[str, TaskExecutionOutcome]] = []
        self.settled: list[tuple[str, str]] = []

    async def apply_execution_outcome(
        self,
        *,
        reservation_id: str,
        outcome: TaskExecutionOutcome,
    ) -> object:
        """Record one partial, terminal, or no-fill outcome."""

        self.outcomes.append((reservation_id, outcome))
        return object()

    async def settle_entry_budget(
        self,
        *,
        reservation_id: str,
        terminal_fill_id: str,
    ) -> bool:
        """Record a terminal transition with no new fill delta."""

        self.settled.append((reservation_id, terminal_fill_id))
        return True


class OrderReader:
    """Return one persisted v1 order request."""

    async def get_by_order_id(self, order_id: str) -> dict[str, object]:
        """Return task-budget recovery metadata for the requested order."""

        assert order_id == "order-a"
        return {
            "request": {
                "side": "BUY",
                "recovery_metadata": {
                    "task_budget_reservation_id": "task-budget-a",
                },
            }
        }


@pytest.mark.asyncio
async def test_task_budget_reconciliation_emits_stable_partial_and_terminal_evidence() -> None:
    """Use cumulative order evidence for replay-safe task fill identities."""

    lifecycle = LifecycleRecorder()
    service = TaskBudgetReconciliationService(
        lifecycle=lifecycle,
        order_reader=OrderReader(),
        clock=FixedClock(),
    )

    await service.record_fill(
        record={"order_id": "order-a"},
        side="BUY",
        order_id="order-a",
        cumulative_matched=Decimal("4"),
        delta_matched=Decimal("4"),
        average_price=Decimal("0.50"),
        terminal=False,
    )
    await service.record_fill(
        record={"order_id": "order-a"},
        side="BUY",
        order_id="order-a",
        cumulative_matched=Decimal("5"),
        delta_matched=Decimal("1"),
        average_price=Decimal("0.60"),
        terminal=True,
    )

    assert [outcome.disposition for _, outcome in lifecycle.outcomes] == [
        TaskExecutionDisposition.PARTIAL_FILL,
        TaskExecutionDisposition.TERMINAL_FILL,
    ]
    assert lifecycle.outcomes[0][1].fill_id == "order-a:4"
    assert lifecycle.outcomes[0][1].filled_notional == Decimal("2.00")
    assert lifecycle.outcomes[1][1].fill_id == "order-a:5"
    assert lifecycle.outcomes[1][1].filled_notional == Decimal("0.60")
    assert all(outcome.occurred_at == _NOW for _, outcome in lifecycle.outcomes)


@pytest.mark.asyncio
async def test_task_budget_reconciliation_releases_no_fill_and_settles_existing_fill() -> None:
    """Resolve retained reservations for terminal evidence with or without delta."""

    lifecycle = LifecycleRecorder()
    service = TaskBudgetReconciliationService(
        lifecycle=lifecycle,
        order_reader=OrderReader(),
        clock=FixedClock(),
    )

    await service.record_terminal_no_fill(
        record={"order_id": "order-a"},
        side="BUY",
        order_id="order-a",
    )
    await service.settle_existing_fill(
        record={"order_id": "order-a"},
        side="BUY",
        order_id="order-a",
        cumulative_matched=Decimal("5"),
    )

    assert lifecycle.outcomes == [
        (
            "task-budget-a",
            TaskExecutionOutcome(TaskExecutionDisposition.TERMINAL_NO_FILL),
        )
    ]
    assert lifecycle.settled == [("task-budget-a", "order-a:5")]


class OrderedSettlementSink:
    """Capture reconciliation ordering relative to durable repositories."""

    def __init__(self, calls: list[str]) -> None:
        self._calls = calls

    async def record_terminal_no_fill(self, **_kwargs: object) -> None:
        """Record terminal no-fill task settlement."""

        self._calls.append("task_no_fill")

    async def record_fill(self, **_kwargs: object) -> None:
        """Record partial or terminal task fill settlement."""

        self._calls.append("task_fill")

    async def settle_existing_fill(self, **_kwargs: object) -> None:
        """Record terminal settlement without a new delta."""

        self._calls.append("task_existing_fill")


class ReconciliationOrders:
    """Minimal concrete-record order repository."""

    def __init__(self, calls: list[str]) -> None:
        self._calls = calls

    async def get_by_order_id(self, _order_id: str) -> dict[str, object]:
        """Return an order with no previous matched watermark."""

        return {"status": "submitted_unconfirmed", "response": {"size_matched": "0"}}

    async def delete_processed_trade(self, _trade_key: str) -> None:
        """Record terminal no-fill deletion."""

        self._calls.append("delete")

    async def update_submission_status(self, **_kwargs: object) -> None:
        """Record the durable order watermark update."""

        self._calls.append("watermark")

    async def mark_processed_trade(self, **_kwargs: object) -> None:
        """Record the terminal processed-trade transition."""

        self._calls.append("terminal")


class ReconciliationFills:
    """Persist one deterministic fill."""

    def __init__(self, calls: list[str]) -> None:
        self._calls = calls

    async def append_fill_and_apply(self, *_args: object, **_kwargs: object) -> tuple[int, None]:
        """Record durable fill insertion before task settlement."""

        self._calls.append("fill")
        return 1, None


class ReconciliationPositions:
    """Placeholder position repository applied by the fill adapter."""


def _record() -> dict[str, object]:
    return {
        "trade_key": "trade-a",
        "user_id": "user-a",
        "leader_id": "leader-a",
        "asset": "token-a",
        "condition_id": "condition-a",
        "order_id": "order-a",
        "request": {
            "recovery_metadata": {
                "task_budget_reservation_id": "task-budget-a",
            }
        },
    }


@pytest.mark.asyncio
async def test_canonical_reconciler_settles_task_no_fill_before_deleting_record() -> None:
    """Never lose the only durable link to a retained task reservation."""

    calls: list[str] = []
    service = ReconciliationService(
        orders_repo=ReconciliationOrders(calls),
        fills_repo=ReconciliationFills(calls),
        positions_repo=ReconciliationPositions(),
        task_budget_settlement=OrderedSettlementSink(calls),
    )

    applied = await service.reconcile_trade_record(
        _record(),
        "BUY",
        confirm_fill=lambda _order_id: _resolved({"status": "cancelled"}),
    )

    assert applied is True
    assert calls[:2] == ["task_no_fill", "delete"]


@pytest.mark.asyncio
async def test_canonical_reconciler_settles_task_fill_before_terminal_watermark() -> None:
    """Replay task settlement if a later order-state write crashes."""

    calls: list[str] = []
    service = ReconciliationService(
        orders_repo=ReconciliationOrders(calls),
        fills_repo=ReconciliationFills(calls),
        positions_repo=ReconciliationPositions(),
        task_budget_settlement=OrderedSettlementSink(calls),
    )

    applied = await service.reconcile_trade_record(
        _record(),
        "BUY",
        confirm_fill=lambda _order_id: _resolved(
            {"status": "confirmed", "size_matched": "2", "avg_price": "0.40"}
        ),
    )

    assert applied is True
    assert calls.index("fill") < calls.index("task_fill") < calls.index("terminal")


async def _resolved(value: dict[str, str]) -> dict[str, str]:
    """Return deterministic async venue evidence."""

    return value


class PendingOrders:
    """Return one orders-table-only v1 submission."""

    def __init__(self) -> None:
        self.calls: list[tuple[str | None, int]] = []

    async def get_unconfirmed_order_submissions(
        self,
        *,
        user_id: str | None,
        limit: int,
    ) -> list[dict[str, object]]:
        """Return one sanitized pending order."""

        self.calls.append((user_id, limit))
        return [
            {
                "idempotency_key": "copy-task:abc",
                "user_id": "user-a",
                "leader_id": "leader-a",
                "order_id": "order-a",
                "status": "submitted_unconfirmed",
                "request": {
                    "side": "BUY",
                    "signal_id": "source-a",
                    "token_id": "token-a",
                    "condition_id": "condition-a",
                    "recovery_metadata": {
                        "task_budget_reservation_id": "task-budget-a",
                    },
                },
            }
        ]


@pytest.mark.asyncio
async def test_pending_order_query_builds_bounded_reconciliation_records() -> None:
    """Include v1 orders rows even when no processed-trade row exists."""

    repository = PendingOrders()
    service = PendingOrderReconciliationQueryService(repository, max_batch_size=100)

    records = await service.list_records(user_id="user-a", limit=25)

    assert repository.calls == [("user-a", 25)]
    assert records == (
        (
            "BUY",
            {
                "trade_key": "source-a",
                "user_id": "user-a",
                "leader_id": "leader-a",
                "tx_hash": "source-a",
                "asset": "token-a",
                "condition_id": "condition-a",
                "order_id": "order-a",
                "request": {
                    "side": "BUY",
                    "signal_id": "source-a",
                    "token_id": "token-a",
                    "condition_id": "condition-a",
                    "recovery_metadata": {
                        "task_budget_reservation_id": "task-budget-a",
                    },
                },
                "status": "submitted_unconfirmed",
            },
        ),
    )

    with pytest.raises(ValueError, match="limit"):
        await service.list_records(user_id="user-a", limit=101)
