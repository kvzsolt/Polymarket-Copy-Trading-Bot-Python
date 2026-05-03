import pytest
import asyncio
from typing import Any, Dict
from decimal import Decimal
from datetime import datetime, timezone

from domain.order_lifecycle import OrderIntent, OrderIntentKey, IntentSide, IntentReason, OrderLifecycleStatus
from services.execution.coordinator import ExecutionCoordinator, VenueSubmitResult, RetryableVenueError
from services.execution.state_machine import assert_transition_allowed

class MockReservation:
    def __init__(self, res_id):
        self.id = res_id

class MockReservationService:
    def __init__(self):
        self.released = []

    async def reserve_for_intent(self, intent: OrderIntent):
        return MockReservation("res1")

    def release_terminal(self, reservation_id: str, reason: str):
        self.released.append((reservation_id, reason))


class MockOrder:
    def __init__(self, order_id):
        self.id = order_id


class MockOrdersRepo:
    def __init__(self):
        self.transitions = []

    async def create_from_intent(self, intent, reservation_id, status):
        return MockOrder("ord1")

    async def transition(self, order_id, from_status, to_status, venue_order_id=None, metadata=None):
        self.transitions.append((from_status, to_status))


class MockFillsRepo:
    async def persist_from_venue_result(self, order_id, venue_result):
        return {"id": "fill1", "order_id": order_id}


class MockPositionsRepo:
    def __init__(self):
        self.upserted = []

    def upsert_position(self, fill):
        self.upserted.append(fill)


class MockClobGateway:
    def __init__(self, result=None, exception=None):
        self.result = result
        self.exception = exception

    async def submit_order(self, intent: OrderIntent):
        if self.exception:
            raise self.exception
        return self.result


class MockReconciler:
    def __init__(self):
        self.enqueued = []

    async def enqueue(self, order_id: str):
        self.enqueued.append(order_id)


class MockAuditLogger:
    def __init__(self):
        self.events = []

    def log_event(self, event_name: str, payload: dict[str, Any]) -> None:
        self.events.append((event_name, payload))


def create_intent():
    return OrderIntent(
        key=OrderIntentKey(
            user_id="u1",
            leader_wallet="0xleader",
            condition_id="c1",
            token_id="t1",
            side=IntentSide.BUY,
            reason=IntentReason.COPY_BUY,
        ),
        size=Decimal("10"),
        price=Decimal("0.5"),
        max_slippage_bps=200,
        created_at=datetime.now(timezone.utc),
    )


@pytest.mark.asyncio
async def test_coordinator_retryable_failed():
    gateway = MockClobGateway(exception=RetryableVenueError("Timeout"))
    res_svc = MockReservationService()
    orders_repo = MockOrdersRepo()
    coordinator = ExecutionCoordinator(
        reservation_service=res_svc,
        orders_repo=orders_repo,
        fills_repo=MockFillsRepo(),
        positions_repo=MockPositionsRepo(),
        clob_gateway=gateway,
        reconciler=MockReconciler(),
        audit_logger=MockAuditLogger()
    )

    intent = create_intent()
    result = await coordinator.execute_intent(intent)

    assert result.status == OrderLifecycleStatus.RETRYABLE_FAILED
    assert result.error == "Timeout"
    
    # Verify transitions
    assert (OrderLifecycleStatus.RESERVED, OrderLifecycleStatus.SUBMITTING) in orders_repo.transitions
    assert (OrderLifecycleStatus.SUBMITTING, OrderLifecycleStatus.RETRYABLE_FAILED) in orders_repo.transitions
    
    # Reservation should NOT be released on retryable error
    assert len(res_svc.released) == 0


@pytest.mark.asyncio
async def test_coordinator_filled_sync():
    gateway = MockClobGateway(result=VenueSubmitResult(order_id="v1", status="filled"))
    res_svc = MockReservationService()
    orders_repo = MockOrdersRepo()
    pos_repo = MockPositionsRepo()
    
    coordinator = ExecutionCoordinator(
        reservation_service=res_svc,
        orders_repo=orders_repo,
        fills_repo=MockFillsRepo(),
        positions_repo=pos_repo,
        clob_gateway=gateway,
        reconciler=MockReconciler(),
        audit_logger=MockAuditLogger()
    )

    intent = create_intent()
    result = await coordinator.execute_intent(intent)

    assert result.status == OrderLifecycleStatus.FILLED
    assert len(res_svc.released) == 1
    assert res_svc.released[0] == ("res1", "filled")
    assert len(pos_repo.upserted) == 1
    assert (OrderLifecycleStatus.SUBMITTING, OrderLifecycleStatus.FILLED) in orders_repo.transitions


@pytest.mark.asyncio
async def test_coordinator_unconfirmed_enqueues_reconciliation():
    gateway = MockClobGateway(result=VenueSubmitResult(order_id="v1", status="live"))
    res_svc = MockReservationService()
    reconciler = MockReconciler()
    
    coordinator = ExecutionCoordinator(
        reservation_service=res_svc,
        orders_repo=MockOrdersRepo(),
        fills_repo=MockFillsRepo(),
        positions_repo=MockPositionsRepo(),
        clob_gateway=gateway,
        reconciler=reconciler,
        audit_logger=MockAuditLogger()
    )

    intent = create_intent()
    result = await coordinator.execute_intent(intent)

    assert result.status == OrderLifecycleStatus.SUBMITTED_UNCONFIRMED
    assert len(reconciler.enqueued) == 1
    assert reconciler.enqueued[0] == "ord1"
    
    # Reservation must NOT be released
    assert len(res_svc.released) == 0


@pytest.mark.asyncio
async def test_coordinator_terminal_failed_releases_reservation():
    gateway = MockClobGateway(result=VenueSubmitResult(order_id="v1", status="rejected"))
    res_svc = MockReservationService()
    reconciler = MockReconciler()
    
    coordinator = ExecutionCoordinator(
        reservation_service=res_svc,
        orders_repo=MockOrdersRepo(),
        fills_repo=MockFillsRepo(),
        positions_repo=MockPositionsRepo(),
        clob_gateway=gateway,
        reconciler=reconciler,
        audit_logger=MockAuditLogger()
    )

    intent = create_intent()
    result = await coordinator.execute_intent(intent)

    assert result.status == OrderLifecycleStatus.TERMINAL_FAILED
    assert len(reconciler.enqueued) == 0
    assert len(res_svc.released) == 1
    assert res_svc.released[0] == ("res1", "terminal_failed")
