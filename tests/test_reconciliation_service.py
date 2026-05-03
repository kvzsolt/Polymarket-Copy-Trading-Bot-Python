import pytest
import asyncio
from typing import Any

from domain.order_lifecycle import OrderLifecycleStatus
from services.execution.reconciliation_service import ReconciliationService, ReconciliationResult, OrderRecord


class MockOrder(OrderRecord):
    def __init__(self, order_id, venue_order_id, reservation_id, status):
        self.id = order_id
        self.venue_order_id = venue_order_id
        self.reservation_id = reservation_id
        self.status = status

class MockOrdersRepo:
    def __init__(self, order):
        self.order = order
        self.marked = []

    async def get(self, order_id: str) -> MockOrder:
        return self.order

    async def mark_submitted_unconfirmed(self, order_id: str) -> None:
        self.marked.append(("submitted_unconfirmed", order_id))

    async def mark_terminal(self, order_id: str, status: str) -> None:
        self.marked.append(("terminal", order_id, status))

    async def mark_reconciliation_required(self, order_id: str) -> None:
        self.marked.append(("reconciliation_required", order_id))

    async def mark_filled_or_partially_filled(self, order_id: str) -> None:
        self.marked.append(("filled", order_id))

class MockFillsRepo:
    async def upsert_venue_fill(self, order: MockOrder, venue_fill: Any) -> Any:
        return {"fill": venue_fill}

class MockPositionsRepo:
    def __init__(self):
        self.upserted = []
    def upsert_position(self, fill: Any) -> None:
        self.upserted.append(fill)

class MockReservationService:
    def __init__(self):
        self.released = []
    def release_terminal(self, reservation_id: str, reason: str) -> None:
        self.released.append((reservation_id, reason))

class MockVenueOrder:
    def __init__(self, status):
        self.status = status

class MockClobGateway:
    def __init__(self, order_status=None, fills=None, raise_exc=False):
        self.order_status = order_status
        self.fills = fills or []
        self.raise_exc = raise_exc

    async def get_order(self, venue_order_id: str) -> Any:
        if self.raise_exc:
            raise RuntimeError("API down")
        if self.order_status:
            return MockVenueOrder(self.order_status)
        return None

    async def get_fills(self, venue_order_id: str) -> list[Any]:
        if self.raise_exc:
            raise RuntimeError("API down")
        return self.fills

class MockTransaction:
    async def __aenter__(self): pass
    async def __aexit__(self, exc_type, exc_val, exc_tb): pass

class MockDbProvider:
    def transaction(self):
        return MockTransaction()

class MockAuditLogger:
    def __init__(self):
        self.events = []
    def log_event(self, event_name: str, payload: dict) -> None:
        self.events.append((event_name, payload))

def make_service(order, gateway) -> ReconciliationService:
    return ReconciliationService(
        orders_repo=MockOrdersRepo(order),
        fills_repo=MockFillsRepo(),
        positions_repo=MockPositionsRepo(),
        reservation_service=MockReservationService(),
        clob_gateway=gateway,
        db=MockDbProvider(),
        audit_logger=MockAuditLogger()
    )


@pytest.mark.asyncio
async def test_restart_with_pending_order_preserves_reservation():
    # If API says "live", it preserves reservation
    order = MockOrder("o1", "v1", "r1", "submitted_unconfirmed")
    gateway = MockClobGateway(order_status="live")
    svc = make_service(order, gateway)
    
    res = await svc.reconcile_order("o1")
    
    assert res.is_resolved is False
    assert res.status == OrderLifecycleStatus.SUBMITTED_UNCONFIRMED
    assert ("submitted_unconfirmed", "o1") in svc.orders_repo.marked
    assert len(svc.reservation_service.released) == 0

@pytest.mark.asyncio
async def test_venue_fill_resolves_unconfirmed_order():
    order = MockOrder("o1", "v1", "r1", "submitted_unconfirmed")
    gateway = MockClobGateway(order_status="matched", fills=[{"size": "10"}])
    svc = make_service(order, gateway)
    
    res = await svc.reconcile_order("o1")
    
    assert res.is_resolved is True
    assert res.status == OrderLifecycleStatus.FILLED
    assert ("filled", "o1") in svc.orders_repo.marked
    assert len(svc.positions_repo.upserted) == 1
    assert len(svc.reservation_service.released) == 1
    assert svc.reservation_service.released[0] == ("r1", "venue_fill_confirmed")

@pytest.mark.asyncio
async def test_venue_cancel_releases_reservation():
    order = MockOrder("o1", "v1", "r1", "submitted_unconfirmed")
    gateway = MockClobGateway(order_status="cancelled")
    svc = make_service(order, gateway)
    
    res = await svc.reconcile_order("o1")
    
    assert res.is_resolved is True
    assert res.status == OrderLifecycleStatus.CANCELLED
    assert ("terminal", "o1", "cancelled") in svc.orders_repo.marked
    assert len(svc.reservation_service.released) == 1
    assert svc.reservation_service.released[0] == ("r1", "cancelled")

@pytest.mark.asyncio
async def test_venue_unreachable_keeps_safe_reserved_state():
    order = MockOrder("o1", "v1", "r1", "submitted_unconfirmed")
    gateway = MockClobGateway(raise_exc=True)
    svc = make_service(order, gateway)
    
    res = await svc.reconcile_order("o1")
    
    assert res.is_resolved is False
    assert res.status == OrderLifecycleStatus.UNKNOWN_UNCONFIRMED
    assert len(svc.orders_repo.marked) == 0
    assert len(svc.reservation_service.released) == 0

@pytest.mark.asyncio
async def test_venue_unknown_requires_reconciliation():
    order = MockOrder("o1", "v1", "r1", "submitted_unconfirmed")
    gateway = MockClobGateway(order_status="what_is_this_status")
    svc = make_service(order, gateway)
    
    res = await svc.reconcile_order("o1")
    
    assert res.is_resolved is False
    assert res.status == OrderLifecycleStatus.RECONCILIATION_REQUIRED
    assert ("reconciliation_required", "o1") in svc.orders_repo.marked
    assert len(svc.reservation_service.released) == 0

@pytest.mark.asyncio
async def test_no_local_position_change_without_fill():
    order = MockOrder("o1", "v1", "r1", "submitted_unconfirmed")
    # Status is matched but no fills returned
    gateway = MockClobGateway(order_status="matched", fills=[])
    svc = make_service(order, gateway)
    
    res = await svc.reconcile_order("o1")
    
    assert res.is_resolved is False
    assert res.status == OrderLifecycleStatus.RECONCILIATION_REQUIRED
    assert len(svc.positions_repo.upserted) == 0
    assert len(svc.reservation_service.released) == 0
