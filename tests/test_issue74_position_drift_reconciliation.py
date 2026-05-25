"""Issue #74: Position drift reconciliation when SELL fails with insufficient_balance.

Tests cover:
1. ReconciliationService.reconcile_position_drift() — delete, shrink, no-drift, fetch-failed
2. ExecutionCoordinator — SELL insufficient_balance triggers drift reconciliation
3. RiskMonitor — risk SELL insufficient_balance reconciles and skips risk block
4. Audit event manual_position_drift_reconciled emitted
"""

from __future__ import annotations
import pytest
pytest.skip("sqlite-bound; pending PG port", allow_module_level=True)


import pytest
from decimal import Decimal
from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock

from domain.order_lifecycle import (
    OrderIntent,
    OrderIntentKey,
    IntentSide,
    IntentReason,
    OrderLifecycleStatus,
)
from integrations.polymarket.errors import TerminalVenueError
from services.execution.coordinator import (
    ExecutionCoordinator,
    _is_insufficient_balance_error,
)
from services.execution.reconciliation_service import ReconciliationService


FAKE_TOKEN_ID = "71321045679252212594626385532706912750332728571942532223535764789812338637396"


class MockPositionsRepo:
    def __init__(self):
        self.positions = {}

    def upsert_position(self, record):
        key = (record.user_id, record.condition_id, getattr(record, "token_id", ""))
        self.positions[key] = record

    def get_position(self, user_id, condition_id, *, token_id=""):
        return self.positions.get((user_id, condition_id, token_id))

    def delete_position(self, user_id, condition_id, *, token_id=""):
        self.positions.pop((user_id, condition_id, token_id), None)


class MockAuditLogger:
    def __init__(self):
        self.events = []

    def log_event(self, event_name: str, payload: dict[str, Any]) -> None:
        self.events.append((event_name, payload))


class MockReservationService:
    def __init__(self):
        self.released = []

    async def reserve_for_intent(self, intent):
        return MockReservation("res1")

    def release_terminal(self, reservation_id: str, reason: str):
        self.released.append((reservation_id, reason))


class MockReservation:
    def __init__(self, res_id):
        self.id = res_id


class MockOrder:
    def __init__(self, order_id):
        self.id = order_id


class MockOrdersRepo:
    def __init__(self):
        self.transitions = []
        self._link_reservation_order_called = []

    async def create_from_intent(self, intent, reservation_id, status):
        return MockOrder("ord1")

    async def transition(self, order_id, from_status, to_status, venue_order_id=None, metadata=None):
        self.transitions.append((from_status, to_status))


class MockFillsRepo:
    async def persist_from_venue_result(self, order_id, venue_result):
        return {"id": "fill1"}


class MockClobGateway:
    def __init__(self, result=None, exception=None):
        self.result = result
        self.exception = exception

    async def submit_order(self, intent):
        if self.exception:
            raise self.exception
        return self.result


class MockReconciler:
    def __init__(self):
        self.enqueued = []

    async def enqueue(self, order_id: str):
        self.enqueued.append(order_id)


# ---------------------------------------------------------------------------
# ReconciliationService.reconcile_position_drift tests
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_reconcile_drift_deletes_position_when_live_is_zero():
    pos_repo = MockPositionsRepo()
    audit = MockAuditLogger()
    svc = ReconciliationService(
        orders_repo=MagicMock(),
        fills_repo=MagicMock(),
        positions_repo=pos_repo,
        audit_logger=audit,
    )

    from storage.sqlite.positions_repo import PositionRecord
    pos_repo.upsert_position(PositionRecord(
        user_id="u1", condition_id="c1", token_id="t1",
        leader_id="leader1", size_tokens=Decimal("50"),
        avg_entry_price=Decimal("0.45"),
    ))

    fetcher = AsyncMock(return_value=Decimal("0"))
    result = await svc.reconcile_position_drift(
        user_id="u1",
        condition_id="c1",
        token_id="t1",
        leader_id="leader1",
        live_position_fetcher=fetcher,
    )

    assert result["reconciled"] is True
    assert result["action"] == "deleted"
    assert pos_repo.get_position("u1", "c1", token_id="t1") is None
    assert any(e[0] == "manual_position_drift_reconciled" for e in audit.events)


@pytest.mark.asyncio
async def test_reconcile_drift_shrinks_position_when_live_is_smaller():
    pos_repo = MockPositionsRepo()
    audit = MockAuditLogger()
    svc = ReconciliationService(
        orders_repo=MagicMock(),
        fills_repo=MagicMock(),
        positions_repo=pos_repo,
        audit_logger=audit,
    )

    from storage.sqlite.positions_repo import PositionRecord
    pos_repo.upsert_position(PositionRecord(
        user_id="u1", condition_id="c1", token_id="t1",
        leader_id="leader1", size_tokens=Decimal("100"),
        avg_entry_price=Decimal("0.50"),
    ))

    fetcher = AsyncMock(return_value=Decimal("25"))
    result = await svc.reconcile_position_drift(
        user_id="u1",
        condition_id="c1",
        token_id="t1",
        leader_id="leader1",
        live_position_fetcher=fetcher,
    )

    assert result["reconciled"] is True
    assert result["action"] == "shrunk"
    pos = pos_repo.get_position("u1", "c1", token_id="t1")
    assert pos is not None
    assert pos.size_tokens == Decimal("25")


@pytest.mark.asyncio
async def test_reconcile_drift_no_drift_when_sizes_match():
    pos_repo = MockPositionsRepo()
    audit = MockAuditLogger()
    svc = ReconciliationService(
        orders_repo=MagicMock(),
        fills_repo=MagicMock(),
        positions_repo=pos_repo,
        audit_logger=audit,
    )

    from storage.sqlite.positions_repo import PositionRecord
    pos_repo.upsert_position(PositionRecord(
        user_id="u1", condition_id="c1", token_id="t1",
        leader_id="leader1", size_tokens=Decimal("50"),
        avg_entry_price=Decimal("0.50"),
    ))

    fetcher = AsyncMock(return_value=Decimal("50"))
    result = await svc.reconcile_position_drift(
        user_id="u1",
        condition_id="c1",
        token_id="t1",
        leader_id="leader1",
        live_position_fetcher=fetcher,
    )

    assert result["reconciled"] is False
    assert result["action"] == "no_drift"
    assert not any(e[0] == "manual_position_drift_reconciled" for e in audit.events)


@pytest.mark.asyncio
async def test_reconcile_drift_fetch_failed():
    pos_repo = MockPositionsRepo()
    audit = MockAuditLogger()
    svc = ReconciliationService(
        orders_repo=MagicMock(),
        fills_repo=MagicMock(),
        positions_repo=pos_repo,
        audit_logger=audit,
    )

    fetcher = AsyncMock(side_effect=RuntimeError("network error"))
    result = await svc.reconcile_position_drift(
        user_id="u1",
        condition_id="c1",
        token_id="t1",
        leader_id="leader1",
        live_position_fetcher=fetcher,
    )

    assert result["reconciled"] is False
    assert result["action"] == "fetch_failed"
    assert any(e[0] == "manual_position_drift_fetch_failed" for e in audit.events)


# ---------------------------------------------------------------------------
# _is_insufficient_balance_error helper
# ---------------------------------------------------------------------------
def test_insufficient_balance_detection():
    assert _is_insufficient_balance_error("insufficient_balance | not enough balance")
    assert _is_insufficient_balance_error("Error: insufficient balance for order")
    assert _is_insufficient_balance_error("not enough balance / allowance")
    assert not _is_insufficient_balance_error("rate limit exceeded")
    assert not _is_insufficient_balance_error("timeout after submit")


# ---------------------------------------------------------------------------
# ExecutionCoordinator drift reconciliation on SELL insufficient_balance
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_coordinator_sell_insufficient_balance_triggers_drift_reconciliation():
    res_svc = MockReservationService()
    pos_repo = MockPositionsRepo()
    audit = MockAuditLogger()
    reconciliation_svc = ReconciliationService(
        orders_repo=MagicMock(),
        fills_repo=MagicMock(),
        positions_repo=pos_repo,
        audit_logger=audit,
    )

    from storage.sqlite.positions_repo import PositionRecord
    pos_repo.upsert_position(PositionRecord(
        user_id="u1", condition_id="c1", token_id="t1",
        leader_id="0xleader", size_tokens=Decimal("50"),
        avg_entry_price=Decimal("0.50"),
    ))

    async def mock_live_fetcher(user_id, token_id):
        return Decimal("0")

    gateway = MockClobGateway(
        exception=TerminalVenueError("insufficient_balance | not enough balance"),
    )
    coordinator = ExecutionCoordinator(
        reservation_service=res_svc,
        orders_repo=MockOrdersRepo(),
        fills_repo=MockFillsRepo(),
        positions_repo=pos_repo,
        clob_gateway=gateway,
        reconciler=MockReconciler(),
        audit_logger=audit,
        reconciliation_service=reconciliation_svc,
        live_position_fetcher=mock_live_fetcher,
    )

    sell_intent = OrderIntent(
        key=OrderIntentKey(
            user_id="u1",
            leader_wallet="0xleader",
            condition_id="c1",
            token_id="t1",
            side=IntentSide.SELL,
            reason=IntentReason.RISK_EXIT,
        ),
        size=Decimal("50"),
        price=Decimal("0.50"),
        max_slippage_bps=200,
        created_at=datetime.now(timezone.utc),
    )

    with pytest.raises(TerminalVenueError):
        await coordinator.execute_intent(sell_intent)

    assert pos_repo.get_position("u1", "c1", token_id="t1") is None
    assert any(e[0] == "manual_position_drift_reconciled" for e in audit.events)
    assert len(res_svc.released) == 1


@pytest.mark.asyncio
async def test_coordinator_buy_insufficient_balance_does_not_trigger_drift():
    res_svc = MockReservationService()
    pos_repo = MockPositionsRepo()
    audit = MockAuditLogger()
    reconciliation_svc = ReconciliationService(
        orders_repo=MagicMock(),
        fills_repo=MagicMock(),
        positions_repo=pos_repo,
        audit_logger=audit,
    )

    async def mock_live_fetcher(user_id, token_id):
        return Decimal("0")

    gateway = MockClobGateway(
        exception=TerminalVenueError("insufficient_balance | not enough balance"),
    )
    coordinator = ExecutionCoordinator(
        reservation_service=res_svc,
        orders_repo=MockOrdersRepo(),
        fills_repo=MockFillsRepo(),
        positions_repo=pos_repo,
        clob_gateway=gateway,
        reconciler=MockReconciler(),
        audit_logger=audit,
        reconciliation_service=reconciliation_svc,
        live_position_fetcher=mock_live_fetcher,
    )

    buy_intent = OrderIntent(
        key=OrderIntentKey(
            user_id="u1",
            leader_wallet="0xleader",
            condition_id="c1",
            token_id="t1",
            side=IntentSide.BUY,
            reason=IntentReason.COPY_BUY,
        ),
        size=Decimal("50"),
        price=Decimal("0.50"),
        max_slippage_bps=200,
        created_at=datetime.now(timezone.utc),
    )

    with pytest.raises(TerminalVenueError):
        await coordinator.execute_intent(buy_intent)

    assert not any(e[0] == "manual_position_drift_reconciled" for e in audit.events)


@pytest.mark.asyncio
async def test_coordinator_sell_non_drift_error_does_not_trigger_reconciliation():
    res_svc = MockReservationService()
    pos_repo = MockPositionsRepo()
    audit = MockAuditLogger()
    reconciliation_svc = ReconciliationService(
        orders_repo=MagicMock(),
        fills_repo=MagicMock(),
        positions_repo=pos_repo,
        audit_logger=audit,
    )

    async def mock_live_fetcher(user_id, token_id):
        return Decimal("0")

    gateway = MockClobGateway(
        exception=TerminalVenueError("invalid order: tick size violation"),
    )
    coordinator = ExecutionCoordinator(
        reservation_service=res_svc,
        orders_repo=MockOrdersRepo(),
        fills_repo=MockFillsRepo(),
        positions_repo=pos_repo,
        clob_gateway=gateway,
        reconciler=MockReconciler(),
        audit_logger=audit,
        reconciliation_service=reconciliation_svc,
        live_position_fetcher=mock_live_fetcher,
    )

    sell_intent = OrderIntent(
        key=OrderIntentKey(
            user_id="u1",
            leader_wallet="0xleader",
            condition_id="c1",
            token_id="t1",
            side=IntentSide.SELL,
            reason=IntentReason.COPY_SELL,
        ),
        size=Decimal("50"),
        price=Decimal("0.50"),
        max_slippage_bps=200,
        created_at=datetime.now(timezone.utc),
    )

    with pytest.raises(TerminalVenueError):
        await coordinator.execute_intent(sell_intent)

    assert not any(e[0] == "manual_position_drift_reconciled" for e in audit.events)


@pytest.mark.asyncio
async def test_coordinator_drift_reconciliation_failure_does_not_break_flow():
    res_svc = MockReservationService()
    pos_repo = MockPositionsRepo()
    audit = MockAuditLogger()

    reconciliation_svc = MagicMock()
    reconciliation_svc.reconcile_position_drift = AsyncMock(
        side_effect=RuntimeError("db error"),
    )

    async def mock_live_fetcher(user_id, token_id):
        return Decimal("0")

    gateway = MockClobGateway(
        exception=TerminalVenueError("insufficient_balance | not enough balance"),
    )
    coordinator = ExecutionCoordinator(
        reservation_service=res_svc,
        orders_repo=MockOrdersRepo(),
        fills_repo=MockFillsRepo(),
        positions_repo=pos_repo,
        clob_gateway=gateway,
        reconciler=MockReconciler(),
        audit_logger=audit,
        reconciliation_service=reconciliation_svc,
        live_position_fetcher=mock_live_fetcher,
    )

    sell_intent = OrderIntent(
        key=OrderIntentKey(
            user_id="u1",
            leader_wallet="0xleader",
            condition_id="c1",
            token_id="t1",
            side=IntentSide.SELL,
            reason=IntentReason.RISK_EXIT,
        ),
        size=Decimal("50"),
        price=Decimal("0.50"),
        max_slippage_bps=200,
        created_at=datetime.now(timezone.utc),
    )

    with pytest.raises(TerminalVenueError):
        await coordinator.execute_intent(sell_intent)

    assert len(res_svc.released) == 1


# ---------------------------------------------------------------------------
# Subsequent SELL uses updated live size
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_reconcile_drift_subsequent_sell_uses_updated_size():
    pos_repo = MockPositionsRepo()
    audit = MockAuditLogger()
    svc = ReconciliationService(
        orders_repo=MagicMock(),
        fills_repo=MagicMock(),
        positions_repo=pos_repo,
        audit_logger=audit,
    )

    from storage.sqlite.positions_repo import PositionRecord
    pos_repo.upsert_position(PositionRecord(
        user_id="u1", condition_id="c1", token_id="t1",
        leader_id="leader1", size_tokens=Decimal("100"),
        avg_entry_price=Decimal("0.50"),
    ))

    fetcher = AsyncMock(return_value=Decimal("30"))
    result = await svc.reconcile_position_drift(
        user_id="u1",
        condition_id="c1",
        token_id="t1",
        leader_id="leader1",
        live_position_fetcher=fetcher,
    )

    assert result["reconciled"] is True
    assert result["action"] == "shrunk"

    pos = pos_repo.get_position("u1", "c1", token_id="t1")
    assert pos is not None
    assert pos.size_tokens == Decimal("30")

    assert result["live_size"] == Decimal("30")


# ---------------------------------------------------------------------------
# ReconciliationService with no audit logger does not crash
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_reconcile_drift_works_without_audit_logger():
    pos_repo = MockPositionsRepo()
    svc = ReconciliationService(
        orders_repo=MagicMock(),
        fills_repo=MagicMock(),
        positions_repo=pos_repo,
        audit_logger=None,
    )

    from storage.sqlite.positions_repo import PositionRecord
    pos_repo.upsert_position(PositionRecord(
        user_id="u1", condition_id="c1", token_id="t1",
        leader_id="leader1", size_tokens=Decimal("50"),
        avg_entry_price=Decimal("0.50"),
    ))

    fetcher = AsyncMock(return_value=Decimal("0"))
    result = await svc.reconcile_position_drift(
        user_id="u1",
        condition_id="c1",
        token_id="t1",
        leader_id="leader1",
        live_position_fetcher=fetcher,
    )

    assert result["reconciled"] is True
    assert result["action"] == "deleted"
    assert pos_repo.get_position("u1", "c1", token_id="t1") is None


# ---------------------------------------------------------------------------
# Allowance-only error triggers drift reconciliation
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_coordinator_sell_allowance_only_triggers_drift():
    """Allowance-only error (without insufficient_balance) should trigger drift reconciliation."""
    res_svc = MockReservationService()
    pos_repo = MockPositionsRepo()
    audit = MockAuditLogger()
    reconciliation_svc = ReconciliationService(
        orders_repo=MagicMock(),
        fills_repo=MagicMock(),
        positions_repo=pos_repo,
        audit_logger=audit,
    )

    from storage.sqlite.positions_repo import PositionRecord
    pos_repo.upsert_position(PositionRecord(
        user_id="u1",
        condition_id="c1",
        token_id="t1",
        leader_id="leader1",
        size_tokens=Decimal("100"),
        avg_entry_price=Decimal("0.50"),
    ))

    async def mock_live_fetcher(user_id, token_id):
        return Decimal("0")

    gateway = MockClobGateway(
        exception=TerminalVenueError("allowance exceeded for this token"),
    )
    coordinator = ExecutionCoordinator(
        reservation_service=res_svc,
        orders_repo=MockOrdersRepo(),
        fills_repo=MockFillsRepo(),
        positions_repo=pos_repo,
        clob_gateway=gateway,
        reconciler=MockReconciler(),
        audit_logger=audit,
        reconciliation_service=reconciliation_svc,
        live_position_fetcher=mock_live_fetcher,
    )

    sell_intent = OrderIntent(
        key=OrderIntentKey(
            user_id="u1",
            leader_wallet="0xleader",
            condition_id="c1",
            token_id="t1",
            side=IntentSide.SELL,
            reason=IntentReason.RISK_EXIT,
        ),
        size=Decimal("50"),
        price=Decimal("0.50"),
        max_slippage_bps=200,
        created_at=datetime.now(timezone.utc),
    )

    with pytest.raises(TerminalVenueError):
        await coordinator.execute_intent(sell_intent)

    assert pos_repo.get_position("u1", "c1", token_id="t1") is None
    assert any(e[0] == "manual_position_drift_reconciled" for e in audit.events)
    assert len(res_svc.released) == 1


# ---------------------------------------------------------------------------
# No 3x retry for insufficient-balance TerminalVenueError
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_insufficient_balance_terminal_error_not_retryable():
    """TerminalVenueError with insufficient_balance is not retried."""
    res_svc = MockReservationService()
    pos_repo = MockPositionsRepo()
    audit = MockAuditLogger()
    reconciliation_svc = ReconciliationService(
        orders_repo=MagicMock(),
        fills_repo=MagicMock(),
        positions_repo=pos_repo,
        audit_logger=audit,
    )

    async def mock_live_fetcher(user_id, token_id):
        return Decimal("0")

    gateway = MockClobGateway(
        exception=TerminalVenueError("insufficient_balance"),
    )
    orders_repo = MockOrdersRepo()
    coordinator = ExecutionCoordinator(
        reservation_service=res_svc,
        orders_repo=orders_repo,
        fills_repo=MockFillsRepo(),
        positions_repo=pos_repo,
        clob_gateway=gateway,
        reconciler=MockReconciler(),
        audit_logger=audit,
        reconciliation_service=reconciliation_svc,
        live_position_fetcher=mock_live_fetcher,
    )

    sell_intent = OrderIntent(
        key=OrderIntentKey(
            user_id="u1",
            leader_wallet="0xleader",
            condition_id="c1",
            token_id="t1",
            side=IntentSide.SELL,
            reason=IntentReason.RISK_EXIT,
        ),
        size=Decimal("50"),
        price=Decimal("0.50"),
        max_slippage_bps=200,
        created_at=datetime.now(timezone.utc),
    )

    with pytest.raises(TerminalVenueError):
        await coordinator.execute_intent(sell_intent)

    assert any(t[1] == OrderLifecycleStatus.TERMINAL_FAILED for t in orders_repo.transitions)
    assert not any(t[1] == OrderLifecycleStatus.RETRYABLE_FAILED for t in orders_repo.transitions)


# ---------------------------------------------------------------------------
# Reservation cleanup on delete path
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_reconcile_drift_delete_path_cleans_reservations():
    """When drift results in deletion (live_size=0), active SELL reservations are released."""
    from domain.order_lifecycle import IntentSide, IntentReason
    
    pos_repo = MockPositionsRepo()
    audit = MockAuditLogger()
    
    class MockReservationsRepo:
        def __init__(self):
            self.released = []
        
        def list_active_for_scope(self, user_id, condition_id, token_id):
            from domain.order_lifecycle import Reservation
            from datetime import datetime, timezone
            
            return [
                Reservation(
                    id="res1",
                    user_id=user_id,
                    intent_key=f"{user_id}_{condition_id}_{token_id}_SELL_EXIT",
                    condition_id=condition_id,
                    token_id=token_id,
                    side=IntentSide.SELL,
                    reason=IntentReason.RISK_EXIT,
                    reserved_size=Decimal("100"),
                    reserved_usdc=Decimal("50"),
                    status="active",
                    created_at=datetime.now(timezone.utc),
                    expires_at=datetime.now(timezone.utc),
                ),
                Reservation(
                    id="res2",
                    user_id=user_id,
                    intent_key=f"{user_id}_{condition_id}_{token_id}_SELL_EXIT",
                    condition_id=condition_id,
                    token_id=token_id,
                    side=IntentSide.SELL,
                    reason=IntentReason.RISK_EXIT,
                    reserved_size=Decimal("25"),
                    reserved_usdc=Decimal("12.50"),
                    status="active",
                    created_at=datetime.now(timezone.utc),
                    expires_at=datetime.now(timezone.utc),
                ),
            ]
        
        def mark_released(self, reservation_id, reason, released_at):
            self.released.append((reservation_id, reason))
    
    class MockReservationServiceWithRepo:
        def __init__(self):
            self.reservations_repo = MockReservationsRepo()
            self.release_calls = []
        
        def release_terminal(self, reservation_id, reason):
            self.release_calls.append((reservation_id, reason))
            self.reservations_repo.mark_released(reservation_id, reason, 0)
    
    res_svc = MockReservationServiceWithRepo()
    svc = ReconciliationService(
        orders_repo=MagicMock(),
        fills_repo=MagicMock(),
        positions_repo=pos_repo,
        audit_logger=audit,
    )

    from storage.sqlite.positions_repo import PositionRecord
    pos_repo.upsert_position(PositionRecord(
        user_id="u1", condition_id="c1", token_id="t1",
        leader_id="leader1", size_tokens=Decimal("100"),
        avg_entry_price=Decimal("0.50"),
    ))

    fetcher = AsyncMock(return_value=Decimal("0"))
    result = await svc.reconcile_position_drift(
        user_id="u1",
        condition_id="c1",
        token_id="t1",
        leader_id="leader1",
        live_position_fetcher=fetcher,
        reservation_service=res_svc,
    )

    assert result["reconciled"] is True
    assert result["action"] == "deleted"
    assert pos_repo.get_position("u1", "c1", token_id="t1") is None
    assert len(res_svc.reservations_repo.released) == 2
    assert len(res_svc.release_calls) == 2


# ---------------------------------------------------------------------------
# Reservation cleanup on shrink path
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_reconcile_drift_shrink_path_coversized_reservations():
    """When drift results in shrink, SELL reservations larger than new position are released."""
    from domain.order_lifecycle import IntentSide, IntentReason
    
    pos_repo = MockPositionsRepo()
    audit = MockAuditLogger()
    
    class MockReservationsRepo:
        def __init__(self):
            self.released = []
        
        def list_active_for_scope(self, user_id, condition_id, token_id):
            from domain.order_lifecycle import Reservation
            from datetime import datetime, timezone
            
            return [
                Reservation(
                    id="res1",
                    user_id=user_id,
                    intent_key=f"{user_id}_{condition_id}_{token_id}_SELL_EXIT",
                    condition_id=condition_id,
                    token_id=token_id,
                    side=IntentSide.SELL,
                    reason=IntentReason.RISK_EXIT,
                    reserved_size=Decimal("50"),
                    reserved_usdc=Decimal("25"),
                    status="active",
                    created_at=datetime.now(timezone.utc),
                    expires_at=datetime.now(timezone.utc),
                ),
                Reservation(
                    id="res2",
                    user_id=user_id,
                    intent_key=f"{user_id}_{condition_id}_{token_id}_SELL_EXIT",
                    condition_id=condition_id,
                    token_id=token_id,
                    side=IntentSide.SELL,
                    reason=IntentReason.RISK_EXIT,
                    reserved_size=Decimal("15"),
                    reserved_usdc=Decimal("7.50"),
                    status="active",
                    created_at=datetime.now(timezone.utc),
                    expires_at=datetime.now(timezone.utc),
                ),
            ]
        
        def mark_released(self, reservation_id, reason, released_at):
            self.released.append((reservation_id, reason))
    
    class MockReservationServiceWithRepo:
        def __init__(self):
            self.reservations_repo = MockReservationsRepo()
            self.release_calls = []
        
        def release_terminal(self, reservation_id, reason):
            self.release_calls.append((reservation_id, reason))
            self.reservations_repo.mark_released(reservation_id, reason, 0)
    
    res_svc = MockReservationServiceWithRepo()
    svc = ReconciliationService(
        orders_repo=MagicMock(),
        fills_repo=MagicMock(),
        positions_repo=pos_repo,
        audit_logger=audit,
    )

    from storage.sqlite.positions_repo import PositionRecord
    pos_repo.upsert_position(PositionRecord(
        user_id="u1", condition_id="c1", token_id="t1",
        leader_id="leader1", size_tokens=Decimal("100"),
        avg_entry_price=Decimal("0.50"),
    ))

    fetcher = AsyncMock(return_value=Decimal("30"))
    result = await svc.reconcile_position_drift(
        user_id="u1",
        condition_id="c1",
        token_id="t1",
        leader_id="leader1",
        live_position_fetcher=fetcher,
        reservation_service=res_svc,
    )

    assert result["reconciled"] is True
    assert result["action"] == "shrunk"
    
    pos = pos_repo.get_position("u1", "c1", token_id="t1")
    assert pos is not None
    assert pos.size_tokens == Decimal("30")
    
    # Only the oversized reservation (res1 with 50 > 30) should be released
    assert len(res_svc.reservations_repo.released) == 1
    assert "res1" in [r[0] for r in res_svc.reservations_repo.released]
    assert "res2" not in [r[0] for r in res_svc.reservations_repo.released]
