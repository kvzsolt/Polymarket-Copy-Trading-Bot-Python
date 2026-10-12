"""Issue #74: Position drift reconciliation when SELL fails with insufficient_balance.

Tests cover:
1. ReconciliationService.reconcile_position_drift() — delete, shrink, no-drift, fetch-failed
2. Audit event manual_position_drift_reconciled emitted
3. Reservation cleanup on the drift delete/shrink paths

The former ExecutionCoordinator V2 drift-trigger tests were removed together
with the dead ``execute_intent`` path; the production trigger is covered by
``tests/test_trade_pipeline_safety.py`` (SELL/BUY/non-drift gating) and
``tests/test_trade_pipeline_drift_releases_reservation.py``.
"""

from __future__ import annotations

import pytest
from decimal import Decimal
from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock

from pmcopy.services.execution.reconciliation_service import ReconciliationService


FAKE_TOKEN_ID = "71321045679252212594626385532706912750332728571942532223535764789812338637396"


class MockPositionsRepo:
    def __init__(self):
        self.positions = {}

    async def upsert_position(self, record):
        key = (record.user_id, record.condition_id, getattr(record, "token_id", ""))
        self.positions[key] = record

    async def get_position(self, user_id, condition_id, *, token_id=""):
        return self.positions.get((user_id, condition_id, token_id))

    async def delete_position(self, user_id, condition_id, *, token_id=""):
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

    async def release_terminal(self, reservation_id: str, reason: str):
        self.released.append((reservation_id, reason))


class MockReservation:
    def __init__(self, res_id):
        self.id = res_id


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

    from pmcopy.storage.postgres.positions_repo import PositionRecord

    await pos_repo.upsert_position(
        PositionRecord(
            user_id="u1",
            condition_id="c1",
            token_id="t1",
            leader_id="leader1",
            size_tokens=Decimal("50"),
            avg_entry_price=Decimal("0.45"),
        )
    )

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
    assert await pos_repo.get_position("u1", "c1", token_id="t1") is None
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

    from pmcopy.storage.postgres.positions_repo import PositionRecord

    await pos_repo.upsert_position(
        PositionRecord(
            user_id="u1",
            condition_id="c1",
            token_id="t1",
            leader_id="leader1",
            size_tokens=Decimal("100"),
            avg_entry_price=Decimal("0.50"),
        )
    )

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
    pos = await pos_repo.get_position("u1", "c1", token_id="t1")
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

    from pmcopy.storage.postgres.positions_repo import PositionRecord

    await pos_repo.upsert_position(
        PositionRecord(
            user_id="u1",
            condition_id="c1",
            token_id="t1",
            leader_id="leader1",
            size_tokens=Decimal("50"),
            avg_entry_price=Decimal("0.50"),
        )
    )

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

    from pmcopy.storage.postgres.positions_repo import PositionRecord

    await pos_repo.upsert_position(
        PositionRecord(
            user_id="u1",
            condition_id="c1",
            token_id="t1",
            leader_id="leader1",
            size_tokens=Decimal("100"),
            avg_entry_price=Decimal("0.50"),
        )
    )

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

    pos = await pos_repo.get_position("u1", "c1", token_id="t1")
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

    from pmcopy.storage.postgres.positions_repo import PositionRecord

    await pos_repo.upsert_position(
        PositionRecord(
            user_id="u1",
            condition_id="c1",
            token_id="t1",
            leader_id="leader1",
            size_tokens=Decimal("50"),
            avg_entry_price=Decimal("0.50"),
        )
    )

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
    assert await pos_repo.get_position("u1", "c1", token_id="t1") is None


# ---------------------------------------------------------------------------
# Reservation cleanup on delete path
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_reconcile_drift_delete_path_cleans_reservations():
    """When drift results in deletion (live_size=0), active SELL reservations are released."""
    from pmcopy.domain.order_lifecycle import IntentSide, IntentReason

    pos_repo = MockPositionsRepo()
    audit = MockAuditLogger()

    class MockReservationsRepo:
        def __init__(self):
            self.released = []

        async def list_active_for_scope(self, user_id, condition_id, token_id):
            from pmcopy.domain.order_lifecycle import Reservation
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

        async def mark_released(self, reservation_id, reason, released_at):
            self.released.append((reservation_id, reason))

    class MockReservationServiceWithRepo:
        def __init__(self):
            self.reservations_repo = MockReservationsRepo()
            self.release_calls = []

        async def release_terminal(self, reservation_id, reason):
            self.release_calls.append((reservation_id, reason))
            await self.reservations_repo.mark_released(reservation_id, reason, 0)

    res_svc = MockReservationServiceWithRepo()
    svc = ReconciliationService(
        orders_repo=MagicMock(),
        fills_repo=MagicMock(),
        positions_repo=pos_repo,
        audit_logger=audit,
    )

    from pmcopy.storage.postgres.positions_repo import PositionRecord

    await pos_repo.upsert_position(
        PositionRecord(
            user_id="u1",
            condition_id="c1",
            token_id="t1",
            leader_id="leader1",
            size_tokens=Decimal("100"),
            avg_entry_price=Decimal("0.50"),
        )
    )

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
    assert await pos_repo.get_position("u1", "c1", token_id="t1") is None
    assert len(res_svc.reservations_repo.released) == 2
    assert len(res_svc.release_calls) == 2


# ---------------------------------------------------------------------------
# Reservation cleanup on shrink path
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_reconcile_drift_shrink_path_coversized_reservations():
    """When drift results in shrink, SELL reservations larger than new position are released."""
    from pmcopy.domain.order_lifecycle import IntentSide, IntentReason

    pos_repo = MockPositionsRepo()
    audit = MockAuditLogger()

    class MockReservationsRepo:
        def __init__(self):
            self.released = []

        async def list_active_for_scope(self, user_id, condition_id, token_id):
            from pmcopy.domain.order_lifecycle import Reservation
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

        async def mark_released(self, reservation_id, reason, released_at):
            self.released.append((reservation_id, reason))

    class MockReservationServiceWithRepo:
        def __init__(self):
            self.reservations_repo = MockReservationsRepo()
            self.release_calls = []

        async def release_terminal(self, reservation_id, reason):
            self.release_calls.append((reservation_id, reason))
            await self.reservations_repo.mark_released(reservation_id, reason, 0)

    res_svc = MockReservationServiceWithRepo()
    svc = ReconciliationService(
        orders_repo=MagicMock(),
        fills_repo=MagicMock(),
        positions_repo=pos_repo,
        audit_logger=audit,
    )

    from pmcopy.storage.postgres.positions_repo import PositionRecord

    await pos_repo.upsert_position(
        PositionRecord(
            user_id="u1",
            condition_id="c1",
            token_id="t1",
            leader_id="leader1",
            size_tokens=Decimal("100"),
            avg_entry_price=Decimal("0.50"),
        )
    )

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

    pos = await pos_repo.get_position("u1", "c1", token_id="t1")
    assert pos is not None
    assert pos.size_tokens == Decimal("30")

    # Only the oversized reservation (res1 with 50 > 30) should be released
    assert len(res_svc.reservations_repo.released) == 1
    assert "res1" in [r[0] for r in res_svc.reservations_repo.released]
    assert "res2" not in [r[0] for r in res_svc.reservations_repo.released]
