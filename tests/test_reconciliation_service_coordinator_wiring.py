"""Issue #67 — reconciliation_service per-position coordinator wiring.

Asserts that when a ``position_coordinator`` is wired into
``ReconciliationService``, the per-scope lock is acquired around the
mutation block whenever the order record carries the canonical position
fields, and that legacy records (missing those fields) degrade gracefully
to no-lock behavior without raising.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

import pytest

from domain.position_key import PositionKey
from services.execution.reconciliation_service import ReconciliationService


# ---------------------------------------------------------------------------
# Probe doubles
# ---------------------------------------------------------------------------


class ProbeCoordinator:
    """Records every PositionKey passed to ``acquire``."""

    def __init__(self) -> None:
        self.acquired: list[PositionKey] = []

    @asynccontextmanager
    async def acquire(self, scope: PositionKey):
        self.acquired.append(scope)
        yield


class _Order:
    """Minimal OrderRecord with optional canonical fields (issue #67)."""

    def __init__(
        self,
        order_id: str,
        venue_order_id: str,
        reservation_id: str = "r1",
        status: str = "submitted_unconfirmed",
        market_id: str | None = None,
        user_id: str | None = None,
        condition_id: str | None = None,
        asset: str | None = None,
        outcome: str | None = None,
    ) -> None:
        self.id = order_id
        self.venue_order_id = venue_order_id
        self.reservation_id = reservation_id
        self.status = status
        self.market_id = market_id
        self.user_id = user_id
        self.condition_id = condition_id
        self.asset = asset
        self.outcome = outcome


class _OrdersRepo:
    def __init__(self, order: _Order) -> None:
        self.order = order

    async def get(self, order_id: str) -> _Order:
        return self.order

    async def mark_submitted_unconfirmed(self, order_id: str) -> None: ...

    async def mark_terminal(self, order_id: str, status: str) -> None: ...

    async def mark_reconciliation_required(self, order_id: str) -> None: ...

    async def mark_filled_or_partially_filled(self, order_id: str) -> None: ...


class _FillsRepo:
    async def upsert_venue_fill(self, order: Any, venue_fill: Any) -> Any:
        return {"venue_fill": venue_fill}


class _PositionsRepo:
    def upsert_position(self, fill: Any) -> None: ...


class _ReservationService:
    def release_terminal(self, reservation_id: str, reason: str) -> None: ...


class _VenueOrder:
    def __init__(self, status: str) -> None:
        self.status = status


class _Gateway:
    def __init__(self, status: str | None = None, fills: list[Any] | None = None) -> None:
        self.order_status = status
        self.fills = fills or []

    async def get_order(self, venue_order_id: str) -> Any:
        if self.order_status:
            return _VenueOrder(self.order_status)
        return None

    async def get_fills(self, market: str | None = None) -> list[Any]:
        return self.fills


class _Tx:
    async def __aenter__(self):
        return None

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return None


class _Db:
    def transaction(self):
        return _Tx()


class _Audit:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict]] = []

    def log_event(self, event_name: str, payload: dict) -> None:
        self.events.append((event_name, payload))


def _build_service(order: _Order, gateway: _Gateway, coordinator=None):
    audit = _Audit()
    svc = ReconciliationService(
        orders_repo=_OrdersRepo(order),
        fills_repo=_FillsRepo(),
        positions_repo=_PositionsRepo(),
        reservation_service=_ReservationService(),
        clob_gateway=gateway,
        db=_Db(),
        audit_logger=audit,
        position_coordinator=coordinator,
    )
    return svc, audit


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_filled_path_acquires_scope_lock_when_canonical_fields_present():
    order = _Order(
        order_id="o1",
        venue_order_id="v1",
        market_id="0xC",
        user_id="u1",
        condition_id="0xC",
        asset="0xT",
        outcome="YES",
    )
    gw = _Gateway(fills=[{"side": "BUY", "size": 1}])
    coord = ProbeCoordinator()
    svc, audit = _build_service(order, gw, coordinator=coord)

    res = await svc.reconcile_order("o1")
    assert res.is_resolved is True
    assert len(coord.acquired) == 1
    acquired = coord.acquired[0]
    assert acquired.identity() == ("u1", "0xc", "0xt")
    assert acquired.outcome == "YES"
    # Audit log carries structured position_key fields
    assert any(
        evt == "reconciliation_resolved_filled"
        and payload.get("position_key.user_id") == "u1"
        and payload.get("position_key.condition_id") == "0xc"
        and payload.get("position_key.token_id") == "0xt"
        for evt, payload in audit.events
    )


@pytest.mark.asyncio
async def test_terminal_path_acquires_scope_lock():
    order = _Order(
        order_id="o2",
        venue_order_id="v2",
        market_id="0xC2",
        user_id="u1",
        condition_id="0xC2",
        asset="0xT2",
    )
    gw = _Gateway(status="cancelled")
    coord = ProbeCoordinator()
    svc, _audit = _build_service(order, gw, coordinator=coord)

    res = await svc.reconcile_order("o2")
    assert res.is_resolved is True
    assert len(coord.acquired) == 1
    assert coord.acquired[0].identity() == ("u1", "0xc2", "0xt2")


@pytest.mark.asyncio
async def test_legacy_record_without_canonical_fields_skips_lock_gracefully():
    """Order missing user_id/condition_id/asset must not raise; lock is opt-in."""
    order = _Order(
        order_id="o3",
        venue_order_id="v3",
        market_id="0xC3",
    )
    gw = _Gateway(fills=[{"venue_fill": "x"}])
    coord = ProbeCoordinator()
    svc, _audit = _build_service(order, gw, coordinator=coord)

    res = await svc.reconcile_order("o3")
    assert res.is_resolved is True
    assert coord.acquired == []  # no lock attempted


@pytest.mark.asyncio
async def test_no_coordinator_supplied_still_works():
    """Default constructor (no coordinator kwarg) preserves legacy behavior."""
    order = _Order(
        order_id="o4",
        venue_order_id="v4",
        market_id="0xC4",
        user_id="u1",
        condition_id="0xC4",
        asset="0xT4",
    )
    gw = _Gateway(status="live")
    svc, _audit = _build_service(order, gw, coordinator=None)
    res = await svc.reconcile_order("o4")
    assert res.is_resolved is False  # live → pending


@pytest.mark.asyncio
async def test_pending_path_does_not_acquire_lock():
    """`live`/`open`/`pending` venue status mutates only orders_repo; no
    scoped state change → no lock acquisition needed."""
    order = _Order(
        order_id="o5",
        venue_order_id="v5",
        market_id="0xC5",
        user_id="u1",
        condition_id="0xC5",
        asset="0xT5",
    )
    gw = _Gateway(status="open")
    coord = ProbeCoordinator()
    svc, _audit = _build_service(order, gw, coordinator=coord)

    res = await svc.reconcile_order("o5")
    assert res.is_resolved is False
    assert coord.acquired == []
