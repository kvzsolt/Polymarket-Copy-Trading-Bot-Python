"""Regression tests for authenticated user-channel order evidence.

The user channel identifies trade UUIDs and order hashes separately. These
tests pin exact identity extraction and terminal cancellation evidence so an
ambiguous FAK reservation is released only from authoritative venue facts.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from services.execution.reconciliation_service import ReconciliationService
from services.execution.user_order_evidence import (
    UserOrderEvidenceKind,
    UserOrderEvidenceParser,
)


class _OrdersRepo:
    """Record durable order-evidence updates for reconciliation tests."""

    def __init__(self, row: dict[str, Any]) -> None:
        self.row = row
        self.updates: list[dict[str, Any]] = []

    async def get_by_order_id(self, order_id: str) -> dict[str, Any] | None:
        """Return the configured row when its order identity matches."""

        if order_id != self.row.get("order_id"):
            return None
        return self.row

    async def update_submission_status(self, **kwargs: Any) -> None:
        """Capture and merge one evidence update."""

        self.updates.append(kwargs)
        response = dict(self.row.get("response") or {})
        response.update(kwargs.get("response_patch") or {})
        self.row["response"] = response
        self.row["status"] = kwargs["status"]


class _PositionsRepo:
    """Return an injected exact token position."""

    def __init__(self, size: str = "62.2902") -> None:
        self._size = Decimal(size)

    async def get_position(self, *_args: Any, **_kwargs: Any) -> Any:
        """Return a position-shaped object with exact Decimal shares."""

        return MagicMock(size_tokens=self._size)


def _order_row() -> dict[str, Any]:
    """Build one unresolved SELL order record."""

    return {
        "order_id": "0xorder",
        "status": "submitted_unconfirmed",
        "user_id": "user-1",
        "condition_id": "condition-1",
        "asset": "token-1",
        "request": {
            "side": "SELL",
            "amount": "54.1778",
            "recovery_metadata": {
                "pre_submit_live_position_size": "62.2902",
            },
        },
        "response": {"size_matched": "0"},
    }


def test_parser_uses_taker_order_hash_instead_of_trade_uuid() -> None:
    """A user trade UUID must never be logged or persisted as an order hash."""

    evidence = UserOrderEvidenceParser().parse(
        {
            "event_type": "trade",
            "id": "trade-uuid",
            "taker_order_id": "0xorder",
            "status": "MATCHED",
            "size": "7.0992",
            "timestamp": "1720000000000",
        }
    )

    assert evidence is not None
    assert evidence.order_id == "0xorder"
    assert evidence.trade_id == "trade-uuid"
    assert evidence.kind is UserOrderEvidenceKind.TRADE_MATCHED
    assert evidence.size_matched == Decimal("7.0992")


def test_parser_classifies_zero_fill_cancellation_as_terminal_absence() -> None:
    """An authenticated cancellation with zero matched shares proves no fill."""

    evidence = UserOrderEvidenceParser().parse(
        {
            "event_type": "order",
            "id": "0xorder",
            "type": "CANCELLATION",
            "original_size": "54.1778",
            "size_matched": "0",
            "timestamp": "1720000000001",
        }
    )

    assert evidence is not None
    assert evidence.kind is UserOrderEvidenceKind.CANCELLED
    assert evidence.size_matched == Decimal("0")
    assert evidence.is_terminal is True


@pytest.mark.parametrize("malformed_size", ["-1", "NaN", "not-a-number"])
def test_parser_rejects_malformed_cancellation_matched_size(malformed_size: str) -> None:
    """Malformed matched-size evidence must never become terminal zero-fill proof."""

    evidence = UserOrderEvidenceParser().parse(
        {
            "event_type": "order",
            "id": "0xorder",
            "type": "CANCELLATION",
            "size_matched": malformed_size,
            "timestamp": "1720000000001",
        }
    )

    assert evidence is None


def test_parser_rejects_cancellation_without_matched_size() -> None:
    """Incomplete cancellation data cannot be treated as terminal zero fill."""

    evidence = UserOrderEvidenceParser().parse(
        {
            "event_type": "order",
            "id": "0xorder",
            "type": "CANCELLATION",
            "timestamp": "1720000000001",
        }
    )

    assert evidence is None


@pytest.mark.asyncio
async def test_persisted_wss_cancellation_short_circuits_broken_order_poll() -> None:
    """Terminal user-channel evidence must settle without a failing REST poll."""

    orders = _OrdersRepo(_order_row())
    gateway = MagicMock()
    gateway.get_order = AsyncMock(side_effect=RuntimeError("invalid order response"))
    gateway.list_open_orders = AsyncMock(return_value=[])
    gateway.get_fills = AsyncMock(return_value=[])
    service = ReconciliationService(
        orders_repo=orders,
        fills_repo=None,
        positions_repo=_PositionsRepo(),
        clob_gateway=gateway,
    )

    recorded = await service.record_user_order_evidence(
        {
            "event_type": "order",
            "id": "0xorder",
            "type": "CANCELLATION",
            "original_size": "54.1778",
            "size_matched": "0",
            "timestamp": "1720000000001",
        }
    )
    result = await service.confirm_fill("0xorder")

    assert recorded is True
    assert orders.updates[0]["status"] == "submitted_unconfirmed"
    assert result == {
        "status": "cancelled",
        "size_matched": "0",
        "confirmation_source": "user_wss_order",
    }
    gateway.get_order.assert_not_awaited()


@pytest.mark.asyncio
async def test_older_wss_order_evidence_cannot_overwrite_newer_cancellation() -> None:
    """Out-of-order placement evidence must not erase a terminal cancellation."""

    row = _order_row()
    row["response"]["user_wss_order_evidence"] = {
        "order_id": "0xorder",
        "kind": "cancelled",
        "size_matched": "0",
        "timestamp": 1720000000001,
        "terminal": True,
    }
    orders = _OrdersRepo(row)
    service = ReconciliationService(
        orders_repo=orders,
        fills_repo=None,
        positions_repo=_PositionsRepo(),
        clob_gateway=MagicMock(),
    )

    recorded = await service.record_user_order_evidence(
        {
            "event_type": "order",
            "id": "0xorder",
            "type": "PLACEMENT",
            "size_matched": "0",
            "timestamp": "1720000000000",
        }
    )

    assert recorded is False
    assert orders.updates == []


@pytest.mark.asyncio
async def test_cancellation_supersedes_non_terminal_evidence_at_same_timestamp() -> None:
    """Terminal cancellation wins when venue timestamps have equal resolution."""

    row = _order_row()
    row["response"]["user_wss_order_evidence"] = {
        "order_id": "0xorder",
        "kind": "placed",
        "size_matched": "0",
        "timestamp": 1720000000001,
        "terminal": False,
    }
    orders = _OrdersRepo(row)
    service = ReconciliationService(
        orders_repo=orders,
        fills_repo=None,
        positions_repo=_PositionsRepo(),
        clob_gateway=MagicMock(),
    )

    recorded = await service.record_user_order_evidence(
        {
            "event_type": "order",
            "id": "0xorder",
            "type": "CANCELLATION",
            "size_matched": "0",
            "timestamp": "1720000000001",
        }
    )

    assert recorded is True
    assert orders.updates[0]["response_patch"]["user_wss_order_evidence"]["kind"] == ("cancelled")


@pytest.mark.asyncio
async def test_wss_wake_persists_order_evidence_before_reconciliation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Runtime glue records cancellation before scanning unresolved orders."""

    import app
    import orchestrators.trade_pipeline as pipeline

    call_order: list[str] = []
    reconciler = MagicMock()

    async def _record(_event: dict[str, Any]) -> bool:
        call_order.append("record")
        return True

    async def _reconcile(_context: Any) -> int:
        call_order.append("reconcile")
        return 0

    reconciler.record_user_order_evidence = _record
    monkeypatch.setattr(pipeline, "get_reconciliation_service", lambda _context: reconciler)
    monkeypatch.setattr(pipeline, "reconcile_submitted_unconfirmed", _reconcile)
    context = MagicMock()
    context.user_id = "user-1"
    context.extra = {}
    context.execution_coordinator = None
    wake = app._build_reconcile_wake_closure(context)

    await wake(
        {
            "event_type": "order",
            "id": "0xorder",
            "type": "CANCELLATION",
            "size_matched": "0",
        }
    )

    assert call_order == ["record", "reconcile"]


@pytest.mark.asyncio
async def test_wss_wake_records_evidence_while_reconcile_is_in_progress(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A CONFIRMED trade must be stored even when an earlier wake still holds the lock.

    User-channel MATCHED starts reconciliation. MINED and CONFIRMED follow
    immediately. Dropping those events leaves the order unconfirmed after the
    trade has already settled.
    """

    import app
    import orchestrators.trade_pipeline as pipeline

    call_order: list[str] = []
    started = asyncio.Event()
    release = asyncio.Event()
    reconciler = MagicMock()

    async def _record(_event: dict[str, Any]) -> bool:
        call_order.append("record")
        return True

    async def _reconcile(_context: Any) -> int:
        call_order.append("reconcile")
        started.set()
        await release.wait()
        return 0

    reconciler.record_user_order_evidence = _record
    monkeypatch.setattr(pipeline, "get_reconciliation_service", lambda _context: reconciler)
    monkeypatch.setattr(pipeline, "reconcile_submitted_unconfirmed", _reconcile)
    context = MagicMock()
    context.user_id = "user-1"
    context.extra = {}
    context.execution_coordinator = None
    wake = app._build_reconcile_wake_closure(context)
    first = {
        "event_type": "trade",
        "id": "trade-1",
        "taker_order_id": "0xorder",
        "status": "MATCHED",
        "size": "15",
    }
    confirmed = {**first, "status": "CONFIRMED"}

    inflight = asyncio.create_task(wake(first))
    await started.wait()
    await wake(confirmed)
    release.set()
    await inflight

    assert call_order == ["record", "reconcile", "record"]
