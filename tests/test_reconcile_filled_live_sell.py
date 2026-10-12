"""A fully filled sell must leave the unconfirmed loop.

Incident: a near-max FAK sell matched, mined, and confirmed, but the venue
order row stayed ``live`` (and an id-filtered open-order query still returned
it). Reconciliation applied the fill as non-terminal, then the next skipped
poll (``poll_deferred``, size 0) rewrote the row to ``ambiguous_timeout``.
The following real poll wrote ``order_live`` again. Those two writers
alternated for the life of the process.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from pmcopy.config.app_config import OrderConfirmationConfig
from pmcopy.integrations.polymarket.models import VenueFill, VenueOrderState
from pmcopy.services.execution.reconciliation_service import ReconciliationService
from tests.conftest import make_clob_gateway

_ORDER_ID = "0x8d50b1def78c4b4473e1570818b7f22aee994ee001ae217d697b316e46473394"


def _filled_live_order() -> VenueOrderState:
    return VenueOrderState(
        order_id=_ORDER_ID,
        status="live",
        side="SELL",
        price=Decimal("0.999"),
        size=Decimal("15"),
        filled_size=Decimal("15"),
    )


def _confirmed_fill() -> VenueFill:
    return VenueFill(
        fill_id="fill-near-max",
        order_id=_ORDER_ID,
        side="SELL",
        size=Decimal("15"),
        price=Decimal("0.999"),
        fee=Decimal("0"),
        status="CONFIRMED",
    )


class _OrdersRepo:
    def __init__(self, row: dict) -> None:
        self.row = row
        self.updates: list[dict] = []

    async def get_by_order_id(self, order_id: str) -> dict | None:
        if order_id == self.row.get("order_id"):
            return self.row
        return None

    async def update_submission_status(self, **kwargs) -> None:
        self.updates.append(kwargs)
        self.row["status"] = kwargs.get("status")
        patch = kwargs.get("response_patch") or {}
        response = dict(self.row.get("response") or {})
        response.update(patch)
        self.row["response"] = response

    async def mark_processed_trade(self, **kwargs) -> None:
        self.updates.append({"mark_processed_trade": kwargs})


class _FillsRepo:
    def __init__(self) -> None:
        self.appends: list = []

    async def append_fill_and_apply(self, record, positions_repo, *, raw_payload=None):
        self.appends.append(record)
        return 1, None


class _Reservations:
    def __init__(self) -> None:
        self.releases: list[tuple[str, str]] = []

    async def release_terminal(self, reservation_id: str, reason: str) -> None:
        self.releases.append((reservation_id, reason))


def _row() -> dict:
    return {
        "order_id": _ORDER_ID,
        "user_id": "0xUSER",
        "leader_id": "risk-monitor",
        "trade_key": "risk:near_max_full_exit:cond:token:1",
        "asset": "622063961735",
        "condition_id": "0x6659cb5686",
        "status": "submitted_unconfirmed",
        "created_at": 1_700_000_000.0,
        "request": {
            "side": "SELL",
            "amount": "15",
            "reservation_id": "res-near-max",
            "condition_id": "0x6659cb5686",
            "token_id": "622063961735",
        },
        "response": {"status": "matched", "size_matched": "0"},
    }


def _service(gateway, orders: _OrdersRepo, reservations: _Reservations) -> ReconciliationService:
    return ReconciliationService(
        orders_repo=orders,
        fills_repo=_FillsRepo(),
        positions_repo=SimpleNamespace(get_position=AsyncMock(return_value=None)),
        reservation_service=reservations,
        clob_gateway=gateway,
        order_confirmation_cfg=OrderConfirmationConfig(
            initial_delay_seconds=0.0,
            poll_interval_seconds=30.0,
            max_wait_seconds=1.0,
        ),
    )


@pytest.mark.asyncio
async def test_confirmed_fills_close_a_stale_live_order() -> None:
    order = _filled_live_order()
    gateway = make_clob_gateway()
    gateway.get_order.return_value = order
    gateway.list_open_orders = AsyncMock(return_value=[order])
    gateway.get_fills.return_value = [_confirmed_fill()]
    service = ReconciliationService(
        orders_repo=SimpleNamespace(get_by_order_id=AsyncMock(return_value=_row())),
        fills_repo=object(),
        positions_repo=object(),
        clob_gateway=gateway,
    )

    evidence = await service.confirm_fill(_ORDER_ID)

    assert evidence["status"] == "confirmed"
    assert evidence["size_matched"] == "15"


@pytest.mark.asyncio
async def test_full_sell_releases_and_skipped_poll_does_not_reopen_it() -> None:
    order = _filled_live_order()
    gateway = make_clob_gateway()
    gateway.get_order.return_value = order
    gateway.list_open_orders = AsyncMock(return_value=[order])
    gateway.get_fills.return_value = [_confirmed_fill()]
    orders = _OrdersRepo(_row())
    reservations = _Reservations()
    service = _service(gateway, orders, reservations)
    record = dict(orders.row)

    applied = await service.reconcile_trade_record(
        record,
        "SELL",
        confirm_fill=service.confirm_fill,
    )
    # A second pass inside the poll interval must not see the skipped poll as
    # "no fill" and rewrite the settled row back to ambiguous_timeout.
    await service.reconcile_trade_record(
        record,
        "SELL",
        confirm_fill=service.confirm_fill,
    )

    assert applied is True
    assert reservations.releases == [("res-near-max", "confirmed")]
    assert orders.row["status"] == "filled_reconciled"
    assert all(update.get("status") != "ambiguous_timeout" for update in orders.updates)
