"""Confirmed individual trades do not close the unfilled remainder of an order."""

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from integrations.polymarket.models import VenueFill, VenueOrderState
from services.execution.reconciliation_service import ReconciliationService
from tests.conftest import make_clob_gateway


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "open_evidence", ["open", "unavailable", "closed", "stale_absence", "ccxt_open"]
)
async def test_known_confirmed_trade_ids_preserve_order_remainder(open_evidence: str) -> None:
    fill = VenueFill(
        fill_id="fill-1",
        order_id="order-1",
        size=Decimal(5),
        price=Decimal("0.3"),
        fee=Decimal(0),
        status="CONFIRMED",
    )
    order = VenueOrderState(
        order_id="order-1",
        status="cancelled"
        if open_evidence == "closed"
        else ("open" if open_evidence == "ccxt_open" else "live"),
        size=Decimal(10),
        filled_size=Decimal(5),
    )
    gateway = make_clob_gateway()
    gateway.get_order.return_value = order
    gateway.get_fills.return_value = [fill]
    gateway.list_open_orders = AsyncMock(
        return_value=[order] if open_evidence == "open" else [],
        side_effect=ValueError("synthetic unavailable") if open_evidence == "unavailable" else None,
    )
    service = ReconciliationService(
        orders_repo=SimpleNamespace(
            get_by_order_id=AsyncMock(
                return_value={
                    "response": {"trade_ids": ["fill-1"], "size_matched": "5"},
                    "request": {"amount": "10", "side": "SELL"},
                }
            )
        ),
        fills_repo=object(),
        positions_repo=object(),
        clob_gateway=gateway,
    )
    evidence = await service.confirm_fill("order-1")
    assert evidence["size_matched"] == "5"
    assert evidence["status"] == ("confirmed" if open_evidence == "closed" else "live")
    gateway.list_open_orders.assert_awaited_once()


@pytest.mark.asyncio
async def test_submission_transaction_hash_does_not_prove_confirmed_fill() -> None:
    gateway = make_clob_gateway()
    gateway.get_order.return_value = VenueOrderState(
        order_id="order-1", status="matched", size=Decimal(10), filled_size=Decimal(5)
    )
    gateway.list_open_orders.return_value = []
    gateway.get_fills.return_value = []
    service = ReconciliationService(
        orders_repo=SimpleNamespace(
            get_by_order_id=AsyncMock(
                return_value={
                    "response": {"transaction_hashes": ["0xsynthetic"], "size_matched": "5"},
                    "request": {"amount": "10", "side": "SELL"},
                }
            )
        ),
        fills_repo=object(),
        positions_repo=object(),
        clob_gateway=gateway,
    )
    evidence = await service.confirm_fill("order-1")
    assert evidence["size_matched"] == "0"
    assert evidence["status"] == "matched"
    gateway.get_order.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("fill_status", ["CONFIRMED", "FAILED"])
async def test_account_fill_evidence_requires_known_order_continuity(fill_status) -> None:
    gateway = make_clob_gateway()
    gateway.get_order.side_effect = ValueError("unavailable")
    gateway.list_open_orders.side_effect = ValueError("unavailable")
    gateway.get_fills.return_value = [
        VenueFill(
            fill_id="fill-1",
            order_id="order-1",
            size=Decimal(5),
            price=Decimal("0.3"),
            fee=Decimal(0),
            status=fill_status,
        )
    ]
    service = ReconciliationService(
        orders_repo=SimpleNamespace(
            get_by_order_id=AsyncMock(
                return_value={
                    "response": {},
                    "request": {"amount": "10", "side": "SELL"},
                }
            )
        ),
        fills_repo=object(),
        positions_repo=object(),
        clob_gateway=gateway,
    )
    evidence = await service.confirm_fill("order-1")
    assert evidence["status"] not in {"confirmed", "filled", "failed"}
    assert evidence["size_matched"] == ("5" if fill_status == "CONFIRMED" else "0")


@pytest.mark.asyncio
@pytest.mark.parametrize("status,expected", [("open", "live"), ("closed", "matched")])
async def test_normalized_order_status_without_settlement_remains_pending(status, expected) -> None:
    gateway = make_clob_gateway()
    gateway.get_order.return_value = VenueOrderState(
        order_id="order-1", status=status, size=Decimal(10), filled_size=Decimal(5)
    )
    gateway.list_open_orders.return_value = []
    gateway.get_fills.return_value = []
    service = ReconciliationService(
        orders_repo=SimpleNamespace(get_by_order_id=AsyncMock(return_value={})),
        fills_repo=object(),
        positions_repo=object(),
        clob_gateway=gateway,
    )
    evidence = await service.confirm_fill("order-1")
    assert evidence["status"] == expected
    assert evidence["size_matched"] == "0"


@pytest.mark.asyncio
@pytest.mark.parametrize("account_evidence", ["complete", "unavailable", "empty"])
async def test_later_order_fills_are_not_lost_after_initial_trade_ids_resolve(
    account_evidence,
) -> None:
    first = VenueFill(
        fill_id="fill-1",
        order_id="order-1",
        size=Decimal(5),
        price=Decimal("0.3"),
        fee=Decimal(0),
        status="CONFIRMED",
    )
    later = VenueFill(
        fill_id="fill-2",
        order_id="order-1",
        size=Decimal(5),
        price=Decimal("0.5"),
        fee=Decimal(0),
        status="CONFIRMED",
    )

    async def get_fills(*, trade_id=None, **kwargs):
        if trade_id is not None:
            return [first]
        if account_evidence == "unavailable":
            raise ValueError("unavailable")
        return [first, later] if account_evidence == "complete" else []

    gateway = make_clob_gateway()
    gateway.get_order.return_value = VenueOrderState(
        order_id="order-1", status="closed", size=Decimal(10), filled_size=Decimal(10)
    )
    gateway.list_open_orders.return_value = []
    gateway.get_fills.side_effect = get_fills
    service = ReconciliationService(
        orders_repo=SimpleNamespace(
            get_by_order_id=AsyncMock(
                return_value={
                    "response": {"trade_ids": ["fill-1"], "size_matched": "5"},
                    "request": {"amount": "10", "side": "SELL"},
                }
            )
        ),
        fills_repo=object(),
        positions_repo=object(),
        clob_gateway=gateway,
    )
    evidence = await service.confirm_fill("order-1")
    assert evidence["size_matched"] == ("10" if account_evidence == "complete" else "5")
    assert evidence["status"] == ("confirmed" if account_evidence == "complete" else "live")
    if account_evidence == "complete":
        assert Decimal(evidence["avg_price"]) == Decimal("0.4")


@pytest.mark.asyncio
async def test_confirmed_partial_is_accountable_while_later_fill_is_pending() -> None:
    gateway = make_clob_gateway()
    gateway.get_order.return_value = VenueOrderState(
        order_id="order-1", status="closed", size=Decimal(10), filled_size=Decimal(10)
    )
    gateway.list_open_orders.return_value = []
    gateway.get_fills.return_value = [
        VenueFill(
            fill_id="fill-1",
            order_id="order-1",
            size=Decimal(5),
            price=Decimal("0.3"),
            fee=Decimal(0),
            status="CONFIRMED",
        ),
        VenueFill(
            fill_id="fill-2",
            order_id="order-1",
            size=Decimal(5),
            price=Decimal("0.5"),
            fee=Decimal(0),
            status="MINED",
        ),
    ]
    service = ReconciliationService(
        orders_repo=SimpleNamespace(get_by_order_id=AsyncMock(return_value={})),
        fills_repo=object(),
        positions_repo=object(),
        clob_gateway=gateway,
    )
    evidence = await service.confirm_fill("order-1")
    assert evidence["status"] == "live"
    assert evidence["size_matched"] == "5"
    assert Decimal(evidence["avg_price"]) == Decimal("0.3")
