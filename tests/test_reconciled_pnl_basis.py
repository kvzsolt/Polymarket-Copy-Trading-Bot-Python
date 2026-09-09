"""Reconciliation preserves unknown basis and exact financial units."""

from decimal import Decimal
from unittest.mock import AsyncMock, Mock

import pytest

from tests.test_reconciler_idempotent_transitions import _make_service, _record
from services.execution.fill_accounting import AccountedOrderFill


@pytest.mark.asyncio
@pytest.mark.parametrize("risk_exit", [False, True])
async def test_confirmed_sell_does_not_invent_missing_entry_cost(risk_exit):
    """A manual or imported holding with unknown basis stays unknown in PnL."""
    service = _make_service()
    tracker = Mock()
    arguments = dict(
        pnl_tracker=tracker,
        get_position=AsyncMock(return_value=None),
        confirm_fill=AsyncMock(
            return_value={"status": "filled", "size_matched": "5", "avg_price": "0.55"}
        ),
    )
    if risk_exit:
        await service.reconcile_risk_record(
            _record(),
            user_id="user-1",
            condition_id="cond-idem",
            token_id="0xtok-idem",
            **arguments,
        )
    else:
        await service.reconcile_trade_record(_record(), "SELL", **arguments)
    recorded = tracker.record_sell.call_args.kwargs
    assert recorded["entry_price"] is None
    assert recorded["realized_pnl"] is None
    assert recorded["price"] == Decimal("0.55")
    assert isinstance(recorded["size_tokens"], Decimal)


@pytest.mark.asyncio
async def test_risk_reconciliation_uses_the_explicit_account_scope():
    """Legacy risk queue records need not duplicate the method's account binding."""
    service = _make_service()
    service._accounted_fill_reader = AsyncMock()
    service._accounted_fill_reader.accounted_order_fill.return_value = AccountedOrderFill()
    record = _record()
    record.pop("user_id", None)
    await service.reconcile_risk_record(
        record,
        user_id="user-1",
        condition_id="cond-idem",
        token_id="0xtok-idem",
        confirm_fill=AsyncMock(
            return_value={"status": "filled", "size_matched": "5", "avg_price": "0.55"}
        ),
    )
    service._accounted_fill_reader.accounted_order_fill.assert_awaited_once_with(
        "user-1",
        record["order_id"],
    )
