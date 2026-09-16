"""Protective submissions retain their risk settlement owner during batch recovery."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from orchestrators.trade_pipeline import _reconcile_one


@pytest.mark.asyncio
@pytest.mark.parametrize("with_request", [True, False])
async def test_pending_protective_sell_uses_risk_settlement(with_request: bool) -> None:
    reconciler = SimpleNamespace(
        reconcile_trade_record=AsyncMock(return_value=True),
        reconcile_risk_record=AsyncMock(return_value=True),
        confirm_fill=AsyncMock(),
        reservation_service=None,
    )
    context = SimpleNamespace(
        user_id="account-a",
        execution_service=object(),
        positions_repo=SimpleNamespace(get_position=AsyncMock()),
        orders_repo=SimpleNamespace(
            get_by_order_id=AsyncMock(
                return_value={
                    "request": {"signal_id": "risk:stop_loss:condition:123", "reason": "stop_loss"}
                }
            )
        ),
        pnl_tracker=object(),
        extra={"_reconciliation_service": reconciler},
    )
    record = {
        "trade_key": "risk:stop_loss:condition:123",
        "user_id": "account-a",
        "asset": "token",
        "condition_id": "condition",
        "order_id": "protective-order",
    }
    if with_request:
        record["request"] = {"signal_id": record["trade_key"], "side": "SELL"}
    assert await _reconcile_one(context, record, "SELL") is True
    reconciler.reconcile_trade_record.assert_not_awaited()
    reconciler.reconcile_risk_record.assert_awaited_once()
    call = reconciler.reconcile_risk_record.await_args
    assert call.kwargs["user_id"] == "account-a"
    assert call.kwargs["condition_id"] == "condition"
    assert call.kwargs["token_id"] == "token"
    if not with_request:
        context.orders_repo.get_by_order_id.assert_awaited_once_with("protective-order")
        assert call.args[0]["request"]["reason"] == "stop_loss"
