"""Release-gate: end-to-end dry-run copy SELL (Issue #75 item 16).

Same harness as the BUY-side gate but exercises the SELL path so the
gateway request emitted by ``ExecutionService`` carries ``side="SELL"``.
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from integrations.polymarket.models import (
    MarketableOrderRequest,
    VenueSubmitResult,
)
from services.executionService import ExecutionRequest, ExecutionService
from services.orderRepository import OrderRepository
from tests.conftest import make_clob_gateway
from utils.createClobClient import MarketMeta


class _FeeSvc:
    async def get_fee_bps(self, _token_id: str) -> int:
        return 0


class _MetaSvc:
    async def get(self, token_id: str, refresh: bool = False) -> MarketMeta:
        return MarketMeta(
            token_id=token_id,
            tick_size=Decimal("0.01"),
            min_order_size=Decimal("0"),
            neg_risk=False,
            best_bid=Decimal("0.49"),
            best_ask=Decimal("0.51"),
        )


@pytest.fixture(autouse=True)
def _reset_circuit():
    import services.executionService as _es
    from unittest.mock import patch
    _es._CLOB_CIRCUIT.reset()
    with patch.dict("config.env.ENV", {"MAX_BOOK_SPREAD_BPS": "5000"}, clear=False):
        yield
    _es._CLOB_CIRCUIT.reset()


@pytest.mark.asyncio
async def test_dry_run_sell_routes_through_gateway(
    fresh_db_path,
    exec_config,
    stub_clob_client,
) -> None:
    repo = OrderRepository(fresh_db_path)
    gateway = make_clob_gateway(
        submit_result=VenueSubmitResult(
            success=True, order_id="ord-sell-e2e", status="submitted"
        )
    )
    svc = ExecutionService(
        stub_clob_client, _MetaSvc(), _FeeSvc(), repo,
        config=exec_config,
        clob_gateway=gateway,
    )
    request = ExecutionRequest(
        user_id="u1",
        leader_id="leader-1",
        token_id="0xtok",
        side="SELL",
        amount=Decimal("5"),
        max_slippage_bps=100,
        reason="copy_trade",
        signal_id="sig-e2e-sell",
    )
    result = await svc.execute(request)

    assert result.accepted is True
    gateway.submit_marketable_order.assert_awaited_once()
    submitted = gateway.submit_marketable_order.await_args.args[0]
    assert isinstance(submitted, MarketableOrderRequest)
    assert submitted.token_id == "0xtok"
    assert submitted.side == "SELL"
