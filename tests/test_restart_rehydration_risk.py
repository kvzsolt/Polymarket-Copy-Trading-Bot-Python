from __future__ import annotations

from decimal import Decimal

import pytest

from domain.trading_context import TradingContext
from services.executionService import ExecutionResult
from services.positionManager import PositionManager
from services.riskMonitor import rehydrate_position_manager, run_risk_check_for_context
from storage.sqlite.connection import create_connection
from storage.sqlite.fills_repo import FillRecord, FillsRepo
from storage.sqlite.migrations import apply_migrations
from storage.sqlite.orders_repo import OrdersRepo
from storage.sqlite.positions_repo import PositionsRepo

FAKE_TOKEN_ID = "71321045679252212594626385532706912750332728571942532223535764789812338637396"


class _FakeClient:
    async def get_price(self, _token_id: str, _side: str):
        return {"price": "0.3"}


class _FakeExecutionService:
    async def execute(self, req):
        return ExecutionResult(
            accepted=True,
            order_id="risk-exit-1",
            status="filled",
            requested_amount=Decimal(str(req.amount)),
            filled_amount=Decimal(str(req.amount)),
            avg_price=Decimal("0.3"),
            fees_paid=Decimal("0"),
            error=None,
        )


class _FakePnlTracker:
    def __init__(self):
        self.sells = 0

    def record_sell(self, **_kwargs):
        self.sells += 1


@pytest.mark.asyncio
async def test_restart_rehydration_and_risk_exit_uses_persisted_positions(tmp_path):
    db_path = tmp_path / "state.db"
    with create_connection(str(db_path)) as conn:
        apply_migrations(conn)

    fills_repo = FillsRepo(str(db_path))
    positions_repo = PositionsRepo(str(db_path))
    orders_repo = OrdersRepo(str(db_path))
    pnl_tracker = _FakePnlTracker()

    fills_repo.append_fill(
        FillRecord(
            user_id="u1",
            leader_id="l1",
            order_id="buy-1",
            condition_id="cond-1",
            token_id=FAKE_TOKEN_ID,
            side="BUY",
            size_tokens=Decimal("10"),
            price=Decimal("0.6"),
            fee=Decimal("0"),
            fill_ts=1_700_000_000,
        )
    )
    positions_repo.rebuild_user_positions_from_fills("u1", fills_repo)
    assert positions_repo.get_position("u1", "cond-1") is not None

    context = TradingContext(
        user_id="u1",
        wallet_address="wallet-u1",
        client=_FakeClient(),
        positions_repo=positions_repo,
        fills_repo=fills_repo,
        orders_repo=orders_repo,
        pnl_tracker=pnl_tracker,
        risk_engine=object(),
        accounting_service=object(),
        position_manager=PositionManager(),
        execution_service=_FakeExecutionService(),
        extra={"slippage_tolerance_bps": 50},
    )

    rehydrate_position_manager(context)
    assert context.position_manager.get_position("cond-1") is not None

    await run_risk_check_for_context(context)

    assert positions_repo.get_position("u1", "cond-1") is None
    fills = fills_repo.list_fills_for_user_condition("u1", "cond-1")
    assert len(fills) >= 2
    assert pnl_tracker.sells >= 1
