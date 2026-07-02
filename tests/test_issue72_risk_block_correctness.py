"""Risk-block lifecycle correctness (PG-A6 port).

Pins:
  * mark_risk_block is idempotent (no duplicate rows).
  * Blocks are token-scoped, not condition-scoped.
  * Rejected SELL leaves no block.
  * Reconciliation full-close creates exactly one block (no dup on
    restart).
  * Risk triggers recorded for full close + rejection.
"""
from __future__ import annotations

import contextlib
from decimal import Decimal

import pytest

from domain.trading_context import TradingContext
from orchestrators.runtime_bootstrap import startup_reconcile_unconfirmed
from services.executionService import ExecutionResult
from services.positionManager import PositionManager
from services.risk.risk_monitor import (
    rehydrate_position_manager,
    run_risk_check_for_context,
)
from storage.postgres.fills_repo import FillRecord, FillsRepo
from storage.postgres.orders_repo import OrderSubmissionRecord, OrdersRepo
from storage.postgres.positions_repo import PositionRecord, PositionsRepo
from storage.postgres.risk_blocks_repo import RiskBlocksRepo
from storage.postgres.risk_triggers_repo import RiskTriggersRepo

pytestmark = pytest.mark.integration

FAKE_TOKEN_ID = "71321045679252212594626385532706912750332728571942532223535764789812338637396"
TOKEN_B = "61321045679252212594626385532706912750332728571942532223535764789812338637396"

_FILL_RESPONSES = {
    "venue-risk-no-fill-1": {"status": "cancelled", "size_matched": "0", "avg_price": None, "orderID": "venue-risk-no-fill-1"},
    "venue-risk-dup-1": {"status": "filled", "size_matched": "10", "avg_price": "0.30", "orderID": "venue-risk-dup-1"},
}


@contextlib.asynccontextmanager
async def _pg_pool():
    try:
        from testcontainers.postgres import PostgresContainer
        import asyncpg
        from storage.postgres.migrations import apply_migrations as pg_apply
    except Exception as exc:
        pytest.skip(f"Docker / postgres image unavailable: {exc!r}")
    ctr = PostgresContainer("postgres:16-alpine")
    try:
        ctr.start()
    except Exception as exc:
        pytest.skip(f"Docker / postgres image unavailable: {exc!r}")
    try:
        dsn = ctr.get_connection_url(driver="asyncpg").replace(
            "postgresql+asyncpg://", "postgresql://"
        )
        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=5)
        async with pool.acquire() as conn:
            await pg_apply(conn)
        try:
            yield pool
        finally:
            await pool.close()
    finally:
        ctr.stop()


class _FakeClobGateway:
    def __init__(self, responses=None):
        self._responses = responses or dict(_FILL_RESPONSES)

    async def get_order(self, order_id: str):
        return self._responses.get(order_id, {"status": "unknown", "size_matched": "0"})

    async def get_market_info(self, token_id: str):
        return {}

    async def get_orderbook(self, token_id: str):
        return None

    async def submit_marketable_order(self, request):
        return None

    async def submit_limit_order(self, request):
        return None

    async def get_fills(self, market=None):
        return []

    async def cancel_order(self, order_id: str):
        return None

    async def check_compatibility(self):
        return {"sdk_available": True}


class _FakeClient:
    def __init__(self, price: str = "0.97"):
        self.price = price

    async def get_price(self, _token_id: str, _side: str):
        return {"price": self.price}


class _FakePnlTracker:
    def __init__(self):
        self.sells = 0

    def record_sell(self, **_kwargs):
        self.sells += 1


class _FakeExecutionService:
    async def execute(self, req):
        return ExecutionResult(
            accepted=True,
            order_id=f"risk-exit-{req.token_id[:8]}",
            status="filled",
            requested_amount=Decimal(str(req.amount)),
            filled_amount=Decimal(str(req.amount)),
            avg_price=Decimal("0.3"),
            fees_paid=Decimal("0"),
            error=None,
        )


class _RejectingExecutionService:
    async def execute(self, req):
        return ExecutionResult(
            accepted=False,
            order_id=None,
            status="rejected",
            requested_amount=Decimal(str(req.amount)),
            filled_amount=Decimal("0"),
            avg_price=None,
            fees_paid=Decimal("0"),
            error="synthetic reject",
        )


class _ReconcilingExecutionService(_RejectingExecutionService):
    async def _confirm_fill(self, order_id: str):
        return {
            "status": "filled",
            "size_matched": "10",
            "avg_price": "0.30",
            "orderID": order_id,
        }


class _ReconcilingNoFillExecutionService(_RejectingExecutionService):
    async def _confirm_fill(self, order_id: str):
        return {
            "status": "cancelled",
            "size_matched": "0",
            "avg_price": None,
            "orderID": order_id,
        }


def _make_context(pool, *, execution_service=None, risk_triggers_repo=None, clob_gateway=None):
    return TradingContext(
        user_id="u1",
        wallet_address="wallet-u1",
        client=_FakeClient(price="0.97"),
        positions_repo=PositionsRepo(pool),
        fills_repo=FillsRepo(pool),
        orders_repo=OrdersRepo(pool),
        risk_blocks_repo=RiskBlocksRepo(pool),
        risk_triggers_repo=risk_triggers_repo or RiskTriggersRepo(pool),
        pnl_tracker=_FakePnlTracker(),
        risk_engine=object(),
        accounting_service=object(),
        position_manager=PositionManager(),
        execution_service=execution_service or _FakeExecutionService(),
        extra={"slippage_tolerance_bps": 50, "clob_gateway": clob_gateway or _FakeClobGateway()},
    )


@pytest.mark.asyncio
async def test_mark_risk_block_idempotent_on_duplicate_calls():
    async with _pg_pool() as pool:
        repo = RiskBlocksRepo(pool)

        await repo.mark_risk_block(
            user_id="u1",
            condition_id="cond-1",
            token_id=FAKE_TOKEN_ID,
            reason="test",
            source="test",
        )
        await repo.mark_risk_block(
            user_id="u1",
            condition_id="cond-1",
            token_id=FAKE_TOKEN_ID,
            reason="test",
            source="test",
        )

        blocks = await repo.list_active_risk_blocks(user_id="u1")
        assert len(blocks) == 1, f"expected exactly 1 block row, got {len(blocks)}"
        assert blocks[0]["user_id"] == "u1"
        assert blocks[0]["condition_id"] == "cond-1"
        assert blocks[0]["token_id"] == FAKE_TOKEN_ID


@pytest.mark.asyncio
async def test_risk_block_is_token_scoped_not_condition_scoped():
    async with _pg_pool() as pool:
        positions_repo = PositionsRepo(pool)

        await positions_repo.upsert_position(
            PositionRecord(
                user_id="u1",
                condition_id="cond-1",
                token_id=FAKE_TOKEN_ID,
                leader_id="l1",
                size_tokens=Decimal("10"),
                avg_entry_price=Decimal("0.60"),
            )
        )
        await positions_repo.upsert_position(
            PositionRecord(
                user_id="u1",
                condition_id="cond-1",
                token_id=TOKEN_B,
                leader_id="l1",
                size_tokens=Decimal("7"),
                avg_entry_price=Decimal("0.42"),
            )
        )

        context = _make_context(pool)
        await rehydrate_position_manager(context)

        await run_risk_check_for_context(context)

        risk_blocks_repo = context.risk_blocks_repo
        block_a = await risk_blocks_repo.get_active_risk_block(
            user_id="u1", condition_id="cond-1", token_id=FAKE_TOKEN_ID,
        )
        block_b = await risk_blocks_repo.get_active_risk_block(
            user_id="u1", condition_id="cond-1", token_id=TOKEN_B,
        )

        assert block_a is not None, "block should exist for token A (full close)"
        assert block_b is not None, "block should exist for token B (full close)"

        assert await positions_repo.get_position("u1", "cond-1", token_id=FAKE_TOKEN_ID) is None
        assert await positions_repo.get_position("u1", "cond-1", token_id=TOKEN_B) is None


@pytest.mark.asyncio
async def test_reconciliation_rejected_risk_sell_no_block():
    async with _pg_pool() as pool:
        positions_repo = PositionsRepo(pool)
        orders_repo = OrdersRepo(pool)

        await positions_repo.upsert_position(
            PositionRecord(
                user_id="u1",
                condition_id="cond-1",
                token_id=FAKE_TOKEN_ID,
                leader_id="l1",
                size_tokens=Decimal("10"),
                avg_entry_price=Decimal("0.60"),
            )
        )
        await orders_repo.save_submission(
            OrderSubmissionRecord(
                idempotency_key="risk-idem-no-fill",
                user_id="u1",
                leader_id="l1",
                order_id="venue-risk-no-fill-1",
                status="submitted_unconfirmed",
                request_payload={
                    "user_id": "u1",
                    "leader_id": "l1",
                    "token_id": FAKE_TOKEN_ID,
                    "condition_id": "cond-1",
                    "side": "SELL",
                    "amount": "10",
                    "reason": "near_max_full_exit",
                    "signal_id": "risk:near_max_full_exit:cond-1:1700000003",
                    "recovery_metadata": {
                        "trigger_price": 0.97,
                        "trigger_size_tokens": 10.0,
                    },
                },
                response_payload={"status": "submitted_unconfirmed", "orderID": "venue-risk-no-fill-1"},
            )
        )

        context = _make_context(
            pool,
            execution_service=_ReconcilingNoFillExecutionService(),
        )

        result = await startup_reconcile_unconfirmed([context])

        assert result == {"u1": 1}
        assert await context.risk_blocks_repo.get_active_risk_block(
            user_id="u1", condition_id="cond-1", token_id=FAKE_TOKEN_ID,
        ) is None

        pos = await positions_repo.get_position("u1", "cond-1", token_id=FAKE_TOKEN_ID)
        assert pos is not None, "position should remain when SELL was cancelled/rejected"


@pytest.mark.asyncio
async def test_restart_full_close_then_risk_recheck_no_duplicate_block():
    async with _pg_pool() as pool:
        positions_repo = PositionsRepo(pool)
        orders_repo = OrdersRepo(pool)

        await positions_repo.upsert_position(
            PositionRecord(
                user_id="u1",
                condition_id="cond-1",
                token_id=FAKE_TOKEN_ID,
                leader_id="l1",
                size_tokens=Decimal("10"),
                avg_entry_price=Decimal("0.60"),
            )
        )
        await orders_repo.save_submission(
            OrderSubmissionRecord(
                idempotency_key="risk-idem-dup",
                user_id="u1",
                leader_id="l1",
                order_id="venue-risk-dup-1",
                status="submitted_unconfirmed",
                request_payload={
                    "user_id": "u1",
                    "leader_id": "l1",
                    "token_id": FAKE_TOKEN_ID,
                    "condition_id": "cond-1",
                    "side": "SELL",
                    "amount": "10",
                    "reason": "near_max_full_exit",
                    "signal_id": "risk:near_max_full_exit:cond-1:1700000004",
                    "recovery_metadata": {
                        "trigger_price": 0.97,
                        "trigger_size_tokens": 10.0,
                    },
                },
                response_payload={"status": "submitted_unconfirmed", "orderID": "venue-risk-dup-1"},
            )
        )

        context = _make_context(
            pool,
            execution_service=_ReconcilingExecutionService(),
        )

        await startup_reconcile_unconfirmed([context])

        block = await context.risk_blocks_repo.get_active_risk_block(
            user_id="u1", condition_id="cond-1", token_id=FAKE_TOKEN_ID,
        )
        assert block is not None, "block should exist after reconciliation full close"

        assert await positions_repo.get_position("u1", "cond-1", token_id=FAKE_TOKEN_ID) is None

        await rehydrate_position_manager(context)
        await run_risk_check_for_context(context)

        blocks = await context.risk_blocks_repo.list_active_risk_blocks(user_id="u1")
        assert len(blocks) == 1, f"expected exactly 1 block after recheck, got {len(blocks)}"


@pytest.mark.asyncio
async def test_risk_trigger_recorded_on_full_close():
    async with _pg_pool() as pool:
        fills_repo = FillsRepo(pool)
        positions_repo = PositionsRepo(pool)
        risk_triggers_repo = RiskTriggersRepo(pool)

        await fills_repo.append_fill(
            FillRecord(
                user_id="u1",
                leader_id="l1",
                order_id="buy-1",
                condition_id="cond-1",
                token_id=FAKE_TOKEN_ID,
                side="BUY",
                size_tokens=Decimal("10"),
                price=Decimal("0.60"),
                fee=Decimal("0"),
                fill_ts=1_700_000_000,
            )
        )
        await positions_repo.rebuild_user_positions_from_fills("u1", fills_repo)

        context = _make_context(
            pool,
            risk_triggers_repo=risk_triggers_repo,
        )
        await rehydrate_position_manager(context)

        await run_risk_check_for_context(context)

        triggers = await risk_triggers_repo.list_triggers_for_position(
            user_id="u1", condition_id="cond-1", token_id=FAKE_TOKEN_ID,
        )
        assert len(triggers) >= 1, "at least one trigger should be recorded on full close"
        trigger = triggers[0]
        assert trigger["fill_status"] == "filled"
        assert trigger["source"] == "risk_monitor"
        assert trigger["fill_order_id"] == f"risk-exit-{FAKE_TOKEN_ID[:8]}"
        assert trigger["reason"] == "near_max_full_exit"
        metadata = trigger.get("metadata") or {}
        assert metadata.get("trigger_price") == pytest.approx(0.97)


@pytest.mark.asyncio
async def test_risk_trigger_recorded_on_rejection():
    async with _pg_pool() as pool:
        fills_repo = FillsRepo(pool)
        positions_repo = PositionsRepo(pool)
        risk_triggers_repo = RiskTriggersRepo(pool)

        await fills_repo.append_fill(
            FillRecord(
                user_id="u1",
                leader_id="l1",
                order_id="buy-1",
                condition_id="cond-1",
                token_id=FAKE_TOKEN_ID,
                side="BUY",
                size_tokens=Decimal("10"),
                price=Decimal("0.60"),
                fee=Decimal("0"),
                fill_ts=1_700_000_000,
            )
        )
        await positions_repo.rebuild_user_positions_from_fills("u1", fills_repo)

        context = _make_context(
            pool,
            execution_service=_RejectingExecutionService(),
            risk_triggers_repo=risk_triggers_repo,
        )
        await rehydrate_position_manager(context)

        await run_risk_check_for_context(context)

        triggers = await risk_triggers_repo.list_triggers_for_position(
            user_id="u1", condition_id="cond-1", token_id=FAKE_TOKEN_ID,
        )
        assert len(triggers) >= 1, "at least one trigger should be recorded on rejection"
        trigger = triggers[0]
        assert trigger["fill_status"] == "rejected"
        assert trigger["source"] == "risk_monitor"

        assert await positions_repo.get_position("u1", "cond-1") is not None, (
            "position should remain open after rejected SELL"
        )
