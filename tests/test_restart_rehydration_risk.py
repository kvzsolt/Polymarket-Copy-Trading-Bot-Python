from __future__ import annotations

import contextlib
from decimal import Decimal

import pytest

from application.trading_context import TradingContext
from orchestrators.runtime_bootstrap import startup_reconcile_unconfirmed
from services.executionService import ExecutionResult
from services.positionManager import PositionManager
from services.risk.risk_monitor import (
    VenueCircuitBreaker,
    _get_context_cached_price,
    rehydrate_position_manager,
    run_risk_check_for_context,
)
from storage.postgres.fills_repo import FillRecord, FillsRepo
from storage.postgres.orders_repo import OrderSubmissionRecord, OrdersRepo
from storage.postgres.positions_repo import PositionRecord, PositionsRepo
from storage.postgres.risk_blocks_repo import RiskBlocksRepo

FAKE_TOKEN_ID = "71321045679252212594626385532706912750332728571942532223535764789812338637396"


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
        self._responses = responses or {}

    async def get_order(self, order_id: str):
        if order_id in self._responses:
            return self._responses[order_id]
        return {"status": "unknown", "size_matched": "0"}

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
        self.calls = 0

    async def get_price(self, _token_id: str, _side: str):
        self.calls += 1
        return {"price": self.price}


class _FailingPriceClient:
    def __init__(self):
        self.calls = 0

    async def get_price(self, _token_id: str, _side: str):
        self.calls += 1
        return None


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


class _PartialRiskExecutionService:
    async def execute(self, req):
        requested = Decimal(str(req.amount))
        return ExecutionResult(
            accepted=True,
            order_id="risk-exit-partial-1",
            status="cancelled",
            requested_amount=requested,
            filled_amount=requested / Decimal("2"),
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


class _ReconcilingPartialExecutionService(_RejectingExecutionService):
    async def _confirm_fill(self, order_id: str):
        return {
            "status": "cancelled",
            "size_matched": "4",
            "avg_price": "0.30",
            "orderID": order_id,
        }


class _ReconcilingRiskProgressExecutionService(_RejectingExecutionService):
    async def _confirm_fill(self, order_id: str):
        return {
            "status": "partially_filled",
            "size_matched": "5",
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


class _FakePnlTracker:
    def __init__(self):
        self.sells = 0

    def record_sell(self, **_kwargs):
        self.sells += 1


@pytest.mark.asyncio
async def test_restart_rehydration_and_risk_exit_uses_persisted_positions():
    async with _pg_pool() as pool:
        fills_repo = FillsRepo(pool)
        positions_repo = PositionsRepo(pool)
        orders_repo = OrdersRepo(pool)
        risk_blocks_repo = RiskBlocksRepo(pool)
        pnl_tracker = _FakePnlTracker()

        await fills_repo.append_fill(
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
        await positions_repo.rebuild_user_positions_from_fills("u1", fills_repo)
        assert await positions_repo.get_position("u1", "cond-1") is not None

        context = TradingContext(
            user_id="u1",
            wallet_address="wallet-u1",
            client=_FakeClient(price="0.97"),
            positions_repo=positions_repo,
            fills_repo=fills_repo,
            orders_repo=orders_repo,
            risk_blocks_repo=risk_blocks_repo,
            pnl_tracker=pnl_tracker,
            risk_engine=object(),
            accounting_service=object(),
            position_manager=PositionManager(),
            execution_service=_FakeExecutionService(),
            extra={"slippage_tolerance_bps": 50},
        )

        await rehydrate_position_manager(context)
        assert context.position_manager.get_position("cond-1", FAKE_TOKEN_ID) is not None

        await run_risk_check_for_context(context)

        assert await positions_repo.get_position("u1", "cond-1") is None
        fills = await fills_repo.list_fills_for_user_condition("u1", "cond-1")
        assert len(fills) >= 2
        assert pnl_tracker.sells >= 1


@pytest.mark.asyncio
async def test_rehydrate_preserves_token_distinct_positions():
    async with _pg_pool() as pool:
        positions_repo = PositionsRepo(pool)
        fills_repo = FillsRepo(pool)
        orders_repo = OrdersRepo(pool)
        risk_blocks_repo = RiskBlocksRepo(pool)
        token_b = "61321045679252212594626385532706912750332728571942532223535764789812338637396"

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
                token_id=token_b,
                leader_id="l1",
                size_tokens=Decimal("7"),
                avg_entry_price=Decimal("0.42"),
            )
        )

        context = TradingContext(
            user_id="u1",
            wallet_address="wallet-u1",
            client=_FakeClient(price="0.97"),
            positions_repo=positions_repo,
            fills_repo=fills_repo,
            orders_repo=orders_repo,
            risk_blocks_repo=risk_blocks_repo,
            pnl_tracker=_FakePnlTracker(),
            risk_engine=object(),
            accounting_service=object(),
            position_manager=PositionManager(),
            execution_service=_FakeExecutionService(),
            extra={"slippage_tolerance_bps": 50},
        )

        await rehydrate_position_manager(context)

        assert context.position_manager.get_position("cond-1", FAKE_TOKEN_ID) is not None
        assert context.position_manager.get_position("cond-1", token_b) is not None
        assert context.position_manager.get_position("cond-1") is None
        assert len(context.position_manager.open_positions) == 2


@pytest.mark.asyncio
async def test_failed_risk_sell_does_not_create_risk_block():
    """Issue #27: a failed protective SELL must NOT create a durable risk block.

    Previously, mark_risk_block was called before execution, causing a permanent
    re-entry ban even when the SELL was rejected.  After the fix, the block is
    only written after a confirmed fill.
    """
    async with _pg_pool() as pool:
        fills_repo = FillsRepo(pool)
        positions_repo = PositionsRepo(pool)
        orders_repo = OrdersRepo(pool)
        risk_blocks_repo = RiskBlocksRepo(pool)
        pnl_tracker = _FakePnlTracker()

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

        context = TradingContext(
            user_id="u1",
            wallet_address="wallet-u1",
            client=_FakeClient(price="0.97"),
            positions_repo=positions_repo,
            fills_repo=fills_repo,
            orders_repo=orders_repo,
            risk_blocks_repo=risk_blocks_repo,
            pnl_tracker=pnl_tracker,
            risk_engine=object(),
            accounting_service=object(),
            position_manager=PositionManager(),
            execution_service=_RejectingExecutionService(),
            extra={"slippage_tolerance_bps": 50},
        )

        await run_risk_check_for_context(context)

        # No risk block should exist — the SELL was rejected.
        quarantine = await risk_blocks_repo.get_active_risk_block(
            user_id="u1",
            condition_id="cond-1",
            token_id=FAKE_TOKEN_ID,
        )
        assert quarantine is None, "risk block must NOT be written when the protective SELL fails"

        # Position should still be open (SELL didn't fill).
        assert await positions_repo.get_position("u1", "cond-1") is not None


@pytest.mark.asyncio
async def test_successful_risk_sell_creates_risk_block_with_fill_metadata():
    """Issue #27: a filled protective SELL must persist a risk block with fill metadata."""
    async with _pg_pool() as pool:
        fills_repo = FillsRepo(pool)
        positions_repo = PositionsRepo(pool)
        orders_repo = OrdersRepo(pool)
        risk_blocks_repo = RiskBlocksRepo(pool)
        pnl_tracker = _FakePnlTracker()

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

        context = TradingContext(
            user_id="u1",
            wallet_address="wallet-u1",
            client=_FakeClient(price="0.97"),
            positions_repo=positions_repo,
            fills_repo=fills_repo,
            orders_repo=orders_repo,
            risk_blocks_repo=risk_blocks_repo,
            pnl_tracker=pnl_tracker,
            risk_engine=object(),
            accounting_service=object(),
            position_manager=PositionManager(),
            execution_service=_FakeExecutionService(),
            extra={"slippage_tolerance_bps": 50},
        )

        await run_risk_check_for_context(context)

        quarantine = await risk_blocks_repo.get_active_risk_block(
            user_id="u1",
            condition_id="cond-1",
            token_id=FAKE_TOKEN_ID,
        )
        assert quarantine is not None, "risk block must be written after successful fill"
        assert quarantine["reason"] == "near_max_full_exit"
        metadata = quarantine.get("metadata") or {}
        # Trigger-time metadata preserved
        assert metadata.get("leader_notional_at_trigger") == pytest.approx(9.7)
        assert metadata.get("trigger_price") == pytest.approx(0.97)
        assert metadata.get("trigger_size_tokens") == pytest.approx(10.0)
        assert metadata.get("trigger_fraction") == pytest.approx(1.0)
        # Fill-time metadata enriched by the fix
        assert metadata.get("fill_price") == pytest.approx(0.3)
        assert metadata.get("fill_size_tokens") == pytest.approx(10.0)
        assert metadata.get("fill_order_id") == "risk-exit-1"


@pytest.mark.asyncio
async def test_partial_risk_sell_updates_position_but_does_not_create_risk_block():
    async with _pg_pool() as pool:
        fills_repo = FillsRepo(pool)
        positions_repo = PositionsRepo(pool)
        orders_repo = OrdersRepo(pool)
        risk_blocks_repo = RiskBlocksRepo(pool)
        pnl_tracker = _FakePnlTracker()

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

        context = TradingContext(
            user_id="u1",
            wallet_address="wallet-u1",
            client=_FakeClient(price="0.97"),
            positions_repo=positions_repo,
            fills_repo=fills_repo,
            orders_repo=orders_repo,
            risk_blocks_repo=risk_blocks_repo,
            pnl_tracker=pnl_tracker,
            risk_engine=object(),
            accounting_service=object(),
            position_manager=PositionManager(),
            execution_service=_PartialRiskExecutionService(),
            extra={"slippage_tolerance_bps": 50},
        )

        await run_risk_check_for_context(context)

        assert (
            await risk_blocks_repo.get_active_risk_block(
                user_id="u1",
                condition_id="cond-1",
                token_id=FAKE_TOKEN_ID,
            )
            is None
        )
        remaining = await positions_repo.get_position("u1", "cond-1", token_id=FAKE_TOKEN_ID)
        assert remaining is not None
        assert remaining.size_tokens == Decimal("5")
        assert pnl_tracker.sells >= 1


@pytest.mark.asyncio
async def test_venue_circuit_breaker_suppresses_repeated_price_polls(monkeypatch):
    async with _pg_pool() as pool:
        fills_repo = FillsRepo(pool)
        positions_repo = PositionsRepo(pool)
        orders_repo = OrdersRepo(pool)
        risk_blocks_repo = RiskBlocksRepo(pool)
        pnl_tracker = _FakePnlTracker()

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

        client = _FailingPriceClient()
        context = TradingContext(
            user_id="u1",
            wallet_address="wallet-u1",
            client=client,
            positions_repo=positions_repo,
            fills_repo=fills_repo,
            orders_repo=orders_repo,
            risk_blocks_repo=risk_blocks_repo,
            pnl_tracker=pnl_tracker,
            risk_engine=object(),
            accounting_service=object(),
            position_manager=PositionManager(),
            execution_service=_RejectingExecutionService(),
            extra={"slippage_tolerance_bps": 50, "risk_check_interval_seconds": 0.25},
        )
        context.extra["venue_circuit_breaker"] = VenueCircuitBreaker(threshold=2, cooldown=60)

        first = await _get_context_cached_price(context, FAKE_TOKEN_ID, refresh_interval=0.25)
        second = await _get_context_cached_price(context, FAKE_TOKEN_ID, refresh_interval=0.25)
        third = await _get_context_cached_price(context, FAKE_TOKEN_ID, refresh_interval=0.25)

        assert first is None
        assert second is None
        assert third is None
        assert client.calls == 2


@pytest.mark.asyncio
async def test_startup_reconcile_closes_unconfirmed_risk_sell_and_persists_block():
    async with _pg_pool() as pool:
        fills_repo = FillsRepo(pool)
        positions_repo = PositionsRepo(pool)
        orders_repo = OrdersRepo(pool)
        risk_blocks_repo = RiskBlocksRepo(pool)
        pnl_tracker = _FakePnlTracker()

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
        await orders_repo.save_submission(
            OrderSubmissionRecord(
                idempotency_key="risk-idem-1",
                user_id="u1",
                leader_id="l1",
                order_id="venue-risk-1",
                status="submitted_unconfirmed",
                request_payload={
                    "user_id": "u1",
                    "leader_id": "l1",
                    "token_id": FAKE_TOKEN_ID,
                    "condition_id": "cond-1",
                    "side": "SELL",
                    "amount": "10",
                    "reason": "near_max_full_exit",
                    "signal_id": "risk:near_max_full_exit:cond-1:1700000001",
                    "recovery_metadata": {
                        "leader_notional_at_trigger": 9.7,
                        "copied_position_notional_at_trigger": 9.7,
                        "trigger_price": 0.97,
                        "trigger_size_tokens": 10.0,
                        "trigger_fraction": 1.0,
                    },
                },
                response_payload={"status": "submitted_unconfirmed", "orderID": "venue-risk-1"},
            )
        )

        context = TradingContext(
            user_id="u1",
            wallet_address="wallet-u1",
            client=_FakeClient(price="0.97"),
            positions_repo=positions_repo,
            fills_repo=fills_repo,
            orders_repo=orders_repo,
            risk_blocks_repo=risk_blocks_repo,
            pnl_tracker=pnl_tracker,
            risk_engine=object(),
            accounting_service=object(),
            position_manager=PositionManager(),
            execution_service=_ReconcilingExecutionService(),
            extra={
                "slippage_tolerance_bps": 50,
                "clob_gateway": _FakeClobGateway(
                    responses={
                        "venue-risk-1": {
                            "status": "filled",
                            "size_matched": "10",
                            "avg_price": "0.30",
                            "orderID": "venue-risk-1",
                        },
                    }
                ),
            },
        )

        result = await startup_reconcile_unconfirmed([context])

        assert result == {"u1": 1}
        assert await positions_repo.get_position("u1", "cond-1") is None
        order_row = await orders_repo.get_by_order_id("venue-risk-1")
        assert order_row is not None
        assert order_row["status"] == "filled_reconciled"
        block = await risk_blocks_repo.get_active_risk_block(
            user_id="u1",
            condition_id="cond-1",
            token_id=FAKE_TOKEN_ID,
        )
        assert block is not None
        metadata = block.get("metadata") or {}
        assert metadata.get("leader_notional_at_trigger") == pytest.approx(9.7)
        assert metadata.get("fill_price") == pytest.approx(0.3)
        assert metadata.get("fill_size_tokens") == pytest.approx(10.0)
        assert metadata.get("fill_order_id") == "venue-risk-1"


@pytest.mark.asyncio
async def test_startup_reconcile_partial_unconfirmed_risk_sell_keeps_position_open_without_block():
    async with _pg_pool() as pool:
        fills_repo = FillsRepo(pool)
        positions_repo = PositionsRepo(pool)
        orders_repo = OrdersRepo(pool)
        risk_blocks_repo = RiskBlocksRepo(pool)
        pnl_tracker = _FakePnlTracker()

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
        await orders_repo.save_submission(
            OrderSubmissionRecord(
                idempotency_key="risk-idem-partial-1",
                user_id="u1",
                leader_id="l1",
                order_id="venue-risk-partial-1",
                status="submitted_unconfirmed",
                request_payload={
                    "user_id": "u1",
                    "leader_id": "l1",
                    "token_id": FAKE_TOKEN_ID,
                    "condition_id": "cond-1",
                    "side": "SELL",
                    "amount": "10",
                    "reason": "near_max_full_exit",
                    "signal_id": "risk:near_max_full_exit:cond-1:1700000002",
                    "recovery_metadata": {
                        "leader_notional_at_trigger": 9.7,
                        "copied_position_notional_at_trigger": 9.7,
                        "trigger_price": 0.97,
                        "trigger_size_tokens": 10.0,
                        "trigger_fraction": 1.0,
                    },
                },
                response_payload={
                    "status": "submitted_unconfirmed",
                    "orderID": "venue-risk-partial-1",
                },
            )
        )

        context = TradingContext(
            user_id="u1",
            wallet_address="wallet-u1",
            client=_FakeClient(price="0.97"),
            positions_repo=positions_repo,
            fills_repo=fills_repo,
            orders_repo=orders_repo,
            risk_blocks_repo=risk_blocks_repo,
            pnl_tracker=pnl_tracker,
            risk_engine=object(),
            accounting_service=object(),
            position_manager=PositionManager(),
            execution_service=_ReconcilingPartialExecutionService(),
            extra={
                "slippage_tolerance_bps": 50,
                "clob_gateway": _FakeClobGateway(
                    responses={
                        "venue-risk-partial-1": {
                            "status": "cancelled",
                            "size_matched": "4",
                            "avg_price": "0.30",
                            "orderID": "venue-risk-partial-1",
                        },
                    }
                ),
            },
        )

        result = await startup_reconcile_unconfirmed([context])

        assert result == {"u1": 1}
        remaining = await positions_repo.get_position("u1", "cond-1", token_id=FAKE_TOKEN_ID)
        assert remaining is not None
        assert remaining.size_tokens == Decimal("6")
        order_row = await orders_repo.get_by_order_id("venue-risk-partial-1")
        assert order_row is not None
        assert order_row["status"] == "filled_reconciled"
        assert (
            await risk_blocks_repo.get_active_risk_block(
                user_id="u1",
                condition_id="cond-1",
                token_id=FAKE_TOKEN_ID,
            )
            is None
        )


@pytest.mark.asyncio
async def test_startup_reconcile_risk_sell_applies_only_incremental_matched_delta():
    async with _pg_pool() as pool:
        fills_repo = FillsRepo(pool)
        positions_repo = PositionsRepo(pool)
        orders_repo = OrdersRepo(pool)
        risk_blocks_repo = RiskBlocksRepo(pool)
        pnl_tracker = _FakePnlTracker()

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
        await fills_repo.append_fill(
            FillRecord(
                user_id="u1",
                leader_id="l1",
                order_id="venue-risk-progress-1",
                condition_id="cond-1",
                token_id=FAKE_TOKEN_ID,
                side="SELL",
                size_tokens=Decimal("2"),
                price=Decimal("0.30"),
                fee=Decimal("0"),
                fill_ts=1_700_000_100,
            )
        )
        await positions_repo.rebuild_user_positions_from_fills("u1", fills_repo)
        await orders_repo.save_submission(
            OrderSubmissionRecord(
                idempotency_key="risk-idem-progress-1",
                user_id="u1",
                leader_id="l1",
                order_id="venue-risk-progress-1",
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
                        "leader_notional_at_trigger": 9.7,
                        "copied_position_notional_at_trigger": 9.7,
                        "trigger_price": 0.97,
                        "trigger_size_tokens": 10.0,
                        "trigger_fraction": 1.0,
                    },
                },
                response_payload={
                    "status": "submitted_unconfirmed",
                    "orderID": "venue-risk-progress-1",
                    "size_matched": "2",
                },
            )
        )

        context = TradingContext(
            user_id="u1",
            wallet_address="wallet-u1",
            client=_FakeClient(price="0.97"),
            positions_repo=positions_repo,
            fills_repo=fills_repo,
            orders_repo=orders_repo,
            risk_blocks_repo=risk_blocks_repo,
            pnl_tracker=pnl_tracker,
            risk_engine=object(),
            accounting_service=object(),
            position_manager=PositionManager(),
            execution_service=_ReconcilingRiskProgressExecutionService(),
            extra={
                "slippage_tolerance_bps": 50,
                "clob_gateway": _FakeClobGateway(
                    responses={
                        "venue-risk-progress-1": {
                            "status": "partially_filled",
                            "size_matched": "5",
                            "avg_price": "0.30",
                            "orderID": "venue-risk-progress-1",
                        },
                    }
                ),
            },
        )

        result = await startup_reconcile_unconfirmed([context])

        assert result == {"u1": 1}
        remaining = await positions_repo.get_position("u1", "cond-1", token_id=FAKE_TOKEN_ID)
        assert remaining is not None
        assert remaining.size_tokens == Decimal("5")
        order_row = await orders_repo.get_by_order_id("venue-risk-progress-1")
        assert order_row is not None
        assert order_row["status"] == "submitted_unconfirmed"
        assert order_row["response"].get("matched_previous") == "2"
        assert order_row["response"].get("matched_delta") == "3"
        assert (
            await risk_blocks_repo.get_active_risk_block(
                user_id="u1",
                condition_id="cond-1",
                token_id=FAKE_TOKEN_ID,
            )
            is None
        )


@pytest.mark.asyncio
async def test_risk_monitor_reconciles_unconfirmed_sell_before_evaluating_positions():
    async with _pg_pool() as pool:
        fills_repo = FillsRepo(pool)
        positions_repo = PositionsRepo(pool)
        orders_repo = OrdersRepo(pool)
        risk_blocks_repo = RiskBlocksRepo(pool)
        pnl_tracker = _FakePnlTracker()

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
        await orders_repo.mark_processed_trade(
            trade_key="sell-restart-1",
            user_id="u1",
            leader_id="l1",
            tx_hash="0xsell",
            side="SELL",
            asset=FAKE_TOKEN_ID,
            condition_id="cond-1",
            status="submitted_unconfirmed",
            order_id="venue-sell-1",
        )

        context = TradingContext(
            user_id="u1",
            wallet_address="wallet-u1",
            client=_FakeClient(price="0.97"),
            positions_repo=positions_repo,
            fills_repo=fills_repo,
            orders_repo=orders_repo,
            risk_blocks_repo=risk_blocks_repo,
            pnl_tracker=pnl_tracker,
            risk_engine=object(),
            accounting_service=object(),
            position_manager=PositionManager(),
            execution_service=_ReconcilingExecutionService(),
            extra={
                "slippage_tolerance_bps": 50,
                "clob_gateway": _FakeClobGateway(
                    responses={
                        "venue-sell-1": {
                            "status": "filled",
                            "size_matched": "10",
                            "avg_price": "0.30",
                            "orderID": "venue-sell-1",
                        },
                    }
                ),
            },
        )

        await run_risk_check_for_context(context)

        # The restart-time submitted_unconfirmed SELL should have been reconciled
        # before any new protective SELL decision, so the position is gone and no
        # duplicate protective risk block is created.
        assert await positions_repo.get_position("u1", "cond-1") is None
        processed = await orders_repo.get_processed_trade("sell-restart-1")
        assert processed is not None
        assert processed["status"] == "filled_reconciled"
        assert (
            await risk_blocks_repo.get_active_risk_block(
                user_id="u1",
                condition_id="cond-1",
                token_id=FAKE_TOKEN_ID,
            )
            is None
        )


@pytest.mark.asyncio
async def test_risk_price_cache_reuses_recent_price_between_checks():
    async with _pg_pool() as pool:
        fills_repo = FillsRepo(pool)
        positions_repo = PositionsRepo(pool)
        orders_repo = OrdersRepo(pool)
        risk_blocks_repo = RiskBlocksRepo(pool)
        pnl_tracker = _FakePnlTracker()

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

        client = _FakeClient(price="0.60")
        context = TradingContext(
            user_id="u1",
            wallet_address="wallet-u1",
            client=client,
            positions_repo=positions_repo,
            fills_repo=fills_repo,
            orders_repo=orders_repo,
            risk_blocks_repo=risk_blocks_repo,
            pnl_tracker=pnl_tracker,
            risk_engine=object(),
            accounting_service=object(),
            position_manager=PositionManager(),
            execution_service=_FakeExecutionService(),
            extra={"slippage_tolerance_bps": 50, "risk_check_interval_seconds": 0.25},
        )

        await run_risk_check_for_context(context)
        await run_risk_check_for_context(context)

        assert await positions_repo.get_position("u1", "cond-1") is not None
        assert pnl_tracker.sells == 0
        assert client.calls == 1


# ---------------------------------------------------------------------------
# Durable near-max warn state: a 60% partial trim must not re-fire after a
# restart. The trim's ``risk_triggers`` row (reason="near_max_partial_trim")
# is the durable record; rehydration restores ``near_max_warn_done`` from it.
# ---------------------------------------------------------------------------


async def _rehydrated_pm_with_trigger(pool, fill_status):
    from storage.postgres.risk_triggers_repo import RiskTriggersRepo

    fills_repo = FillsRepo(pool)
    positions_repo = PositionsRepo(pool)
    triggers_repo = RiskTriggersRepo(pool)

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
    if fill_status is not None:
        trigger_id = await triggers_repo.record_trigger(
            user_id="u1",
            condition_id="cond-1",
            token_id=FAKE_TOKEN_ID,
            reason="near_max_partial_trim",
            fill_status="pending",
        )
        if fill_status != "pending":
            await triggers_repo.update_fill_status(trigger_id, fill_status)

    context = TradingContext(
        user_id="u1",
        wallet_address="wallet-u1",
        client=_FakeClient(price="0.96"),
        positions_repo=positions_repo,
        fills_repo=fills_repo,
        orders_repo=OrdersRepo(pool),
        risk_blocks_repo=RiskBlocksRepo(pool),
        pnl_tracker=_FakePnlTracker(),
        risk_engine=object(),
        accounting_service=object(),
        position_manager=PositionManager(),
        execution_service=_FakeExecutionService(),
        risk_triggers_repo=triggers_repo,
        extra={"slippage_tolerance_bps": 50},
    )
    await rehydrate_position_manager(context)
    return context.position_manager


@pytest.mark.asyncio
async def test_rehydrate_restores_near_max_warn_done_and_suppresses_repeat_trim():
    async with _pg_pool() as pool:
        pm = await _rehydrated_pm_with_trigger(pool, "filled")
        pos = pm.get_position("cond-1", FAKE_TOKEN_ID)
        assert pos is not None
        assert pos.exit_state.near_max_warn_done is True

        # cp in the warn band [0.92, 0.95): the trim must NOT re-fire after restart.
        assert pm.evaluate_risk_action("cond-1", FAKE_TOKEN_ID, 0.93) is None
        # The full-exit tier (>= 0.95 default) is unaffected by warn persistence.
        action = pm.evaluate_risk_action("cond-1", FAKE_TOKEN_ID, 0.96)
        assert action is not None
        assert action["reason"] == "near_max_full_exit"


@pytest.mark.asyncio
async def test_rehydrate_pending_trim_counts_as_done():
    async with _pg_pool() as pool:
        pm = await _rehydrated_pm_with_trigger(pool, "pending")
        pos = pm.get_position("cond-1", FAKE_TOKEN_ID)
        assert pos is not None
        assert pos.exit_state.near_max_warn_done is True
        assert pm.evaluate_risk_action("cond-1", FAKE_TOKEN_ID, 0.93) is None


@pytest.mark.asyncio
async def test_rehydrate_failed_trim_rearms_warn():
    async with _pg_pool() as pool:
        pm = await _rehydrated_pm_with_trigger(pool, "failed")
        pos = pm.get_position("cond-1", FAKE_TOKEN_ID)
        assert pos is not None
        assert pos.exit_state.near_max_warn_done is False
        action = pm.evaluate_risk_action("cond-1", FAKE_TOKEN_ID, 0.93)
        assert action is not None
        assert action["reason"] == "near_max_partial_trim"


@pytest.mark.asyncio
async def test_rehydrate_without_trigger_rows_keeps_warn_armed():
    async with _pg_pool() as pool:
        pm = await _rehydrated_pm_with_trigger(pool, None)
        pos = pm.get_position("cond-1", FAKE_TOKEN_ID)
        assert pos is not None
        assert pos.exit_state.near_max_warn_done is False
