from __future__ import annotations
import pytest
pytestmark = pytest.mark.skip(reason="sqlite-bound; pending PG port")


from decimal import Decimal

import pytest

from domain.trading_context import TradingContext
from orchestrators.runtime_bootstrap import startup_reconcile_unconfirmed
from services.executionService import ExecutionResult
from services.positionManager import PositionManager
from services.riskMonitor import (
    VenueCircuitBreaker,
    _get_context_cached_price,
    rehydrate_position_manager,
    run_risk_check_for_context,
)
from storage.sqlite.connection import create_connection
from storage.sqlite.fills_repo import FillRecord, FillsRepo
from storage.sqlite.migrations import apply_migrations
from storage.sqlite.orders_repo import OrderSubmissionRecord, OrdersRepo
from storage.sqlite.positions_repo import PositionRecord, PositionsRepo
from storage.sqlite.risk_blocks_repo import RiskBlocksRepo

FAKE_TOKEN_ID = "71321045679252212594626385532706912750332728571942532223535764789812338637396"


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
async def test_restart_rehydration_and_risk_exit_uses_persisted_positions(tmp_path):
    db_path = tmp_path / "state.db"
    with create_connection(str(db_path)) as conn:
        apply_migrations(conn)

    fills_repo = FillsRepo(str(db_path))
    positions_repo = PositionsRepo(str(db_path))
    orders_repo = OrdersRepo(str(db_path))
    risk_blocks_repo = RiskBlocksRepo(str(db_path))
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

    rehydrate_position_manager(context)
    assert context.position_manager.get_position("cond-1", FAKE_TOKEN_ID) is not None

    await run_risk_check_for_context(context)

    assert positions_repo.get_position("u1", "cond-1") is None
    fills = fills_repo.list_fills_for_user_condition("u1", "cond-1")
    assert len(fills) >= 2
    assert pnl_tracker.sells >= 1


def test_rehydrate_preserves_token_distinct_positions(tmp_path):
    db_path = tmp_path / "state.db"
    with create_connection(str(db_path)) as conn:
        apply_migrations(conn)

    positions_repo = PositionsRepo(str(db_path))
    fills_repo = FillsRepo(str(db_path))
    orders_repo = OrdersRepo(str(db_path))
    risk_blocks_repo = RiskBlocksRepo(str(db_path))
    token_b = "61321045679252212594626385532706912750332728571942532223535764789812338637396"

    positions_repo.upsert_position(
        PositionRecord(
            user_id="u1",
            condition_id="cond-1",
            token_id=FAKE_TOKEN_ID,
            leader_id="l1",
            size_tokens=Decimal("10"),
            avg_entry_price=Decimal("0.60"),
        )
    )
    positions_repo.upsert_position(
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

    rehydrate_position_manager(context)

    assert context.position_manager.get_position("cond-1", FAKE_TOKEN_ID) is not None
    assert context.position_manager.get_position("cond-1", token_b) is not None
    assert context.position_manager.get_position("cond-1") is None
    assert len(context.position_manager.open_positions) == 2


@pytest.mark.asyncio
async def test_failed_risk_sell_does_not_create_risk_block(tmp_path):
    """Issue #27: a failed protective SELL must NOT create a durable risk block.

    Previously, mark_risk_block was called before execution, causing a permanent
    re-entry ban even when the SELL was rejected.  After the fix, the block is
    only written after a confirmed fill.
    """
    db_path = tmp_path / "state.db"
    with create_connection(str(db_path)) as conn:
        apply_migrations(conn)

    fills_repo = FillsRepo(str(db_path))
    positions_repo = PositionsRepo(str(db_path))
    orders_repo = OrdersRepo(str(db_path))
    risk_blocks_repo = RiskBlocksRepo(str(db_path))
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
            price=Decimal("0.60"),
            fee=Decimal("0"),
            fill_ts=1_700_000_000,
        )
    )
    positions_repo.rebuild_user_positions_from_fills("u1", fills_repo)

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
    quarantine = risk_blocks_repo.get_active_risk_block(
        user_id="u1",
        condition_id="cond-1",
        token_id=FAKE_TOKEN_ID,
    )
    assert quarantine is None, (
        "risk block must NOT be written when the protective SELL fails"
    )

    # Position should still be open (SELL didn't fill).
    assert positions_repo.get_position("u1", "cond-1") is not None


@pytest.mark.asyncio
async def test_successful_risk_sell_creates_risk_block_with_fill_metadata(tmp_path):
    """Issue #27: a filled protective SELL must persist a risk block with fill metadata."""
    db_path = tmp_path / "state.db"
    with create_connection(str(db_path)) as conn:
        apply_migrations(conn)

    fills_repo = FillsRepo(str(db_path))
    positions_repo = PositionsRepo(str(db_path))
    orders_repo = OrdersRepo(str(db_path))
    risk_blocks_repo = RiskBlocksRepo(str(db_path))
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
            price=Decimal("0.60"),
            fee=Decimal("0"),
            fill_ts=1_700_000_000,
        )
    )
    positions_repo.rebuild_user_positions_from_fills("u1", fills_repo)

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

    quarantine = risk_blocks_repo.get_active_risk_block(
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
async def test_partial_risk_sell_updates_position_but_does_not_create_risk_block(tmp_path):
    db_path = tmp_path / "state.db"
    with create_connection(str(db_path)) as conn:
        apply_migrations(conn)

    fills_repo = FillsRepo(str(db_path))
    positions_repo = PositionsRepo(str(db_path))
    orders_repo = OrdersRepo(str(db_path))
    risk_blocks_repo = RiskBlocksRepo(str(db_path))
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
            price=Decimal("0.60"),
            fee=Decimal("0"),
            fill_ts=1_700_000_000,
        )
    )
    positions_repo.rebuild_user_positions_from_fills("u1", fills_repo)

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

    assert risk_blocks_repo.get_active_risk_block(
        user_id="u1",
        condition_id="cond-1",
        token_id=FAKE_TOKEN_ID,
    ) is None
    remaining = positions_repo.get_position("u1", "cond-1", token_id=FAKE_TOKEN_ID)
    assert remaining is not None
    assert remaining.size_tokens == Decimal("5")
    assert pnl_tracker.sells >= 1


@pytest.mark.asyncio
async def test_venue_circuit_breaker_suppresses_repeated_price_polls(tmp_path, monkeypatch):
    db_path = tmp_path / "state.db"
    with create_connection(str(db_path)) as conn:
        apply_migrations(conn)

    fills_repo = FillsRepo(str(db_path))
    positions_repo = PositionsRepo(str(db_path))
    orders_repo = OrdersRepo(str(db_path))
    risk_blocks_repo = RiskBlocksRepo(str(db_path))
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
            price=Decimal("0.60"),
            fee=Decimal("0"),
            fill_ts=1_700_000_000,
        )
    )
    positions_repo.rebuild_user_positions_from_fills("u1", fills_repo)

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
async def test_startup_reconcile_closes_unconfirmed_risk_sell_and_persists_block(tmp_path):
    db_path = tmp_path / "state.db"
    with create_connection(str(db_path)) as conn:
        apply_migrations(conn)

    fills_repo = FillsRepo(str(db_path))
    positions_repo = PositionsRepo(str(db_path))
    orders_repo = OrdersRepo(str(db_path))
    risk_blocks_repo = RiskBlocksRepo(str(db_path))
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
            price=Decimal("0.60"),
            fee=Decimal("0"),
            fill_ts=1_700_000_000,
        )
    )
    positions_repo.rebuild_user_positions_from_fills("u1", fills_repo)
    orders_repo.save_submission(
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
        extra={"slippage_tolerance_bps": 50, "clob_gateway": _FakeClobGateway(responses={
            "venue-risk-1": {"status": "filled", "size_matched": "10", "avg_price": "0.30", "orderID": "venue-risk-1"},
        })},
    )

    result = await startup_reconcile_unconfirmed([context])

    assert result == {"u1": 1}
    assert positions_repo.get_position("u1", "cond-1") is None
    order_row = orders_repo.get_by_order_id("venue-risk-1")
    assert order_row is not None
    assert order_row["status"] == "filled_reconciled"
    block = risk_blocks_repo.get_active_risk_block(
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
async def test_startup_reconcile_partial_unconfirmed_risk_sell_keeps_position_open_without_block(tmp_path):
    db_path = tmp_path / "state.db"
    with create_connection(str(db_path)) as conn:
        apply_migrations(conn)

    fills_repo = FillsRepo(str(db_path))
    positions_repo = PositionsRepo(str(db_path))
    orders_repo = OrdersRepo(str(db_path))
    risk_blocks_repo = RiskBlocksRepo(str(db_path))
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
            price=Decimal("0.60"),
            fee=Decimal("0"),
            fill_ts=1_700_000_000,
        )
    )
    positions_repo.rebuild_user_positions_from_fills("u1", fills_repo)
    orders_repo.save_submission(
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
            response_payload={"status": "submitted_unconfirmed", "orderID": "venue-risk-partial-1"},
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
        extra={"slippage_tolerance_bps": 50, "clob_gateway": _FakeClobGateway(responses={
            "venue-risk-partial-1": {"status": "cancelled", "size_matched": "4", "avg_price": "0.30", "orderID": "venue-risk-partial-1"},
        })},
    )

    result = await startup_reconcile_unconfirmed([context])

    assert result == {"u1": 1}
    remaining = positions_repo.get_position("u1", "cond-1", token_id=FAKE_TOKEN_ID)
    assert remaining is not None
    assert remaining.size_tokens == Decimal("6")
    order_row = orders_repo.get_by_order_id("venue-risk-partial-1")
    assert order_row is not None
    assert order_row["status"] == "filled_reconciled"
    assert risk_blocks_repo.get_active_risk_block(
        user_id="u1",
        condition_id="cond-1",
        token_id=FAKE_TOKEN_ID,
    ) is None


@pytest.mark.asyncio
async def test_startup_reconcile_risk_sell_applies_only_incremental_matched_delta(tmp_path):
    db_path = tmp_path / "state.db"
    with create_connection(str(db_path)) as conn:
        apply_migrations(conn)

    fills_repo = FillsRepo(str(db_path))
    positions_repo = PositionsRepo(str(db_path))
    orders_repo = OrdersRepo(str(db_path))
    risk_blocks_repo = RiskBlocksRepo(str(db_path))
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
            price=Decimal("0.60"),
            fee=Decimal("0"),
            fill_ts=1_700_000_000,
        )
    )
    fills_repo.append_fill(
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
    positions_repo.rebuild_user_positions_from_fills("u1", fills_repo)
    orders_repo.save_submission(
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
        extra={"slippage_tolerance_bps": 50, "clob_gateway": _FakeClobGateway(responses={
            "venue-risk-progress-1": {"status": "partially_filled", "size_matched": "5", "avg_price": "0.30", "orderID": "venue-risk-progress-1"},
        })},
    )

    result = await startup_reconcile_unconfirmed([context])

    assert result == {"u1": 1}
    remaining = positions_repo.get_position("u1", "cond-1", token_id=FAKE_TOKEN_ID)
    assert remaining is not None
    assert remaining.size_tokens == Decimal("5")
    order_row = orders_repo.get_by_order_id("venue-risk-progress-1")
    assert order_row is not None
    assert order_row["status"] == "submitted_unconfirmed"
    assert order_row["response"].get("matched_previous") == "2"
    assert order_row["response"].get("matched_delta") == "3"
    assert risk_blocks_repo.get_active_risk_block(
        user_id="u1",
        condition_id="cond-1",
        token_id=FAKE_TOKEN_ID,
    ) is None


@pytest.mark.asyncio
async def test_risk_monitor_reconciles_unconfirmed_sell_before_evaluating_positions(tmp_path):
    db_path = tmp_path / "state.db"
    with create_connection(str(db_path)) as conn:
        apply_migrations(conn)

    fills_repo = FillsRepo(str(db_path))
    positions_repo = PositionsRepo(str(db_path))
    orders_repo = OrdersRepo(str(db_path))
    risk_blocks_repo = RiskBlocksRepo(str(db_path))
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
            price=Decimal("0.60"),
            fee=Decimal("0"),
            fill_ts=1_700_000_000,
        )
    )
    positions_repo.rebuild_user_positions_from_fills("u1", fills_repo)
    orders_repo.mark_processed_trade(
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
        extra={"slippage_tolerance_bps": 50, "clob_gateway": _FakeClobGateway(responses={
            "venue-sell-1": {"status": "filled", "size_matched": "10", "avg_price": "0.30", "orderID": "venue-sell-1"},
        })},
    )

    await run_risk_check_for_context(context)

    # The restart-time submitted_unconfirmed SELL should have been reconciled
    # before any new protective SELL decision, so the position is gone and no
    # duplicate protective risk block is created.
    assert positions_repo.get_position("u1", "cond-1") is None
    processed = orders_repo.get_processed_trade("sell-restart-1")
    assert processed is not None
    assert processed["status"] == "filled_reconciled"
    assert risk_blocks_repo.get_active_risk_block(
        user_id="u1",
        condition_id="cond-1",
        token_id=FAKE_TOKEN_ID,
    ) is None


@pytest.mark.asyncio
async def test_risk_price_cache_reuses_recent_price_between_checks(tmp_path):
    db_path = tmp_path / "state.db"
    with create_connection(str(db_path)) as conn:
        apply_migrations(conn)

    fills_repo = FillsRepo(str(db_path))
    positions_repo = PositionsRepo(str(db_path))
    orders_repo = OrdersRepo(str(db_path))
    risk_blocks_repo = RiskBlocksRepo(str(db_path))
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
            price=Decimal("0.60"),
            fee=Decimal("0"),
            fill_ts=1_700_000_000,
        )
    )
    positions_repo.rebuild_user_positions_from_fills("u1", fills_repo)

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

    assert positions_repo.get_position("u1", "cond-1") is not None
    assert pnl_tracker.sells == 0
    assert client.calls == 1
