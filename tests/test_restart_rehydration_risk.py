from __future__ import annotations

from decimal import Decimal

import pytest

from domain.trading_context import TradingContext
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
from storage.sqlite.orders_repo import OrdersRepo
from storage.sqlite.positions_repo import PositionRecord, PositionsRepo
from storage.sqlite.risk_blocks_repo import RiskBlocksRepo

FAKE_TOKEN_ID = "71321045679252212594626385532706912750332728571942532223535764789812338637396"


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
