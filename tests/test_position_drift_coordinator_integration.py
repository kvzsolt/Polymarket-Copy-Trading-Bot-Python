"""PR7: ExecutionCoordinator honours PositionDriftService scope pauses."""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from pmcopy.services.execution.coordinator import (
    ExecutionCoordinator,
    ExecutionIntent,
)


def _intent(side: str = "BUY") -> ExecutionIntent:
    return ExecutionIntent(
        user_id="u1",
        condition_id="cond1",
        token_id="0xtok",
        side=side,
        size=Decimal("5"),
        price=Decimal("0.5"),
        notional_usdc=Decimal("2.5"),
        idempotency_key="sig-1",
        source="copy_trade",
        signal_id="sig-1",
        leader_wallet="0xleader",
        max_slippage_bps=200,
        reason="copy_trade",
    )


def _make_coord(*, scope_paused: bool, exec_status: str = "submitted_unconfirmed"):
    reservation_service = MagicMock()
    reservation_service.reservations_repo = MagicMock()
    reservation_service.reserve_for_intent = AsyncMock(return_value=MagicMock(id="res-1"))
    reservation_service.release_terminal = MagicMock()
    execution_service = MagicMock()
    execution_service.execute = AsyncMock(
        return_value=MagicMock(
            status=exec_status,
            accepted=True,
            order_id="ord-1",
            filled_amount=Decimal("0"),
            avg_price=None,
            fees_paid=Decimal("0"),
            error=None,
        )
    )
    coord = ExecutionCoordinator(
        reservation_service=reservation_service,
        positions_repo=MagicMock(),
        execution_service=execution_service,
    )
    drift = MagicMock()
    drift.is_scope_paused = MagicMock(return_value=scope_paused)
    coord.position_drift_service = drift
    return coord, reservation_service, execution_service, drift


@pytest.mark.asyncio
async def test_paused_scope_blocks_copy_buy_submit():
    coord, reservation_service, execution_service, drift = _make_coord(scope_paused=True)
    result = await coord.submit_copy_trade(_intent("BUY"))
    assert result.accepted is False
    assert result.status == "position_drift_unresolved"
    assert result.skip_code == "position_drift_unresolved"
    reservation_service.reserve_for_intent.assert_not_awaited()
    execution_service.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_paused_scope_blocks_copy_sell_submit():
    coord, reservation_service, execution_service, _ = _make_coord(scope_paused=True)
    result = await coord.submit_copy_trade(_intent("SELL"))
    assert result.status == "position_drift_unresolved"
    reservation_service.reserve_for_intent.assert_not_awaited()
    execution_service.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_paused_scope_blocks_risk_exit():
    coord, reservation_service, execution_service, _ = _make_coord(scope_paused=True)
    result = await coord.submit_risk_exit(_intent("SELL"))
    assert result.skip_code == "position_drift_unresolved"
    execution_service.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_clean_scope_passes_through_to_execution_service():
    coord, reservation_service, execution_service, drift = _make_coord(
        scope_paused=False, exec_status="submitted_unconfirmed"
    )
    result = await coord.submit_copy_trade(_intent("BUY"))
    assert result.accepted is True
    assert result.lifecycle_status == "submitted_unconfirmed"
    reservation_service.reserve_for_intent.assert_awaited_once()
    execution_service.execute.assert_awaited_once()
    drift.is_scope_paused.assert_called_once_with(
        user_id="u1", condition_id="cond1", token_id="0xtok"
    )
