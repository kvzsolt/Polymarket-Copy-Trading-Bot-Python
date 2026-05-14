"""PR 8: ExecutionCoordinator enforces live canary caps."""
from __future__ import annotations

import logging
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from config.app_config import LiveCanaryConfig
from services.execution.coordinator import (
    ExecutionCoordinator,
    ExecutionCoordinatorResult,
    ExecutionIntent,
)


def _canary(*, enabled: bool = True, max_notional: float = 5.0, max_orders: int = 1) -> LiveCanaryConfig:
    return LiveCanaryConfig(
        enabled=enabled,
        max_notional_usdc=max_notional,
        max_orders=max_orders,
    )


def _intent(*, side: str = "BUY", size: str = "2", price: str = "0.5", notional: str = "1.0") -> ExecutionIntent:
    return ExecutionIntent(
        user_id="u1",
        condition_id="cond1",
        token_id="0xtok",
        side=side,
        size=Decimal(size),
        price=Decimal(price),
        notional_usdc=Decimal(notional),
        idempotency_key="sig-1",
        source="copy_trade",
        signal_id="sig-1",
        leader_wallet="0xleader",
        max_slippage_bps=200,
        reason="copy_trade",
    )


def _make_coord(
    *,
    canary: LiveCanaryConfig | None,
    exec_status: str = "submitted_unconfirmed",
    accepted: bool = True,
):
    reservation_service = MagicMock()
    reservation_service.reservations_repo = MagicMock()
    reservation_service.reserve_for_intent = AsyncMock(
        return_value=MagicMock(id="res-1")
    )
    reservation_service.release_terminal = MagicMock()
    execution_service = MagicMock()
    execution_service.execute = AsyncMock(
        return_value=MagicMock(
            status=exec_status,
            accepted=accepted,
            order_id="ord-1" if accepted else None,
            filled_amount=Decimal("0"),
            avg_price=None,
            fees_paid=Decimal("0"),
            error=None,
        )
    )
    coord = ExecutionCoordinator(
        reservation_service=reservation_service,
        orders_repo=MagicMock(),
        fills_repo=MagicMock(),
        positions_repo=MagicMock(),
        execution_service=execution_service,
    )
    coord.live_canary = canary
    return coord, reservation_service, execution_service


@pytest.mark.asyncio
async def test_canary_disabled_passes_through():
    coord, _, exec_svc = _make_coord(canary=_canary(enabled=False))
    result = await coord.submit_copy_trade(_intent())
    assert result.accepted is True
    exec_svc.execute.assert_awaited_once()


@pytest.mark.asyncio
async def test_canary_caps_notional():
    coord, reservation_service, exec_svc = _make_coord(
        canary=_canary(max_notional=0.5)
    )
    result = await coord.submit_copy_trade(_intent(notional="2.0"))
    assert isinstance(result, ExecutionCoordinatorResult)
    assert result.accepted is False
    assert result.skip_code == "live_canary_limit"
    assert "notional" in (result.reason or "").lower()
    exec_svc.execute.assert_not_awaited()
    reservation_service.reserve_for_intent.assert_not_awaited()


@pytest.mark.asyncio
async def test_canary_caps_order_count():
    coord, _, exec_svc = _make_coord(canary=_canary(max_orders=1))
    first = await coord.submit_copy_trade(_intent())
    second = await coord.submit_copy_trade(_intent())
    assert first.accepted is True
    assert second.accepted is False
    assert second.skip_code == "live_canary_limit"
    assert exec_svc.execute.await_count == 1


@pytest.mark.asyncio
async def test_canary_emits_visible_log(caplog):
    coord, _, _ = _make_coord(canary=_canary(max_orders=0))
    caplog.set_level(logging.WARNING, logger=ExecutionCoordinator.__module__)
    result = await coord.submit_copy_trade(_intent())
    assert result.skip_code == "live_canary_limit"
    matched = [r for r in caplog.records if "LIVE CANARY LIMIT REACHED" in r.getMessage()]
    assert matched, "expected LIVE CANARY LIMIT REACHED log"


@pytest.mark.asyncio
async def test_canary_counter_does_not_increment_on_rejection():
    # First submit rejected (status=rejected); counter stays 0; second can fire.
    coord, _, exec_svc = _make_coord(
        canary=_canary(max_orders=1),
        exec_status="rejected",
        accepted=False,
    )
    await coord.submit_copy_trade(_intent())
    assert coord._canary_submit_count == 0  # rejected does not count
    # second attempt still allowed under the cap
    await coord.submit_copy_trade(_intent())
    assert exec_svc.execute.await_count == 2


@pytest.mark.asyncio
async def test_canary_gates_risk_exit():
    coord, _, exec_svc = _make_coord(canary=_canary(max_orders=0))
    result = await coord.submit_risk_exit(_intent(side="SELL"))
    assert result.skip_code == "live_canary_limit"
    exec_svc.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_canary_gates_near_one_exit():
    coord, _, exec_svc = _make_coord(canary=_canary(max_orders=0))
    result = await coord.submit_near_one_exit(_intent(side="SELL"))
    assert result.skip_code == "live_canary_limit"
    exec_svc.execute.assert_not_awaited()
