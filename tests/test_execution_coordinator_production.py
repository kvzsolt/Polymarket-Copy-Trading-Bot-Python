"""PR 6: ExecutionCoordinator production-path wrapper around ExecutionService.

Pins the layered-defense dup guard:
  - ReservationConflict blocks ExecutionService.execute (mock asserts not awaited)
    and returns skip_code="reservation_conflict". Existing reservation untouched.
  - ExecutionService duplicate_ignored releases the just-created reservation.
  - ExecutionService inflight_lock releases the just-created reservation.
  - ExecutionService rejected/failed releases the just-created reservation.
  - submitted_unconfirmed leaves reservation active and links it to the order.
  - reservation_id flows through ExecutionRequest.recovery_metadata.
"""
from __future__ import annotations

from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from services.execution.coordinator import (
    ExecutionCoordinator,
    ExecutionCoordinatorResult,
    ExecutionIntent,
)
from services.execution.reservation_service import (
    InsufficientFreeQuote,
    InsufficientFreeShares,
    ReservationConflict,
)


def _make_reservation(reservation_id: str = "res-1"):
    res = MagicMock()
    res.id = reservation_id
    return res


def _intent(
    *,
    side: str = "BUY",
    source: str = "copy_trade",
    size: Decimal = Decimal("5"),
    price: Decimal = Decimal("0.5"),
) -> ExecutionIntent:
    return ExecutionIntent(
        user_id="user-1",
        condition_id="cond-1",
        token_id="0xtok",
        side=side,
        size=size,
        price=price,
        notional_usdc=size * price,
        idempotency_key="sig-1",
        source=source,
        signal_id="sig-1",
        leader_wallet="0xleader",
        max_slippage_bps=200,
        reason=source,
    )


def _make_exec_result(*, status: str, accepted: bool = True, order_id: str | None = "ord-1") -> MagicMock:
    r = MagicMock()
    r.status = status
    r.accepted = accepted
    r.order_id = order_id
    r.filled_amount = Decimal("0")
    r.avg_price = None
    r.fees_paid = Decimal("0")
    r.error = None
    return r


def _make_coordinator(
    *,
    reserve_return=None,
    reserve_side_effect=None,
    execute_return=None,
    reservations_repo=None,
):
    reservation_service = MagicMock()
    reservation_service.reservations_repo = reservations_repo or MagicMock()
    reservation_service.reserve_for_intent = AsyncMock(
        return_value=reserve_return,
        side_effect=reserve_side_effect,
    )
    reservation_service.release_terminal = MagicMock()
    execution_service = MagicMock()
    execution_service.execute = AsyncMock(return_value=execute_return)
    coord = ExecutionCoordinator(
        reservation_service=reservation_service,
        orders_repo=MagicMock(),
        fills_repo=MagicMock(),
        positions_repo=MagicMock(),
        execution_service=execution_service,
    )
    return coord, reservation_service, execution_service


@pytest.mark.asyncio
async def test_reservation_conflict_skips_execute_and_keeps_existing_reservation():
    coord, reservation_service, execution_service = _make_coordinator(
        reserve_side_effect=ReservationConflict("already reserved"),
    )

    result = await coord.submit_copy_trade(_intent())

    assert isinstance(result, ExecutionCoordinatorResult)
    assert result.accepted is False
    assert result.status == "duplicate_ignored"
    assert result.skip_code == "reservation_conflict"
    execution_service.execute.assert_not_awaited()
    reservation_service.release_terminal.assert_not_called()


@pytest.mark.asyncio
async def test_insufficient_free_quote_returns_reservation_failed():
    coord, reservation_service, execution_service = _make_coordinator(
        reserve_side_effect=InsufficientFreeQuote("need 5, free 0"),
    )

    result = await coord.submit_copy_trade(_intent())

    assert result.accepted is False
    assert result.status == "reservation_failed"
    assert result.skip_code == "reservation_insufficient"
    execution_service.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_insufficient_free_shares_for_sell():
    coord, reservation_service, execution_service = _make_coordinator(
        reserve_side_effect=InsufficientFreeShares("no shares"),
    )

    result = await coord.submit_copy_trade(_intent(side="SELL"))

    assert result.skip_code == "reservation_insufficient"


@pytest.mark.asyncio
async def test_submitted_unconfirmed_keeps_reservation_and_links_order():
    reservation = _make_reservation("res-keep")
    coord, reservation_service, execution_service = _make_coordinator(
        reserve_return=reservation,
        execute_return=_make_exec_result(status="submitted_unconfirmed", order_id="ord-keep"),
    )

    result = await coord.submit_copy_trade(_intent())

    assert result.accepted is True
    assert result.status == "submitted_unconfirmed"
    assert result.reservation_id == "res-keep"
    assert result.order_id == "ord-keep"
    reservation_service.release_terminal.assert_not_called()
    # reservation_id is linked to the new order.
    reservation_service.reservations_repo.link_order.assert_called_once_with(
        "res-keep", order_id="ord-keep"
    )


@pytest.mark.asyncio
async def test_duplicate_ignored_releases_just_created_reservation():
    reservation = _make_reservation("res-dup")
    coord, reservation_service, execution_service = _make_coordinator(
        reserve_return=reservation,
        execute_return=_make_exec_result(status="duplicate_ignored", accepted=True, order_id="ord-existing"),
    )

    result = await coord.submit_copy_trade(_intent())

    assert result.accepted is False
    assert result.status == "duplicate_ignored"
    assert result.skip_code == "idempotency_duplicate"
    reservation_service.release_terminal.assert_called_once_with(
        "res-dup", reason="idempotency_duplicate"
    )


@pytest.mark.asyncio
async def test_inflight_lock_releases_just_created_reservation():
    reservation = _make_reservation("res-inflight")
    coord, reservation_service, execution_service = _make_coordinator(
        reserve_return=reservation,
        execute_return=_make_exec_result(status="inflight_lock", accepted=False, order_id=None),
    )

    result = await coord.submit_copy_trade(_intent())

    assert result.accepted is False
    assert result.skip_code == "inflight_lock"
    reservation_service.release_terminal.assert_called_once_with(
        "res-inflight", reason="inflight_lock"
    )


@pytest.mark.parametrize("terminal_status", [
    "rejected", "failed", "book_no_liquidity", "book_spread_wide",
    "stale_market_data", "market_not_ready", "circuit_open",
])
@pytest.mark.asyncio
async def test_terminal_failure_releases_reservation(terminal_status):
    reservation = _make_reservation(f"res-{terminal_status}")
    coord, reservation_service, execution_service = _make_coordinator(
        reserve_return=reservation,
        execute_return=_make_exec_result(status=terminal_status, accepted=False, order_id=None),
    )

    result = await coord.submit_copy_trade(_intent())

    assert result.accepted is False
    assert result.skip_code == terminal_status
    reservation_service.release_terminal.assert_called_once_with(
        f"res-{terminal_status}", reason=terminal_status
    )


@pytest.mark.asyncio
async def test_execute_exception_releases_reservation_and_reraises():
    reservation = _make_reservation("res-exc")
    coord, reservation_service, execution_service = _make_coordinator(
        reserve_return=reservation,
        execute_return=None,
    )
    execution_service.execute.side_effect = RuntimeError("network down")

    with pytest.raises(RuntimeError):
        await coord.submit_copy_trade(_intent())

    reservation_service.release_terminal.assert_called_once_with(
        "res-exc", reason="execute_exception"
    )


@pytest.mark.asyncio
async def test_reservation_id_flows_through_recovery_metadata():
    reservation = _make_reservation("res-meta")
    coord, reservation_service, execution_service = _make_coordinator(
        reserve_return=reservation,
        execute_return=_make_exec_result(status="submitted_unconfirmed"),
    )

    await coord.submit_copy_trade(_intent())

    exec_request = execution_service.execute.await_args.args[0]
    assert exec_request.recovery_metadata is not None
    assert exec_request.recovery_metadata["reservation_id"] == "res-meta"
    assert exec_request.recovery_metadata["source"] == "copy_trade"


@pytest.mark.asyncio
async def test_submit_risk_exit_routes_through_funnel():
    reservation = _make_reservation("res-risk")
    coord, reservation_service, execution_service = _make_coordinator(
        reserve_return=reservation,
        execute_return=_make_exec_result(status="submitted_unconfirmed"),
    )

    result = await coord.submit_risk_exit(_intent(side="SELL", source="risk_exit"))

    assert result.accepted is True
    exec_request = execution_service.execute.await_args.args[0]
    assert exec_request.recovery_metadata["source"] == "risk_exit"


@pytest.mark.asyncio
async def test_submit_near_one_exit_acquires_scope_when_position_coordinator_set():
    reservation = _make_reservation("res-near-one")
    coord, reservation_service, execution_service = _make_coordinator(
        reserve_return=reservation,
        execute_return=_make_exec_result(status="submitted_unconfirmed"),
    )
    scope_mgr = MagicMock()
    captured = []

    class _Scope:
        async def __aenter__(self):
            captured.append("enter")
            return self

        async def __aexit__(self, *a):
            captured.append("exit")
            return False

    scope_mgr.acquire = MagicMock(return_value=_Scope())
    coord.position_coordinator = scope_mgr

    result = await coord.submit_near_one_exit(_intent(side="SELL", source="near_one_exit"))

    assert result.accepted is True
    assert captured == ["enter", "exit"]
    scope_mgr.acquire.assert_called_once()


@pytest.mark.asyncio
async def test_submit_protective_exit_uses_protective_source():
    reservation = _make_reservation("res-prot")
    coord, reservation_service, execution_service = _make_coordinator(
        reserve_return=reservation,
        execute_return=_make_exec_result(status="submitted_unconfirmed"),
    )

    await coord.submit_protective_exit(_intent(side="SELL", source="protective_exit"))

    exec_request = execution_service.execute.await_args.args[0]
    assert exec_request.recovery_metadata["source"] == "protective_exit"
