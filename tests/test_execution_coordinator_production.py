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
from types import SimpleNamespace
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
    res.reserved_size = Decimal("0")
    return res


def _intent(
    *,
    side: str = "BUY",
    source: str = "copy_trade",
    size: Decimal = Decimal("5"),
    price: Decimal = Decimal("0.5"),
    live_position_size: Decimal | None = None,
) -> ExecutionIntent:
    notional = size if side == "BUY" else size * price
    return ExecutionIntent(
        user_id="user-1",
        condition_id="cond-1",
        token_id="0xtok",
        side=side,
        size=size,
        price=price,
        notional_usdc=notional,
        idempotency_key="sig-1",
        source=source,
        signal_id="sig-1",
        leader_wallet="0xleader",
        max_slippage_bps=200,
        reason=source,
        live_position_size=live_position_size,
    )


def _make_exec_result(
    *, status: str, accepted: bool = True, order_id: str | None = "ord-1"
) -> MagicMock:
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
        execute_return=_make_exec_result(
            status="duplicate_ignored", accepted=True, order_id="ord-existing"
        ),
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


@pytest.mark.parametrize(
    "terminal_status",
    [
        "rejected",
        "failed",
        "book_no_liquidity",
        "book_spread_wide",
        "stale_market_data",
        "market_not_ready",
        "circuit_open",
    ],
)
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
async def test_buy_reserves_and_submits_exact_collateral_amount() -> None:
    """Keep BUY amount in pUSD while reservation math remains exact."""

    reservation = _make_reservation("res-buy-units")
    coord, reservation_service, execution_service = _make_coordinator(
        reserve_return=reservation,
        execute_return=_make_exec_result(status="submitted_unconfirmed"),
    )

    await coord.submit_copy_trade(_intent(side="BUY", size=Decimal("10"), price=Decimal("0.4")))

    reserved_intent = reservation_service.reserve_for_intent.await_args.args[0]
    assert reserved_intent.size == Decimal("25")
    assert reserved_intent.size * reserved_intent.price == Decimal("10")
    request = execution_service.execute.await_args.args[0]
    assert request.amount == Decimal("10")


@pytest.mark.asyncio
async def test_sell_reserves_and_submits_exact_share_amount() -> None:
    """Keep SELL amount in outcome shares through both lifecycle layers."""

    reservation = _make_reservation("res-sell-units")
    reservation.reserved_size = Decimal("7")
    coord, reservation_service, execution_service = _make_coordinator(
        reserve_return=reservation,
        execute_return=_make_exec_result(status="submitted_unconfirmed"),
    )

    await coord.submit_copy_trade(_intent(side="SELL", size=Decimal("7"), price=Decimal("0.4")))

    reserved_intent = reservation_service.reserve_for_intent.await_args.args[0]
    assert reserved_intent.size == Decimal("7")
    request = execution_service.execute.await_args.args[0]
    assert request.amount == Decimal("7")


@pytest.mark.asyncio
async def test_sell_submits_atomic_reservation_remainder_not_stale_requested_size() -> None:
    """Coordinator submits the exact shares returned by the locked reservation."""

    reservation = _make_reservation("res-sell-remainder")
    reservation.reserved_size = Decimal("8.11236964730804071782")
    coord, reservation_service, execution_service = _make_coordinator(
        reserve_return=reservation,
        execute_return=_make_exec_result(status="submitted_unconfirmed"),
    )
    intent = _intent(
        side="SELL",
        size=Decimal("62.29"),
        price=Decimal("0.21"),
        live_position_size=Decimal("62.2902"),
    )

    await coord.submit_copy_trade(intent)

    request = execution_service.execute.await_args.args[0]
    assert request.amount == Decimal("8.11236964730804071782")
    assert request.recovery_metadata["requested_sell_size"] == "62.29"
    assert request.recovery_metadata["reserved_sell_size"] == "8.11236964730804071782"
    assert request.recovery_metadata["pre_submit_live_position_size"] == "62.2902"


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
async def test_forced_exit_submitted_unconfirmed_enqueues_reconciliation():
    """An ambiguous forced-exit ack is enqueued for reconciliation immediately.

    Otherwise the reservation would stay active (kept for the reconciler to
    release on terminal) but nothing would settle the killed FAK until its TTL
    expired and the risk-loop reaper picked it up — the deadlock this fixes.
    """
    reservation = _make_reservation("res-recon")
    coord, reservation_service, execution_service = _make_coordinator(
        reserve_return=reservation,
        execute_return=_make_exec_result(status="submitted_unconfirmed", order_id="ord-recon"),
    )
    coord.reconciler = AsyncMock()
    coord.reservations_repo.link_order = AsyncMock()

    result = await coord.submit_risk_exit(_intent(side="SELL", source="risk_exit"))

    assert result.accepted is True
    assert result.status == "submitted_unconfirmed"
    # Reservation stays active — reconciler owns terminal release.
    reservation_service.release_terminal.assert_not_called()
    # ...but reconciliation is kicked off now, not at TTL.
    coord.reconciler.enqueue.assert_awaited_once_with("ord-recon")


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


@pytest.mark.asyncio
async def test_buy_without_prior_position_records_a_zero_baseline() -> None:
    """A fresh BUY has a knowable baseline of zero, not an absent one.

    ``pre_submit_live_position_size`` is the only local corroboration the
    ambiguous-order auto-release has. Omitting it for a token we hold nothing
    in made the position-delta probe permanently ``inconclusive``, and because
    the release requires every probe clean, the reservation could never be
    freed — it stayed parked across restarts, shrinking the balance that copy
    sizing is derived from.
    """
    reservation = _make_reservation("res-fresh-buy")
    coord, _reservation_service, execution_service = _make_coordinator(
        reserve_return=reservation,
        execute_return=_make_exec_result(status="submitted_unconfirmed"),
    )
    coord.positions_repo.get_position = AsyncMock(return_value=None)
    intent = _intent(side="BUY", live_position_size=None)

    await coord.submit_copy_trade(intent)

    request = execution_service.execute.await_args.args[0]
    assert request.recovery_metadata["pre_submit_live_position_size"] == "0"


@pytest.mark.asyncio
async def test_buy_with_an_existing_local_position_records_that_size() -> None:
    """When we already hold the token the baseline is the held size."""
    reservation = _make_reservation("res-topup-buy")
    coord, _reservation_service, execution_service = _make_coordinator(
        reserve_return=reservation,
        execute_return=_make_exec_result(status="submitted_unconfirmed"),
    )
    coord.positions_repo.get_position = AsyncMock(
        return_value=SimpleNamespace(size_tokens=Decimal("12.5"))
    )
    intent = _intent(side="BUY", live_position_size=None)

    await coord.submit_copy_trade(intent)

    request = execution_service.execute.await_args.args[0]
    assert request.recovery_metadata["pre_submit_live_position_size"] == "12.5"


@pytest.mark.asyncio
async def test_an_explicit_live_position_size_still_wins() -> None:
    """A caller-supplied live size is authoritative over the local lookup."""
    reservation = _make_reservation("res-explicit")
    coord, _reservation_service, execution_service = _make_coordinator(
        reserve_return=reservation,
        execute_return=_make_exec_result(status="submitted_unconfirmed"),
    )
    coord.positions_repo.get_position = AsyncMock(
        return_value=SimpleNamespace(size_tokens=Decimal("99"))
    )
    intent = _intent(side="SELL", live_position_size=Decimal("62.2902"))

    await coord.submit_copy_trade(intent)

    request = execution_service.execute.await_args.args[0]
    assert request.recovery_metadata["pre_submit_live_position_size"] == "62.2902"


@pytest.mark.asyncio
async def test_a_failing_position_lookup_does_not_block_the_order() -> None:
    """Baseline capture is best-effort: it must never fail a submission."""
    reservation = _make_reservation("res-lookup-fails")
    coord, _reservation_service, execution_service = _make_coordinator(
        reserve_return=reservation,
        execute_return=_make_exec_result(status="submitted_unconfirmed"),
    )
    coord.positions_repo.get_position = AsyncMock(side_effect=RuntimeError("db down"))
    intent = _intent(side="BUY", live_position_size=None)

    result = await coord.submit_copy_trade(intent)

    assert result.accepted is True
    request = execution_service.execute.await_args.args[0]
    assert "pre_submit_live_position_size" not in request.recovery_metadata
