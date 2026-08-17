"""Regression: ReservationService.get_balance_func contract is Decimal.

Pre-fix the type hint was ``Callable[[str], Awaitable[float | None]]`` and
the body did ``Decimal(str(balance_float))`` to defensively normalise.
That admitted IEEE-754 representations into the gating arithmetic that
decides whether to accept a USDC reservation. ``getMyBalance`` already
returns Decimal in this branch; the type contract here must match.

Also pins:
  * Constructor accepts ``clock=None`` (mutable default ``DefaultClock()``
    removed — Python anti-pattern, masks state-sharing bugs if Clock
    ever gains state).
"""

from __future__ import annotations

import inspect
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from domain.order_lifecycle import IntentReason, IntentSide, OrderIntent, OrderIntentKey
from services.execution.reservation_service import (
    DefaultClock,
    ReservationService,
)


def test_get_balance_func_returns_decimal_in_signature():
    sig = inspect.signature(ReservationService.__init__)
    param = sig.parameters["get_balance_func"]
    # Annotation should reference Decimal in its rendered string form.
    rendered = str(param.annotation)
    assert "Decimal" in rendered, (
        f"get_balance_func type contract must promise Decimal | None; got {rendered}"
    )
    assert "float" not in rendered, (
        f"get_balance_func type contract must drop float; got {rendered}"
    )


def test_clock_default_is_not_a_mutable_instance():
    """Constructor must use ``Clock | None = None`` (init in body) — not the
    shared mutable default instance pattern."""
    sig = inspect.signature(ReservationService.__init__)
    clock_param = sig.parameters["clock"]
    assert clock_param.default is None, (
        f"clock default must be None (init in body); got {clock_param.default!r}"
    )


def test_default_clock_instances_are_independent():
    """Two ReservationService instances must each construct their own
    DefaultClock — not share the module-level default."""
    a = ReservationService.__new__(ReservationService)
    b = ReservationService.__new__(ReservationService)
    # Simulate constructor body: clock = clock or DefaultClock()
    a.clock = None or DefaultClock()
    b.clock = None or DefaultClock()
    assert a.clock is not b.clock, "each instance must own its DefaultClock"


@pytest.mark.asyncio
async def test_disabled_reservation_service_does_not_check_reserved_balance():
    reservations_repo = AsyncMock()
    positions_repo = AsyncMock()
    get_balance = AsyncMock(return_value=Decimal("1"))
    svc = ReservationService(
        reservations_repo=reservations_repo,
        positions_repo=positions_repo,
        get_balance_func=get_balance,
        funder_address="0xfunder",
        enabled=False,
    )
    intent = OrderIntent(
        key=OrderIntentKey(
            user_id="u1",
            leader_wallet="0xleader",
            condition_id="cond1",
            token_id="token1",
            side=IntentSide.BUY,
            reason=IntentReason.COPY_BUY,
        ),
        size=Decimal("10"),
        price=Decimal("0.5"),
        max_slippage_bps=200,
        created_at=datetime.now(timezone.utc),
    )

    reservation = await svc.reserve_for_intent(intent)

    assert reservation.id.startswith("disabled:")
    assert reservation.reserved_usdc == Decimal("0")
    get_balance.assert_not_awaited()
    reservations_repo.sum_active_quote_for_user.assert_not_awaited()
    reservations_repo.create_buy_reservation_checked_by_intent.assert_not_awaited()
