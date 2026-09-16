"""Phase 4 — IntentRegistry / PositionCoordinator behavioral tests."""

from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest

from services.intentRegistry import (
    DuplicateIntentError,
    IllegalTransitionError,
    IntentRegistry,
    IntentSide,
    IntentState,
    Reservation,
    UnknownIntentError,
)
from services.positionActionCoordinator import PositionScope
from services.positionCoordinator import PositionCoordinator


def _scope(token: str = "TKN-A") -> PositionScope:
    return PositionScope(
        user_id="0xUSER",
        condition_id="0xCOND",
        token_id=token,
    )


# ---------------------------------------------------------------------------
# Reservation lifecycle
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_begin_intent_holds_quote_reservation():
    reg = IntentRegistry()
    scope = _scope()
    record = await reg.begin_intent(
        scope=scope,
        side=IntentSide.BUY,
        reservation=Reservation(quote_usdc=Decimal("100.50")),
    )
    assert record.state is IntentState.INTENT_PENDING
    assert record.side is IntentSide.BUY
    held = reg.reserved_for(scope)
    assert held.quote_usdc == Decimal("100.50")
    assert held.token_shares == Decimal(0)


@pytest.mark.asyncio
async def test_begin_intent_holds_share_reservation():
    reg = IntentRegistry()
    scope = _scope("TKN-SELL")
    record = await reg.begin_intent(
        scope=scope,
        side=IntentSide.SELL,
        reservation=Reservation(token_shares=Decimal("42")),
    )
    assert record.side is IntentSide.SELL
    held = reg.reserved_for(scope)
    assert held.token_shares == Decimal("42")
    assert held.quote_usdc == Decimal(0)


@pytest.mark.asyncio
async def test_release_on_cancel_releases_reservation():
    reg = IntentRegistry()
    scope = _scope()
    record = await reg.begin_intent(
        scope=scope,
        side=IntentSide.BUY,
        reservation=Reservation(quote_usdc=Decimal("75")),
    )
    released = await reg.release_on_cancel_or_fail(record.intent_id, IntentState.CANCELLED)
    assert released.state is IntentState.CANCELLED
    assert reg.reserved_for(scope).is_zero()
    assert reg.open_intent_for(scope) is None


@pytest.mark.asyncio
async def test_release_on_fail_releases_reservation():
    reg = IntentRegistry()
    scope = _scope()
    record = await reg.begin_intent(
        scope=scope,
        side=IntentSide.SELL,
        reservation=Reservation(token_shares=Decimal("10")),
    )
    released = await reg.release_on_cancel_or_fail(record.intent_id, IntentState.ERROR)
    assert released.state is IntentState.ERROR
    assert reg.reserved_for(scope).is_zero()


@pytest.mark.asyncio
async def test_release_on_cancel_or_fail_rejects_done_terminal():
    reg = IntentRegistry()
    scope = _scope()
    record = await reg.begin_intent(
        scope=scope,
        side=IntentSide.BUY,
        reservation=Reservation(quote_usdc=Decimal("1")),
    )
    with pytest.raises(IllegalTransitionError):
        await reg.release_on_cancel_or_fail(record.intent_id, IntentState.DONE)


# ---------------------------------------------------------------------------
# Duplicate-intent blocking
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_duplicate_unresolved_intent_blocked_for_same_scope():
    reg = IntentRegistry()
    scope = _scope()
    await reg.begin_intent(
        scope=scope,
        side=IntentSide.BUY,
        reservation=Reservation(quote_usdc=Decimal("10")),
    )
    with pytest.raises(DuplicateIntentError):
        await reg.begin_intent(
            scope=scope,
            side=IntentSide.SELL,
            reservation=Reservation(token_shares=Decimal("1")),
        )


@pytest.mark.asyncio
async def test_yes_no_token_scopes_do_not_collide():
    reg = IntentRegistry()
    yes_scope = _scope("TKN-YES")
    no_scope = _scope("TKN-NO")
    await reg.begin_intent(
        scope=yes_scope,
        side=IntentSide.BUY,
        reservation=Reservation(quote_usdc=Decimal("5")),
    )
    record = await reg.begin_intent(
        scope=no_scope,
        side=IntentSide.BUY,
        reservation=Reservation(quote_usdc=Decimal("5")),
    )
    assert record.state is IntentState.INTENT_PENDING
    assert reg.open_intent_count() == 2


@pytest.mark.asyncio
async def test_after_release_new_intent_allowed():
    reg = IntentRegistry()
    scope = _scope()
    first = await reg.begin_intent(
        scope=scope,
        side=IntentSide.BUY,
        reservation=Reservation(quote_usdc=Decimal("1")),
    )
    await reg.release_on_cancel_or_fail(first.intent_id, IntentState.CANCELLED)
    second = await reg.begin_intent(
        scope=scope,
        side=IntentSide.BUY,
        reservation=Reservation(quote_usdc=Decimal("2")),
    )
    assert second.intent_id != first.intent_id
    assert reg.reserved_for(scope).quote_usdc == Decimal("2")


# ---------------------------------------------------------------------------
# Reconciler-owned terminal transitions
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_finalize_requires_by_reconciler_flag():
    reg = IntentRegistry()
    scope = _scope()
    record = await reg.begin_intent(
        scope=scope,
        side=IntentSide.BUY,
        reservation=Reservation(quote_usdc=Decimal("1")),
    )
    with pytest.raises(IllegalTransitionError):
        await reg.finalize_intent(record.intent_id, IntentState.DONE, by_reconciler=False)


@pytest.mark.asyncio
async def test_reconciler_finalizes_to_done_and_releases_reservation():
    reg = IntentRegistry()
    scope = _scope()
    record = await reg.begin_intent(
        scope=scope,
        side=IntentSide.BUY,
        reservation=Reservation(quote_usdc=Decimal("100")),
    )
    final = await reg.finalize_intent(record.intent_id, IntentState.DONE, by_reconciler=True)
    assert final.state is IntentState.DONE
    assert reg.reserved_for(scope).is_zero()
    assert reg.open_intent_for(scope) is None


@pytest.mark.asyncio
async def test_reconciler_finalizes_to_error():
    reg = IntentRegistry()
    scope = _scope()
    record = await reg.begin_intent(
        scope=scope,
        side=IntentSide.SELL,
        reservation=Reservation(token_shares=Decimal("3")),
    )
    final = await reg.finalize_intent(record.intent_id, IntentState.ERROR, by_reconciler=True)
    assert final.state is IntentState.ERROR
    assert reg.reserved_for(scope).is_zero()


@pytest.mark.asyncio
async def test_finalize_rejects_non_terminal_state():
    reg = IntentRegistry()
    scope = _scope()
    record = await reg.begin_intent(
        scope=scope,
        side=IntentSide.BUY,
        reservation=Reservation(quote_usdc=Decimal("1")),
    )
    with pytest.raises(IllegalTransitionError):
        await reg.finalize_intent(record.intent_id, IntentState.SUBMITTED, by_reconciler=True)


@pytest.mark.asyncio
async def test_submitter_transition_cannot_write_terminal():
    reg = IntentRegistry()
    scope = _scope()
    record = await reg.begin_intent(
        scope=scope,
        side=IntentSide.BUY,
        reservation=Reservation(quote_usdc=Decimal("1")),
    )
    with pytest.raises(IllegalTransitionError):
        await reg.transition(record.intent_id, IntentState.DONE)
    with pytest.raises(IllegalTransitionError):
        await reg.transition(record.intent_id, IntentState.CANCELLED)


@pytest.mark.asyncio
async def test_submitter_can_progress_through_non_terminal_states():
    reg = IntentRegistry()
    scope = _scope()
    record = await reg.begin_intent(
        scope=scope,
        side=IntentSide.BUY,
        reservation=Reservation(quote_usdc=Decimal("1")),
    )
    for state in (
        IntentState.SUBMITTED,
        IntentState.ACK_LIVE,
        IntentState.PARTIAL,
        IntentState.FILLED,
        IntentState.CONFIRMING,
    ):
        moved = await reg.transition(record.intent_id, state)
        assert moved.state is state


@pytest.mark.asyncio
async def test_finalize_unknown_intent_raises():
    reg = IntentRegistry()
    with pytest.raises(UnknownIntentError):
        await reg.finalize_intent("no-such-id", IntentState.DONE, by_reconciler=True)


@pytest.mark.asyncio
async def test_finalize_after_cancel_is_idempotent():
    reg = IntentRegistry()
    scope = _scope()
    record = await reg.begin_intent(
        scope=scope,
        side=IntentSide.BUY,
        reservation=Reservation(quote_usdc=Decimal("1")),
    )
    await reg.release_on_cancel_or_fail(record.intent_id, IntentState.CANCELLED)
    final = await reg.finalize_intent(record.intent_id, IntentState.DONE, by_reconciler=True)
    assert final.state is IntentState.CANCELLED
    assert reg.reserved_for(scope).is_zero()


# ---------------------------------------------------------------------------
# Concurrency: lock-serialized BUY vs SELL on same scope
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_concurrent_buy_sell_serialized_via_coordinator_lock():
    coord = PositionCoordinator()
    scope = _scope()
    order: list[str] = []

    async def buy_attempt() -> None:
        async with coord.acquire(scope):
            order.append("buy_in")
            try:
                rec = await coord.begin_intent(
                    scope=scope,
                    side=IntentSide.BUY,
                    reservation=Reservation(quote_usdc=Decimal("1")),
                )
            except DuplicateIntentError:
                order.append("buy_blocked")
                return
            await asyncio.sleep(0.01)
            await coord.release_on_cancel_or_fail(rec.intent_id)
            order.append("buy_out")

    async def sell_attempt() -> None:
        async with coord.acquire(scope):
            order.append("sell_in")
            try:
                rec = await coord.begin_intent(
                    scope=scope,
                    side=IntentSide.SELL,
                    reservation=Reservation(token_shares=Decimal("1")),
                )
            except DuplicateIntentError:
                order.append("sell_blocked")
                return
            await asyncio.sleep(0.01)
            await coord.release_on_cancel_or_fail(rec.intent_id)
            order.append("sell_out")

    await asyncio.gather(buy_attempt(), sell_attempt())

    assert len(order) == 4
    assert order[1] == order[0].split("_")[0] + "_out"
    assert order[3] == order[2].split("_")[0] + "_out"
    assert {order[0], order[2]} == {"buy_in", "sell_in"}


@pytest.mark.asyncio
async def test_duplicate_unresolved_blocked_under_lock():
    coord = PositionCoordinator()
    scope = _scope()

    async with coord.acquire(scope):
        await coord.begin_intent(
            scope=scope,
            side=IntentSide.BUY,
            reservation=Reservation(quote_usdc=Decimal("5")),
        )
        with pytest.raises(DuplicateIntentError):
            await coord.begin_intent(
                scope=scope,
                side=IntentSide.BUY,
                reservation=Reservation(quote_usdc=Decimal("5")),
            )


@pytest.mark.asyncio
async def test_position_coordinator_round_trip_done_via_reconciler():
    coord = PositionCoordinator()
    scope = _scope()

    async with coord.acquire(scope):
        rec = await coord.begin_intent(
            scope=scope,
            side=IntentSide.BUY,
            reservation=Reservation(quote_usdc=Decimal("12.5")),
        )
        assert coord.reserved_for(scope).quote_usdc == Decimal("12.5")
        await coord.transition(rec.intent_id, IntentState.SUBMITTED)
        await coord.transition(rec.intent_id, IntentState.ACK_LIVE)
        await coord.transition(rec.intent_id, IntentState.FILLED)

    final = await coord.finalize_intent(rec.intent_id, IntentState.DONE, by_reconciler=True)
    assert final.state is IntentState.DONE
    assert coord.reserved_for(scope).is_zero()
    assert coord.open_intent_for(scope) is None


@pytest.mark.asyncio
async def test_negative_reservation_rejected():
    reg = IntentRegistry()
    scope = _scope()
    with pytest.raises(ValueError):
        await reg.begin_intent(
            scope=scope,
            side=IntentSide.BUY,
            reservation=Reservation(quote_usdc=Decimal("-1")),
        )
