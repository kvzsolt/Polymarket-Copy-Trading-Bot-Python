"""Release-gate: reservation lifecycle (Issue #75 item 8).

Asserts:
* a BUY reservation refuses to insert when the user's free balance is
  insufficient (atomic, no race with concurrent BUYs);
* a SELL reservation refuses to insert when the user's held shares are
  insufficient for the requested size;
* an active reservation can be released and the row leaves the active set
  (so reconciliation / cancel paths free capacity).
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from domain.order_lifecycle import (
    IntentReason,
    IntentSide,
    OrderIntent,
    OrderIntentKey,
)
from storage.sqlite.reservations_repo import ReservationsRepo


def _intent(side: IntentSide = IntentSide.BUY) -> OrderIntent:
    key = OrderIntentKey(
        user_id="user-1",
        leader_wallet="0xleader",
        condition_id="0xcond",
        token_id="0xtok",
        side=side,
        reason=IntentReason.COPY_BUY if side == IntentSide.BUY else IntentReason.COPY_SELL,
    )
    return OrderIntent(
        key=key,
        size=Decimal("5"),
        price=Decimal("0.5"),
        max_slippage_bps=100,
        created_at=datetime.now(tz=timezone.utc),
        source_signal_id="sig-1",
    )


def test_buy_reservation_respects_available_balance(fresh_db_path: str) -> None:
    repo = ReservationsRepo(fresh_db_path)
    intent = _intent(IntentSide.BUY)
    reservation = repo.create_buy_reservation_checked(
        intent=intent,
        reserved_usdc=Decimal("2.50"),
        expires_at=int(intent.created_at.timestamp()) + 120,
        available_balance=Decimal("10.00"),
    )
    assert reservation is not None
    assert reservation.reserved_usdc == Decimal("2.50")


def test_buy_reservation_rejected_when_balance_short(fresh_db_path: str) -> None:
    repo = ReservationsRepo(fresh_db_path)
    intent = _intent(IntentSide.BUY)
    reservation = repo.create_buy_reservation_checked(
        intent=intent,
        reserved_usdc=Decimal("100.00"),
        expires_at=int(intent.created_at.timestamp()) + 120,
        available_balance=Decimal("5.00"),
    )
    assert reservation is None


def test_sell_reservation_respects_held_shares(fresh_db_path: str) -> None:
    repo = ReservationsRepo(fresh_db_path)
    intent = _intent(IntentSide.SELL)
    reservation = repo.create_sell_reservation_checked(
        intent=intent,
        reserved_size=Decimal("3"),
        expires_at=int(intent.created_at.timestamp()) + 120,
        held_size=Decimal("5"),
    )
    assert reservation is not None
    assert reservation.reserved_size == Decimal("3")


def test_sell_reservation_rejected_when_shares_short(fresh_db_path: str) -> None:
    repo = ReservationsRepo(fresh_db_path)
    intent = _intent(IntentSide.SELL)
    reservation = repo.create_sell_reservation_checked(
        intent=intent,
        reserved_size=Decimal("10"),
        expires_at=int(intent.created_at.timestamp()) + 120,
        held_size=Decimal("4"),
    )
    assert reservation is None


def test_mark_released_clears_active_lookup(fresh_db_path: str) -> None:
    repo = ReservationsRepo(fresh_db_path)
    intent = _intent(IntentSide.BUY)
    reservation = repo.create_buy_reservation_checked(
        intent=intent,
        reserved_usdc=Decimal("2.50"),
        expires_at=int(intent.created_at.timestamp()) + 120,
        available_balance=Decimal("10.00"),
    )
    assert reservation is not None
    repo.mark_released(
        reservation.id,
        reason="cancelled",
        released_at=int(intent.created_at.timestamp()) + 5,
    )
    assert repo.get_active_by_intent_key(intent.key.stable_key()) is None
