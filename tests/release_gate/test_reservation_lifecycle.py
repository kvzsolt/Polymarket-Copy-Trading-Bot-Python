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

import contextlib
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from domain.order_lifecycle import (
    IntentReason,
    IntentSide,
    OrderIntent,
    OrderIntentKey,
)
from storage.postgres.reservations_repo import ReservationsRepo

pytestmark = [pytest.mark.asyncio, pytest.mark.release_gate]


@contextlib.asynccontextmanager
async def _pg_pool():
    """Spin a Postgres testcontainer + return a pool with the full
    schema applied. Auto-skips when Docker is unavailable."""
    try:
        from testcontainers.postgres import PostgresContainer
        import asyncpg
        from storage.postgres.migrations import apply_migrations as pg_apply
    except Exception as exc:
        pytest.skip(f"Docker / postgres image unavailable: {exc!r}")
    try:
        ctr = PostgresContainer("postgres:16-alpine")
        ctr.start()
    except Exception as exc:
        pytest.skip(f"Docker / postgres image unavailable: {exc!r}")
    try:
        dsn = ctr.get_connection_url(driver="asyncpg").replace(
            "postgresql+asyncpg://", "postgresql://"
        )
        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=3)
        async with pool.acquire() as conn:
            await pg_apply(conn)
        try:
            yield pool
        finally:
            await pool.close()
    finally:
        ctr.stop()


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


async def test_buy_reservation_respects_available_balance() -> None:
    async with _pg_pool() as pool:
        repo = ReservationsRepo(pool)
        intent = _intent(IntentSide.BUY)
        reservation = await repo.create_buy_reservation_checked(
            intent=intent,
            reserved_usdc=Decimal("2.50"),
            expires_at=int(intent.created_at.timestamp()) + 120,
            available_balance=Decimal("10.00"),
        )
    assert reservation is not None
    assert reservation.reserved_usdc == Decimal("2.50")


async def test_buy_reservation_rejected_when_balance_short() -> None:
    async with _pg_pool() as pool:
        repo = ReservationsRepo(pool)
        intent = _intent(IntentSide.BUY)
        reservation = await repo.create_buy_reservation_checked(
            intent=intent,
            reserved_usdc=Decimal("100.00"),
            expires_at=int(intent.created_at.timestamp()) + 120,
            available_balance=Decimal("5.00"),
        )
    assert reservation is None


async def test_sell_reservation_respects_held_shares() -> None:
    async with _pg_pool() as pool:
        repo = ReservationsRepo(pool)
        intent = _intent(IntentSide.SELL)
        reservation = await repo.create_sell_reservation_checked(
            intent=intent,
            reserved_size=Decimal("3"),
            expires_at=int(intent.created_at.timestamp()) + 120,
            held_size=Decimal("5"),
        )
    assert reservation is not None
    assert reservation.reserved_size == Decimal("3")


async def test_sell_reservation_rejected_when_shares_short() -> None:
    async with _pg_pool() as pool:
        repo = ReservationsRepo(pool)
        intent = _intent(IntentSide.SELL)
        reservation = await repo.create_sell_reservation_checked(
            intent=intent,
            reserved_size=Decimal("10"),
            expires_at=int(intent.created_at.timestamp()) + 120,
            held_size=Decimal("4"),
        )
    assert reservation is None


async def test_sell_by_intent_short_circuits_on_active_duplicate() -> None:
    """SELL ``_by_intent`` returns the active row instead of inserting a
    second; mirrors the BUY parity test that PR #108 shipped."""
    async with _pg_pool() as pool:
        repo = ReservationsRepo(pool)
        intent = _intent(IntentSide.SELL)
        existing_a, first = await repo.create_sell_reservation_checked_by_intent(
            intent=intent,
            reserved_size=Decimal("3"),
            expires_at=int(intent.created_at.timestamp()) + 120,
            held_size=Decimal("5"),
        )
        assert existing_a is None
        assert first is not None

        existing_b, second = await repo.create_sell_reservation_checked_by_intent(
            intent=intent,
            reserved_size=Decimal("3"),
            expires_at=int(intent.created_at.timestamp()) + 120,
            held_size=Decimal("5"),
        )
        assert existing_b is not None
        assert existing_b.id == first.id
        assert second is None


async def test_sell_by_intent_rejects_when_held_short() -> None:
    async with _pg_pool() as pool:
        repo = ReservationsRepo(pool)
        intent = _intent(IntentSide.SELL)
        existing, result = await repo.create_sell_reservation_checked_by_intent(
            intent=intent,
            reserved_size=Decimal("10"),
            expires_at=int(intent.created_at.timestamp()) + 120,
            held_size=Decimal("4"),
        )
    assert existing is None
    assert result is None


async def test_mark_released_clears_active_lookup() -> None:
    async with _pg_pool() as pool:
        repo = ReservationsRepo(pool)
        intent = _intent(IntentSide.BUY)
        reservation = await repo.create_buy_reservation_checked(
            intent=intent,
            reserved_usdc=Decimal("2.50"),
            expires_at=int(intent.created_at.timestamp()) + 120,
            available_balance=Decimal("10.00"),
        )
        assert reservation is not None
        await repo.mark_released(
            reservation.id,
            reason="cancelled",
            released_at=int(intent.created_at.timestamp()) + 5,
        )
        active = await repo.get_active_by_intent_key(intent.key.stable_key())
    assert active is None
