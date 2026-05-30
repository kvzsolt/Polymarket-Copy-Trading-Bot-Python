"""ReservationService end-to-end against async PG repos (PG-A6 port)."""
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
from services.execution.reservation_service import (
    Clock,
    InsufficientFreeQuote,
    InsufficientFreeShares,
    ReservationConflict,
    ReservationService,
)
from storage.postgres.positions_repo import PositionsRepo
from storage.postgres.reservations_repo import ReservationsRepo


@contextlib.asynccontextmanager
async def _pg_pool():
    try:
        from testcontainers.postgres import PostgresContainer
        import asyncpg
        from storage.postgres.migrations import apply_migrations as pg_apply
    except Exception as exc:
        pytest.skip(f"Docker / postgres image unavailable: {exc!r}")
    ctr = PostgresContainer("postgres:16-alpine")
    try:
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


class MockClock(Clock):
    def __init__(self, current_time: int):
        self._current_time = current_time

    def utcnow_timestamp(self) -> int:
        return self._current_time

    def advance(self, seconds: int):
        self._current_time += seconds


async def mock_get_balance(address: str) -> Decimal | None:
    if address == "0xfunder":
        return Decimal("100.0")
    return None


def _build_service(pool, clock: MockClock) -> ReservationService:
    reservations_repo = ReservationsRepo(pool)
    positions_repo = PositionsRepo(pool)
    svc = ReservationService(
        reservations_repo=reservations_repo,
        positions_repo=positions_repo,
        get_balance_func=mock_get_balance,
        funder_address="0xfunder",
        clock=clock,
        ttl_seconds=60,
    )

    async def fake_reconciler(_res_id: str) -> bool:
        return True

    svc.reconciler_callback = fake_reconciler
    return svc


def create_mock_intent(
    side: IntentSide,
    size: Decimal = Decimal("10"),
    price: Decimal = Decimal("0.5"),
) -> OrderIntent:
    return OrderIntent(
        key=OrderIntentKey(
            user_id="u1",
            leader_wallet="0xleader",
            condition_id="cond1",
            token_id="1" * 70,
            side=side,
            reason=(
                IntentReason.COPY_BUY
                if side == IntentSide.BUY
                else IntentReason.COPY_SELL
            ),
        ),
        size=size,
        price=price,
        max_slippage_bps=200,
        created_at=datetime.fromtimestamp(1000, tz=timezone.utc),
    )


@pytest.mark.asyncio
async def test_reserve_buy_success():
    async with _pg_pool() as pool:
        svc = _build_service(pool, MockClock(1000))
        intent = create_mock_intent(IntentSide.BUY, size=Decimal("10"), price=Decimal("0.5"))
        res = await svc.reserve_for_intent(intent)

        assert res.status == "active"
        assert res.reserved_usdc == Decimal("5.0")
        assert res.reserved_size == Decimal("0")
        assert res.expires_at.timestamp() == 1060


@pytest.mark.asyncio
async def test_reserve_sell_success():
    async with _pg_pool() as pool:
        svc = _build_service(pool, MockClock(1000))
        positions_repo = PositionsRepo(pool)
        await positions_repo.apply_fill(
            user_id="u1",
            leader_id="0xleader",
            condition_id="cond1",
            token_id="1" * 70,
            side="BUY",
            fill_size=Decimal("20"),
            fill_price=Decimal("0.5"),
        )

        intent = create_mock_intent(IntentSide.SELL, size=Decimal("15"))
        res = await svc.reserve_for_intent(intent)

        assert res.status == "active"
        assert res.reserved_size == Decimal("15")
        assert res.reserved_usdc == Decimal("0")


@pytest.mark.asyncio
async def test_reserve_duplicate_intent_raises_conflict():
    async with _pg_pool() as pool:
        svc = _build_service(pool, MockClock(1000))
        intent = create_mock_intent(IntentSide.BUY)
        await svc.reserve_for_intent(intent)

        with pytest.raises(ReservationConflict):
            await svc.reserve_for_intent(intent)


@pytest.mark.asyncio
async def test_reserve_insufficient_usdc():
    async with _pg_pool() as pool:
        svc = _build_service(pool, MockClock(1000))
        intent = create_mock_intent(IntentSide.BUY, size=Decimal("300"), price=Decimal("0.5"))
        with pytest.raises(InsufficientFreeQuote, match="insufficient free USDC"):
            await svc.reserve_for_intent(intent)


@pytest.mark.asyncio
async def test_reserve_insufficient_position():
    async with _pg_pool() as pool:
        svc = _build_service(pool, MockClock(1000))
        intent = create_mock_intent(IntentSide.SELL, size=Decimal("15"))
        with pytest.raises(InsufficientFreeShares, match="no position found"):
            await svc.reserve_for_intent(intent)


@pytest.mark.asyncio
async def test_expired_reservation_allows_new_reservation():
    async with _pg_pool() as pool:
        clock = MockClock(1000)
        svc = _build_service(pool, clock)
        intent = create_mock_intent(IntentSide.BUY)
        res = await svc.reserve_for_intent(intent)

        clock.advance(61)

        res2 = await svc.reserve_for_intent(intent)
        assert res2.id != res.id
        assert res2.status == "active"


async def _insert_legacy_reservation(pool, intent: OrderIntent, *, expires_at: int):
    """Write a reservation row under the pre-source_event_id 6-field key,
    simulating one created by a build that predates that key component."""
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO order_reservations (
                id, user_id, order_intent_key, condition_id, token_id, side,
                reason, reserved_usdc, reserved_size, status,
                created_at, updated_at, expires_at, metadata_json
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'active',$10,$10,$11,'{}'::jsonb)
            """,
            "legacy-res-1",
            intent.key.user_id,
            intent.key.legacy_stable_key(),
            intent.key.condition_id,
            intent.key.token_id,
            intent.key.side.value,
            intent.key.reason.value,
            "5.0",
            "0",
            1000,
            expires_at,
        )


def _new_key_buy_intent(source_event_id: str) -> OrderIntent:
    return OrderIntent(
        key=OrderIntentKey(
            user_id="u1",
            leader_wallet="0xleader",
            condition_id="cond1",
            token_id="1" * 70,
            side=IntentSide.BUY,
            reason=IntentReason.COPY_BUY,
            source_event_id=source_event_id,
        ),
        size=Decimal("10"),
        price=Decimal("0.5"),
        max_slippage_bps=200,
        created_at=datetime.fromtimestamp(1000, tz=timezone.utc),
    )


@pytest.mark.asyncio
async def test_legacy_format_reservation_blocks_new_key_intent():
    # Rollout safety: an ACTIVE reservation stored under the legacy 6-field
    # key must still block a new (7-field) intent on the same scope, so the
    # key-format change cannot double-reserve during deployment.
    async with _pg_pool() as pool:
        svc = _build_service(pool, MockClock(1000))
        await _insert_legacy_reservation(
            pool, create_mock_intent(IntentSide.BUY), expires_at=1060
        )
        with pytest.raises(ReservationConflict):
            await svc.reserve_for_intent(_new_key_buy_intent("0xdifferenttx"))


@pytest.mark.asyncio
async def test_expired_legacy_reservation_allows_new_key_intent():
    # Once the orphaned legacy reservation passes its TTL, the dual-read
    # expire-reconciles it and the new-key intent proceeds — the transient
    # self-heals.
    async with _pg_pool() as pool:
        clock = MockClock(1000)
        svc = _build_service(pool, clock)
        await _insert_legacy_reservation(
            pool, create_mock_intent(IntentSide.BUY), expires_at=1060
        )
        clock.advance(61)  # now past the legacy reservation's expiry
        res = await svc.reserve_for_intent(_new_key_buy_intent("0xtx"))
        assert res.status == "active"


@pytest.mark.asyncio
async def test_release_terminal():
    async with _pg_pool() as pool:
        svc = _build_service(pool, MockClock(1000))
        reservations_repo = ReservationsRepo(pool)
        intent = create_mock_intent(IntentSide.BUY)
        res = await svc.reserve_for_intent(intent)

        await svc.release_terminal(res.id, "cancelled")

        active = await reservations_repo.get_active_by_intent_key(intent.key.stable_key())
        assert active is None
