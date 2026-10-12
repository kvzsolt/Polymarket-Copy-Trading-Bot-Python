"""Disposable-Postgres gate: expiry never frees unresolved no-ID capacity.

A14 final gate. The unit contracts in
``tests/test_reservation_expiry_no_id_release.py`` pin the typed decision
tree against fakes; these tests run the REAL repositories, the REAL
``ReservationService`` expiry sweep, and the REAL bootstrap expiry callback
against a throwaway Postgres (testcontainers):

* a lost-POST ``submitted_unconfirmed`` row without a venue identity keeps
  its reservation active across repeated expiry sweeps (no double-spend
  window), while the row keeps blocking;
* a failed orders lookup keeps the reservation (outage != absence);
* a crash before the write-ahead submission row releases exactly once via
  the pre-submit-orphan proof path;
* a delayed fill discovered after the keep is reconciled and applied exactly
  once, and only then releases the reservation.

The repo's conftest auto-marks this file ``integration`` (it mentions
PostgresContainer), so run it explicitly:

    .venv/bin/python -m pytest -m integration \
        tests/test_reservation_expiry_no_id_release_postgres.py -q
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from domain.order_lifecycle import (
    IntentReason,
    IntentSide,
    OrderIntent,
    OrderIntentKey,
)
from orchestrators.runtime_bootstrap import _bind_reservation_runtime
from services.execution.reservation_expiry import ReservationExpiryDecision
from services.execution.reservation_service import ReservationService
from services.positionActionCoordinator import PositionActionCoordinator
from storage.postgres.fills_repo import FillsRepo
from storage.postgres.orders_repo import OrderSubmissionRecord, OrdersRepo
from storage.postgres.positions_repo import PositionsRepo
from storage.postgres.reservations_repo import ReservationsRepo
from tests.config_helpers import load_test_config

pytestmark = pytest.mark.asyncio

_TOKEN = "71321045679252212594626385532706912750332728571942532289631379312455583992561"
_CONDITION = "0x" + "ab" * 32
_USER = "user-1"
_IDEMPOTENCY_KEY = "copy:leader-1:event-1"


@asynccontextmanager
async def _pg_pool():
    """Spin a Postgres testcontainer and apply the full schema."""
    try:
        from testcontainers.postgres import PostgresContainer
        import asyncpg
        from storage.postgres.migrations import apply_migrations as pg_apply
    except Exception as exc:  # pragma: no cover - dependency guard
        pytest.skip(f"Docker / postgres image unavailable: {exc!r}")
    try:
        ctr = PostgresContainer("postgres:16-alpine")
        ctr.start()
    except Exception as exc:  # pragma: no cover - environment guard
        pytest.skip(f"Docker / postgres image unavailable: {exc!r}")
    try:
        dsn = ctr.get_connection_url(driver="asyncpg").replace(
            "postgresql+asyncpg://", "postgresql://"
        )
        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=4)
        async with pool.acquire() as conn:
            await pg_apply(conn)
        try:
            yield pool
        finally:
            await pool.close()
    finally:
        ctr.stop()


class _Clock:
    def __init__(self, now: int) -> None:
        self._now = now

    def utcnow_timestamp(self) -> int:
        return self._now

    def advance(self, seconds: int) -> None:
        self._now += seconds


async def _get_balance(_address: str) -> Decimal:
    return Decimal("1000")


def _buy_intent() -> OrderIntent:
    return OrderIntent(
        key=OrderIntentKey(
            user_id=_USER,
            leader_wallet="0x" + "11" * 20,
            condition_id=_CONDITION,
            token_id=_TOKEN,
            side=IntentSide.BUY,
            reason=IntentReason.COPY_BUY,
            source_event_id="event-1",
        ),
        size=Decimal("10"),
        price=Decimal("0.5"),
        max_slippage_bps=200,
        created_at=datetime(2026, 10, 11, 12, 0, tzinfo=timezone.utc),
    )


def _request_payload(reservation_id: str) -> dict:
    return {
        "user_id": _USER,
        "leader_id": "0x" + "11" * 20,
        "token_id": _TOKEN,
        "condition_id": _CONDITION,
        "side": "BUY",
        "amount": "10",
        "signal_id": "event-1",
        "recovery_metadata": {"reservation_id": reservation_id},
    }


async def _save_no_id_row(
    orders_repo: OrdersRepo,
    reservation_id: str,
    *,
    status: str = "submitted_unconfirmed",
    order_id: str | None = None,
) -> None:
    """Persist the write-ahead submission row exactly as ExecutionService does."""
    await orders_repo.save_submission(
        OrderSubmissionRecord(
            idempotency_key=_IDEMPOTENCY_KEY,
            user_id=_USER,
            leader_id="0x" + "11" * 20,
            order_id=order_id,
            status=status,
            request_payload=_request_payload(reservation_id),
            response_payload={
                "status": status,
                "success": False,
                "pending_since": 1_760_160_400.0,
            },
        )
    )


async def _backdate_row(pool, *, hours: int = 2) -> None:
    """Age the submission row past the reconciler's initial poll delay.

    Models the real timeline: recovery attaches the venue identity well after
    the original submission, so the venue poll is not deferred as
    ``poll_deferred`` (a fresh row defers its first poll).
    """
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE orders
            SET created_at = now() - ($1 || ' hours')::interval,
                updated_at = now() - ($1 || ' hours')::interval
            WHERE idempotency_key = $2
            """,
            str(hours),
            _IDEMPOTENCY_KEY,
        )


class _RaisingOrdersRepo(OrdersRepo):
    """Real repo whose reservation-keyed lookup always fails."""

    async def get_by_reservation_id(self, reservation_id: str):  # pragma: no cover
        raise RuntimeError("orders table unavailable")


class _Venue:
    """CLOB gateway stand-in exposing one venue order state."""

    def __init__(self, state) -> None:
        self._state = state

    async def get_order(self, order_id: str):
        return self._state


def _bind_context(pool, *, clock: _Clock, orders_repo, gateway=None):
    reservations_repo = ReservationsRepo(pool)
    positions_repo = PositionsRepo(pool)
    reservation_service = ReservationService(
        reservations_repo=reservations_repo,
        positions_repo=positions_repo,
        get_balance_func=_get_balance,
        funder_address="0x" + "22" * 20,
        clock=clock,
        ttl_seconds=60,
    )
    ctx = SimpleNamespace(
        user_id=_USER,
        wallet_address="0x" + "22" * 20,
        execution_service=object(),
        config=load_test_config(),
        position_coordinator=PositionActionCoordinator(),
        extra={"clob_gateway": gateway} if gateway is not None else {},
        orders_repo=orders_repo,
        fills_repo=FillsRepo(pool),
        positions_repo=positions_repo,
        risk_blocks_repo=None,
        risk_triggers_repo=None,
        audit_logger=None,
        pnl_tracker=None,
    )
    coord = SimpleNamespace(
        reservation_service=reservation_service,
        reservations_repo=reservations_repo,
    )
    _bind_reservation_runtime(ctx, coord)
    return reservation_service, reservations_repo


async def _release_reason(pool, reservation_id: str) -> str | None:
    async with pool.acquire() as conn:
        return await conn.fetchval(
            "SELECT metadata_json->>'release_reason' FROM order_reservations WHERE id = $1",
            reservation_id,
        )


async def test_unresolved_no_id_row_survives_repeated_expiry_sweeps() -> None:
    async with _pg_pool() as pool:
        clock = _Clock(1_000)
        orders_repo = OrdersRepo(pool)
        svc, reservations_repo = _bind_context(pool, clock=clock, orders_repo=orders_repo)

        reservation = await svc.reserve_for_intent(_buy_intent())
        await _save_no_id_row(orders_repo, reservation.id)

        clock.advance(61)
        for _ in range(3):
            assert await svc.release_expired_active() == []

        active = await reservations_repo.get_by_id(reservation.id)
        assert active is not None and str(active.status).lower() == "active"
        row = await orders_repo.get_by_reservation_id(reservation.id)
        assert row is not None and row["status"] == "submitted_unconfirmed"
        assert await _release_reason(pool, reservation.id) is None


async def test_orders_lookup_failure_keeps_reservation_active() -> None:
    async with _pg_pool() as pool:
        clock = _Clock(1_000)
        orders_repo = OrdersRepo(pool)
        svc, reservations_repo = _bind_context(
            pool, clock=clock, orders_repo=_RaisingOrdersRepo(pool)
        )

        reservation = await svc.reserve_for_intent(_buy_intent())
        await _save_no_id_row(orders_repo, reservation.id)

        clock.advance(61)
        assert await svc.release_expired_active() == []

        active = await reservations_repo.get_by_id(reservation.id)
        assert active is not None and str(active.status).lower() == "active"
        # The original row is untouched and still blocking.
        row = await orders_repo.get_by_reservation_id(reservation.id)
        assert row is not None and row["order_id"] is None


async def test_crash_before_write_ahead_row_releases_pre_submit_orphan_once() -> None:
    async with _pg_pool() as pool:
        clock = _Clock(1_000)
        svc, reservations_repo = _bind_context(pool, clock=clock, orders_repo=OrdersRepo(pool))

        reservation = await svc.reserve_for_intent(_buy_intent())
        # No submission row is ever persisted: the process died between the
        # reservation acquisition and ExecutionService's pre-POST save.

        clock.advance(61)
        assert await svc.release_expired_active() == [reservation.id]
        assert await svc.release_expired_active() == []

        active = await reservations_repo.get_by_id(reservation.id)
        assert active is not None and str(active.status).lower() == "released"
        assert await _release_reason(pool, reservation.id) == "expired_pre_submit_orphan"


async def test_delayed_fill_after_keep_is_applied_once_and_releases_once() -> None:
    from integrations.polymarket.models import VenueOrderState

    async with _pg_pool() as pool:
        clock = _Clock(1_000)
        orders_repo = OrdersRepo(pool)
        venue_state = VenueOrderState(
            order_id="venue-9",
            status="confirmed",
            filled_size=Decimal("10"),
            avg_fill_price=Decimal("0.50"),
        )
        svc, reservations_repo = _bind_context(
            pool,
            clock=clock,
            orders_repo=orders_repo,
            gateway=_Venue(venue_state),
        )
        fills_repo = FillsRepo(pool)

        reservation = await svc.reserve_for_intent(_buy_intent())
        await _save_no_id_row(orders_repo, reservation.id)

        clock.advance(61)
        with patch(
            "orchestrators.runtime_bootstrap.rehydrate_position_manager",
            new=AsyncMock(return_value=None),
        ):
            # Sweep 1: the unresolved no-ID row keeps the reservation.
            assert await svc.release_expired_active() == []
            assert (
                await svc.reconciler_callback(reservation.id)
                is ReservationExpiryDecision.KEEP_PENDING_RECOVERY
            )

            # Recovery attaches the venue identity (T8 attach path); the row
            # is aged past the reconciler's initial poll delay.
            await _save_no_id_row(
                orders_repo, reservation.id, status="submitted", order_id="venue-9"
            )
            await _backdate_row(pool)

            # Sweep 2: the linked order reconciles the delayed fill and the
            # reconciler releases the reservation (reason "confirmed").
            decision = await svc.reconciler_callback(reservation.id)
            assert decision is ReservationExpiryDecision.RELEASE_TERMINAL_EVIDENCE
            await svc.release_expired_active()

            # Sweep 3+: no second fill, no second release.
            assert (
                await svc.reconciler_callback(reservation.id)
                is ReservationExpiryDecision.RELEASE_TERMINAL_EVIDENCE
            )
            await svc.release_expired_active()

        async with pool.acquire() as conn:
            fill_count = await conn.fetchval(
                "SELECT count(*) FROM fills WHERE order_id = $1", "venue-9"
            )
        assert fill_count == 1
        position = await PositionsRepo(pool).get_position(_USER, _CONDITION, token_id=_TOKEN)
        assert position is not None
        assert Decimal(str(position.size_tokens)) == Decimal("10")
        active = await reservations_repo.get_by_id(reservation.id)
        assert active is not None and str(active.status).lower() == "released"
        assert await _release_reason(pool, reservation.id) == "confirmed"
        del fills_repo
