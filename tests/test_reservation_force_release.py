"""Reservation expiry remains fail-closed without terminal venue evidence.

When the reconciler can never confirm a reservation's linked order is terminal
(stuck deferred "matched", unqueryable order id), the expiry-release path refuses
to release and the reservation pins its (user, condition, token, side) scope
forever. Elapsed time is not authoritative evidence that a delayed FAK did not
fill, so even a configured legacy grace must keep the reservation locked.

These are fast unit tests against a fake repo — no Postgres/Docker required.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from domain.order_lifecycle import IntentReason, IntentSide, Reservation
from services.execution.reservation_service import ReservationService

pytestmark = pytest.mark.asyncio


class _Clock:
    def __init__(self, now: float):
        self._now = now

    def utcnow_timestamp(self) -> float:
        return self._now


class _FakeReservationsRepo:
    def __init__(self, expired: list[Reservation]):
        self._expired = expired
        self.released: list[tuple[str, str, float]] = []

    async def list_expired_active(self, now):
        return list(self._expired)

    async def mark_released(self, reservation_id, reason, released_at):
        self.released.append((reservation_id, reason, released_at))


def _res(*, rid: str = "r1", expires_ts: float, side: IntentSide = IntentSide.BUY) -> Reservation:
    return Reservation(
        id=rid,
        user_id="u1",
        intent_key="u1:cond:tok:BUY:copy",
        condition_id="cond",
        token_id="tok",
        side=side,
        reason=(IntentReason.COPY_BUY if side == IntentSide.BUY else IntentReason.COPY_SELL),
        reserved_usdc=Decimal("5"),
        reserved_size=Decimal("0"),
        status="active",
        created_at=datetime.fromtimestamp(0, tz=timezone.utc),
        expires_at=datetime.fromtimestamp(expires_ts, tz=timezone.utc),
    )


def _svc(repo, *, now: float, grace, callback) -> ReservationService:
    svc = ReservationService(
        reservations_repo=repo,
        positions_repo=object(),
        get_balance_func=None,
        funder_address="0xfunder",
        clock=_Clock(now),
        ttl_seconds=60,
        force_release_grace_seconds=grace,
    )
    svc.reconciler_callback = callback
    return svc


async def _deferred(_id):
    """Reconciler that can never confirm terminal — the deadlock trigger."""
    return False


async def test_deferred_callback_never_force_releases_past_grace():
    """A deferred reconciliation result keeps an expired reservation active."""

    repo = _FakeReservationsRepo([_res(expires_ts=100)])
    svc = _svc(repo, now=200, grace=60, callback=_deferred)
    out = await svc.release_expired_active()
    assert out == []
    assert repo.released == []


async def test_no_force_release_within_grace():
    # expired at t=100, now=130 → only 30s past expiry < grace=60 → keep active.
    repo = _FakeReservationsRepo([_res(expires_ts=100)])
    svc = _svc(repo, now=130, grace=60, callback=_deferred)
    out = await svc.release_expired_active()
    assert out == []
    assert repo.released == []


async def test_no_force_release_when_grace_disabled():
    # grace=None (default/legacy) → never force-release even long past expiry.
    repo = _FakeReservationsRepo([_res(expires_ts=100)])
    svc = _svc(repo, now=10_000, grace=None, callback=_deferred)
    out = await svc.release_expired_active()
    assert out == []
    assert repo.released == []


async def test_callback_error_never_force_releases_past_grace():
    async def _boom(_id):
        raise RuntimeError("venue down")

    repo = _FakeReservationsRepo([_res(expires_ts=100)])
    svc = _svc(repo, now=200, grace=60, callback=_boom)
    out = await svc.release_expired_active()
    assert out == []
    assert repo.released == []


async def test_terminal_callback_still_releases_normally():
    async def _terminal(_id):
        return True

    repo = _FakeReservationsRepo([_res(expires_ts=100)])
    svc = _svc(repo, now=200, grace=60, callback=_terminal)
    out = await svc.release_expired_active()
    assert out == ["r1"]
    # Normal terminal path, NOT the forced reason.
    assert repo.released == [("r1", "expired_terminal", 200)]


async def test_expire_with_reconciliation_stays_blocked_past_grace():
    """A new intent cannot consume shares held by unresolved prior submission."""

    repo = _FakeReservationsRepo([])
    svc = _svc(repo, now=200, grace=60, callback=_deferred)
    released = await svc._expire_with_reconciliation(_res(expires_ts=100))
    assert released is False
    assert repo.released == []


async def test_expire_with_reconciliation_blocks_within_grace():
    repo = _FakeReservationsRepo([])
    svc = _svc(repo, now=130, grace=60, callback=_deferred)
    released = await svc._expire_with_reconciliation(_res(expires_ts=100))
    assert released is False
    assert repo.released == []
