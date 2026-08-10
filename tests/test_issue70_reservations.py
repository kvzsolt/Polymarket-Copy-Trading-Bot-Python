"""Issue #70 — Reservations and intent lifecycle ownership.

Tests cover:
- Two BUYs cannot collectively exceed free quote
- Two SELLs cannot collectively exceed held shares
- Reservations release on create failure and venue rejection
- Ambiguous POST keeps reservation pending until reconciliation
- Stale reservation expiry triggers reconciliation before release
- Balance/held-size checks account for reservations
- Reservations are recoverable after restart
"""

from __future__ import annotations

import contextlib


import asyncio
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from domain.order_lifecycle import (
    IntentReason,
    IntentSide,
    OrderIntent,
    OrderIntentKey,
    OrderLifecycleStatus,
)
from services.execution.coordinator import (
    ExecutionCoordinator,
    RetryableVenueError,
    VenueSubmitResult,
)
from services.execution.reservation_service import (
    InsufficientFreeQuote,
    InsufficientFreeShares,
    ReservationConflict,
    ReservationService,
)
from services.intentRegistry import (
    DuplicateIntentError,
    IntentRegistry,
    IntentSide as RegistryIntentSide,
    IntentState,
    Reservation,
)
from services.positionActionCoordinator import PositionScope
from services.positionCoordinator import PositionCoordinator
from storage.postgres.positions_repo import PositionsRepo
from storage.postgres.reservations_repo import ReservationsRepo

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_FAKE_TOKEN = "1" * 70
_ALT_TOKEN = "2" * 70


class MockClock:
    def __init__(self, start: int = 1000):
        self._t = start

    def utcnow_timestamp(self) -> int:
        return self._t

    def advance(self, seconds: int) -> None:
        self._t += seconds


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
        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=5)
        async with pool.acquire() as conn:
            await pg_apply(conn)
        try:
            yield pool
        finally:
            await pool.close()
    finally:
        ctr.stop()


def _make_intent(
    side: IntentSide = IntentSide.BUY,
    size: Decimal = Decimal("10"),
    price: Decimal = Decimal("0.5"),
    token_id: str = _FAKE_TOKEN,
    user_id: str = "u1",
    condition_id: str = "cond1",
    reason: IntentReason = IntentReason.COPY_BUY,
) -> OrderIntent:
    if side == IntentSide.SELL and reason == IntentReason.COPY_BUY:
        reason = IntentReason.COPY_SELL
    return OrderIntent(
        key=OrderIntentKey(
            user_id=user_id,
            leader_wallet="0xleader",
            condition_id=condition_id,
            token_id=token_id,
            side=side,
            reason=reason,
        ),
        size=size,
        price=price,
        max_slippage_bps=200,
        created_at=datetime.fromtimestamp(1000, tz=timezone.utc),
    )


async def _balance_func(address: str) -> float | None:
    if address == "0xfunder":
        return 100.0
    return None


def _svc(
    pool,
    clock: MockClock | None = None,
    balance: float = 100.0,
    ttl: int = 60,
    reconciler_callback=None,
) -> tuple[ReservationService, ReservationsRepo, PositionsRepo, MockClock]:
    res_repo = ReservationsRepo(pool)
    pos_repo = PositionsRepo(pool)
    clk = clock or MockClock()

    async def bal(address: str) -> float | None:
        return balance if address == "0xfunder" else None

    svc = ReservationService(
        reservations_repo=res_repo,
        positions_repo=pos_repo,
        get_balance_func=bal,
        funder_address="0xfunder",
        clock=clk,
        ttl_seconds=ttl,
        reconciler_callback=reconciler_callback,
    )
    return svc, res_repo, pos_repo, clk


# ---------------------------------------------------------------------------
# Test: Two BUYs cannot collectively exceed free quote
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_two_buys_cannot_exceed_free_quote(tmp_path):
    async with _pg_pool() as pool:
        svc, *_ = _svc(pool, balance=100.0)
        intent1 = _make_intent(IntentSide.BUY, size=Decimal("150"), price=Decimal("0.5"))
        res1 = await svc.reserve_for_intent(intent1)
        assert res1.reserved_usdc == Decimal("75.0")

        intent2 = _make_intent(
            IntentSide.BUY,
            size=Decimal("100"),
            price=Decimal("0.5"),
            reason=IntentReason.RISK_EXIT,
        )
        with pytest.raises(InsufficientFreeQuote):
            await svc.reserve_for_intent(intent2)


@pytest.mark.asyncio
async def test_second_buy_fits_in_remaining_quote(tmp_path):
    async with _pg_pool() as pool:
        svc, *_ = _svc(pool, balance=100.0)
        intent1 = _make_intent(IntentSide.BUY, size=Decimal("50"), price=Decimal("0.5"))
        res1 = await svc.reserve_for_intent(intent1)
        assert res1.reserved_usdc == Decimal("25.0")

        intent2 = _make_intent(
            IntentSide.BUY,
            size=Decimal("140"),
            price=Decimal("0.5"),
            reason=IntentReason.RISK_EXIT,
        )
        res2 = await svc.reserve_for_intent(intent2)
        assert res2.reserved_usdc == Decimal("70.0")


@pytest.mark.asyncio
async def test_second_buy_just_exceeds_remaining(tmp_path):
    async with _pg_pool() as pool:
        svc, *_ = _svc(pool, balance=100.0)
        intent1 = _make_intent(IntentSide.BUY, size=Decimal("50"), price=Decimal("0.5"))
        await svc.reserve_for_intent(intent1)

        intent2 = _make_intent(
            IntentSide.BUY,
            size=Decimal("151"),
            price=Decimal("0.5"),
            reason=IntentReason.RISK_EXIT,
        )
        with pytest.raises(InsufficientFreeQuote):
            await svc.reserve_for_intent(intent2)


# ---------------------------------------------------------------------------
# Test: Two SELLs cannot collectively exceed held shares
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_two_sells_cannot_exceed_held_shares(tmp_path):
    async with _pg_pool() as pool:
        svc, res_repo, pos_repo, clk = _svc(pool)
        await pos_repo.apply_fill(
            user_id="u1",
            leader_id="0xleader",
            condition_id="cond1",
            token_id=_FAKE_TOKEN,
            side="BUY",
            fill_size=Decimal("20"),
            fill_price=Decimal("0.5"),
        )
        intent1 = _make_intent(IntentSide.SELL, size=Decimal("15"))
        await svc.reserve_for_intent(intent1)

        intent2 = _make_intent(
            IntentSide.SELL,
            size=Decimal("10"),
            reason=IntentReason.NEG_RISK_ARB,
        )
        with pytest.raises(InsufficientFreeShares):
            await svc.reserve_for_intent(intent2)


@pytest.mark.asyncio
async def test_second_sell_fits_in_remaining_shares(tmp_path):
    async with _pg_pool() as pool:
        svc, res_repo, pos_repo, clk = _svc(pool)
        await pos_repo.apply_fill(
            user_id="u1",
            leader_id="0xleader",
            condition_id="cond1",
            token_id=_FAKE_TOKEN,
            side="BUY",
            fill_size=Decimal("30"),
            fill_price=Decimal("0.5"),
        )
        intent1 = _make_intent(IntentSide.SELL, size=Decimal("10"))
        await svc.reserve_for_intent(intent1)

        intent2 = _make_intent(
            IntentSide.SELL,
            size=Decimal("20"),
            reason=IntentReason.NEG_RISK_ARB,
        )
        await svc.reserve_for_intent(intent2)


@pytest.mark.asyncio
async def test_sell_with_no_position_fails(tmp_path):
    async with _pg_pool() as pool:
        svc, *_ = _svc(pool)
        intent = _make_intent(IntentSide.SELL, size=Decimal("5"))
        with pytest.raises(InsufficientFreeShares):
            await svc.reserve_for_intent(intent)


# ---------------------------------------------------------------------------
# Test: Reservations release on create failure and venue rejection
# ---------------------------------------------------------------------------


class _MockRes:
    def __init__(self, rid="r1"):
        self.id = rid


class _MockResSvc:
    def __init__(self):
        self.released: list[tuple[str, str]] = []
        self._next_res = _MockRes()

    async def reserve_for_intent(self, intent):
        return self._next_res

    async def release_terminal(self, rid, reason):
        self.released.append((rid, reason))


class _MockOrder:
    def __init__(self, oid="ord1"):
        self.id = oid


class _MockOrdersRepo:
    def __init__(self, fail_create=False):
        self.transitions: list[tuple] = []
        self.fail_create = fail_create

    async def create_from_intent(self, intent, reservation_id, status):
        if self.fail_create:
            raise RuntimeError("DB write failed")
        return _MockOrder()

    async def transition(self, oid, from_status, to_status, venue_order_id=None, metadata=None):
        self.transitions.append((from_status, to_status))


class _MockFillsRepo:
    async def persist_from_venue_result(self, order_id, venue_result):
        return {"id": "f1"}


class _MockPositionsRepo:
    async def upsert_position(self, fill):
        pass


class _MockGateway:
    def __init__(self, result=None, exception=None):
        self.result = result
        self.exception = exception

    async def submit_order(self, intent):
        if self.exception:
            raise self.exception
        return self.result


class _MockReconciler:
    def __init__(self):
        self.enqueued: list[str] = []

    async def enqueue(self, order_id):
        self.enqueued.append(order_id)


class _MockAudit:
    def __init__(self):
        self.events: list[tuple] = []

    def log_event(self, name, payload):
        self.events.append((name, payload))


@pytest.mark.asyncio
async def test_reservation_released_on_create_failure():
    res_svc = _MockResSvc()
    orders_repo = _MockOrdersRepo(fail_create=True)
    coord = ExecutionCoordinator(
        reservation_service=res_svc,
        orders_repo=orders_repo,
        fills_repo=_MockFillsRepo(),
        positions_repo=_MockPositionsRepo(),
        clob_gateway=_MockGateway(),
        reconciler=_MockReconciler(),
        audit_logger=_MockAudit(),
    )
    intent = _make_intent()
    with pytest.raises(RuntimeError, match="DB write failed"):
        await coord.execute_intent(intent)
    assert len(res_svc.released) == 1
    assert "create_failed" in res_svc.released[0][1]


@pytest.mark.asyncio
async def test_reservation_released_on_venue_rejection():
    res_svc = _MockResSvc()
    gw = _MockGateway(result=VenueSubmitResult(order_id="v1", status="rejected"))
    coord = ExecutionCoordinator(
        reservation_service=res_svc,
        orders_repo=_MockOrdersRepo(),
        fills_repo=_MockFillsRepo(),
        positions_repo=_MockPositionsRepo(),
        clob_gateway=gw,
        reconciler=_MockReconciler(),
        audit_logger=_MockAudit(),
    )
    intent = _make_intent()
    result = await coord.execute_intent(intent)
    assert result.status == OrderLifecycleStatus.TERMINAL_FAILED
    assert len(res_svc.released) == 1


@pytest.mark.asyncio
async def test_reservation_released_on_terminal_exception():
    res_svc = _MockResSvc()
    gw = _MockGateway(exception=RuntimeError("Connection refused"))
    coord = ExecutionCoordinator(
        reservation_service=res_svc,
        orders_repo=_MockOrdersRepo(),
        fills_repo=_MockFillsRepo(),
        positions_repo=_MockPositionsRepo(),
        clob_gateway=gw,
        reconciler=_MockReconciler(),
        audit_logger=_MockAudit(),
    )
    intent = _make_intent()
    with pytest.raises(RuntimeError, match="Connection refused"):
        await coord.execute_intent(intent)
    assert len(res_svc.released) == 1
    assert "terminal_failed" in res_svc.released[0][1]


@pytest.mark.asyncio
async def test_reservation_not_released_on_retryable_error():
    res_svc = _MockResSvc()
    gw = _MockGateway(exception=RetryableVenueError("Timeout"))
    coord = ExecutionCoordinator(
        reservation_service=res_svc,
        orders_repo=_MockOrdersRepo(),
        fills_repo=_MockFillsRepo(),
        positions_repo=_MockPositionsRepo(),
        clob_gateway=gw,
        reconciler=_MockReconciler(),
        audit_logger=_MockAudit(),
    )
    intent = _make_intent()
    result = await coord.execute_intent(intent)
    assert result.status == OrderLifecycleStatus.RETRYABLE_FAILED
    # Issue #98 fix: RetryableVenueError before a live venue_order_id has no
    # durable retry owner — the coordinator must release immediately to
    # avoid stranding capital. Order stays RETRYABLE_FAILED for sweeper pickup.
    assert len(res_svc.released) == 1
    assert res_svc.released[0][1].startswith("retryable_venue_error")


# ---------------------------------------------------------------------------
# Test: Ambiguous POST keeps reservation pending until reconciliation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ambiguous_post_keeps_reservation_pending():
    res_svc = _MockResSvc()
    reconciler = _MockReconciler()
    gw = _MockGateway(result=VenueSubmitResult(order_id="v1", status="live"))
    coord = ExecutionCoordinator(
        reservation_service=res_svc,
        orders_repo=_MockOrdersRepo(),
        fills_repo=_MockFillsRepo(),
        positions_repo=_MockPositionsRepo(),
        clob_gateway=gw,
        reconciler=reconciler,
        audit_logger=_MockAudit(),
    )
    intent = _make_intent()
    result = await coord.execute_intent(intent)
    assert result.status == OrderLifecycleStatus.SUBMITTED_UNCONFIRMED
    assert len(res_svc.released) == 0
    assert len(reconciler.enqueued) == 1


@pytest.mark.asyncio
async def test_unknown_status_keeps_reservation_pending():
    res_svc = _MockResSvc()
    reconciler = _MockReconciler()
    gw = _MockGateway(result=VenueSubmitResult(order_id="v1", status="unknown_weird_status"))
    coord = ExecutionCoordinator(
        reservation_service=res_svc,
        orders_repo=_MockOrdersRepo(),
        fills_repo=_MockFillsRepo(),
        positions_repo=_MockPositionsRepo(),
        clob_gateway=gw,
        reconciler=reconciler,
        audit_logger=_MockAudit(),
    )
    intent = _make_intent()
    result = await coord.execute_intent(intent)
    assert result.status == OrderLifecycleStatus.UNKNOWN_UNCONFIRMED
    assert len(res_svc.released) == 0
    assert len(reconciler.enqueued) == 1


# ---------------------------------------------------------------------------
# Test: Stale reservation expiry triggers reconciliation before release
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_expired_reservation_triggers_reconciliation_before_release(tmp_path):
    async with _pg_pool() as pool:
        reconciled_ids: list[str] = []

        async def on_expire(res_id: str) -> None:
            reconciled_ids.append(res_id)

        svc, res_repo, pos_repo, clk = _svc(pool, ttl=60, reconciler_callback=on_expire)
        intent = _make_intent(IntentSide.BUY, size=Decimal("10"), price=Decimal("0.5"))
        res = await svc.reserve_for_intent(intent)
        await res_repo.link_order(res.id, order_id="order-123")
        assert res.status == "active"

        clk.advance(61)
        released = await svc.release_expired_active()

        assert len(released) == 1
        assert released[0] == res.id
        assert len(reconciled_ids) == 1
        assert reconciled_ids[0] == res.id

        active = await res_repo.get_active_by_intent_key(intent.key.stable_key())
        assert active is not None
        assert active.id == res.id
        assert active.order_id == "order-123"


@pytest.mark.asyncio
async def test_expired_unlinked_reservation_reconciles_before_release(tmp_path):
    async with _pg_pool() as pool:
        reconciled_ids: list[str] = []

        async def on_expire(res_id: str) -> bool:
            reconciled_ids.append(res_id)
            return False

        svc, res_repo, pos_repo, clk = _svc(pool, ttl=60, reconciler_callback=on_expire)
        intent = _make_intent(IntentSide.BUY, size=Decimal("10"), price=Decimal("0.5"))
        res = await svc.reserve_for_intent(intent)

        clk.advance(61)
        released = await svc.release_expired_active()

        assert released == []
        assert reconciled_ids == [res.id]
        active = await res_repo.get_active_by_intent_key(intent.key.stable_key())
        assert active is not None
        assert active.id == res.id
        assert active.order_id is None
        assert active.venue_order_id is None


@pytest.mark.asyncio
async def test_expired_reservation_new_reserve_acquires_after_expiry(tmp_path):
    async with _pg_pool() as pool:
        reconciled_ids: list[str] = []

        async def on_expire(res_id: str) -> bool:
            reconciled_ids.append(res_id)
            return True

        svc, res_repo, pos_repo, clk = _svc(pool, ttl=60, reconciler_callback=on_expire)
        intent = _make_intent(IntentSide.BUY, size=Decimal("10"), price=Decimal("0.5"))
        res1 = await svc.reserve_for_intent(intent)

        clk.advance(61)
        intent2 = _make_intent(IntentSide.BUY, size=Decimal("10"), price=Decimal("0.5"))
        res2 = await svc.reserve_for_intent(intent2)
        assert res2.id != res1.id
        assert res2.status == "active"
        assert len(reconciled_ids) == 1


@pytest.mark.asyncio
async def test_reconciliation_failure_keeps_reservation_active(tmp_path):
    async with _pg_pool() as pool:

        async def bad_callback(res_id: str) -> None:
            raise RuntimeError("Reconciler crashed")

        svc, res_repo, pos_repo, clk = _svc(pool, ttl=60, reconciler_callback=bad_callback)
        intent = _make_intent(IntentSide.BUY, size=Decimal("10"), price=Decimal("0.5"))
        res = await svc.reserve_for_intent(intent)
        await res_repo.link_order(res.id, order_id="order-123")

        clk.advance(61)
        released = await svc.release_expired_active()
        assert released == []

        active = await res_repo.get_active_by_intent_key(intent.key.stable_key())
        assert active is not None
        assert active.id == res.id
        assert active.order_id == "order-123"


# ---------------------------------------------------------------------------
# Test: Scope totals / introspection
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_expired_reservation_callback_failure_blocks_new_reserve(tmp_path):
    async with _pg_pool() as pool:

        async def bad_callback(res_id: str) -> None:
            raise RuntimeError("Reconciler crashed")

        svc, res_repo, pos_repo, clk = _svc(pool, ttl=60, reconciler_callback=bad_callback)
        intent = _make_intent(IntentSide.BUY, size=Decimal("10"), price=Decimal("0.5"))
        res = await svc.reserve_for_intent(intent)

        clk.advance(61)
        with pytest.raises(ReservationConflict):
            await svc.reserve_for_intent(intent)

        active = await res_repo.get_active_by_intent_key(intent.key.stable_key())
        assert active is not None
        assert active.id == res.id


@pytest.mark.asyncio
async def test_scope_totals_tracks_buy_and_sell(tmp_path):
    async with _pg_pool() as pool:
        svc, res_repo, pos_repo, clk = _svc(pool)
        await pos_repo.apply_fill(
            user_id="u1",
            leader_id="0xleader",
            condition_id="cond1",
            token_id=_FAKE_TOKEN,
            side="BUY",
            fill_size=Decimal("50"),
            fill_price=Decimal("0.5"),
        )

        buy_intent = _make_intent(IntentSide.BUY, size=Decimal("20"), price=Decimal("0.5"))
        await svc.reserve_for_intent(buy_intent)

        totals = await svc.get_scope_totals("u1", "cond1", _FAKE_TOKEN)
        assert totals.reserved_usdc == Decimal("10.0")
        assert totals.reserved_size == Decimal("0")

        sell_intent = _make_intent(IntentSide.SELL, size=Decimal("5"))
        await svc.reserve_for_intent(sell_intent)

        totals2 = await svc.get_scope_totals("u1", "cond1", _FAKE_TOKEN)
        assert totals2.reserved_usdc == Decimal("10.0")
        assert totals2.reserved_size == Decimal("5")


# ---------------------------------------------------------------------------
# Test: IntentRegistry duplicate blocking + release + restart recovery
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_duplicate_unresolved_blocks_second_intent():
    reg = IntentRegistry()
    scope = PositionScope(user_id="u1", condition_id="c1", token_id="t1")
    await reg.begin_intent(scope, RegistryIntentSide.BUY, Reservation(quote_usdc=Decimal("10")))
    with pytest.raises(DuplicateIntentError):
        await reg.begin_intent(scope, RegistryIntentSide.BUY, Reservation(quote_usdc=Decimal("5")))


@pytest.mark.asyncio
async def test_release_then_re_open():
    reg = IntentRegistry()
    scope = PositionScope(user_id="u1", condition_id="c1", token_id="t1")
    r1 = await reg.begin_intent(
        scope, RegistryIntentSide.BUY, Reservation(quote_usdc=Decimal("10"))
    )
    await reg.release_on_cancel_or_fail(r1.intent_id, IntentState.CANCELLED)
    assert reg.reserved_for(scope).is_zero()

    await reg.begin_intent(scope, RegistryIntentSide.BUY, Reservation(quote_usdc=Decimal("5")))
    assert reg.reserved_for(scope).quote_usdc == Decimal("5")


@pytest.mark.asyncio
async def test_reconciler_finalize_releases_reservation():
    reg = IntentRegistry()
    scope = PositionScope(user_id="u1", condition_id="c1", token_id="t1")
    r = await reg.begin_intent(
        scope, RegistryIntentSide.SELL, Reservation(token_shares=Decimal("20"))
    )
    await reg.transition(r.intent_id, IntentState.SUBMITTED)
    final = await reg.finalize_intent(r.intent_id, IntentState.DONE, by_reconciler=True)
    assert final.state is IntentState.DONE
    assert reg.reserved_for(scope).is_zero()
    assert reg.open_intent_for(scope) is None


@pytest.mark.asyncio
async def test_position_coordinator_full_lifecycle():
    coord = PositionCoordinator()
    scope = PositionScope(user_id="u1", condition_id="c1", token_id="t1")

    async with coord.acquire(scope):
        rec = await coord.begin_intent(
            scope, RegistryIntentSide.BUY, Reservation(quote_usdc=Decimal("50"))
        )
        assert coord.reserved_for(scope).quote_usdc == Decimal("50")
        await coord.transition(rec.intent_id, IntentState.SUBMITTED)
        await coord.transition(rec.intent_id, IntentState.ACK_LIVE)

    final = await coord.finalize_intent(rec.intent_id, IntentState.DONE, by_reconciler=True)
    assert final.state is IntentState.DONE
    assert coord.reserved_for(scope).is_zero()


# ---------------------------------------------------------------------------
# Test: Different tokens under same condition run independently
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_different_tokens_same_condition_independent_reservations(tmp_path):
    async with _pg_pool() as pool:
        svc, res_repo, pos_repo, clk = _svc(pool)
        await pos_repo.apply_fill(
            user_id="u1",
            leader_id="0xleader",
            condition_id="cond1",
            token_id=_FAKE_TOKEN,
            side="BUY",
            fill_size=Decimal("50"),
            fill_price=Decimal("0.5"),
        )
        await pos_repo.apply_fill(
            user_id="u1",
            leader_id="0xleader",
            condition_id="cond1",
            token_id=_ALT_TOKEN,
            side="BUY",
            fill_size=Decimal("50"),
            fill_price=Decimal("0.5"),
        )

        intent_a = _make_intent(
            IntentSide.BUY, size=Decimal("50"), price=Decimal("0.5"), token_id=_FAKE_TOKEN
        )
        await svc.reserve_for_intent(intent_a)

        intent_b = _make_intent(
            IntentSide.BUY,
            size=Decimal("50"),
            price=Decimal("0.5"),
            token_id=_ALT_TOKEN,
            reason=IntentReason.RISK_EXIT,
        )
        await svc.reserve_for_intent(intent_b)

        totals_a = await svc.get_scope_totals("u1", "cond1", _FAKE_TOKEN)
        totals_b = await svc.get_scope_totals("u1", "cond1", _ALT_TOKEN)
        assert totals_a.reserved_usdc == Decimal("25.0")
        assert totals_b.reserved_usdc == Decimal("25.0")


@pytest.mark.asyncio
async def test_quote_reservations_are_account_wide_across_tokens(tmp_path):
    async with _pg_pool() as pool:
        svc, *_ = _svc(pool, balance=100.0)
        intent_a = _make_intent(
            IntentSide.BUY,
            size=Decimal("150"),
            price=Decimal("0.5"),
            token_id=_FAKE_TOKEN,
        )
        await svc.reserve_for_intent(intent_a)

        intent_b = _make_intent(
            IntentSide.BUY,
            size=Decimal("60"),
            price=Decimal("0.5"),
            token_id=_ALT_TOKEN,
            reason=IntentReason.RISK_EXIT,
        )
        with pytest.raises(InsufficientFreeQuote):
            await svc.reserve_for_intent(intent_b)


@pytest.mark.asyncio
async def test_concurrent_buy_reservations_serialize_capacity(tmp_path):
    async with _pg_pool() as pool:
        svc, *_ = _svc(pool, balance=100.0)
        intent_a = _make_intent(
            IntentSide.BUY,
            size=Decimal("80"),
            price=Decimal("1"),
            reason=IntentReason.COPY_BUY,
        )
        intent_b = _make_intent(
            IntentSide.BUY,
            size=Decimal("80"),
            price=Decimal("1"),
            reason=IntentReason.RISK_EXIT,
        )

        results = await asyncio.gather(
            svc.reserve_for_intent(intent_a),
            svc.reserve_for_intent(intent_b),
            return_exceptions=True,
        )

        successes = [r for r in results if not isinstance(r, Exception)]
        failures = [r for r in results if isinstance(r, InsufficientFreeQuote)]
        assert len(successes) == 1
        assert len(failures) == 1


@pytest.mark.asyncio
async def test_decimal_reserved_totals_are_exact(tmp_path):
    async with _pg_pool() as pool:
        svc, res_repo, *_ = _svc(pool, balance=1.0)
        await svc.reserve_for_intent(
            _make_intent(IntentSide.BUY, size=Decimal("1"), price=Decimal("0.1"))
        )
        await svc.reserve_for_intent(
            _make_intent(
                IntentSide.BUY,
                size=Decimal("1"),
                price=Decimal("0.2"),
                reason=IntentReason.RISK_EXIT,
            )
        )

        assert await res_repo.sum_active_quote_for_user("u1") == Decimal("0.3")


# ---------------------------------------------------------------------------
# Test: Reservation amounts are recovered from DB after restart
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reservations_recoverable_after_restart(tmp_path):
    async with _pg_pool() as pool:
        db = pool  # _db_path removed, name="recover.db")
        res_repo = ReservationsRepo(db)
        pos_repo = PositionsRepo(db)
        clk = MockClock(1000)

        async def bal(addr: str) -> float | None:
            return 100.0 if addr == "0xfunder" else None

        svc1 = ReservationService(
            reservations_repo=res_repo,
            positions_repo=pos_repo,
            get_balance_func=bal,
            funder_address="0xfunder",
            clock=clk,
            ttl_seconds=60,
        )
        intent = _make_intent(IntentSide.BUY, size=Decimal("20"), price=Decimal("0.5"))
        res = await svc1.reserve_for_intent(intent)
        assert res.reserved_usdc == Decimal("10.0")

        totals = await res_repo.sum_active_for_scope("u1", "cond1", _FAKE_TOKEN, "BUY")
        assert totals == (Decimal("10.0"), Decimal("0"))

        svc2 = ReservationService(
            reservations_repo=res_repo,
            positions_repo=pos_repo,
            get_balance_func=bal,
            funder_address="0xfunder",
            clock=clk,
            ttl_seconds=60,
        )

        intent2 = _make_intent(
            IntentSide.BUY,
            size=Decimal("190"),
            price=Decimal("0.5"),
            reason=IntentReason.RISK_EXIT,
        )
        with pytest.raises(InsufficientFreeQuote):
            await svc2.reserve_for_intent(intent2)

        intent3 = _make_intent(
            IntentSide.BUY,
            size=Decimal("180"),
            price=Decimal("0.5"),
            reason=IntentReason.NEG_RISK_ARB,
        )
        res3 = await svc2.reserve_for_intent(intent3)
        assert res3.reserved_usdc == Decimal("90.0")


@pytest.mark.asyncio
async def test_sell_reservations_recoverable_after_restart(tmp_path):
    async with _pg_pool() as pool:
        db = pool  # _db_path removed, name="sell_recover.db")
        res_repo = ReservationsRepo(db)
        pos_repo = PositionsRepo(db)
        clk = MockClock(1000)

        async def bal(addr: str) -> float | None:
            return 100.0 if addr == "0xfunder" else None

        await pos_repo.apply_fill(
            user_id="u1",
            leader_id="0xleader",
            condition_id="cond1",
            token_id=_FAKE_TOKEN,
            side="BUY",
            fill_size=Decimal("30"),
            fill_price=Decimal("0.5"),
        )

        svc1 = ReservationService(
            reservations_repo=res_repo,
            positions_repo=pos_repo,
            get_balance_func=bal,
            funder_address="0xfunder",
            clock=clk,
            ttl_seconds=60,
        )
        intent1 = _make_intent(IntentSide.SELL, size=Decimal("15"))
        await svc1.reserve_for_intent(intent1)

        _, reserved_size = await res_repo.sum_active_for_scope("u1", "cond1", _FAKE_TOKEN, "SELL")
        assert reserved_size == Decimal("15")

        svc2 = ReservationService(
            reservations_repo=res_repo,
            positions_repo=pos_repo,
            get_balance_func=bal,
            funder_address="0xfunder",
            clock=clk,
            ttl_seconds=60,
        )
        intent2 = _make_intent(IntentSide.SELL, size=Decimal("20"), reason=IntentReason.RISK_EXIT)
        with pytest.raises(InsufficientFreeShares):
            await svc2.reserve_for_intent(intent2)

        intent3 = _make_intent(
            IntentSide.SELL, size=Decimal("15"), reason=IntentReason.NEG_RISK_ARB
        )
        await svc2.reserve_for_intent(intent3)


# ---------------------------------------------------------------------------
# Test: Reservation release restores capacity
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_release_restores_buy_capacity(tmp_path):
    async with _pg_pool() as pool:
        svc, res_repo, pos_repo, clk = _svc(pool, balance=100.0)
        intent = _make_intent(IntentSide.BUY, size=Decimal("100"), price=Decimal("0.5"))
        res = await svc.reserve_for_intent(intent)
        assert res.reserved_usdc == Decimal("50.0")

        intent2 = _make_intent(
            IntentSide.BUY,
            size=Decimal("110"),
            price=Decimal("0.5"),
            reason=IntentReason.RISK_EXIT,
        )
        with pytest.raises(InsufficientFreeQuote):
            await svc.reserve_for_intent(intent2)

        await svc.release_terminal(res.id, "cancelled")

        intent3 = _make_intent(IntentSide.BUY, size=Decimal("110"), price=Decimal("0.5"))
        res3 = await svc.reserve_for_intent(intent3)
        assert res3.reserved_usdc == Decimal("55.0")


@pytest.mark.asyncio
async def test_release_restores_sell_capacity(tmp_path):
    async with _pg_pool() as pool:
        svc, res_repo, pos_repo, clk = _svc(pool)
        await pos_repo.apply_fill(
            user_id="u1",
            leader_id="0xleader",
            condition_id="cond1",
            token_id=_FAKE_TOKEN,
            side="BUY",
            fill_size=Decimal("30"),
            fill_price=Decimal("0.5"),
        )

        intent1 = _make_intent(IntentSide.SELL, size=Decimal("25"))
        res1 = await svc.reserve_for_intent(intent1)

        intent2 = _make_intent(IntentSide.SELL, size=Decimal("10"), reason=IntentReason.RISK_EXIT)
        with pytest.raises(InsufficientFreeShares):
            await svc.reserve_for_intent(intent2)

        await svc.release_terminal(res1.id, "cancelled")

        intent3 = _make_intent(IntentSide.SELL, size=Decimal("25"))
        await svc.reserve_for_intent(intent3)


# ---------------------------------------------------------------------------
# Test: Coordinator audit logging
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_coordinator_logs_reservation_acquire_and_release():
    res_svc = _MockResSvc()
    audit = _MockAudit()
    gw = _MockGateway(result=VenueSubmitResult(order_id="v1", status="filled"))
    coord = ExecutionCoordinator(
        reservation_service=res_svc,
        orders_repo=_MockOrdersRepo(),
        fills_repo=_MockFillsRepo(),
        positions_repo=_MockPositionsRepo(),
        clob_gateway=gw,
        reconciler=_MockReconciler(),
        audit_logger=audit,
    )
    intent = _make_intent()
    await coord.execute_intent(intent)

    event_names = [e[0] for e in audit.events]
    assert "execution_filled" in event_names
    assert len(res_svc.released) == 1


@pytest.mark.asyncio
async def test_coordinator_logs_create_failure():
    res_svc = _MockResSvc()
    audit = _MockAudit()
    coord = ExecutionCoordinator(
        reservation_service=res_svc,
        orders_repo=_MockOrdersRepo(fail_create=True),
        fills_repo=_MockFillsRepo(),
        positions_repo=_MockPositionsRepo(),
        clob_gateway=_MockGateway(),
        reconciler=_MockReconciler(),
        audit_logger=audit,
    )
    intent = _make_intent()
    with pytest.raises(RuntimeError):
        await coord.execute_intent(intent)

    event_names = [e[0] for e in audit.events]
    assert "execution_create_failed" in event_names
    assert len(res_svc.released) == 1
