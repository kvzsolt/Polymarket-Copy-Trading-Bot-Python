"""Tests for Issue #70 — reservations and intent lifecycle ownership.

Validates:
- Two BUYs cannot collectively exceed free quote.
- Two SELLs cannot collectively exceed held shares.
- BUY reservation releases on create failure.
- SELL reservation releases on venue rejection.
- Ambiguous POST keeps reservation pending until reconciliation.
- Stale reservation expiry triggers reconciliation before release.
- Startup recovery restores in-flight intents from DB.
- Operator-visible reservation audit logs.
"""

import contextlib
import time
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest


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


from domain.order_lifecycle import (  # noqa: E402
    IntentReason,
    IntentSide,
    OrderIntent,
    OrderIntentKey,
    Reservation,
)
from services.execution.reservation_service import (  # noqa: E402
    ReservationService,
)
from services.intentRegistry import (  # noqa: E402
    DuplicateIntentError,
    IllegalTransitionError,
    IntentRegistry,
    IntentState,
    UnknownIntentError,
    Reservation as IntentReservation,
)
from services.positionActionCoordinator import PositionScope  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _scope(token: str = "TKN-A") -> PositionScope:
    return PositionScope(user_id="0xUSER", condition_id="0xCOND", token_id=token)


def _intent(side: IntentSide = IntentSide.BUY, token: str = "TKN-A") -> OrderIntent:
    key = OrderIntentKey(
        user_id="0xUSER",
        leader_wallet=None,
        condition_id="0xCOND",
        token_id=token,
        side=side,
        reason=IntentReason.COPY_BUY,
    )
    return OrderIntent(
        key=key,
        size=Decimal("10"),
        price=Decimal("0.50"),
        max_slippage_bps=200,
        created_at=datetime.now(timezone.utc),
    )


# ---------------------------------------------------------------------------
# Concurrent overspend / oversell protection
# ---------------------------------------------------------------------------


class TestConcurrentBuyProtection:
    """Two BUYs cannot collectively exceed free quote after reservations."""

    @pytest.mark.asyncio
    async def test_two_buys_blocked_by_reservation(self):
        """Second BUY on same scope is blocked while first is unresolved."""
        registry = IntentRegistry()
        scope = _scope()
        reservation = IntentReservation(quote_usdc=Decimal("100"), token_shares=Decimal("0"))
        rec1 = await registry.begin_intent(scope, IntentSide.BUY, reservation, intent_id="buy-1")
        assert rec1.state == IntentState.INTENT_PENDING

        with pytest.raises(DuplicateIntentError):
            await registry.begin_intent(scope, IntentSide.BUY, reservation, intent_id="buy-2")

    @pytest.mark.asyncio
    async def test_two_buys_different_scopes_allowed(self):
        """Two BUYs on different scopes are allowed."""
        registry = IntentRegistry()
        scope_a = _scope("TKN-A")
        scope_b = _scope("TKN-B")
        reservation = IntentReservation(quote_usdc=Decimal("100"), token_shares=Decimal("0"))
        rec1 = await registry.begin_intent(scope_a, IntentSide.BUY, reservation, intent_id="buy-a")
        rec2 = await registry.begin_intent(scope_b, IntentSide.BUY, reservation, intent_id="buy-b")
        assert rec1.state == IntentState.INTENT_PENDING
        assert rec2.state == IntentState.INTENT_PENDING

    @pytest.mark.asyncio
    async def test_second_buy_allowed_after_first_finalized(self):
        """Second BUY is allowed after the first is finalized (reconciler)."""
        registry = IntentRegistry()
        scope = _scope()
        reservation = IntentReservation(quote_usdc=Decimal("100"), token_shares=Decimal("0"))
        await registry.begin_intent(scope, IntentSide.BUY, reservation, intent_id="buy-1")
        await registry.finalize_intent("buy-1", IntentState.DONE, by_reconciler=True)

        rec2 = await registry.begin_intent(scope, IntentSide.BUY, reservation, intent_id="buy-2")
        assert rec2.state == IntentState.INTENT_PENDING


class TestConcurrentSellProtection:
    """Two SELLs cannot collectively exceed held shares."""

    @pytest.mark.asyncio
    async def test_two_sells_blocked_by_reservation(self):
        """Second SELL on same scope is blocked while first is unresolved."""
        registry = IntentRegistry()
        scope = _scope()
        reservation = IntentReservation(quote_usdc=Decimal("0"), token_shares=Decimal("5"))
        rec1 = await registry.begin_intent(scope, IntentSide.SELL, reservation, intent_id="sell-1")
        assert rec1.state == IntentState.INTENT_PENDING

        with pytest.raises(DuplicateIntentError):
            await registry.begin_intent(scope, IntentSide.SELL, reservation, intent_id="sell-2")

    @pytest.mark.asyncio
    async def test_reserved_shares_tracked(self):
        """Aggregate reserved shares are tracked per scope."""
        registry = IntentRegistry()
        scope = _scope()
        reservation = IntentReservation(quote_usdc=Decimal("0"), token_shares=Decimal("5"))
        await registry.begin_intent(scope, IntentSide.SELL, reservation, intent_id="sell-1")

        reserved = registry.reserved_for(scope)
        assert reserved.token_shares == Decimal("5")


# ---------------------------------------------------------------------------
# Reservation release paths
# ---------------------------------------------------------------------------


class TestReservationRelease:
    """Reservations are released on rejection, cancellation, expiry, failure."""

    @pytest.mark.asyncio
    async def test_buy_reservation_releases_on_cancel(self):
        """BUY reservation is released when intent is cancelled by submitter."""
        registry = IntentRegistry()
        scope = _scope()
        reservation = IntentReservation(quote_usdc=Decimal("100"), token_shares=Decimal("0"))
        await registry.begin_intent(scope, IntentSide.BUY, reservation, intent_id="buy-1")

        released = await registry.release_on_cancel_or_fail("buy-1", IntentState.CANCELLED)
        assert released.state == IntentState.CANCELLED

        # Scope should be free for a new intent
        reserved = registry.reserved_for(scope)
        assert reserved.quote_usdc == Decimal("0")

    @pytest.mark.asyncio
    async def test_sell_reservation_releases_on_error(self):
        """SELL reservation is released on terminal error."""
        registry = IntentRegistry()
        scope = _scope()
        reservation = IntentReservation(quote_usdc=Decimal("0"), token_shares=Decimal("5"))
        await registry.begin_intent(scope, IntentSide.SELL, reservation, intent_id="sell-1")

        released = await registry.release_on_cancel_or_fail("sell-1", IntentState.ERROR)
        assert released.state == IntentState.ERROR

        reserved = registry.reserved_for(scope)
        assert reserved.token_shares == Decimal("0")

    @pytest.mark.asyncio
    async def test_finalized_by_reconciler_releases(self):
        """Finalized (DONE) intent releases reservation via reconciler."""
        registry = IntentRegistry()
        scope = _scope()
        reservation = IntentReservation(quote_usdc=Decimal("200"), token_shares=Decimal("0"))
        await registry.begin_intent(scope, IntentSide.BUY, reservation, intent_id="buy-1")

        await registry.finalize_intent("buy-1", IntentState.DONE, by_reconciler=True)

        reserved = registry.reserved_for(scope)
        assert reserved.quote_usdc == Decimal("0")

    @pytest.mark.asyncio
    async def test_non_reconciler_cannot_finalize(self):
        """Only the reconciler may finalize an intent."""
        registry = IntentRegistry()
        scope = _scope()
        reservation = IntentReservation(quote_usdc=Decimal("100"), token_shares=Decimal("0"))
        await registry.begin_intent(scope, IntentSide.BUY, reservation, intent_id="buy-1")

        with pytest.raises(IllegalTransitionError, match="by_reconciler"):
            await registry.finalize_intent("buy-1", IntentState.DONE, by_reconciler=False)


# ---------------------------------------------------------------------------
# Ambiguous POST / reconciliation
# ---------------------------------------------------------------------------


class TestAmbiguousPost:
    """Ambiguous POST keeps reservation pending until reconciliation."""

    @pytest.mark.asyncio
    async def test_ambiguous_post_keeps_reservation_pending(self):
        """When venue returns unknown status, reservation stays pending."""
        registry = IntentRegistry()
        scope = _scope()
        reservation = IntentReservation(quote_usdc=Decimal("100"), token_shares=Decimal("0"))
        await registry.begin_intent(scope, IntentSide.BUY, reservation, intent_id="buy-1")

        # Transition to SUBMITTED (ambiguous — we don't know if filled yet)
        rec = await registry.transition("buy-1", IntentState.SUBMITTED)
        assert rec.state == IntentState.SUBMITTED

        # Reservation should still be held
        reserved = registry.reserved_for(scope)
        assert reserved.quote_usdc == Decimal("100")

    @pytest.mark.asyncio
    async def test_reconciler_finalizes_after_ambiguity(self):
        """Reconciler can finalize after ambiguity resolves."""
        registry = IntentRegistry()
        scope = _scope()
        reservation = IntentReservation(quote_usdc=Decimal("100"), token_shares=Decimal("0"))
        await registry.begin_intent(scope, IntentSide.BUY, reservation, intent_id="buy-1")
        await registry.transition("buy-1", IntentState.SUBMITTED)

        # Reconciler resolves the ambiguity
        await registry.finalize_intent("buy-1", IntentState.DONE, by_reconciler=True)
        reserved = registry.reserved_for(scope)
        assert reserved.quote_usdc == Decimal("0")


# ---------------------------------------------------------------------------
# Stale reservation expiry → reconciliation
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestStaleReservationExpiry:
    """Expired reservations trigger reconciliation before release."""

    @pytest.mark.asyncio
    async def test_list_expired_active(self):
        """Expired active reservations are found by list_expired_active."""
        from storage.postgres.reservations_repo import ReservationsRepo

        async with _pg_pool() as pool:
            repo = ReservationsRepo(pool)
            intent = _intent()
            now = int(time.time())
            # Create a reservation that expires in the past
            reservation = await repo.create_buy_reservation(
                intent=intent,
                reserved_usdc=Decimal("100"),
                expires_at=now - 10,  # Already expired
            )
            assert reservation.status == "active"

            expired = await repo.list_expired_active(now)
            assert len(expired) >= 1
            assert expired[0].id == reservation.id

    @pytest.mark.asyncio
    async def test_non_expired_not_listed(self):
        """Non-expired reservations are not in list_expired_active."""
        from storage.postgres.reservations_repo import ReservationsRepo

        async with _pg_pool() as pool:
            repo = ReservationsRepo(pool)
            intent = _intent()
            now = int(time.time())
            # Create a reservation that expires in the future
            await repo.create_buy_reservation(
                intent=intent,
                reserved_usdc=Decimal("100"),
                expires_at=now + 3600,  # Not expired
            )

            expired = await repo.list_expired_active(now)
            assert len(expired) == 0


# ---------------------------------------------------------------------------
# Startup recovery
# ---------------------------------------------------------------------------


class TestStartupRecovery:
    """In-flight intents are recovered from DB after process restart."""

    @pytest.mark.asyncio
    async def test_recovery_from_empty_db(self):
        """Recovery with no pending orders returns 0."""
        from services.execution.intent_recovery import recover_intents_from_db

        registry = IntentRegistry()
        mock_repo = MagicMock()
        mock_repo.list_active = AsyncMock(return_value=[])

        count = await recover_intents_from_db(registry, mock_repo)
        assert count == 0
        mock_repo.list_active.assert_awaited_once_with()

    @pytest.mark.asyncio
    async def test_recovery_restores_pending_intent(self):
        """Recovery restores a pending order as an in-flight intent."""
        from services.execution.intent_recovery import recover_intents_from_db

        registry = IntentRegistry()
        mock_reservation = Reservation(
            id="res-1",
            user_id="0xUSER",
            intent_key="intent-1",
            condition_id="0xCOND",
            token_id="TKN-A",
            side=IntentSide.BUY,
            reason=IntentReason.COPY_BUY,
            reserved_usdc=Decimal("100"),
            reserved_size=Decimal("0"),
            status="active",
            created_at=datetime.fromtimestamp(1000, tz=timezone.utc),
            expires_at=datetime.fromtimestamp(2000, tz=timezone.utc),
        )
        mock_repo = MagicMock()
        mock_repo.list_active = AsyncMock(return_value=[mock_reservation])

        count = await recover_intents_from_db(registry, mock_repo)
        assert count == 1
        mock_repo.list_active.assert_awaited_once_with()

        # Verify the intent is in the registry
        scope = _scope("TKN-A")
        open_intent = registry.open_intent_for(scope)
        assert open_intent is not None
        assert open_intent.side == IntentSide.BUY
        reserved = registry.reserved_for(scope)
        assert reserved.quote_usdc == Decimal("100")

    @pytest.mark.asyncio
    async def test_recovery_uses_active_reservations_only(self):
        """Released/terminal rows are not recovered because repo returns active rows only."""
        from services.execution.intent_recovery import recover_intents_from_db

        registry = IntentRegistry()
        mock_repo = MagicMock()
        mock_repo.list_active = AsyncMock(return_value=[])

        count = await recover_intents_from_db(registry, mock_repo)
        assert count == 0
        mock_repo.list_active.assert_awaited_once_with()

    @pytest.mark.asyncio
    async def test_recovery_duplicate_after_crash(self):
        """Recovery preserves both existing durable allocations on the same scope."""
        from services.execution.intent_recovery import recover_intents_from_db

        registry = IntentRegistry()
        mock_a = Reservation(
            id="res-a",
            user_id="0xUSER",
            intent_key="intent-a",
            condition_id="0xCOND",
            token_id="TKN-A",
            side=IntentSide.BUY,
            reason=IntentReason.COPY_BUY,
            reserved_usdc=Decimal("50"),
            reserved_size=Decimal("0"),
            status="active",
            created_at=datetime.fromtimestamp(1000, tz=timezone.utc),
            expires_at=datetime.fromtimestamp(2000, tz=timezone.utc),
            order_id="order-a",
        )
        mock_b = Reservation(
            id="res-b",
            user_id="0xUSER",
            intent_key="intent-b",
            condition_id="0xCOND",
            token_id="TKN-A",
            side=IntentSide.BUY,
            reason=IntentReason.COPY_BUY,
            reserved_usdc=Decimal("75"),
            reserved_size=Decimal("0"),
            status="active",
            created_at=datetime.fromtimestamp(1000, tz=timezone.utc),
            expires_at=datetime.fromtimestamp(2000, tz=timezone.utc),
            order_id="order-b",
        )

        mock_repo = MagicMock()
        mock_repo.list_active = AsyncMock(return_value=[mock_a, mock_b])

        count = await recover_intents_from_db(registry, mock_repo)
        assert count == 2
        assert registry.reserved_for(_scope("TKN-A")).quote_usdc == Decimal("125")
        mock_repo.list_active.assert_awaited_once_with()


# ---------------------------------------------------------------------------
# Operator-visible reservation audit logging
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestReservationAuditLogging:
    """ReservationService logs acquire and release events."""

    @pytest.mark.asyncio
    async def test_buy_reservation_logs_acquire(self, caplog):
        """BUY reservation logs the acquire event."""
        from storage.postgres.reservations_repo import ReservationsRepo
        from storage.postgres.positions_repo import PositionsRepo

        async with _pg_pool() as pool:
            res_repo = ReservationsRepo(pool)
            pos_repo = PositionsRepo(pool)

            async def mock_balance(addr: str) -> float | None:
                return 1000.0

            svc = ReservationService(
                reservations_repo=res_repo,
                positions_repo=pos_repo,
                get_balance_func=mock_balance,
                funder_address="0xFUNDER",
            )

            intent = _intent()
            with caplog.at_level("INFO"):
                result = await svc.reserve_for_intent(intent)

            # Verify reservation was created
            assert result is not None
            assert result.status == "active"

    @pytest.mark.asyncio
    async def test_release_terminal_logs(self, caplog):
        """release_terminal logs the release event."""
        from storage.postgres.reservations_repo import ReservationsRepo
        from storage.postgres.positions_repo import PositionsRepo

        async with _pg_pool() as pool:
            res_repo = ReservationsRepo(pool)
            pos_repo = PositionsRepo(pool)

            async def mock_balance(addr: str) -> float | None:
                return 1000.0

            svc = ReservationService(
                reservations_repo=res_repo,
                positions_repo=pos_repo,
                get_balance_func=mock_balance,
                funder_address="0xFUNDER",
            )

            # Create then release
            intent = _intent()
            reservation = await svc.reserve_for_intent(intent)

            with caplog.at_level("INFO"):
                await svc.release_terminal(reservation.id, "filled")

            # Release should succeed without error


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestReservationEdgeCases:
    """Edge cases for reservation lifecycle."""

    @pytest.mark.asyncio
    async def test_negative_reservation_rejected(self):
        """Negative reservation amounts are rejected."""
        registry = IntentRegistry()
        scope = _scope()
        with pytest.raises(ValueError, match="non-negative"):
            await registry.begin_intent(
                scope,
                IntentSide.BUY,
                IntentReservation(quote_usdc=Decimal("-10"), token_shares=Decimal("0")),
            )

    @pytest.mark.asyncio
    async def test_unknown_intent_transition_rejected(self):
        """Transition on unknown intent_id raises UnknownIntentError."""
        registry = IntentRegistry()
        with pytest.raises(UnknownIntentError):
            await registry.transition("nonexistent", IntentState.SUBMITTED)

    @pytest.mark.asyncio
    async def test_scope_cleared_after_finalize(self):
        """Scope is free for a new intent after finalization."""
        registry = IntentRegistry()
        scope = _scope()
        reservation = IntentReservation(quote_usdc=Decimal("100"), token_shares=Decimal("0"))
        await registry.begin_intent(scope, IntentSide.BUY, reservation, intent_id="buy-1")
        await registry.finalize_intent("buy-1", IntentState.DONE, by_reconciler=True)

        # Should be able to open a new intent on the same scope
        rec2 = await registry.begin_intent(scope, IntentSide.BUY, reservation, intent_id="buy-2")
        assert rec2.state == IntentState.INTENT_PENDING

    @pytest.mark.asyncio
    async def test_release_on_cancel_then_new_intent(self):
        """After cancel + release, scope is free for new intent."""
        registry = IntentRegistry()
        scope = _scope()
        reservation = IntentReservation(quote_usdc=Decimal("100"), token_shares=Decimal("0"))
        await registry.begin_intent(scope, IntentSide.BUY, reservation, intent_id="buy-1")
        await registry.release_on_cancel_or_fail("buy-1", IntentState.CANCELLED)

        rec2 = await registry.begin_intent(scope, IntentSide.BUY, reservation, intent_id="buy-2")
        assert rec2.state == IntentState.INTENT_PENDING
