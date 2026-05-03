"""Phase 5 — hardening test gaps for phases 1-4.

These tests fill the remaining high-value coverage gaps in the Phase 5
release-gate matrix:

* PositionScope (canonical PositionKey) identity invariants
* PositionActionCoordinator N-way serialization stress
* PositionPriceCache TTL boundary precision
* Market + user WSS heartbeat interval literal
* Reservation aggregate consistency across multiple lifecycle cycles
* IntentRegistry resolved-record pruning (memory leak guard)
* Reconciler-owned terminal transition enforcement under contention

The intent is proof-by-test of invariants that prior phases asserted
through review but did not pin down with executable assertions.
"""

from __future__ import annotations

import asyncio
import time
from decimal import Decimal

import pytest

from clients.market_ws import _HEARTBEAT_INTERVAL as MARKET_HB
from clients.user_ws import _HEARTBEAT_INTERVAL as USER_HB
from services.intentRegistry import (
    DuplicateIntentError,
    IllegalTransitionError,
    IntentRegistry,
    IntentSide,
    IntentState,
    Reservation,
)
from services.positionActionCoordinator import (
    PositionActionCoordinator,
    PositionScope,
)
from services.positionCoordinator import PositionCoordinator
from services.price_cache import PositionPriceCache


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _scope(
    user: str = "u1",
    condition: str = "cond-1",
    token: str = "tkn-1",
) -> PositionScope:
    return PositionScope(user_id=user, condition_id=condition, token_id=token)


# ---------------------------------------------------------------------------
# 1. PositionScope (PositionKey) uniqueness invariants
# ---------------------------------------------------------------------------


class TestPositionScopeIdentity:
    """PositionScope is the canonical position identity (issue #26).

    Phases 1-4 rely on equality + hashability so dict-keyed lock and
    reservation maps stay consistent. Pin those invariants down.
    """

    def test_same_fields_compare_equal(self) -> None:
        a = _scope("u", "c", "t")
        b = _scope("u", "c", "t")
        assert a == b
        assert hash(a) == hash(b)

    def test_different_user_distinct_scope(self) -> None:
        a = _scope("u-A", "c", "t")
        b = _scope("u-B", "c", "t")
        assert a != b
        assert hash(a) != hash(b)

    def test_different_condition_distinct_scope(self) -> None:
        a = _scope("u", "c-A", "t")
        b = _scope("u", "c-B", "t")
        assert a != b

    def test_different_token_distinct_scope(self) -> None:
        a = _scope("u", "c", "TKN-YES")
        b = _scope("u", "c", "TKN-NO")
        assert a != b

    def test_scope_is_frozen(self) -> None:
        s = _scope()
        with pytest.raises(Exception):  # FrozenInstanceError
            s.user_id = "mutated"  # type: ignore[misc]

    def test_scope_usable_as_dict_key(self) -> None:
        d: dict[PositionScope, int] = {}
        a = _scope("u", "c", "t")
        b = _scope("u", "c", "t")
        d[a] = 1
        d[b] = 2  # same identity → overwrite
        assert len(d) == 1
        assert d[a] == 2


# ---------------------------------------------------------------------------
# 2. Coordinator lock — N-way serialization stress
# ---------------------------------------------------------------------------


class TestCoordinatorSerializationStress:
    """The action lock must serialize *all* contenders on the same scope.

    Existing tests use 8 contenders; the stress version below uses 32 to
    flush out subtle starvation/race issues under heavy contention.
    """

    @pytest.mark.asyncio
    async def test_n_way_same_scope_strictly_serialized(self) -> None:
        coord = PositionActionCoordinator()
        scope = _scope()
        N = 32
        active = 0
        peak = 0
        completed = 0

        async def worker() -> None:
            nonlocal active, peak, completed
            async with coord.acquire(scope):
                active += 1
                peak = max(peak, active)
                # yield to peers — if not strictly serialized, peak rises
                await asyncio.sleep(0)
                await asyncio.sleep(0)
                active -= 1
                completed += 1

        await asyncio.gather(*(worker() for _ in range(N)))

        assert peak == 1
        assert completed == N
        assert coord.locks_held() == 0

    @pytest.mark.asyncio
    async def test_different_scopes_proven_concurrent(self) -> None:
        coord = PositionActionCoordinator()
        scope_a = _scope(token="tkn-A")
        scope_b = _scope(token="tkn-B")
        peak_concurrent = 0
        active = 0

        async def worker(scope: PositionScope) -> None:
            nonlocal peak_concurrent, active
            async with coord.acquire(scope):
                active += 1
                peak_concurrent = max(peak_concurrent, active)
                await asyncio.sleep(0.01)
                active -= 1

        await asyncio.gather(
            worker(scope_a),
            worker(scope_b),
            worker(scope_a),
            worker(scope_b),
        )

        # Different scopes can run concurrently — peak should be 2.
        assert peak_concurrent == 2


# ---------------------------------------------------------------------------
# 3. Cache TTL boundary precision
# ---------------------------------------------------------------------------


class TestCacheTTLBoundary:
    """TTL boundary semantics: ``> ttl`` evicts, exact-equal stays fresh.

    The cache uses ``(now - ts) > ttl`` so ts == ttl is still valid.
    """

    def test_quote_just_within_ttl_kept(self) -> None:
        cache = PositionPriceCache(quote_ttl=10.0, tick_ttl=300.0)
        cache.set_quote(
            "tkn", best_bid=Decimal("0.5"), best_ask=Decimal("0.55")
        )
        # Immediate read is well inside TTL
        assert cache.get_quote("tkn") is not None

    def test_quote_past_ttl_evicted(self) -> None:
        cache = PositionPriceCache(quote_ttl=0.02, tick_ttl=0.02)
        cache.set_quote(
            "tkn", best_bid=Decimal("0.5"), best_ask=Decimal("0.55")
        )
        time.sleep(0.05)
        assert cache.get_quote("tkn") is None

    def test_invalidate_removes_quote_only_for_target_token(self) -> None:
        cache = PositionPriceCache()
        cache.set_quote("a", best_bid=Decimal("0.1"), best_ask=Decimal("0.2"))
        cache.set_quote("b", best_bid=Decimal("0.3"), best_ask=Decimal("0.4"))
        cache.set_tick(
            "a",
            tick_size=Decimal("0.01"),
            min_order_size=Decimal("5"),
            neg_risk=False,
        )

        cache.invalidate_on_fill("a")

        assert cache.get_quote("a") is None
        # Tick survives a fill invalidation
        assert cache.get_tick("a") is not None
        # Other token unaffected
        assert cache.get_quote("b") is not None


# ---------------------------------------------------------------------------
# 4. WSS heartbeat interval literal at 5s (per spec)
# ---------------------------------------------------------------------------


class TestHeartbeatInterval:
    def test_market_ws_heartbeat_is_five_seconds(self) -> None:
        assert MARKET_HB == 5

    def test_user_ws_heartbeat_is_five_seconds(self) -> None:
        assert USER_HB == 5


# ---------------------------------------------------------------------------
# 5. Reservation aggregate consistency across cycles
# ---------------------------------------------------------------------------


class TestReservationAggregate:
    """Aggregate reserved totals must net to zero after any begin → release
    cycle. This protects against drift across many copy-trade signals.
    """

    @pytest.mark.asyncio
    async def test_aggregate_zero_after_many_buy_cycles(self) -> None:
        reg = IntentRegistry()
        scope = _scope()
        for i in range(50):
            rec = await reg.begin_intent(
                scope=scope,
                side=IntentSide.BUY,
                reservation=Reservation(quote_usdc=Decimal(f"{i + 1}.25")),
            )
            await reg.release_on_cancel_or_fail(
                rec.intent_id, IntentState.CANCELLED
            )
        assert reg.reserved_for(scope).is_zero()
        assert reg.open_intent_for(scope) is None

    @pytest.mark.asyncio
    async def test_aggregate_zero_after_mixed_buy_sell_cycles(self) -> None:
        reg = IntentRegistry()
        scope = _scope()
        for i in range(20):
            side = IntentSide.BUY if i % 2 == 0 else IntentSide.SELL
            res = (
                Reservation(quote_usdc=Decimal("3"))
                if side is IntentSide.BUY
                else Reservation(token_shares=Decimal("7"))
            )
            rec = await reg.begin_intent(
                scope=scope, side=side, reservation=res
            )
            terminal = (
                IntentState.DONE if i % 3 == 0 else IntentState.CANCELLED
            )
            if terminal is IntentState.DONE:
                await reg.finalize_intent(
                    rec.intent_id, terminal, by_reconciler=True
                )
            else:
                await reg.release_on_cancel_or_fail(
                    rec.intent_id, IntentState.CANCELLED
                )
        assert reg.reserved_for(scope).is_zero()
        assert reg.open_intent_count() == 0

    @pytest.mark.asyncio
    async def test_release_is_idempotent_on_repeat_call(self) -> None:
        reg = IntentRegistry()
        scope = _scope()
        rec = await reg.begin_intent(
            scope=scope,
            side=IntentSide.BUY,
            reservation=Reservation(quote_usdc=Decimal("10")),
        )
        first = await reg.release_on_cancel_or_fail(
            rec.intent_id, IntentState.CANCELLED
        )
        second = await reg.release_on_cancel_or_fail(
            rec.intent_id, IntentState.CANCELLED
        )
        # Idempotent: second call returns the already-resolved record
        assert first.state is IntentState.CANCELLED
        assert second.state is IntentState.CANCELLED
        # Aggregate did not double-decrement (no negative residual)
        assert reg.reserved_for(scope).is_zero()


# ---------------------------------------------------------------------------
# 6. IntentRegistry resolved-record pruning
# ---------------------------------------------------------------------------


class TestRegistryPruning:
    """Once resolved-intent count exceeds ``_max_resolved``, oldest entries
    are pruned. This guards long-lived runtimes against unbounded memory.
    """

    @pytest.mark.asyncio
    async def test_prune_removes_oldest_resolved_when_over_threshold(
        self,
    ) -> None:
        reg = IntentRegistry()
        reg._max_resolved = 5  # tiny threshold for the test

        # Use unique token per intent so each gets its own scope and the
        # duplicate-blocking rule does not apply.
        ids: list[str] = []
        for i in range(12):
            scope = _scope(token=f"tkn-{i:02d}")
            rec = await reg.begin_intent(
                scope=scope,
                side=IntentSide.BUY,
                reservation=Reservation(quote_usdc=Decimal("1")),
            )
            await reg.release_on_cancel_or_fail(
                rec.intent_id, IntentState.CANCELLED
            )
            ids.append(rec.intent_id)

        # Only the most recent _max_resolved should still be retrievable.
        retained = sum(1 for iid in ids if reg.get(iid) is not None)
        assert retained == 5

    @pytest.mark.asyncio
    async def test_open_intents_never_pruned(self) -> None:
        reg = IntentRegistry()
        reg._max_resolved = 2

        # Create one open intent that must survive pruning.
        open_scope = _scope(token="open")
        open_rec = await reg.begin_intent(
            scope=open_scope,
            side=IntentSide.BUY,
            reservation=Reservation(quote_usdc=Decimal("1")),
        )

        # Generate many resolved intents on other scopes to trigger prune.
        for i in range(20):
            scope = _scope(token=f"tkn-{i:02d}")
            rec = await reg.begin_intent(
                scope=scope,
                side=IntentSide.BUY,
                reservation=Reservation(quote_usdc=Decimal("1")),
            )
            await reg.release_on_cancel_or_fail(
                rec.intent_id, IntentState.CANCELLED
            )

        # Open intent must still be present and queryable by scope.
        still_open = reg.open_intent_for(open_scope)
        assert still_open is not None
        assert still_open.intent_id == open_rec.intent_id


# ---------------------------------------------------------------------------
# 7. Reconciler terminal-ownership under contention
# ---------------------------------------------------------------------------


class TestReconcilerTerminalOwnership:
    """Only the reconciler may flip an intent into a terminal state via
    ``finalize_intent``. Submitters writing terminal states through
    ``transition`` must be rejected even under concurrent access.
    """

    @pytest.mark.asyncio
    async def test_submitter_cannot_terminal_under_contention(self) -> None:
        reg = IntentRegistry()
        scope = _scope()
        rec = await reg.begin_intent(
            scope=scope,
            side=IntentSide.BUY,
            reservation=Reservation(quote_usdc=Decimal("5")),
        )

        async def submitter_attempt() -> Exception | None:
            try:
                await reg.transition(rec.intent_id, IntentState.DONE)
                return None
            except IllegalTransitionError as e:
                return e

        async def reconciler_finalize() -> IntentState:
            await asyncio.sleep(0)
            final = await reg.finalize_intent(
                rec.intent_id, IntentState.DONE, by_reconciler=True
            )
            return final.state

        rejection, terminal = await asyncio.gather(
            submitter_attempt(), reconciler_finalize()
        )

        assert isinstance(rejection, IllegalTransitionError)
        assert terminal is IntentState.DONE
        assert reg.reserved_for(scope).is_zero()

    @pytest.mark.asyncio
    async def test_finalize_requires_explicit_reconciler_flag(self) -> None:
        reg = IntentRegistry()
        scope = _scope()
        rec = await reg.begin_intent(
            scope=scope,
            side=IntentSide.BUY,
            reservation=Reservation(quote_usdc=Decimal("1")),
        )
        # Default by_reconciler=False must reject — even with terminal state
        with pytest.raises(IllegalTransitionError):
            await reg.finalize_intent(rec.intent_id, IntentState.DONE)


# ---------------------------------------------------------------------------
# 8. PositionCoordinator end-to-end happy path
# ---------------------------------------------------------------------------


class TestPositionCoordinatorHappyPath:
    """End-to-end: lock + begin + transition* + reconciler finalize."""

    @pytest.mark.asyncio
    async def test_full_lifecycle_clean_release(self) -> None:
        coord = PositionCoordinator()
        scope = _scope()

        async with coord.acquire(scope):
            rec = await coord.begin_intent(
                scope=scope,
                side=IntentSide.BUY,
                reservation=Reservation(quote_usdc=Decimal("25.00")),
            )
            assert coord.is_locked(scope)
            await coord.transition(rec.intent_id, IntentState.SUBMITTED)
            await coord.transition(rec.intent_id, IntentState.ACK_LIVE)
            await coord.transition(rec.intent_id, IntentState.PARTIAL)
            await coord.transition(rec.intent_id, IntentState.FILLED)
            await coord.transition(rec.intent_id, IntentState.CONFIRMING)

        # Outside the lock, the reconciler finalizes the intent.
        final = await coord.finalize_intent(
            rec.intent_id, IntentState.DONE, by_reconciler=True
        )
        assert final.state is IntentState.DONE
        assert coord.reserved_for(scope).is_zero()
        assert coord.open_intent_for(scope) is None
        assert coord.open_intent_count() == 0
        assert not coord.is_locked(scope)

    @pytest.mark.asyncio
    async def test_dup_intent_blocked_then_freed_after_release(self) -> None:
        coord = PositionCoordinator()
        scope = _scope()

        rec = await coord.begin_intent(
            scope=scope,
            side=IntentSide.BUY,
            reservation=Reservation(quote_usdc=Decimal("3")),
        )
        with pytest.raises(DuplicateIntentError):
            await coord.begin_intent(
                scope=scope,
                side=IntentSide.SELL,
                reservation=Reservation(token_shares=Decimal("1")),
            )

        await coord.release_on_cancel_or_fail(
            rec.intent_id, IntentState.CANCELLED
        )

        # After release, scope is open for a new intent.
        rec2 = await coord.begin_intent(
            scope=scope,
            side=IntentSide.SELL,
            reservation=Reservation(token_shares=Decimal("1")),
        )
        assert rec2.intent_id != rec.intent_id
