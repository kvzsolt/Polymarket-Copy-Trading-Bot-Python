"""Tests for Phase 1 — PositionKey, IntentRecord, and PositionCoordinator.

Covers:
  1. YES and NO positions under the same condition do not collide (distinct keys).
  2. Same-position lock serializes concurrent operations.
  3. Intent state transitions smoke test (happy path + invalid rejection).
"""

from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest

from domain.intent import IntentRecord, IntentState, InvalidTransition
from domain.position_key import PositionKey
from domain.position_coordinator import PositionCoordinator


# ---------------------------------------------------------------------------
# Fixtures & helpers
# ---------------------------------------------------------------------------

FAKE_TOKEN_YES = "1" * 60
FAKE_TOKEN_NO = "2" * 60


def _key(
    *,
    user: str = "0xuser1",
    condition: str = "cond-1",
    token: str = FAKE_TOKEN_YES,
    outcome: str | None = None,
) -> PositionKey:
    return PositionKey(
        user=user,
        condition_id=condition,
        token_id=token,
        outcome=outcome,
    )


# ---------------------------------------------------------------------------
# 1. YES/NO positions under same condition do not collide
# ---------------------------------------------------------------------------


class TestPositionKeyDistinctness:
    """YES and NO positions under the same condition must be distinct keys."""

    def test_same_condition_different_token_are_distinct(self):
        """Two different token_ids under the same condition ≠ same key."""
        yes_key = _key(condition="cond-shared", token=FAKE_TOKEN_YES, outcome="Yes")
        no_key = _key(condition="cond-shared", token=FAKE_TOKEN_NO, outcome="No")

        assert yes_key != no_key
        assert hash(yes_key) != hash(no_key)

    def test_same_condition_same_token_are_equal(self):
        """Same (user, condition, token, outcome) → same key."""
        key_a = _key(condition="cond-1", token=FAKE_TOKEN_YES, outcome="Yes")
        key_b = _key(condition="cond-1", token=FAKE_TOKEN_YES, outcome="Yes")

        assert key_a == key_b
        assert hash(key_a) == hash(key_b)

    def test_different_outcome_same_token_are_distinct(self):
        """Even with the same token_id, different outcome labels are distinct."""
        key_a = _key(token=FAKE_TOKEN_YES, outcome="Yes")
        key_b = _key(token=FAKE_TOKEN_YES, outcome="No")

        assert key_a != key_b

    def test_outcome_none_vs_set_are_distinct(self):
        """A key with outcome=None is distinct from one with an explicit outcome."""
        key_none = _key(token=FAKE_TOKEN_YES, outcome=None)
        key_yes = _key(token=FAKE_TOKEN_YES, outcome="Yes")

        assert key_none != key_yes

    def test_key_is_frozen(self):
        """PositionKey is immutable."""
        key = _key()
        with pytest.raises(AttributeError):
            key.user = "other"  # type: ignore[misc]

    def test_key_usable_as_dict_key(self):
        """PositionKey works as a dict key and set member."""
        yes_key = _key(condition="cond-shared", token=FAKE_TOKEN_YES, outcome="Yes")
        no_key = _key(condition="cond-shared", token=FAKE_TOKEN_NO, outcome="No")

        d: dict[PositionKey, str] = {yes_key: "yes_val", no_key: "no_val"}
        assert d[yes_key] == "yes_val"
        assert d[no_key] == "no_val"
        assert len(d) == 2

    @pytest.mark.asyncio
    async def test_yes_no_do_not_block_each_other_in_coordinator(self):
        """YES and NO positions under the same condition get independent locks."""
        coord = PositionCoordinator()
        yes_key = _key(condition="cond-shared", token=FAKE_TOKEN_YES, outcome="Yes")
        no_key = _key(condition="cond-shared", token=FAKE_TOKEN_NO, outcome="No")

        inside_yes = asyncio.Event()
        proceed_yes = asyncio.Event()
        inside_no = asyncio.Event()

        async def hold_yes():
            async with coord.acquire(yes_key):
                inside_yes.set()
                await proceed_yes.wait()

        async def run_no():
            await inside_yes.wait()
            async with coord.acquire(no_key):
                inside_no.set()

        task_yes = asyncio.create_task(hold_yes())
        task_no = asyncio.create_task(run_no())

        # NO must enter even while YES lock is held
        await asyncio.wait_for(inside_no.wait(), timeout=1.0)
        proceed_yes.set()
        await asyncio.gather(task_yes, task_no)


# ---------------------------------------------------------------------------
# 2. Same-position lock serializes concurrent operations
# ---------------------------------------------------------------------------


class TestSamePositionSerialization:
    """Same PositionKey must serialize concurrent operations."""

    @pytest.mark.asyncio
    async def test_same_key_serializes_workers(self):
        coord = PositionCoordinator()
        key = _key()

        active = 0
        max_active = 0

        async def worker() -> None:
            nonlocal active, max_active
            async with coord.acquire(key):
                active += 1
                max_active = max(max_active, active)
                await asyncio.sleep(0.01)
                active -= 1

        await asyncio.gather(*(worker() for _ in range(8)))

        assert max_active == 1, "same-key actions must never overlap"

    @pytest.mark.asyncio
    async def test_different_keys_run_in_parallel(self):
        coord = PositionCoordinator()
        key_a = _key(condition="cond-a")
        key_b = _key(condition="cond-b")

        inside_a = asyncio.Event()
        proceed_a = asyncio.Event()
        inside_b = asyncio.Event()

        async def hold_a():
            async with coord.acquire(key_a):
                inside_a.set()
                await proceed_a.wait()
                return "a"

        async def run_b():
            await inside_a.wait()
            async with coord.acquire(key_b):
                inside_b.set()
                return "b"

        task_a = asyncio.create_task(hold_a())
        task_b = asyncio.create_task(run_b())

        await asyncio.wait_for(inside_b.wait(), timeout=1.0)
        proceed_a.set()
        results = await asyncio.gather(task_a, task_b)
        assert set(results) == {"a", "b"}

    @pytest.mark.asyncio
    async def test_acquire_yields_intent_record(self):
        """The acquire context manager yields the IntentRecord for the key."""
        coord = PositionCoordinator()
        key = _key()

        async with coord.acquire(key) as intent:
            assert isinstance(intent, IntentRecord)
            assert intent.state == IntentState.IDLE


# ---------------------------------------------------------------------------
# 3. Intent state transitions smoke test
# ---------------------------------------------------------------------------


class TestIntentStateMachine:
    """Smoke test the IntentRecord state machine transitions."""

    def test_happy_path_lifecycle(self):
        """Walk the full happy-path lifecycle: IDLE -> ... -> DONE -> IDLE."""
        rec = IntentRecord()
        assert rec.state == IntentState.IDLE

        rec.transition(IntentState.INTENT_PENDING)
        assert rec.state == IntentState.INTENT_PENDING

        rec.transition(IntentState.SUBMITTED)
        assert rec.state == IntentState.SUBMITTED

        rec.transition(IntentState.ACK_LIVE)
        assert rec.state == IntentState.ACK_LIVE

        rec.transition(IntentState.PARTIAL)
        assert rec.state == IntentState.PARTIAL

        # Progressive partial fill
        rec.transition(IntentState.PARTIAL)
        assert rec.state == IntentState.PARTIAL

        rec.transition(IntentState.FILLED)
        assert rec.state == IntentState.FILLED

        rec.transition(IntentState.CONFIRMING)
        assert rec.state == IntentState.CONFIRMING

        rec.transition(IntentState.DONE)
        assert rec.state == IntentState.DONE
        assert rec.is_terminal

        # Reset back to idle
        rec.transition(IntentState.IDLE)
        assert rec.state == IntentState.IDLE

    def test_error_from_submitted(self):
        """SUBMITTED can transition to ERROR."""
        rec = IntentRecord()
        rec.transition(IntentState.INTENT_PENDING)
        rec.transition(IntentState.SUBMITTED)
        rec.transition(IntentState.ERROR)

        assert rec.state == IntentState.ERROR
        assert rec.is_terminal

    def test_cancel_from_intent_pending(self):
        """INTENT_PENDING can be cancelled."""
        rec = IntentRecord()
        rec.transition(IntentState.INTENT_PENDING)
        rec.transition(IntentState.CANCELLED)

        assert rec.state == IntentState.CANCELLED
        assert rec.is_terminal

    def test_immediate_fill_from_submitted(self):
        """An order can fill immediately from SUBMITTED."""
        rec = IntentRecord()
        rec.transition(IntentState.INTENT_PENDING)
        rec.transition(IntentState.SUBMITTED)
        rec.transition(IntentState.FILLED)
        assert rec.state == IntentState.FILLED

    def test_invalid_transition_raises(self):
        """Cannot go from IDLE to DONE directly."""
        rec = IntentRecord()
        with pytest.raises(InvalidTransition):
            rec.transition(IntentState.DONE)

    def test_invalid_transition_from_idle_to_submitted(self):
        """Cannot skip INTENT_PENDING and go straight to SUBMITTED."""
        rec = IntentRecord()
        with pytest.raises(InvalidTransition):
            rec.transition(IntentState.SUBMITTED)

    def test_invalid_transition_backwards(self):
        """Cannot go backwards: SUBMITTED -> INTENT_PENDING."""
        rec = IntentRecord()
        rec.transition(IntentState.INTENT_PENDING)
        rec.transition(IntentState.SUBMITTED)
        with pytest.raises(InvalidTransition):
            rec.transition(IntentState.INTENT_PENDING)

    def test_reset_clears_fields(self):
        """reset() returns to IDLE and clears transient fields."""
        rec = IntentRecord()
        rec.transition(IntentState.INTENT_PENDING)
        rec.side = "BUY"
        rec.size = Decimal("10.0")
        rec.price = Decimal("0.50")
        rec.order_id = "ord-123"

        rec.reset()

        assert rec.state == IntentState.IDLE
        assert rec.side is None
        assert rec.size == Decimal(0)
        assert rec.price == Decimal(0)
        assert rec.order_id is None
        assert rec.error is None

    def test_is_active_property(self):
        """is_active is True only for in-flight states."""
        rec = IntentRecord()
        assert not rec.is_active  # IDLE

        rec.transition(IntentState.INTENT_PENDING)
        assert rec.is_active

        rec.transition(IntentState.SUBMITTED)
        assert rec.is_active

        rec.transition(IntentState.ERROR)
        assert not rec.is_active  # terminal

    def test_decimal_fields_default_to_zero(self):
        """Money fields default to Decimal(0), not float."""
        rec = IntentRecord()
        assert isinstance(rec.size, Decimal)
        assert isinstance(rec.price, Decimal)
        assert isinstance(rec.filled_size, Decimal)
        assert rec.size == Decimal(0)
        assert rec.price == Decimal(0)


# ---------------------------------------------------------------------------
# 4. Coordinator introspection / cleanup
# ---------------------------------------------------------------------------


class TestCoordinatorIntrospection:
    """Test coordinator diagnostic and cleanup methods."""

    @pytest.mark.asyncio
    async def test_intent_count_tracks_keys(self):
        coord = PositionCoordinator()
        key = _key()
        coord.get_intent(key)
        assert coord.intent_count() == 1

    @pytest.mark.asyncio
    async def test_active_intents_lists_inflight_only(self):
        coord = PositionCoordinator()
        key_a = _key(condition="a")
        key_b = _key(condition="b")

        intent_a = coord.get_intent(key_a)
        intent_a.transition(IntentState.INTENT_PENDING)

        coord.get_intent(key_b)  # stays IDLE

        active = coord.active_intents()
        assert len(active) == 1
        assert active[0][0] == key_a

    @pytest.mark.asyncio
    async def test_prune_removes_idle_keys(self):
        coord = PositionCoordinator()
        key = _key()
        coord.get_lock(key)
        coord.get_intent(key)

        assert coord.scope_count() == 1
        removed = coord.prune()
        assert removed == 1
        assert coord.scope_count() == 0

    @pytest.mark.asyncio
    async def test_release_key_refuses_while_locked(self):
        coord = PositionCoordinator()
        key = _key()
        lock = coord.get_lock(key)
        await lock.acquire()
        try:
            assert coord.release_key(key) is False
        finally:
            lock.release()
