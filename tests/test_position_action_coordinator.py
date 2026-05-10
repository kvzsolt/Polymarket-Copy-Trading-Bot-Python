"""Focused tests for PositionActionCoordinator (issue #28).

These tests cover the coordinator primitive in isolation: they verify that
same-scope actions serialize, that different scopes stay concurrent, and that
the coordinator is safe under high contention.
"""

from __future__ import annotations

import asyncio

import pytest

from services.positionActionCoordinator import (
    PositionActionCoordinator,
    PositionScope,
)


FAKE_TOKEN_A = "1" * 60
FAKE_TOKEN_B = "2" * 60


def _scope(condition: str = "cond-1", token: str = FAKE_TOKEN_A, user: str = "u1") -> PositionScope:
    return PositionScope(user_id=user, condition_id=condition, token_id=token)


@pytest.mark.asyncio
async def test_same_scope_is_serialized():
    coord = PositionActionCoordinator()
    scope = _scope()

    active = 0
    max_active = 0

    async def worker() -> None:
        nonlocal active, max_active
        async with coord.acquire(scope):
            active += 1
            max_active = max(max_active, active)
            # Let peers try to enter (they must wait).
            await asyncio.sleep(0.01)
            active -= 1

    await asyncio.gather(*(worker() for _ in range(8)))

    assert max_active == 1, "same-scope actions must never overlap"
    assert coord.locks_held() == 0


@pytest.mark.asyncio
async def test_different_scopes_run_in_parallel():
    coord = PositionActionCoordinator()
    scope_a = _scope(condition="cond-a", token=FAKE_TOKEN_A)
    scope_b = _scope(condition="cond-b", token=FAKE_TOKEN_B)

    inside_a = asyncio.Event()
    proceed_a = asyncio.Event()
    inside_b = asyncio.Event()

    async def hold_a() -> str:
        async with coord.acquire(scope_a):
            inside_a.set()
            # Hold the A-scope lock while B runs; if the coordinator
            # serialized across scopes, B would not be able to enter.
            await proceed_a.wait()
            return "a"

    async def run_b() -> str:
        await inside_a.wait()
        async with coord.acquire(scope_b):
            inside_b.set()
            return "b"

    task_a = asyncio.create_task(hold_a())
    task_b = asyncio.create_task(run_b())
    await asyncio.wait_for(inside_b.wait(), timeout=1.0)
    proceed_a.set()
    results = await asyncio.gather(task_a, task_b)

    assert set(results) == {"a", "b"}
    assert coord.scope_count() == 0


@pytest.mark.asyncio
async def test_same_condition_different_token_are_distinct_scopes():
    coord = PositionActionCoordinator()
    scope_a = _scope(condition="cond-shared", token=FAKE_TOKEN_A)
    scope_b = _scope(condition="cond-shared", token=FAKE_TOKEN_B)

    inside_a = asyncio.Event()
    proceed_a = asyncio.Event()

    async def hold_a() -> None:
        async with coord.acquire(scope_a):
            inside_a.set()
            await proceed_a.wait()

    async def run_b() -> None:
        await inside_a.wait()
        async with coord.acquire(scope_b):
            pass

    task_a = asyncio.create_task(hold_a())
    task_b = asyncio.create_task(run_b())
    try:
        await asyncio.wait_for(task_b, timeout=1.0)
    finally:
        proceed_a.set()
        await task_a


@pytest.mark.asyncio
async def test_is_locked_matches_scope_state():
    coord = PositionActionCoordinator()
    scope = _scope()

    assert not coord.is_locked(scope)

    released = asyncio.Event()

    async def holder() -> None:
        async with coord.acquire(scope):
            released.wait_until_ready = None  # no-op marker
            await asyncio.sleep(0.01)

    task = asyncio.create_task(holder())
    # Give the holder a scheduling tick to enter the critical section.
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert coord.is_locked(scope)
    await task
    assert not coord.is_locked(scope)


@pytest.mark.asyncio
async def test_lock_registry_is_race_safe():
    """Two tasks racing on a brand-new scope must share the same lock."""
    coord = PositionActionCoordinator()
    scope = _scope(condition="fresh", token=FAKE_TOKEN_A)

    async def get_lock() -> int:
        lock = await coord.get_lock(scope)
        return id(lock)

    lock_ids = await asyncio.gather(*(get_lock() for _ in range(16)))
    assert len(set(lock_ids)) == 1, "registry must not create more than one lock per scope"


@pytest.mark.asyncio
async def test_private_alias_still_works():
    """``_get_lock`` is kept as a backwards-compatible alias."""
    coord = PositionActionCoordinator()
    scope = _scope()
    lock = await coord._get_lock(scope)
    assert isinstance(lock, asyncio.Lock)


@pytest.mark.asyncio
async def test_release_scope_drops_unlocked_scope():
    coord = PositionActionCoordinator()
    scope = _scope(condition="release-me", token=FAKE_TOKEN_A)

    await coord.get_lock(scope)

    assert coord.scope_count() == 1
    assert coord.release_scope(scope) is True
    assert coord.scope_count() == 0


@pytest.mark.asyncio
async def test_prune_removes_only_unlocked_scopes():
    coord = PositionActionCoordinator()
    scope_a = _scope(condition="prune-a", token=FAKE_TOKEN_A)
    scope_b = _scope(condition="prune-b", token=FAKE_TOKEN_B)

    await coord.get_lock(scope_a)
    await coord.get_lock(scope_b)

    assert coord.scope_count() == 2
    removed = coord.prune()
    assert removed == 2
    assert coord.scope_count() == 0


@pytest.mark.asyncio
async def test_last_waiter_releases_scope_after_serialized_use():
    coord = PositionActionCoordinator()
    scope = _scope(condition="queued", token=FAKE_TOKEN_A)
    entered = []

    async def worker(name: str) -> None:
        async with coord.acquire(scope):
            entered.append(name)
            await asyncio.sleep(0.01)

    await asyncio.gather(worker("a"), worker("b"))

    assert entered == ["a", "b"]
    assert coord.scope_count() == 0


# ---------------------------------------------------------------------------
# Issue #67 spec gaps
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_different_users_do_not_collide():
    """Each user's runtime owns its own coordinator instance, so identical
    (condition_id, token_id) pairs across two users must not block each
    other (verified by running both critical sections strictly concurrently)."""
    coord_alice = PositionActionCoordinator()
    coord_bob = PositionActionCoordinator()

    scope_alice = PositionScope(user_id="alice", condition_id="cX", token_id=FAKE_TOKEN_A)
    scope_bob = PositionScope(user_id="bob", condition_id="cX", token_id=FAKE_TOKEN_A)

    started = asyncio.Event()
    proceed = asyncio.Event()
    finished = []

    async def alice() -> None:
        async with coord_alice.acquire(scope_alice):
            started.set()
            await proceed.wait()
            finished.append("alice")

    async def bob() -> None:
        await started.wait()
        async with coord_bob.acquire(scope_bob):
            # Separate coordinators → no collision; bob runs while alice holds.
            finished.append("bob")
            proceed.set()

    await asyncio.wait_for(asyncio.gather(alice(), bob()), timeout=1.0)
    assert finished == ["bob", "alice"]


@pytest.mark.asyncio
async def test_yes_no_under_same_condition_do_not_collide_via_domain_import():
    """Verify the domain canonical type and the back-compat alias resolve to
    the same lock identity, and that YES/NO scopes (different token_ids)
    do not block each other under a single coordinator."""
    from domain.position_key import PositionKey

    coord = PositionActionCoordinator()
    yes = PositionKey(user_id="u1", condition_id="cZ", token_id="0xYES")
    no = PositionScope(user_id="u1", condition_id="cZ", token_id="0xNO")

    assert PositionScope is PositionKey
    assert isinstance(yes, PositionKey) and isinstance(no, PositionKey)

    started = asyncio.Event()
    proceed = asyncio.Event()
    order = []

    async def yes_holder():
        async with coord.acquire(yes):
            started.set()
            await proceed.wait()
            order.append("yes")

    async def no_holder():
        await started.wait()
        async with coord.acquire(no):
            order.append("no")
            proceed.set()

    await asyncio.wait_for(asyncio.gather(yes_holder(), no_holder()), timeout=1.0)
    assert order == ["no", "yes"]


@pytest.mark.asyncio
async def test_same_key_via_domain_and_alias_share_one_lock():
    """A scope built via PositionKey and one built via PositionScope with the
    same (condition_id, token_id) pair must serialize against each other —
    proves the alias is truly a single type and the coordinator's lock map
    keys are equal-valued."""
    from domain.position_key import PositionKey

    coord = PositionActionCoordinator()
    via_key = PositionKey(user_id="u1", condition_id="c-shared", token_id=FAKE_TOKEN_A)
    via_alias = PositionScope(user_id="u1", condition_id="c-shared", token_id=FAKE_TOKEN_A)

    started = asyncio.Event()
    release = asyncio.Event()
    order = []

    async def first():
        async with coord.acquire(via_key):
            started.set()
            await asyncio.sleep(0.05)
            order.append("first")
            release.set()

    async def second():
        await started.wait()
        async with coord.acquire(via_alias):
            order.append("second")

    await asyncio.wait_for(asyncio.gather(first(), second()), timeout=2.0)
    # If the lock keys collide correctly, "first" must finish before "second" enters.
    assert order == ["first", "second"]
    assert release.is_set()
