"""Focused tests for PositionActionCoordinator (issue #28).

These tests cover the coordinator primitive in isolation: they verify that
same-scope actions serialize, that different scopes stay concurrent, and that
the coordinator is safe under high contention.
"""

from __future__ import annotations

import asyncio

import pytest

from services.position_action_coordinator import (
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
    assert coord.scope_count() == 2


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
