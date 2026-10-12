"""P1-f regression: a task cancelled while *waiting* to acquire a scope lock
must not strand its entry in ``_waiter_counts``.

The risk loop wraps protective-exit critical sections in ``asyncio.wait_for``;
when that times out it cancels the coroutine, which may be blocked inside
``acquire()`` on ``await lock``. The original code incremented the waiter
counter before the wait but only decremented it *after* the lock was held, so
a cancel-while-waiting left the counter stuck at 1 forever. ``release_scope``
and ``prune`` both refuse to drop a scope with waiters, so the registry grew
unbounded and the scope reported permanently "busy".
"""

from __future__ import annotations

import asyncio

import pytest

from pmcopy.services.positionActionCoordinator import (
    PositionActionCoordinator,
    PositionScope,
)

FAKE_TOKEN = "1" * 60


def _scope(condition: str = "cond-1", token: str = FAKE_TOKEN, user: str = "u1") -> PositionScope:
    return PositionScope(user_id=user, condition_id=condition, token_id=token)


@pytest.mark.asyncio
async def test_cancelled_waiter_does_not_strand_scope():
    coord = PositionActionCoordinator()
    scope = _scope()
    key = coord._scope_key(scope)

    holding = asyncio.Event()
    release = asyncio.Event()

    async def holder() -> None:
        async with coord.acquire(scope):
            holding.set()
            await release.wait()

    h = asyncio.create_task(holder())
    await holding.wait()  # holder now owns the lock

    async def waiter() -> None:
        async with coord.acquire(scope):
            pass

    w = asyncio.create_task(waiter())
    await asyncio.sleep(0.05)  # let the waiter block inside `await lock`
    assert coord._has_waiters(key), "waiter should be queued before cancel"

    w.cancel()
    with pytest.raises(asyncio.CancelledError):
        await w

    # The cancelled waiter must have released its waiter slot.
    assert not coord._has_waiters(key), "cancelled waiter stranded the counter"

    # Now that the holder finishes, the idle scope must be reclaimable.
    release.set()
    await h
    assert coord.scope_count() == 0, "scope was not pruned (stranded waiter)"
