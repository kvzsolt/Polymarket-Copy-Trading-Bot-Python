"""Release-gate: coordinator serializes same-scope actions (Issue #75 item 2).

The position-action coordinator must:
* serialize concurrent actions targeting the same ``PositionKey`` scope so a
  protective SELL cannot race a fresh BUY;
* run unrelated scopes (different ``(condition_id, token_id)``) fully
  concurrently;
* release idle scope locks once their queue drains so the registry does not
  grow without bound.
"""

from __future__ import annotations

import asyncio

import pytest

from domain.position_key import PositionKey
from services.positionActionCoordinator import PositionActionCoordinator


def _scope(token: str = "tokA") -> PositionKey:
    return PositionKey("user-1", "0xcond", token)


@pytest.mark.asyncio
async def test_same_scope_actions_are_serialized() -> None:
    coord = PositionActionCoordinator()
    order: list[str] = []
    enter_a = asyncio.Event()
    leave_a = asyncio.Event()

    async def first() -> None:
        async with coord.acquire(_scope()):
            order.append("a-enter")
            enter_a.set()
            await leave_a.wait()
            order.append("a-leave")

    async def second() -> None:
        await enter_a.wait()
        async with coord.acquire(_scope()):
            order.append("b-enter")
            order.append("b-leave")

    task1 = asyncio.create_task(first())
    task2 = asyncio.create_task(second())
    await enter_a.wait()
    # Pre-fix the gate only checked ordering; add the locked-state
    # invariant so a future refactor that replaces asyncio.Lock with a
    # non-locking primitive (or simply forgets to acquire) is caught
    # even when execution happens to interleave correctly.
    assert coord.is_locked(_scope()) is True, "scope must report locked while task1 holds it"
    assert order == ["a-enter"]
    leave_a.set()
    await asyncio.gather(task1, task2)
    assert order == ["a-enter", "a-leave", "b-enter", "b-leave"]
    # After both tasks drain, the scope lock must be released (idle
    # cleanup tested separately below — re-asserting here ties the
    # serialization contract to the cleanup contract).
    assert coord.is_locked(_scope()) is False


@pytest.mark.asyncio
async def test_different_scopes_run_concurrently() -> None:
    coord = PositionActionCoordinator()
    enter_a = asyncio.Event()
    enter_b = asyncio.Event()
    proceed = asyncio.Event()

    async def hold_a() -> None:
        async with coord.acquire(_scope("tokA")):
            enter_a.set()
            await proceed.wait()

    async def run_b() -> None:
        async with coord.acquire(_scope("tokB")):
            enter_b.set()

    task_a = asyncio.create_task(hold_a())
    task_b = asyncio.create_task(run_b())
    await asyncio.wait_for(enter_b.wait(), timeout=1.0)
    assert enter_a.is_set() is True
    proceed.set()
    await asyncio.gather(task_a, task_b)


@pytest.mark.asyncio
async def test_idle_scope_lock_is_released() -> None:
    coord = PositionActionCoordinator()
    scope = _scope()
    async with coord.acquire(scope):
        pass
    assert coord.scope_count() == 0
    assert coord.is_locked(scope) is False
