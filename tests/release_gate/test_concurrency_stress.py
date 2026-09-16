"""Release-gate: same-position concurrency stress (Issue #75 item 14).

Floods the coordinator with 50 concurrent actions targeting the same scope
to confirm the lock cleanly serializes them and that no two critical
sections execute simultaneously.
"""

from __future__ import annotations

import asyncio

import pytest

from domain.position_key import PositionKey
from services.positionActionCoordinator import PositionActionCoordinator


@pytest.mark.asyncio
async def test_fifty_same_scope_actions_are_serialized() -> None:
    coord = PositionActionCoordinator()
    scope = PositionKey("user-1", "0xcond", "0xtokA")
    concurrent: list[int] = []
    peak: list[int] = [0]
    current: list[int] = [0]

    async def worker(_idx: int) -> None:
        async with coord.acquire(scope):
            current[0] += 1
            peak[0] = max(peak[0], current[0])
            concurrent.append(current[0])
            await asyncio.sleep(0)
            current[0] -= 1

    await asyncio.gather(*(worker(i) for i in range(50)))

    assert peak[0] == 1, f"coordinator allowed overlap (peak={peak[0]})"
    assert all(c == 1 for c in concurrent)


@pytest.mark.asyncio
async def test_different_scopes_under_stress_run_in_parallel() -> None:
    coord = PositionActionCoordinator()
    counter = {"in_flight": 0, "peak": 0}
    started = asyncio.Event()

    async def worker(token: str) -> None:
        scope = PositionKey("user-1", "0xcond", token)
        async with coord.acquire(scope):
            counter["in_flight"] += 1
            counter["peak"] = max(counter["peak"], counter["in_flight"])
            started.set()
            await asyncio.sleep(0)
            counter["in_flight"] -= 1

    tokens = [f"tok-{i}" for i in range(10)]
    await asyncio.gather(*(worker(t) for t in tokens))
    assert counter["peak"] >= 2, "different scopes should run concurrently under stress"
