"""Contracts for bounded, serial-per-leader asynchronous processing."""

from __future__ import annotations

import asyncio

import pytest

from services.copy_tasks.worker import (
    LeaderLimitReached,
    LeaderWorkerSupervisor,
    WorkerQueueFull,
)


@pytest.mark.asyncio
async def test_events_are_serial_per_leader_but_different_leaders_run_concurrently() -> None:
    active: dict[str, int] = {}
    max_active: dict[str, int] = {}
    started = asyncio.Event()
    release = asyncio.Event()

    async def handler(leader: str, value: int) -> None:
        active[leader] = active.get(leader, 0) + 1
        max_active[leader] = max(max_active.get(leader, 0), active[leader])
        started.set()
        await release.wait()
        active[leader] -= 1

    supervisor = LeaderWorkerSupervisor(
        handler=handler,
        queue_size=2,
        max_concurrent_leaders=2,
    )
    await supervisor.start()
    try:
        await supervisor.submit("a", 1)
        await supervisor.submit("a", 2)
        await supervisor.submit("b", 1)
        await asyncio.wait_for(started.wait(), timeout=1)
        await asyncio.sleep(0)
        assert max_active["a"] == 1
        assert max_active["b"] == 1
    finally:
        release.set()
        await supervisor.close()


@pytest.mark.asyncio
async def test_queue_is_bounded_and_close_is_idempotent() -> None:
    release = asyncio.Event()

    async def handler(_leader: str, _value: int) -> None:
        await release.wait()

    supervisor = LeaderWorkerSupervisor(
        handler=handler,
        queue_size=1,
        max_concurrent_leaders=1,
    )
    await supervisor.start()
    await supervisor.submit("a", 1)
    await asyncio.sleep(0)
    await supervisor.submit("a", 2)
    with pytest.raises(WorkerQueueFull):
        await supervisor.submit("a", 3)
    release.set()
    await supervisor.close()
    await supervisor.close()


@pytest.mark.asyncio
async def test_submit_before_start_is_rejected() -> None:
    async def handler(_leader: str, _value: int) -> None:
        pass

    supervisor = LeaderWorkerSupervisor(handler=handler)
    with pytest.raises(RuntimeError, match="not started"):
        await supervisor.submit("a", 1)
    await supervisor.close()


@pytest.mark.asyncio
async def test_leader_limit_is_enforced() -> None:
    release = asyncio.Event()

    async def handler(_leader: str, _value: int) -> None:
        await release.wait()

    supervisor = LeaderWorkerSupervisor(
        handler=handler,
        queue_size=1,
        max_concurrent_leaders=1,
        max_leaders=2,
    )
    await supervisor.start()
    await supervisor.submit("a", 1)
    await supervisor.submit("b", 1)
    with pytest.raises(LeaderLimitReached):
        await supervisor.submit("c", 1)
    release.set()
    await supervisor.close()


@pytest.mark.asyncio
async def test_empty_leader_identity_is_rejected() -> None:
    async def handler(_leader: str, _value: int) -> None:
        pass

    supervisor = LeaderWorkerSupervisor(handler=handler)
    await supervisor.start()
    with pytest.raises(ValueError, match="leader identity"):
        await supervisor.submit("  ", 1)
    await supervisor.close()


@pytest.mark.asyncio
async def test_invalid_worker_limits_are_rejected() -> None:
    async def handler(_leader: str, _value: int) -> None:
        pass

    with pytest.raises(ValueError, match="positive"):
        LeaderWorkerSupervisor(handler=handler, queue_size=0)