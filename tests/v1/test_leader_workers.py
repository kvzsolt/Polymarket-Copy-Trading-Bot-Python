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


@pytest.mark.asyncio
async def test_handler_exception_does_not_kill_leader_consumer() -> None:
    """Continue FIFO processing after one event-specific handler failure."""

    processed: list[int] = []
    second_processed = asyncio.Event()

    async def handler(_leader: str, value: int) -> None:
        if value == 1:
            raise RuntimeError("deterministic event failure")
        processed.append(value)
        second_processed.set()

    supervisor = LeaderWorkerSupervisor(handler=handler, queue_size=2)
    await supervisor.start()
    await supervisor.submit("a", 1)
    await supervisor.submit("a", 2)

    await asyncio.wait_for(second_processed.wait(), timeout=0.5)
    await asyncio.wait_for(supervisor.close(), timeout=0.5)

    assert processed == [2]


@pytest.mark.asyncio
async def test_batch_admission_is_all_or_none_when_leader_queue_is_full() -> None:
    """Reject a whole source batch before accepting any partial prefix."""

    started = asyncio.Event()
    release = asyncio.Event()
    processed: list[int] = []

    async def handler(_leader: str, value: int) -> None:
        started.set()
        await release.wait()
        processed.append(value)

    supervisor = LeaderWorkerSupervisor(
        handler=handler,
        queue_size=2,
        max_concurrent_leaders=1,
    )
    await supervisor.start()
    await supervisor.submit("a", 1)
    await asyncio.wait_for(started.wait(), timeout=0.5)

    with pytest.raises(WorkerQueueFull):
        await supervisor.submit_batch("a", [2, 3, 4])

    release.set()
    await supervisor.close()
    assert processed == [1]


@pytest.mark.asyncio
async def test_runtime_counts_report_workers_and_queued_events() -> None:
    """Expose bounded runtime counts without leaking leader identifiers."""

    started = asyncio.Event()
    release = asyncio.Event()

    async def handler(_leader: str, _value: int) -> None:
        started.set()
        await release.wait()

    supervisor = LeaderWorkerSupervisor(
        handler=handler,
        queue_size=2,
        max_concurrent_leaders=1,
    )
    assert supervisor.worker_count == 0
    assert supervisor.queue_depth == 0

    await supervisor.start()
    await supervisor.submit("a", 1)
    await asyncio.wait_for(started.wait(), timeout=0.5)
    await supervisor.submit("a", 2)

    assert supervisor.worker_count == 1
    assert supervisor.queue_depth == 1

    release.set()
    await supervisor.close()
    assert supervisor.worker_count == 0
    assert supervisor.queue_depth == 0
