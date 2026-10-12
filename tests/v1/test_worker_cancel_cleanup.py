"""Outer shutdown cancellation cannot orphan an account-processing worker."""

import asyncio

import pytest

from pmcopy.services.copy_tasks.worker import LeaderWorkerSupervisor


@pytest.mark.asyncio
async def test_cancelled_drain_still_cancels_and_awaits_owned_worker(caplog) -> None:
    started, cleaned = asyncio.Event(), asyncio.Event()

    async def handler(leader: str, event: int) -> None:
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cleaned.set()

    supervisor = LeaderWorkerSupervisor(handler=handler, queue_size=2)
    await supervisor.start()
    await supervisor.submit("leader", 1)
    await started.wait()
    await supervisor.submit("leader", 2)
    closing = asyncio.create_task(supervisor.close())
    asyncio.get_running_loop().call_soon(closing.cancel)
    try:
        with pytest.raises(asyncio.CancelledError):
            await closing
        assert cleaned.is_set()
        assert supervisor.worker_count == 0
        assert "leader_worker_shutdown_discarded" in caplog.text
    finally:
        tasks = [worker._task for worker in supervisor._workers.values() if worker._task]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.asyncio
async def test_failed_worker_logs_only_error_type_and_keeps_processing(caplog) -> None:
    seen = []

    async def handler(leader: str, event: int) -> None:
        seen.append(event)
        if event == 1:
            raise RuntimeError("SYNTHETIC_WORKER_SECRET")

    supervisor = LeaderWorkerSupervisor(handler=handler)
    await supervisor.start()
    await supervisor.submit_batch("leader", (1, 2))
    await supervisor.close()
    assert seen == [1, 2]
    assert "leader_worker_handler_failed" in caplog.text
    assert "SYNTHETIC_WORKER_SECRET" not in caplog.text


@pytest.mark.asyncio
async def test_configured_drain_deadline_stops_inflight_work(monkeypatch, caplog) -> None:
    started, cleaned = asyncio.Event(), asyncio.Event()
    deadlines = []

    class ExpiredDeadline:
        async def __aenter__(self):
            self.task = asyncio.current_task()
            asyncio.get_running_loop().call_soon(self.task.cancel)

        async def __aexit__(self, exc_type, exc, traceback):
            if exc_type is asyncio.CancelledError:
                self.task.uncancel()
                raise TimeoutError from None
            return False

    def timeout(seconds):
        deadlines.append(seconds)
        return ExpiredDeadline()

    async def handler(leader: str, event: int) -> None:
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cleaned.set()

    supervisor = LeaderWorkerSupervisor(handler=handler, shutdown_timeout_seconds=12.5)
    await supervisor.start()
    await supervisor.submit("leader", 1)
    await started.wait()
    monkeypatch.setattr("pmcopy.services.copy_tasks.worker.asyncio.timeout", timeout)
    await supervisor.close()
    assert deadlines == [12.5]
    assert cleaned.is_set()
    assert supervisor.worker_count == 0
    assert "leader_worker_drain_timeout" in caplog.text
