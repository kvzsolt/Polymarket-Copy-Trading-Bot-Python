"""Regression: research_capture fire-and-forget task survives the event loop.

Pre-fix: ``asyncio.create_task(_run_research_capture(...))`` was created
and immediately discarded — no reference stored, no done-callback,
no exception handler. Python's asyncio holds only a weak reference to
created tasks, so the GC could collect mid-execution and the capture
silently dropped (with a "Task was destroyed but it is pending!"
warning at best).

Post-fix: ``_spawn_research_capture`` keeps a strong reference in
``_research_capture_tasks``, adds a done-callback that removes the
task on completion AND logs any unhandled exception, so a crash
inside ``capture_observed_trades`` is observable.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest

from pmcopy.services.tradeMonitor import (
    _research_capture_tasks,
    _spawn_research_capture,
)


@pytest.mark.asyncio
async def test_task_held_strongly_while_running():
    """While the coro is awaiting, the task must live in the registry."""
    started = asyncio.Event()
    finish = asyncio.Event()

    async def slow_capture(trades):
        started.set()
        await finish.wait()

    svc = AsyncMock()
    svc.capture_observed_trades.side_effect = slow_capture

    _research_capture_tasks.clear()
    task = _spawn_research_capture(svc, [{"x": 1}])
    await started.wait()
    assert task in _research_capture_tasks, "task must be strongly referenced"

    finish.set()
    await task
    # Done-callback runs synchronously; task should have been removed.
    assert task not in _research_capture_tasks, "done-callback must remove task"


@pytest.mark.asyncio
async def test_task_exception_does_not_leak_or_destroy_pending():
    """Crashed task must finish cleanly + drop out of the registry — not
    leak as a destroyed-pending warning."""
    svc = AsyncMock()
    svc.capture_observed_trades.side_effect = RuntimeError("boom")

    _research_capture_tasks.clear()
    task = _spawn_research_capture(svc, [{"x": 1}])
    # Deliberate loop-tick polling: the test must let the event loop run the
    # spawned task to completion without changing scheduling order.
    while not task.done():  # noqa: ASYNC110
        await asyncio.sleep(0)
    # done-callbacks are scheduled via call_soon — give the loop a tick.
    await asyncio.sleep(0)

    # Inner _run_research_capture catches and logs; task ends without
    # raising into the event loop. Crucially: registry empty + task done.
    assert task.done()
    assert task.exception() is None, f"inner handler must swallow; got {task.exception()!r}"
    assert task not in _research_capture_tasks, (
        "done-callback must remove the task from the strong-ref set"
    )
