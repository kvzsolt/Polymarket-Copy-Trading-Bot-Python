"""Bounded serial-per-leader asynchronous worker runtime.

One FIFO queue and one consumer task per leader. Different leaders may run
concurrently under a shared bounded global semaphore. Memory is bounded by
per-leader queue size and a maximum leader worker count.
"""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable
from typing import Generic, TypeVar

T = TypeVar("T")
LeaderHandler = Callable[[str, T], Awaitable[None]]


class WorkerQueueFull(RuntimeError):
    """Raised when a leader queue reaches its configured hard bound."""


class LeaderLimitReached(RuntimeError):
    """Raised when more distinct leaders are submitted than configured."""


class _LeaderWorker(Generic[T]):
    """Own one FIFO queue and one consumer task for one leader."""

    def __init__(
        self,
        *,
        leader: str,
        handler: LeaderHandler[T],
        queue_size: int,
        global_semaphore: asyncio.Semaphore,
    ) -> None:
        self.leader = leader
        self._handler = handler
        self._queue: asyncio.Queue[T] = asyncio.Queue(maxsize=queue_size)
        self._global_semaphore = global_semaphore
        self._task: asyncio.Task[None] | None = None
        self._closed = False

    def start(self) -> None:
        """Start the single consumer exactly once."""

        if self._task is None:
            self._task = asyncio.create_task(
                self._run(),
                name=f"leader-worker:{self.leader}",
            )

    def submit_nowait(self, event: T) -> None:
        """Queue one event or raise instead of growing memory without bound."""

        if self._closed:
            raise RuntimeError(f"leader worker {self.leader} is closed")
        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull as exc:
            raise WorkerQueueFull(f"leader queue is full: {self.leader}") from exc

    async def close(self) -> None:
        """Drain accepted work, then stop the consumer idempotently."""

        if self._closed:
            return
        self._closed = True
        await self._queue.join()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _run(self) -> None:
        while True:
            event = await self._queue.get()
            try:
                async with self._global_semaphore:
                    result = self._handler(self.leader, event)
                    if not inspect.isawaitable(result):  # pragma: no cover - typing guard
                        raise TypeError("leader handler must return an awaitable")
                    await result
            finally:
                self._queue.task_done()


class LeaderWorkerSupervisor(Generic[T]):
    """Create bounded leader workers lazily and close them deterministically."""

    def __init__(
        self,
        *,
        handler: LeaderHandler[T],
        queue_size: int = 64,
        max_concurrent_leaders: int = 4,
        max_leaders: int = 1000,
    ) -> None:
        if queue_size < 1 or max_concurrent_leaders < 1 or max_leaders < 1:
            raise ValueError("worker limits must be positive")
        self._handler = handler
        self._queue_size = queue_size
        self._max_leaders = max_leaders
        self._global_semaphore = asyncio.Semaphore(max_concurrent_leaders)
        self._workers: dict[str, _LeaderWorker[T]] = {}
        self._started = False
        self._closed = False
        self._mutation_lock = asyncio.Lock()

    async def start(self) -> None:
        """Mark the supervisor ready to accept events."""

        if self._closed:
            raise RuntimeError("leader worker supervisor is closed")
        self._started = True

    async def submit(self, leader: str, event: T) -> None:
        """Submit one event without allowing an unbounded queue or task fan-out.

        Args:
            leader: Leader identity. Events for the same leader are serialized;
                different leaders may proceed concurrently under the shared
                global semaphore.
            event: The event payload to dispatch to the leader handler.

        Raises:
            RuntimeError: If the supervisor is not started or is closed.
            ValueError: If the leader identity is empty.
            WorkerQueueFull: If the per-leader queue is full.
            LeaderLimitReached: If the maximum leader worker count is reached.
        """

        if not self._started:
            raise RuntimeError("leader worker supervisor is not started")
        if self._closed:
            raise RuntimeError("leader worker supervisor is closed")
        normalized = str(leader).strip().lower()
        if not normalized:
            raise ValueError("leader identity is required")
        worker = self._workers.get(normalized)
        if worker is None:
            async with self._mutation_lock:
                worker = self._workers.get(normalized)
                if worker is None:
                    if len(self._workers) >= self._max_leaders:
                        raise LeaderLimitReached("maximum leader worker count reached")
                    worker = _LeaderWorker(
                        leader=normalized,
                        handler=self._handler,
                        queue_size=self._queue_size,
                        global_semaphore=self._global_semaphore,
                    )
                    self._workers[normalized] = worker
                    worker.start()
        worker.submit_nowait(event)

    async def close(self) -> None:
        """Drain and close every worker exactly once."""

        if self._closed:
            return
        self._closed = True
        await asyncio.gather(*(worker.close() for worker in self._workers.values()))
        self._workers.clear()


__all__ = [
    "LeaderLimitReached",
    "LeaderWorkerSupervisor",
    "WorkerQueueFull",
]
