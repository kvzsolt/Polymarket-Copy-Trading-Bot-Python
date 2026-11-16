"""Fan out shared observations into bounded, serial account work queues."""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from hashlib import sha256

from pmcopy.services.copy_tasks.domain import CopyTask
from pmcopy.services.copy_tasks.event_routing import CopyTaskEventProcessor, CopyTaskSourceEvent
from pmcopy.services.copy_tasks.worker import (
    LeaderLimitReached,
    LeaderWorkerSupervisor,
    WorkerQueueFull,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class _AccountWork:
    """Keep one account's intentionally distinct task allocations together."""

    tasks: tuple[CopyTask, ...]
    event: CopyTaskSourceEvent


class AccountTaskDispatcher:
    """Keep a slow account from blocking subsequent signals for other accounts.

    The shared leader workers perform observation routing only. These bounded
    queues retain per-account ordering and use the same task processor and
    canonical execution coordinator. They do not own financial reservations.
    """

    def __init__(
        self,
        *,
        processor: CopyTaskEventProcessor,
        account_key: Callable[[CopyTask], str | None],
        queue_size: int,
        max_concurrent_accounts: int,
        max_accounts: int,
        shutdown_timeout_seconds: float,
    ) -> None:
        self._processor = processor
        self._account_key = account_key
        self._workers = LeaderWorkerSupervisor[_AccountWork](
            handler=self._process,
            queue_size=queue_size,
            max_concurrent_leaders=max_concurrent_accounts,
            max_leaders=max_accounts,
            shutdown_timeout_seconds=shutdown_timeout_seconds,
        )

    @property
    def queue_depth(self) -> int:
        """Return pending account work excluding already-running operations."""
        return self._workers.queue_depth

    @property
    def worker_count(self) -> int:
        """Return the number of allocated account consumers."""
        return self._workers.worker_count

    async def start(self) -> None:
        """Enable bounded account admission before leader producers start."""
        await self._workers.start()

    async def close(self) -> None:
        """Drain/cancel account work after leader producers stop."""
        await self._workers.close()

    async def abort(self) -> None:
        """Cancel and await account consumers without starting another drain."""
        await self._workers.abort()

    async def __call__(self, leader: str, event: CopyTaskSourceEvent) -> None:
        """Admit each matching account independently, reporting rejected work."""
        tasks = await self._processor.matching_tasks(leader, event)
        grouped: dict[str, list[CopyTask]] = {}
        for task in tasks:
            account = self._account_key(task)
            if account is None:
                self._reject((task,), event, "account_runtime_unavailable")
                continue
            grouped.setdefault(account, []).append(task)
        for account, account_tasks in grouped.items():
            work = _AccountWork(tuple(account_tasks), event)
            worker_key = "account:" + sha256(account.encode("utf-8")).hexdigest()
            try:
                await self._workers.submit(worker_key, work)
            except WorkerQueueFull:
                self._reject(work.tasks, event, "account_queue_full")
            except LeaderLimitReached:
                self._reject(work.tasks, event, "account_worker_limit")

    async def _process(self, worker_key: str, work: _AccountWork) -> None:
        del worker_key
        await self._processor.process_batch(work.tasks, work.event)

    def _reject(self, tasks: tuple[CopyTask, ...], event: CopyTaskSourceEvent, reason: str) -> None:
        logger.warning("copy_account_dispatch_rejected reason=%s task_count=%d", reason, len(tasks))
        self._processor.reject_batch(tasks, event, reason)
