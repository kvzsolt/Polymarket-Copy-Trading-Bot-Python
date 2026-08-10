"""Copy-task lifecycle service and persistence port.

Deleting or duplicating a task never mutates follower positions. Position
management remains owned by the existing position/risk/reconciliation layers.
"""

from __future__ import annotations

import asyncio
from dataclasses import replace
from typing import Protocol

from services.copy_tasks.domain import CopyTask, CopyTaskStatus


class CopyTaskNotFound(LookupError):
    """Raised when a task identity does not exist."""


class CopyTaskConflict(RuntimeError):
    """Raised on duplicate identity or optimistic-version mismatch."""


class CopyTaskRepository(Protocol):
    """Narrow durable store used by the copy-task application service."""

    async def create(self, task: CopyTask) -> None:
        """Create a task or raise ``CopyTaskConflict``."""

    async def get(self, task_id: str) -> CopyTask | None:
        """Return a task by identity."""

    async def save(self, task: CopyTask, *, expected_version: int) -> None:
        """Persist a new version using optimistic concurrency."""

    async def list_active(self) -> tuple[CopyTask, ...]:
        """Return active and paused tasks, excluding deleted rows."""


class InMemoryCopyTaskRepository:
    """Deterministic repository for unit tests and paper composition."""

    def __init__(self) -> None:
        self._tasks: dict[str, CopyTask] = {}
        self._lock = asyncio.Lock()
        self.position_close_requests = 0

    async def create(self, task: CopyTask) -> None:
        """Create one task atomically."""

        async with self._lock:
            if task.task_id in self._tasks:
                raise CopyTaskConflict(f"copy task already exists: {task.task_id}")
            self._tasks[task.task_id] = task

    async def get(self, task_id: str) -> CopyTask | None:
        """Return one task."""

        async with self._lock:
            return self._tasks.get(str(task_id))

    async def save(self, task: CopyTask, *, expected_version: int) -> None:
        """Replace one task only when its current version matches."""

        async with self._lock:
            current = self._tasks.get(task.task_id)
            if current is None:
                raise CopyTaskNotFound(task.task_id)
            if current.version != expected_version:
                raise CopyTaskConflict(
                    f"copy task version conflict: expected {expected_version}, "
                    f"found {current.version}"
                )
            self._tasks[task.task_id] = task

    async def list_active(self) -> tuple[CopyTask, ...]:
        """Return non-deleted tasks in stable identity order."""

        async with self._lock:
            return tuple(
                task
                for _, task in sorted(self._tasks.items())
                if task.status is not CopyTaskStatus.DELETED
            )


class CopyTaskService:
    """Provide small explicit task lifecycle use cases."""

    def __init__(self, repository: CopyTaskRepository) -> None:
        self._repository = repository

    async def create(self, task: CopyTask) -> CopyTask:
        """Create a new version-one task."""

        if task.version != 1:
            raise ValueError("new copy task version must be 1")
        await self._repository.create(task)
        return task

    async def update(
        self,
        task: CopyTask,
        *,
        expected_version: int,
    ) -> CopyTask:
        """Persist a configuration update using optimistic concurrency."""

        current = await self._require(task.task_id)
        if current.version != expected_version:
            raise CopyTaskConflict(
                f"copy task version conflict: expected {expected_version}, found {current.version}"
            )
        updated = replace(task, version=expected_version + 1)
        await self._repository.save(updated, expected_version=expected_version)
        return updated

    async def duplicate(
        self,
        *,
        source_task_id: str,
        new_task_id: str,
        new_name: str,
    ) -> CopyTask:
        """Copy configuration into a new paused task without runtime state."""

        source = await self._require(source_task_id)
        duplicate = replace(
            source,
            task_id=str(new_task_id),
            name=str(new_name),
            status=CopyTaskStatus.PAUSED,
            version=1,
        )
        await self._repository.create(duplicate)
        return duplicate

    async def pause(self, task_id: str, *, expected_version: int) -> CopyTask:
        """Pause new task actions while preserving its positions."""

        return await self._set_status(
            task_id,
            CopyTaskStatus.PAUSED,
            expected_version=expected_version,
        )

    async def resume(self, task_id: str, *, expected_version: int) -> CopyTask:
        """Resume an existing non-deleted task."""

        return await self._set_status(
            task_id,
            CopyTaskStatus.ACTIVE,
            expected_version=expected_version,
        )

    async def delete(self, task_id: str, *, expected_version: int) -> CopyTask:
        """Soft-delete task configuration without requesting position closure."""

        return await self._set_status(
            task_id,
            CopyTaskStatus.DELETED,
            expected_version=expected_version,
        )

    async def _set_status(
        self,
        task_id: str,
        status: CopyTaskStatus,
        *,
        expected_version: int,
    ) -> CopyTask:
        current = await self._require(task_id)
        if current.version != expected_version:
            raise CopyTaskConflict(
                f"copy task version conflict: expected {expected_version}, found {current.version}"
            )
        updated = replace(current, status=status, version=current.version + 1)
        await self._repository.save(updated, expected_version=expected_version)
        return updated

    async def _require(self, task_id: str) -> CopyTask:
        task = await self._repository.get(task_id)
        if task is None:
            raise CopyTaskNotFound(str(task_id))
        return task


__all__ = [
    "CopyTaskConflict",
    "CopyTaskNotFound",
    "CopyTaskRepository",
    "CopyTaskService",
    "InMemoryCopyTaskRepository",
]
