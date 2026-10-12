"""Contracts for persistent copy-task lifecycle semantics."""

from __future__ import annotations

from dataclasses import replace

import pytest

from pmcopy.services.copy_tasks.domain import CopyTask, CopyTaskStatus
from pmcopy.services.copy_tasks.service import (
    CopyTaskConflict,
    CopyTaskNotFound,
    CopyTaskService,
    InMemoryCopyTaskRepository,
)


def _task(task_id: str = "task-1") -> CopyTask:
    return CopyTask(
        task_id=task_id,
        name="leader",
        leader_wallet="0x1111111111111111111111111111111111111111",
        follower_account="deposit",
    )


@pytest.mark.asyncio
async def test_create_persists_version_one_task() -> None:
    repository = InMemoryCopyTaskRepository()
    service = CopyTaskService(repository)
    task = await service.create(_task())
    assert task.version == 1
    fetched = await repository.get("task-1")
    assert fetched is not None
    assert fetched.task_id == "task-1"


@pytest.mark.asyncio
async def test_create_rejects_duplicate_identity() -> None:
    repository = InMemoryCopyTaskRepository()
    service = CopyTaskService(repository)
    await service.create(_task())
    with pytest.raises(CopyTaskConflict):
        await service.create(_task())


@pytest.mark.asyncio
async def test_duplicate_copies_configuration_but_not_runtime_state_and_starts_paused() -> None:
    repository = InMemoryCopyTaskRepository()
    service = CopyTaskService(repository)
    await service.create(_task())

    duplicate = await service.duplicate(
        source_task_id="task-1",
        new_task_id="task-2",
        new_name="copy",
    )
    assert duplicate.task_id == "task-2"
    assert duplicate.name == "copy"
    assert duplicate.status is CopyTaskStatus.PAUSED
    assert duplicate.version == 1


@pytest.mark.asyncio
async def test_update_uses_optimistic_version_and_delete_does_not_close_positions() -> None:
    repository = InMemoryCopyTaskRepository()
    service = CopyTaskService(repository)
    created = await service.create(_task())
    updated = await service.update(
        replace(created, name="renamed"),
        expected_version=1,
    )
    assert updated.version == 2
    with pytest.raises(CopyTaskConflict):
        await service.update(replace(updated, name="stale"), expected_version=1)

    deleted = await service.delete("task-1", expected_version=2)
    assert deleted.status is CopyTaskStatus.DELETED
    assert repository.position_close_requests == 0


@pytest.mark.asyncio
async def test_pause_and_resume_toggle_status() -> None:
    repository = InMemoryCopyTaskRepository()
    service = CopyTaskService(repository)
    await service.create(_task())
    paused = await service.pause("task-1", expected_version=1)
    assert paused.status is CopyTaskStatus.PAUSED
    resumed = await service.resume("task-1", expected_version=2)
    assert resumed.status is CopyTaskStatus.ACTIVE


@pytest.mark.asyncio
async def test_update_unknown_task_raises_not_found() -> None:
    repository = InMemoryCopyTaskRepository()
    service = CopyTaskService(repository)
    with pytest.raises(CopyTaskNotFound):
        await service.update(_task("missing"), expected_version=1)


@pytest.mark.asyncio
async def test_list_active_excludes_deleted() -> None:
    repository = InMemoryCopyTaskRepository()
    service = CopyTaskService(repository)
    await service.create(_task("t1"))
    await service.create(_task("t2"))
    await service.delete("t2", expected_version=1)
    active = await repository.list_active()
    assert {task.task_id for task in active} == {"t1"}


@pytest.mark.asyncio
async def test_create_rejects_non_version_one_task() -> None:
    repository = InMemoryCopyTaskRepository()
    service = CopyTaskService(repository)
    with pytest.raises(ValueError, match="version"):
        await service.create(replace(_task(), version=2))
