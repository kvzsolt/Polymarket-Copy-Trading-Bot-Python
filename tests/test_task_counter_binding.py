"""Tasks cannot claim entry when the opposite-outcome binding is unavailable."""

from dataclasses import replace
from datetime import UTC, datetime

import pytest

from pmcopy.services.copy_tasks.domain import CopyTask, ReactionPolicy
from pmcopy.services.copy_tasks.entry_permission import TaskEntryPermissionService
from pmcopy.services.copy_tasks.service import InMemoryCopyTaskRepository

NOW = datetime(2026, 9, 8, 12, tzinfo=UTC)


class FixedClock:
    """Return a deterministic timestamp."""

    def now(self) -> datetime:
        """Return the fixed UTC time."""

        return NOW


def task() -> CopyTask:
    """Return one valid task identity for permission tests."""

    return CopyTask(
        task_id="task-a",
        name="Primary",
        leader_wallet="0x1111111111111111111111111111111111111111",
        follower_account="account-a",
    )


@pytest.mark.asyncio
async def test_restored_counter_task_is_denied_at_final_entry_permission() -> None:
    counter = replace(task(), reaction=ReactionPolicy(counter_trade=True))
    repository = InMemoryCopyTaskRepository()
    await repository.create(counter)
    permission = TaskEntryPermissionService(
        repository, follower_accounts=(counter.follower_account,), clock=FixedClock()
    )
    assert (
        await permission.denial(counter.task_id, counter.version, counter.leader_wallet)
        == "task_counter_outcome_unavailable"
    )
