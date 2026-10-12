"""Configured protection requires an enabled runtime manager before entry admission."""

from dataclasses import replace
from decimal import Decimal

import pytest

from pmcopy.services.copy_tasks.domain import CopyTask
from pmcopy.services.copy_tasks.entry_permission import TaskEntryPermissionService
from pmcopy.services.copy_tasks.protective_exits import ProtectiveRiskPolicy
from pmcopy.services.copy_tasks.service import InMemoryCopyTaskRepository


def task() -> CopyTask:
    """Return one valid task identity for permission tests."""

    return CopyTask(
        task_id="task-a",
        name="Primary",
        leader_wallet="0x1111111111111111111111111111111111111111",
        follower_account="account-a",
    )


@pytest.mark.asyncio
async def test_restored_active_task_cannot_bypass_missing_protection_at_submission():
    protected = replace(task(), protective_risk=ProtectiveRiskPolicy(stop_loss_percent=Decimal(10)))
    reader = InMemoryCopyTaskRepository()
    await reader.create(protected)
    permission = TaskEntryPermissionService(reader, follower_accounts=(protected.follower_account,))
    assert (
        await permission.denial(protected.task_id, protected.version, protected.leader_wallet)
        == "task_protection_unavailable"
    )
