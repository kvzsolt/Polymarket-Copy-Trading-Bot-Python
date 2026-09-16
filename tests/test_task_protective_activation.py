"""Configured protection requires an enabled runtime manager before entry admission."""

from dataclasses import replace
from decimal import Decimal

import pytest

from orchestrators.control_plane_tasks import LeaderMonitoringState, RuntimeTaskActivationGuard
from services.control_plane.contracts import ControlPlaneError
from services.copy_tasks.entry_permission import TaskEntryPermissionService
from services.copy_tasks.protective_exits import ProtectiveRiskPolicy
from services.copy_tasks.service import InMemoryCopyTaskRepository
from tests.control_plane.test_task_activation import FixedTaskBindingReader, task


def test_protected_task_cannot_activate_without_protective_management():
    protected = replace(task(), protective_risk=ProtectiveRiskPolicy(stop_loss_percent=Decimal(10)))
    guard = RuntimeTaskActivationGuard(
        bindings=FixedTaskBindingReader(True),
        monitoring=LeaderMonitoringState((protected.leader_wallet,)),
    )
    with pytest.raises(ControlPlaneError) as error:
        guard.ensure_available(protected)
    assert error.value.reason.value == "task_protection_unavailable"


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
