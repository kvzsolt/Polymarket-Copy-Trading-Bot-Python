"""Tasks cannot claim activation when the opposite-outcome binding is unavailable."""

from dataclasses import replace

import pytest

from integrations.control_plane.serialization import ControlPlaneErrorMapper
from orchestrators.control_plane_tasks import LeaderMonitoringState, RuntimeTaskActivationGuard
from services.control_plane.contracts import ControlPlaneError, TaskVersionCommand
from services.control_plane.tasks import TaskCommandService
from services.copy_tasks.domain import CopyTaskStatus, ReactionPolicy
from services.copy_tasks.entry_permission import TaskEntryPermissionService
from services.copy_tasks.service import CopyTaskService, InMemoryCopyTaskRepository
from tests.control_plane.test_task_activation import FixedTaskBindingReader, task
from tests.control_plane.test_task_commands import FixedClock, RecordingAuditSink


@pytest.mark.asyncio
async def test_counter_trade_resume_requires_an_authoritative_outcome_binding() -> None:
    counter = replace(
        task(), status=CopyTaskStatus.PAUSED, reaction=ReactionPolicy(counter_trade=True)
    )
    repository = InMemoryCopyTaskRepository()
    await repository.create(counter)
    service = TaskCommandService(
        task_service=CopyTaskService(repository),
        task_reader=repository,
        activation_guard=RuntimeTaskActivationGuard(
            bindings=FixedTaskBindingReader(True),
            monitoring=LeaderMonitoringState((counter.leader_wallet,)),
        ),
        audit_sink=RecordingAuditSink(),
        clock=FixedClock(),
    )
    with pytest.raises(ControlPlaneError) as error:
        await service.resume(
            TaskVersionCommand(
                task_id=counter.task_id, expected_version=counter.version, actor="operator"
            )
        )
    assert error.value.reason.value == "task_counter_outcome_unavailable"
    response = ControlPlaneErrorMapper().response(error.value, correlation_id="counter-test")
    assert response.status == 503
    assert "task_counter_outcome_unavailable" in response.text
    assert await repository.get(counter.task_id) == counter


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
