"""Control-plane task command contracts and lifecycle hardening."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from services.control_plane.contracts import (
    AuditEvent,
    BulkTaskCommand,
    BulkTaskItem,
    ControlPlaneError,
    ControlPlaneReason,
    TaskAction,
    TaskCreateCommand,
    TaskDuplicateCommand,
    TaskVersionCommand,
    TaskView,
)
from services.control_plane.tasks import (
    ActivationGuardedTaskBulkPort,
    TaskBulkCommandService,
    TaskCommandService,
)
from services.copy_tasks.domain import CopyTask, CopyTaskStatus
from services.copy_tasks.service import CopyTaskService, InMemoryCopyTaskRepository


NOW = datetime(2026, 8, 11, 12, 30, tzinfo=UTC)


class FixedClock:
    """Return a deterministic timestamp for audit records."""

    def now(self) -> datetime:
        """Return the fixed UTC time."""

        return NOW


class RecordingAuditSink:
    """Capture immutable audit events in insertion order."""

    def __init__(self) -> None:
        self.events: list[AuditEvent] = []

    async def append(self, event: AuditEvent) -> None:
        """Record one event."""

        self.events.append(event)


class FailingAuditSink:
    """Raise a raw storage exception containing sensitive-looking detail."""

    async def append(self, event: AuditEvent) -> None:
        """Fail every append."""

        del event
        raise RuntimeError("database failed with private_key material")


class FailingCopyTaskRepository(InMemoryCopyTaskRepository):
    """Raise a raw persistence failure containing sensitive-looking detail."""

    async def create(self, task: CopyTask) -> None:
        """Fail before creating the task."""

        del task
        raise RuntimeError("driver failed with password material")


class AllowingTaskActivation:
    """Permit deterministic task activation in service unit tests."""

    def ensure_available(self, task: CopyTask) -> None:
        """Accept the supplied task."""

        del task


class RejectingTaskActivation:
    """Reject activation as an unavailable runtime binding."""

    def ensure_available(self, task: CopyTask) -> None:
        """Raise the stable fail-closed activation error."""

        del task
        raise ControlPlaneError(ControlPlaneReason.TASK_RUNTIME_UNAVAILABLE)


class RecordingBulkPort:
    """Represent one transactional all-or-none task bulk boundary."""

    def __init__(self) -> None:
        self.calls: list[BulkTaskCommand] = []

    async def apply_atomically(self, command: BulkTaskCommand) -> tuple[TaskView, ...]:
        """Record exactly one transactional request."""

        self.calls.append(command)
        return tuple(
            TaskView(
                task_id=item.task_id,
                name=item.task_id,
                leader_wallet="0x1111111111111111111111111111111111111111",
                follower_account="account-a",
                status="paused",
                monitoring_mode="normal",
                sizing_mode="proportional",
                version=item.expected_version + 1,
                notifications_muted=False,
            )
            for item in command.items
        )


def task(task_id: str = "task-a") -> CopyTask:
    """Return one active domain task supplied by an API adapter."""

    return CopyTask(
        task_id=task_id,
        name="Primary",
        leader_wallet="0x1111111111111111111111111111111111111111",
        follower_account="account-a",
    )


def command_service(
    repository: InMemoryCopyTaskRepository,
    audit: RecordingAuditSink,
) -> TaskCommandService:
    """Build the control-plane wrapper around the existing task service."""

    return TaskCommandService(
        task_service=CopyTaskService(repository),
        task_reader=repository,
        activation_guard=AllowingTaskActivation(),
        audit_sink=audit,
        clock=FixedClock(),
    )


@pytest.mark.asyncio
async def test_api_create_and_duplicate_always_start_paused() -> None:
    """Control-plane creation cannot start new entries before review."""

    repository = InMemoryCopyTaskRepository()
    audit = RecordingAuditSink()
    service = command_service(repository, audit)

    created = await service.create(TaskCreateCommand(task=task(), actor="operator-a"))
    duplicated = await service.duplicate(
        TaskDuplicateCommand(
            source_task_id="task-a",
            new_task_id="task-b",
            new_name="Copy",
            actor="operator-a",
        )
    )

    assert created.status == CopyTaskStatus.PAUSED.value
    assert duplicated.status == CopyTaskStatus.PAUSED.value
    assert (await repository.get("task-a")).status is CopyTaskStatus.PAUSED  # type: ignore[union-attr]
    assert [event.action for event in audit.events] == ["task.create", "task.duplicate"]
    assert all(event.outcome == "succeeded" for event in audit.events)


@pytest.mark.asyncio
async def test_deleted_task_cannot_be_resumed_through_control_plane() -> None:
    """The wrapper closes the deleted-to-active gap in CopyTaskService."""

    repository = InMemoryCopyTaskRepository()
    audit = RecordingAuditSink()
    service = command_service(repository, audit)
    await service.create(TaskCreateCommand(task=task(), actor="operator-a"))
    deleted = await service.delete(
        TaskVersionCommand(task_id="task-a", expected_version=1, actor="operator-a")
    )

    with pytest.raises(ControlPlaneError) as captured:
        await service.resume(
            TaskVersionCommand(
                task_id="task-a",
                expected_version=deleted.version,
                actor="operator-a",
            )
        )

    assert captured.value.reason is ControlPlaneReason.TASK_DELETED
    persisted = await repository.get("task-a")
    assert persisted is not None
    assert persisted.status is CopyTaskStatus.DELETED
    assert persisted.version == deleted.version
    assert audit.events[-1].outcome == "rejected"
    assert audit.events[-1].reason_code == ControlPlaneReason.TASK_DELETED.value


@pytest.mark.asyncio
async def test_unbound_task_cannot_be_resumed_through_control_plane() -> None:
    """A dormant leader binding fails closed before persistence is changed."""

    repository = InMemoryCopyTaskRepository()
    audit = RecordingAuditSink()
    service = TaskCommandService(
        task_service=CopyTaskService(repository),
        task_reader=repository,
        activation_guard=RejectingTaskActivation(),
        audit_sink=audit,
        clock=FixedClock(),
    )
    created = await service.create(TaskCreateCommand(task=task(), actor="operator-a"))

    with pytest.raises(ControlPlaneError) as captured:
        await service.resume(
            TaskVersionCommand(
                task_id=created.task_id,
                expected_version=created.version,
                actor="operator-a",
            )
        )

    assert captured.value.reason is ControlPlaneReason.TASK_RUNTIME_UNAVAILABLE
    persisted = await repository.get(created.task_id)
    assert persisted is not None
    assert persisted.status is CopyTaskStatus.PAUSED
    assert persisted.version == created.version
    assert audit.events[-1].outcome == "rejected"
    assert audit.events[-1].reason_code == ControlPlaneReason.TASK_RUNTIME_UNAVAILABLE.value


@pytest.mark.asyncio
async def test_deleted_task_cannot_be_paused_into_a_resumable_state() -> None:
    """The wrapper keeps deletion terminal across every reactivation path."""

    repository = InMemoryCopyTaskRepository()
    audit = RecordingAuditSink()
    service = command_service(repository, audit)
    await service.create(TaskCreateCommand(task=task(), actor="operator-a"))
    deleted = await service.delete(
        TaskVersionCommand(task_id="task-a", expected_version=1, actor="operator-a")
    )

    with pytest.raises(ControlPlaneError) as captured:
        await service.pause(
            TaskVersionCommand(
                task_id="task-a",
                expected_version=deleted.version,
                actor="operator-a",
            )
        )

    assert captured.value.reason is ControlPlaneReason.TASK_DELETED
    persisted = await repository.get("task-a")
    assert persisted is not None and persisted.status is CopyTaskStatus.DELETED
    assert persisted.version == deleted.version


@pytest.mark.asyncio
async def test_task_errors_are_stable_and_do_not_expose_repository_messages() -> None:
    """Not-found and stale-revision failures use fixed public reasons."""

    repository = InMemoryCopyTaskRepository()
    audit = RecordingAuditSink()
    service = command_service(repository, audit)
    await service.create(TaskCreateCommand(task=task(), actor="operator-a"))

    with pytest.raises(ControlPlaneError) as stale:
        await service.pause(
            TaskVersionCommand(task_id="task-a", expected_version=99, actor="operator-a")
        )
    with pytest.raises(ControlPlaneError) as missing:
        await service.delete(
            TaskVersionCommand(task_id="missing", expected_version=1, actor="operator-a")
        )

    assert stale.value.reason is ControlPlaneReason.REVISION_MISMATCH
    assert stale.value.public_message == "The resource revision no longer matches."
    assert missing.value.reason is ControlPlaneReason.NOT_FOUND
    assert "missing" not in missing.value.public_message


@pytest.mark.asyncio
async def test_bulk_commands_cross_one_atomic_port_and_are_bounded() -> None:
    """Validated bulk work is never expanded into partial service calls."""

    port = RecordingBulkPort()
    audit = RecordingAuditSink()
    service = TaskBulkCommandService(port=port, audit_sink=audit, clock=FixedClock())
    command = BulkTaskCommand(
        items=(
            BulkTaskItem(task_id="task-a", action=TaskAction.PAUSE, expected_version=1),
            BulkTaskItem(task_id="task-b", action=TaskAction.DELETE, expected_version=4),
        ),
        actor="operator-a",
    )

    result = await service.apply(command)

    assert result.atomic is True
    assert len(result.tasks) == 2
    assert port.calls == [command]
    assert audit.events[-1].target_id == "2"


@pytest.mark.asyncio
async def test_bulk_rejects_more_than_100_or_duplicate_targets_before_port_call() -> None:
    """Invalid work cannot reach a transaction boundary."""

    port = RecordingBulkPort()
    service = TaskBulkCommandService(
        port=port,
        audit_sink=RecordingAuditSink(),
        clock=FixedClock(),
    )
    too_many = BulkTaskCommand(
        items=tuple(
            BulkTaskItem(
                task_id=f"task-{index}",
                action=TaskAction.PAUSE,
                expected_version=1,
            )
            for index in range(101)
        ),
        actor="operator-a",
    )
    duplicates = BulkTaskCommand(
        items=(
            BulkTaskItem(task_id="task-a", action=TaskAction.PAUSE, expected_version=1),
            BulkTaskItem(task_id="task-a", action=TaskAction.DELETE, expected_version=1),
        ),
        actor="operator-a",
    )

    with pytest.raises(ControlPlaneError) as oversized:
        await service.apply(too_many)
    with pytest.raises(ControlPlaneError) as duplicate:
        await service.apply(duplicates)

    assert oversized.value.reason is ControlPlaneReason.BULK_LIMIT_EXCEEDED
    assert duplicate.value.reason is ControlPlaneReason.DUPLICATE_BULK_TARGET
    assert port.calls == []


@pytest.mark.asyncio
async def test_bulk_resume_requires_every_task_to_have_a_runtime_binding() -> None:
    """An unbound resume is rejected before the atomic mutation port runs."""

    repository = InMemoryCopyTaskRepository()
    await repository.create(task())
    port = RecordingBulkPort()
    guarded = ActivationGuardedTaskBulkPort(
        port=port,
        task_reader=repository,
        activation_guard=RejectingTaskActivation(),
    )
    command = BulkTaskCommand(
        items=(
            BulkTaskItem(
                task_id="task-a",
                action=TaskAction.RESUME,
                expected_version=1,
            ),
        ),
        actor="operator-a",
    )

    with pytest.raises(ControlPlaneError) as captured:
        await guarded.apply_atomically(command)

    assert captured.value.reason is ControlPlaneReason.TASK_RUNTIME_UNAVAILABLE
    assert port.calls == []


@pytest.mark.asyncio
async def test_raw_audit_failure_is_replaced_with_stable_public_error() -> None:
    """Mutation services never return driver or credential-bearing exception text."""

    repository = InMemoryCopyTaskRepository()
    service = TaskCommandService(
        task_service=CopyTaskService(repository),
        task_reader=repository,
        activation_guard=AllowingTaskActivation(),
        audit_sink=FailingAuditSink(),
        clock=FixedClock(),
    )

    with pytest.raises(ControlPlaneError) as captured:
        await service.create(TaskCreateCommand(task=task(), actor="operator-a"))

    assert captured.value.reason is ControlPlaneReason.AUDIT_UNAVAILABLE
    assert "private_key" not in str(captured.value)


@pytest.mark.asyncio
async def test_raw_task_persistence_failure_is_sanitized_and_audited() -> None:
    """Task commands never expose database or credential-bearing error text."""

    repository = FailingCopyTaskRepository()
    audit = RecordingAuditSink()
    service = command_service(repository, audit)

    with pytest.raises(ControlPlaneError) as captured:
        await service.create(TaskCreateCommand(task=task(), actor="operator-a"))

    assert captured.value.reason is ControlPlaneReason.INTERNAL_ERROR
    assert "password" not in str(captured.value)
    assert audit.events[-1].outcome == "rejected"
    assert audit.events[-1].reason_code == ControlPlaneReason.INTERNAL_ERROR.value
