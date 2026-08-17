"""Control-plane wrappers for existing copy-task lifecycle behavior."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from typing import Protocol

from services.control_plane.contracts import (
    AuditEvent,
    BulkTaskCommand,
    BulkTaskResult,
    ControlPlaneError,
    ControlPlaneReason,
    TaskCreateCommand,
    TaskDuplicateCommand,
    TaskUpdateCommand,
    TaskVersionCommand,
    TaskView,
    TaskAction,
)
from services.copy_tasks.domain import CopyTask, CopyTaskStatus
from services.copy_tasks.service import CopyTaskConflict, CopyTaskNotFound, CopyTaskService


class Clock(Protocol):
    """UTC clock used to make audit behavior deterministic."""

    def now(self) -> datetime:
        """Return the current timezone-aware time."""

        ...


class AuditSink(Protocol):
    """Append-only audit boundary."""

    async def append(self, event: AuditEvent) -> None:
        """Append one immutable audit event."""

        ...


class TaskReader(Protocol):
    """Small read seam needed to harden deleted-task resume."""

    async def get(self, task_id: str) -> CopyTask | None:
        """Return one task including soft-deleted state."""

        ...


class TaskActivationGuard(Protocol):
    """Validate that a paused task has a live execution and source binding."""

    def ensure_available(self, task: CopyTask) -> None:
        """Raise a sanitized application error when activation is unsafe."""

        ...


class AtomicTaskBulkPort(Protocol):
    """One transactional all-or-none task mutation boundary."""

    async def apply_atomically(self, command: BulkTaskCommand) -> tuple[TaskView, ...]:
        """Apply every item or none of them."""

        ...


class ActivationGuardedTaskBulkPort:
    """Decorate atomic task mutations with fail-closed resume validation."""

    def __init__(
        self,
        *,
        port: AtomicTaskBulkPort,
        task_reader: TaskReader,
        activation_guard: TaskActivationGuard,
    ) -> None:
        """Store the atomic mutation and runtime binding collaborators.

        Args:
            port: All-or-none durable task mutation boundary.
            task_reader: Current durable task reader.
            activation_guard: Exact follower/leader runtime binding policy.
        """

        self._port = port
        self._task_reader = task_reader
        self._activation_guard = activation_guard

    async def apply_atomically(self, command: BulkTaskCommand) -> tuple[TaskView, ...]:
        """Validate every resume before invoking the atomic mutation port."""

        for item in command.items:
            if item.action is not TaskAction.RESUME:
                continue
            try:
                task = await self._task_reader.get(item.task_id)
            except Exception:
                raise ControlPlaneError(ControlPlaneReason.INTERNAL_ERROR) from None
            if task is None:
                raise ControlPlaneError(ControlPlaneReason.NOT_FOUND)
            if task.status is CopyTaskStatus.DELETED:
                raise ControlPlaneError(ControlPlaneReason.TASK_DELETED)
            try:
                self._activation_guard.ensure_available(task)
            except ControlPlaneError:
                raise
            except Exception:
                raise ControlPlaneError(ControlPlaneReason.TASK_RUNTIME_UNAVAILABLE) from None
        return await self._port.apply_atomically(command)


class TaskCommandService:
    """Expose safe task mutations while reusing :class:`CopyTaskService`."""

    def __init__(
        self,
        *,
        task_service: CopyTaskService,
        task_reader: TaskReader,
        activation_guard: TaskActivationGuard,
        audit_sink: AuditSink,
        clock: Clock,
    ) -> None:
        """Store narrow collaborators.

        Args:
            task_service: Existing lifecycle authority.
            task_reader: Read seam including deleted rows.
            activation_guard: Fail-closed runtime binding policy.
            audit_sink: Append-only operator audit boundary.
            clock: Deterministic UTC clock.
        """

        self._task_service = task_service
        self._task_reader = task_reader
        self._activation_guard = activation_guard
        self._audit_sink = audit_sink
        self._clock = clock

    async def create(self, command: TaskCreateCommand) -> TaskView:
        """Create an API-supplied task in paused state."""

        safe_task = replace(command.task, status=CopyTaskStatus.PAUSED)
        try:
            created = await self._task_service.create(safe_task)
        except CopyTaskConflict:
            await self._reject(
                actor=command.actor,
                action="task.create",
                target_id=safe_task.task_id,
                reason=ControlPlaneReason.CONFLICT,
            )
            raise ControlPlaneError(ControlPlaneReason.CONFLICT) from None
        except ValueError:
            await self._reject(
                actor=command.actor,
                action="task.create",
                target_id=safe_task.task_id,
                reason=ControlPlaneReason.INVALID_REQUEST,
            )
            raise ControlPlaneError(ControlPlaneReason.INVALID_REQUEST) from None
        except Exception:
            await self._reject(
                actor=command.actor,
                action="task.create",
                target_id=safe_task.task_id,
                reason=ControlPlaneReason.INTERNAL_ERROR,
            )
            raise ControlPlaneError(ControlPlaneReason.INTERNAL_ERROR) from None
        view = TaskView.from_domain(created)
        await self._succeed(
            command.actor,
            "task.create",
            view.task_id,
            follower_account=view.follower_account,
        )
        return view

    async def duplicate(self, command: TaskDuplicateCommand) -> TaskView:
        """Duplicate configuration into a paused version-one task."""

        try:
            duplicated = await self._task_service.duplicate(
                source_task_id=command.source_task_id,
                new_task_id=command.new_task_id,
                new_name=command.new_name,
            )
        except CopyTaskNotFound:
            await self._reject(
                actor=command.actor,
                action="task.duplicate",
                target_id=command.source_task_id,
                reason=ControlPlaneReason.NOT_FOUND,
            )
            raise ControlPlaneError(ControlPlaneReason.NOT_FOUND) from None
        except CopyTaskConflict:
            await self._reject(
                actor=command.actor,
                action="task.duplicate",
                target_id=command.new_task_id,
                reason=ControlPlaneReason.CONFLICT,
            )
            raise ControlPlaneError(ControlPlaneReason.CONFLICT) from None
        except ValueError:
            await self._reject(
                actor=command.actor,
                action="task.duplicate",
                target_id=command.new_task_id,
                reason=ControlPlaneReason.INVALID_REQUEST,
            )
            raise ControlPlaneError(ControlPlaneReason.INVALID_REQUEST) from None
        except Exception:
            await self._reject(
                actor=command.actor,
                action="task.duplicate",
                target_id=command.new_task_id,
                reason=ControlPlaneReason.INTERNAL_ERROR,
            )
            raise ControlPlaneError(ControlPlaneReason.INTERNAL_ERROR) from None
        view = TaskView.from_domain(duplicated)
        await self._succeed(
            command.actor,
            "task.duplicate",
            view.task_id,
            follower_account=view.follower_account,
        )
        return view

    async def update(self, command: TaskUpdateCommand) -> TaskView:
        """Update safe configuration while preserving identity and lifecycle."""

        read_command = TaskVersionCommand(
            task_id=command.task.task_id,
            expected_version=command.expected_version,
            actor=command.actor,
        )
        current = await self._read_task(read_command, action="update")
        if current.status is CopyTaskStatus.DELETED:
            await self._reject(
                actor=command.actor,
                action="task.update",
                target_id=current.task_id,
                reason=ControlPlaneReason.TASK_DELETED,
            )
            raise ControlPlaneError(ControlPlaneReason.TASK_DELETED)
        if current.version != command.expected_version:
            await self._reject(
                actor=command.actor,
                action="task.update",
                target_id=current.task_id,
                reason=ControlPlaneReason.REVISION_MISMATCH,
            )
            raise ControlPlaneError(ControlPlaneReason.REVISION_MISMATCH)
        safe_task = replace(
            command.task,
            task_id=current.task_id,
            leader_wallet=current.leader_wallet,
            follower_account=current.follower_account,
            status=current.status,
            version=current.version,
        )
        try:
            updated = await self._task_service.update(
                safe_task,
                expected_version=command.expected_version,
            )
        except CopyTaskNotFound:
            await self._reject(
                actor=command.actor,
                action="task.update",
                target_id=current.task_id,
                reason=ControlPlaneReason.NOT_FOUND,
            )
            raise ControlPlaneError(ControlPlaneReason.NOT_FOUND) from None
        except CopyTaskConflict:
            await self._reject(
                actor=command.actor,
                action="task.update",
                target_id=current.task_id,
                reason=ControlPlaneReason.REVISION_MISMATCH,
            )
            raise ControlPlaneError(ControlPlaneReason.REVISION_MISMATCH) from None
        except ValueError:
            await self._reject(
                actor=command.actor,
                action="task.update",
                target_id=current.task_id,
                reason=ControlPlaneReason.INVALID_REQUEST,
            )
            raise ControlPlaneError(ControlPlaneReason.INVALID_REQUEST) from None
        except Exception:
            await self._reject(
                actor=command.actor,
                action="task.update",
                target_id=current.task_id,
                reason=ControlPlaneReason.INTERNAL_ERROR,
            )
            raise ControlPlaneError(ControlPlaneReason.INTERNAL_ERROR) from None
        view = TaskView.from_domain(updated)
        await self._succeed(
            command.actor,
            "task.update",
            view.task_id,
            follower_account=view.follower_account,
        )
        return view

    async def pause(self, command: TaskVersionCommand) -> TaskView:
        """Pause new task actions while preserving positions."""

        return await self._lifecycle(command, action="pause")

    async def resume(self, command: TaskVersionCommand) -> TaskView:
        """Resume a task only when it has not been soft-deleted."""

        return await self._lifecycle(command, action="resume")

    async def delete(self, command: TaskVersionCommand) -> TaskView:
        """Soft-delete task configuration without closing positions."""

        return await self._lifecycle(command, action="delete")

    async def _lifecycle(
        self,
        command: TaskVersionCommand,
        *,
        action: str,
        current: CopyTask | None = None,
    ) -> TaskView:
        """Run one versioned lifecycle method and map stable failures."""

        if current is None:
            current = await self._read_task(command, action=action)
        if current.status is CopyTaskStatus.DELETED and action != "delete":
            await self._reject(
                actor=command.actor,
                action=f"task.{action}",
                target_id=command.task_id,
                reason=ControlPlaneReason.TASK_DELETED,
            )
            raise ControlPlaneError(ControlPlaneReason.TASK_DELETED)
        if current.version != command.expected_version:
            await self._reject(
                actor=command.actor,
                action=f"task.{action}",
                target_id=command.task_id,
                reason=ControlPlaneReason.REVISION_MISMATCH,
            )
            raise ControlPlaneError(ControlPlaneReason.REVISION_MISMATCH)
        if action == "resume":
            await self._require_activation(command, current)
        operation = getattr(self._task_service, action)
        try:
            updated = await operation(
                command.task_id,
                expected_version=command.expected_version,
            )
        except CopyTaskNotFound:
            await self._reject(
                actor=command.actor,
                action=f"task.{action}",
                target_id=command.task_id,
                reason=ControlPlaneReason.NOT_FOUND,
            )
            raise ControlPlaneError(ControlPlaneReason.NOT_FOUND) from None
        except CopyTaskConflict:
            await self._reject(
                actor=command.actor,
                action=f"task.{action}",
                target_id=command.task_id,
                reason=ControlPlaneReason.REVISION_MISMATCH,
            )
            raise ControlPlaneError(ControlPlaneReason.REVISION_MISMATCH) from None
        except Exception:
            await self._reject(
                actor=command.actor,
                action=f"task.{action}",
                target_id=command.task_id,
                reason=ControlPlaneReason.INTERNAL_ERROR,
            )
            raise ControlPlaneError(ControlPlaneReason.INTERNAL_ERROR) from None
        view = TaskView.from_domain(updated)
        await self._succeed(
            command.actor,
            f"task.{action}",
            view.task_id,
            follower_account=view.follower_account,
        )
        return view

    async def _require_activation(
        self,
        command: TaskVersionCommand,
        task: CopyTask,
    ) -> None:
        """Reject resume when the running process cannot execute the task."""

        try:
            self._activation_guard.ensure_available(task)
        except ControlPlaneError as exc:
            reason = exc.reason
        except Exception:
            reason = ControlPlaneReason.TASK_RUNTIME_UNAVAILABLE
        else:
            return
        await self._reject(
            actor=command.actor,
            action="task.resume",
            target_id=command.task_id,
            reason=reason,
        )
        raise ControlPlaneError(reason) from None

    async def _read_task(self, command: TaskVersionCommand, *, action: str) -> CopyTask:
        """Read one task and convert missing identity to a stable error."""

        try:
            current = await self._task_reader.get(command.task_id)
        except Exception:
            await self._reject(
                actor=command.actor,
                action=f"task.{action}",
                target_id=command.task_id,
                reason=ControlPlaneReason.INTERNAL_ERROR,
            )
            raise ControlPlaneError(ControlPlaneReason.INTERNAL_ERROR) from None
        if current is None:
            await self._reject(
                actor=command.actor,
                action=f"task.{action}",
                target_id=command.task_id,
                reason=ControlPlaneReason.NOT_FOUND,
            )
            raise ControlPlaneError(ControlPlaneReason.NOT_FOUND)
        return current

    async def _succeed(
        self,
        actor: str,
        action: str,
        target_id: str,
        *,
        follower_account: str | None = None,
    ) -> None:
        """Append one success audit event."""

        try:
            await self._audit_sink.append(
                AuditEvent.record(
                    actor=actor,
                    action=action,
                    target_type="copy_task",
                    target_id=target_id,
                    outcome="succeeded",
                    reason_code="ok",
                    occurred_at=self._clock.now(),
                    follower_account=follower_account,
                )
            )
        except Exception:
            raise ControlPlaneError(ControlPlaneReason.AUDIT_UNAVAILABLE) from None

    async def _reject(
        self,
        *,
        actor: str,
        action: str,
        target_id: str,
        reason: ControlPlaneReason,
    ) -> None:
        """Append one sanitized rejection audit event."""

        try:
            await self._audit_sink.append(
                AuditEvent.record(
                    actor=actor,
                    action=action,
                    target_type="copy_task",
                    target_id=target_id,
                    outcome="rejected",
                    reason_code=reason.value,
                    occurred_at=self._clock.now(),
                )
            )
        except Exception:
            raise ControlPlaneError(ControlPlaneReason.AUDIT_UNAVAILABLE) from None


class TaskBulkCommandService:
    """Validate a bounded batch before crossing one atomic persistence seam."""

    MAX_ITEMS = 100

    def __init__(self, *, port: AtomicTaskBulkPort, audit_sink: AuditSink, clock: Clock) -> None:
        """Store the atomic port and audit collaborators."""

        self._port = port
        self._audit_sink = audit_sink
        self._clock = clock

    async def apply(self, command: BulkTaskCommand) -> BulkTaskResult:
        """Apply every item transactionally or reject the whole batch."""

        count = len(command.items)
        if count < 1 or count > self.MAX_ITEMS:
            raise ControlPlaneError(ControlPlaneReason.BULK_LIMIT_EXCEEDED)
        identities = tuple(item.task_id for item in command.items)
        if len(set(identities)) != count:
            raise ControlPlaneError(ControlPlaneReason.DUPLICATE_BULK_TARGET)
        try:
            tasks = await self._port.apply_atomically(command)
        except ControlPlaneError:
            raise
        except Exception:
            raise ControlPlaneError(ControlPlaneReason.BULK_FAILED) from None
        if len(tasks) != count:
            raise ControlPlaneError(ControlPlaneReason.BULK_FAILED)
        try:
            await self._audit_sink.append(
                AuditEvent.record(
                    actor=command.actor,
                    action="task.bulk",
                    target_type="copy_task_batch",
                    target_id=str(count),
                    outcome="succeeded",
                    reason_code="ok",
                    occurred_at=self._clock.now(),
                )
            )
        except Exception:
            raise ControlPlaneError(ControlPlaneReason.AUDIT_UNAVAILABLE) from None
        return BulkTaskResult(tasks=tasks)


__all__ = [
    "ActivationGuardedTaskBulkPort",
    "AtomicTaskBulkPort",
    "AuditSink",
    "Clock",
    "TaskBulkCommandService",
    "TaskCommandService",
    "TaskActivationGuard",
    "TaskReader",
]
