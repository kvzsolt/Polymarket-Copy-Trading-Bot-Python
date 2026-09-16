"""Transactional PostgreSQL adapter for safe task lifecycle bulk commands."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from types import TracebackType
from typing import Protocol

from services.control_plane.contracts import (
    BulkTaskCommand,
    ControlPlaneError,
    ControlPlaneReason,
    TaskAction,
    TaskView,
)


class TransactionContext(Protocol):
    """Async transaction context supplied by the database driver."""

    async def __aenter__(self) -> object:
        """Begin a transaction."""

        ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> object:
        """Commit on success or roll back on failure."""

        ...


class DatabaseConnection(Protocol):
    """Connection surface needed by the bulk adapter."""

    def transaction(self) -> TransactionContext:
        """Return a transaction context."""

        ...

    async def fetch(
        self,
        query: str,
        *args: object,
    ) -> Sequence[Mapping[str, object]]:
        """Return query rows."""

        ...


class AcquireContext(Protocol):
    """Async connection acquisition context."""

    async def __aenter__(self) -> DatabaseConnection:
        """Acquire a connection."""

        ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> object:
        """Release the connection."""

        ...


class DatabasePool(Protocol):
    """Small connection acquisition surface."""

    def acquire(self) -> AcquireContext:
        """Acquire one database connection."""

        ...


class PostgresControlPlaneTaskBulkRepository:
    """Apply pause/resume/delete batches through one all-or-none transaction."""

    MAX_ITEMS = 100

    def __init__(self, pool: DatabasePool) -> None:
        """Store the injected pool."""

        self._pool = pool

    async def apply_atomically(self, command: BulkTaskCommand) -> tuple[TaskView, ...]:
        """Lock, validate, and update every requested row or roll back all."""

        count = len(command.items)
        if count < 1 or count > self.MAX_ITEMS:
            raise ControlPlaneError(ControlPlaneReason.BULK_LIMIT_EXCEEDED)
        task_ids = tuple(item.task_id for item in command.items)
        if len(set(task_ids)) != count:
            raise ControlPlaneError(ControlPlaneReason.DUPLICATE_BULK_TARGET)
        try:
            async with self._pool.acquire() as connection:
                async with connection.transaction():
                    current_rows = await connection.fetch(
                        """
                        SELECT task_id, status, version
                        FROM copy_tasks
                        WHERE task_id = ANY($1::text[])
                        ORDER BY task_id
                        FOR UPDATE
                        """,
                        task_ids,
                    )
                    self._validate_current(command, current_rows)
                    updated_rows = await connection.fetch(
                        """
                        WITH changes AS (
                            SELECT *
                            FROM UNNEST(
                                $1::text[],
                                $2::text[],
                                $3::integer[]
                            ) AS input(task_id, action, expected_version)
                        )
                        UPDATE copy_tasks AS tasks
                        SET status = CASE changes.action
                                WHEN 'pause' THEN 'paused'
                                WHEN 'resume' THEN 'active'
                                WHEN 'delete' THEN 'deleted'
                            END,
                            version = tasks.version + 1,
                            updated_at = NOW(),
                            deleted_at = CASE changes.action
                                WHEN 'delete' THEN NOW()
                                ELSE NULL
                            END
                        FROM changes
                        WHERE tasks.task_id = changes.task_id
                          AND tasks.version = changes.expected_version
                        RETURNING tasks.task_id, tasks.name,
                                  tasks.leader_wallet, tasks.follower_account,
                                  tasks.status, tasks.monitoring_mode,
                                  tasks.sizing_mode, tasks.version,
                                  tasks.notifications_muted
                        """,
                        task_ids,
                        tuple(item.action.value for item in command.items),
                        tuple(item.expected_version for item in command.items),
                    )
                    if len(updated_rows) != count:
                        raise ControlPlaneError(ControlPlaneReason.BULK_FAILED)
                    by_id = {self._text(row, "task_id"): self._view(row) for row in updated_rows}
                    if set(by_id) != set(task_ids):
                        raise ControlPlaneError(ControlPlaneReason.BULK_FAILED)
                    return tuple(by_id[task_id] for task_id in task_ids)
        except ControlPlaneError:
            raise
        except Exception:
            raise ControlPlaneError(ControlPlaneReason.BULK_FAILED) from None

    def _validate_current(
        self,
        command: BulkTaskCommand,
        rows: Sequence[Mapping[str, object]],
    ) -> None:
        """Validate all locked rows before issuing the single update."""

        current = {self._text(row, "task_id"): row for row in rows}
        if len(current) != len(command.items):
            raise ControlPlaneError(ControlPlaneReason.NOT_FOUND)
        for item in command.items:
            row = current.get(item.task_id)
            if row is None:
                raise ControlPlaneError(ControlPlaneReason.NOT_FOUND)
            if self._integer(row, "version") != item.expected_version:
                raise ControlPlaneError(ControlPlaneReason.REVISION_MISMATCH)
            if self._text(row, "status") == "deleted":
                raise ControlPlaneError(ControlPlaneReason.TASK_DELETED)
            if item.action not in {TaskAction.PAUSE, TaskAction.RESUME, TaskAction.DELETE}:
                raise ControlPlaneError(ControlPlaneReason.INVALID_REQUEST)

    def _view(self, row: Mapping[str, object]) -> TaskView:
        """Translate an updated row into a safe task summary."""

        return TaskView(
            task_id=self._text(row, "task_id"),
            name=self._text(row, "name"),
            leader_wallet=self._text(row, "leader_wallet"),
            follower_account=self._text(row, "follower_account"),
            status=self._text(row, "status"),
            monitoring_mode=self._text(row, "monitoring_mode"),
            sizing_mode=self._text(row, "sizing_mode"),
            version=self._integer(row, "version"),
            notifications_muted=bool(row.get("notifications_muted")),
        )

    @staticmethod
    def _text(row: Mapping[str, object], name: str) -> str:
        """Return one non-empty row string."""

        value = row.get(name)
        if not isinstance(value, str) or not value:
            raise ControlPlaneError(ControlPlaneReason.BULK_FAILED)
        return value

    @staticmethod
    def _integer(row: Mapping[str, object], name: str) -> int:
        """Return one integer row value."""

        value = row.get(name)
        if isinstance(value, bool) or not isinstance(value, (int, str)):
            raise ControlPlaneError(ControlPlaneReason.BULK_FAILED)
        try:
            return int(value)
        except (TypeError, ValueError):
            raise ControlPlaneError(ControlPlaneReason.BULK_FAILED) from None


__all__ = ["PostgresControlPlaneTaskBulkRepository"]
