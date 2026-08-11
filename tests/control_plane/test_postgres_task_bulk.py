"""Transactional PostgreSQL implementation of bounded task bulk commands."""

from __future__ import annotations

from types import TracebackType

import pytest

from services.control_plane.contracts import (
    BulkTaskCommand,
    BulkTaskItem,
    ControlPlaneError,
    ControlPlaneReason,
    TaskAction,
)
from storage.postgres.control_plane_task_bulk_repository import (
    PostgresControlPlaneTaskBulkRepository,
)


class Transaction:
    """Record whether the fake transaction committed or rolled back."""

    def __init__(self) -> None:
        self.entered = False
        self.exception_type: type[BaseException] | None = None

    async def __aenter__(self) -> None:
        """Mark the transaction active."""

        self.entered = True

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Capture the exception that causes rollback."""

        del exc_value, traceback
        self.exception_type = exc_type


class Connection:
    """Script row-lock and bulk-update results."""

    def __init__(self, results: list[list[dict[str, object]]]) -> None:
        self.results = results
        self.calls: list[tuple[str, tuple[object, ...]]] = []
        self.transaction_state = Transaction()

    def transaction(self) -> Transaction:
        """Return one transaction context."""

        return self.transaction_state

    async def fetch(self, query: str, *args: object) -> list[dict[str, object]]:
        """Return the next scripted rows."""

        self.calls.append((query, args))
        return self.results.pop(0)


class Acquire:
    """Async pool acquisition context."""

    def __init__(self, connection: Connection) -> None:
        self.connection = connection

    async def __aenter__(self) -> Connection:
        """Return the fake connection."""

        return self.connection

    async def __aexit__(self, *args: object) -> None:
        """Release the fake connection."""


class Pool:
    """Return one owned fake connection."""

    def __init__(self, connection: Connection) -> None:
        self.connection = connection

    def acquire(self) -> Acquire:
        """Acquire the fake connection."""

        return Acquire(self.connection)


def row(task_id: str, *, status: str, version: int) -> dict[str, object]:
    """Return one task summary database row."""

    return {
        "task_id": task_id,
        "name": task_id,
        "leader_wallet": "0x1111111111111111111111111111111111111111",
        "follower_account": "account-a",
        "status": status,
        "monitoring_mode": "normal",
        "sizing_mode": "proportional",
        "version": version,
        "notifications_muted": False,
    }


def command() -> BulkTaskCommand:
    """Return a two-item bulk request with distinct revisions."""

    return BulkTaskCommand(
        items=(
            BulkTaskItem(task_id="task-a", action=TaskAction.PAUSE, expected_version=1),
            BulkTaskItem(task_id="task-b", action=TaskAction.DELETE, expected_version=4),
        ),
        actor="operator-a",
    )


@pytest.mark.asyncio
async def test_bulk_locks_then_updates_all_rows_in_one_statement_and_transaction() -> None:
    """The adapter cannot commit a proper subset of a valid bulk request."""

    connection = Connection(
        [
            [row("task-a", status="active", version=1), row("task-b", status="paused", version=4)],
            [row("task-b", status="deleted", version=5), row("task-a", status="paused", version=2)],
        ]
    )
    repository = PostgresControlPlaneTaskBulkRepository(Pool(connection))

    result = await repository.apply_atomically(command())

    assert [task.task_id for task in result] == ["task-a", "task-b"]
    assert [task.status for task in result] == ["paused", "deleted"]
    assert connection.transaction_state.entered is True
    assert connection.transaction_state.exception_type is None
    assert len(connection.calls) == 2
    assert "FOR UPDATE" in connection.calls[0][0]
    assert "UNNEST" in connection.calls[1][0]
    assert "version = tasks.version + 1" in connection.calls[1][0]


@pytest.mark.asyncio
async def test_stale_item_aborts_before_any_update() -> None:
    """One stale revision rolls back the whole requested batch."""

    connection = Connection(
        [[row("task-a", status="active", version=2), row("task-b", status="paused", version=4)]]
    )
    repository = PostgresControlPlaneTaskBulkRepository(Pool(connection))

    with pytest.raises(ControlPlaneError) as captured:
        await repository.apply_atomically(command())

    assert captured.value.reason is ControlPlaneReason.REVISION_MISMATCH
    assert len(connection.calls) == 1
    assert connection.transaction_state.exception_type is ControlPlaneError


@pytest.mark.asyncio
async def test_deleted_task_cannot_be_resumed_or_paused_by_bulk() -> None:
    """Bulk commands preserve the same deleted-task hardening as single commands."""

    deleted_command = BulkTaskCommand(
        items=(BulkTaskItem(task_id="task-a", action=TaskAction.RESUME, expected_version=3),),
        actor="operator-a",
    )
    connection = Connection([[row("task-a", status="deleted", version=3)]])
    repository = PostgresControlPlaneTaskBulkRepository(Pool(connection))

    with pytest.raises(ControlPlaneError) as captured:
        await repository.apply_atomically(deleted_command)

    assert captured.value.reason is ControlPlaneReason.TASK_DELETED
    assert len(connection.calls) == 1
