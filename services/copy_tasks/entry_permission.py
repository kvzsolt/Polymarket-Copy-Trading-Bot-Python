"""Revalidate durable task permission immediately before a follower entry POST."""

from typing import Protocol

from services.copy_tasks.domain import CopyTask, CopyTaskStatus
from services.copy_tasks.reconciliation import Clock, SystemClock


class TaskPermissionReader(Protocol):
    """Read current task state without exposing mutation operations."""

    async def get(self, task_id: str) -> CopyTask | None:
        """Return the durable task or None when it no longer exists."""
        ...


class TaskEntryPermissionService:
    """Keep account binding, task version, activation, and schedules authoritative."""

    def __init__(
        self,
        reader: TaskPermissionReader,
        *,
        follower_accounts: tuple[str, ...],
        clock: Clock | None = None,
        protective_management_enabled: bool = False,
    ) -> None:
        """Bind identities from runtime configuration, never from a submitted command."""
        self._reader = reader
        self._accounts = frozenset(value.strip().casefold() for value in follower_accounts if value)
        if not self._accounts:
            raise ValueError("Task permission requires a configured follower identity")
        self._clock = clock or SystemClock()
        self._protective_management_enabled = protective_management_enabled

    async def denial(self, task_id: str, version: int, leader_wallet: str) -> str | None:
        """Return a safe reason for refusing an entry, or None when still allowed."""
        try:
            task = await self._reader.get(task_id)
        except Exception:
            return "task_state_unavailable"
        if task is None or task.status is CopyTaskStatus.DELETED:
            return "task_deleted"
        if task.follower_account.casefold() not in self._accounts:
            return "task_account_mismatch"
        if task.leader_wallet.casefold() != leader_wallet.casefold():
            return "task_leader_mismatch"
        if task.status is CopyTaskStatus.PAUSED:
            return "task_paused"
        if task.version != version:
            return "task_version_conflict"
        if task.protective_risk.requires_monitoring and not self._protective_management_enabled:
            return "task_protection_unavailable"
        if task.reaction.counter_trade:
            return "task_counter_outcome_unavailable"
        if not task.schedule.admits_entry(self._clock.now()):
            return "outside_schedule"
        return None
