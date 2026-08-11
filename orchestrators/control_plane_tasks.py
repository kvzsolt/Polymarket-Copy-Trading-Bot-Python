"""Compose safe task activation from runtime binding and monitor state.

This module contains no persistence or transport code. It projects two
read-only runtime facts into the fail-closed policy consumed by the canonical
task command service.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Protocol

from services.control_plane.contracts import ControlPlaneError, ControlPlaneReason
from services.copy_tasks.domain import CopyTask


class TaskBindingReader(Protocol):
    """Report whether a task resolves to the current money-path context."""

    def has_task_binding(self, task: CopyTask) -> bool:
        """Return whether follower execution and leader evidence are bound."""

        ...


class LeaderMonitorReader(Protocol):
    """Report whether one leader currently has a source monitor."""

    def is_monitored(self, leader_wallet: str) -> bool:
        """Return whether normalized leader events can reach v1 ingress."""

        ...


class LeaderMonitoringState:
    """Own the normalized in-process set of successfully started monitors."""

    def __init__(self, leaders: Iterable[str]) -> None:
        """Initialize state from the reconciled startup leader roster.

        Args:
            leaders: Leaders whose monitoring paths are safe to start.
        """

        self._leaders = self._normalize(leaders)

    def add(self, leaders: Iterable[str]) -> None:
        """Record dynamically started leaders after reconciliation succeeds."""

        self._leaders.update(self._normalize(leaders))

    def remove(self, leaders: Iterable[str]) -> None:
        """Forget leaders whose source monitor is no longer running."""

        self._leaders.difference_update(self._normalize(leaders))

    def is_monitored(self, leader_wallet: str) -> bool:
        """Return whether a normalized leader monitor is active."""

        normalized = str(leader_wallet).strip().lower()
        return bool(normalized) and normalized in self._leaders

    @staticmethod
    def _normalize(leaders: Iterable[str]) -> set[str]:
        """Return non-empty case-insensitive leader identities."""

        return {str(leader).strip().lower() for leader in leaders if str(leader).strip()}


class RuntimeTaskActivationGuard:
    """Require both exact money-path binding and active source monitoring."""

    def __init__(
        self,
        *,
        bindings: TaskBindingReader,
        monitoring: LeaderMonitorReader,
    ) -> None:
        """Store narrow injected runtime readers.

        Args:
            bindings: Exact follower and leader context binding reader.
            monitoring: Current successfully started monitor reader.
        """

        self._bindings = bindings
        self._monitoring = monitoring

    def ensure_available(self, task: CopyTask) -> None:
        """Reject activation unless events can reach the bound money path.

        Args:
            task: Paused durable task proposed for activation.

        Raises:
            ControlPlaneError: If either runtime seam is unavailable.
        """

        if not self._bindings.has_task_binding(task) or not self._monitoring.is_monitored(
            task.leader_wallet
        ):
            raise ControlPlaneError(ControlPlaneReason.TASK_RUNTIME_UNAVAILABLE)


__all__ = [
    "LeaderMonitoringState",
    "LeaderMonitorReader",
    "RuntimeTaskActivationGuard",
    "TaskBindingReader",
]
