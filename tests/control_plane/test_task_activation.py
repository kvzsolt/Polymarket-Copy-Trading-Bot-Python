"""Fail-closed runtime activation policy tests for durable copy tasks."""

from __future__ import annotations

import pytest

from services.control_plane.contracts import ControlPlaneError, ControlPlaneReason
from services.copy_tasks.domain import CopyTask


class FixedTaskBindingReader:
    """Return a deterministic follower/leader runtime-binding result."""

    def __init__(self, available: bool) -> None:
        self._available = available

    def has_task_binding(self, task: CopyTask) -> bool:
        """Return the configured availability result."""

        del task
        return self._available


def task() -> CopyTask:
    """Return one valid task identity for activation tests."""

    return CopyTask(
        task_id="task-a",
        name="Primary",
        leader_wallet="0x1111111111111111111111111111111111111111",
        follower_account="account-a",
    )


def test_runtime_activation_requires_exact_binding_and_monitored_leader() -> None:
    """Accept only tasks whose money path and source monitor are both active."""

    from orchestrators.control_plane_tasks import (
        LeaderMonitoringState,
        RuntimeTaskActivationGuard,
    )

    monitoring = LeaderMonitoringState((task().leader_wallet.upper(),))
    guard = RuntimeTaskActivationGuard(
        bindings=FixedTaskBindingReader(True),
        monitoring=monitoring,
    )

    guard.ensure_available(task())
    assert monitoring.is_monitored(task().leader_wallet) is True


@pytest.mark.parametrize(
    ("binding_available", "monitored"),
    [(False, True), (True, False), (False, False)],
)
def test_runtime_activation_fails_closed_when_any_runtime_seam_is_missing(
    binding_available: bool,
    monitored: bool,
) -> None:
    """Reject a resume without changing or probing execution state."""

    from orchestrators.control_plane_tasks import (
        LeaderMonitoringState,
        RuntimeTaskActivationGuard,
    )

    leaders = (task().leader_wallet,) if monitored else ()
    guard = RuntimeTaskActivationGuard(
        bindings=FixedTaskBindingReader(binding_available),
        monitoring=LeaderMonitoringState(leaders),
    )

    with pytest.raises(ControlPlaneError) as captured:
        guard.ensure_available(task())

    assert captured.value.reason is ControlPlaneReason.TASK_RUNTIME_UNAVAILABLE


def test_monitoring_state_adds_normalized_dynamic_leaders() -> None:
    """Dynamic monitor success becomes immediately visible to task resume."""

    from orchestrators.control_plane_tasks import LeaderMonitoringState

    monitoring = LeaderMonitoringState(())
    monitoring.add((task().leader_wallet.upper(),))

    assert monitoring.is_monitored(task().leader_wallet) is True


def test_monitoring_state_removes_crashed_leader() -> None:
    """A crashed source monitor immediately disables future task resume."""

    from orchestrators.control_plane_tasks import LeaderMonitoringState

    monitoring = LeaderMonitoringState((task().leader_wallet,))
    monitoring.remove((task().leader_wallet.upper(),))

    assert monitoring.is_monitored(task().leader_wallet) is False
