"""Expanded typed control-plane status, detail, and safe-update contracts."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from services.control_plane.contracts import (
    AuditEvent,
    CapabilitiesView,
    ControlPlaneError,
    ControlPlaneReason,
    DiscoveryCandidateView,
    DiscoveryRosterView,
    DiscoveryStatusView,
    Page,
    PageRequest,
    ReadinessView,
    SystemStatusView,
    SystemView,
    TaskCreateCommand,
    TaskDetailView,
    TaskUpdateCommand,
)
from services.control_plane.queries import (
    DiscoveryQueryService,
    SystemQueryService,
    TaskQueryService,
)
from services.control_plane.tasks import TaskCommandService
from services.copy_tasks.domain import CopyTask, CopyTaskStatus, TaskSpendPolicy
from services.copy_tasks.service import CopyTaskService, InMemoryCopyTaskRepository


NOW = datetime(2026, 8, 11, 15, 0, tzinfo=UTC)


class FixedClock:
    """Return a deterministic audit time."""

    def now(self) -> datetime:
        """Return a fixed UTC timestamp."""

        return NOW


class AuditSink:
    """Capture mutation audit records."""

    def __init__(self) -> None:
        self.events: list[AuditEvent] = []

    async def append(self, event: AuditEvent) -> None:
        """Append one event."""

        self.events.append(event)


class AllowingTaskActivation:
    """Permit task activation for unrelated control-service tests."""

    def ensure_available(self, task: CopyTask) -> None:
        """Accept the supplied task."""

        del task


class SystemPort:
    """Supply explicit system/readiness/capability projections."""

    def __init__(self) -> None:
        self.system = SystemView(
            service="pmcopy",
            version="test",
            environment="paper",
            observed_at=NOW,
        )
        self.readiness = ReadinessView(
            live=True,
            ready=False,
            failed_checks=("database",),
            observed_at=NOW,
        )
        self.capabilities = CapabilitiesView(
            capabilities=("task.read", "runtime.pause_entries"),
            observed_at=NOW,
        )

    async def get_system(self) -> SystemView:
        """Return system identity."""

        return self.system

    async def get_readiness(self) -> ReadinessView:
        """Return authoritative readiness supplied by the runtime."""

        return self.readiness

    async def get_capabilities(self) -> CapabilitiesView:
        """Return only explicitly wired capabilities."""

        return self.capabilities


class TaskPort:
    """Supply a scripted detail lookup."""

    def __init__(self, detail: TaskDetailView | None) -> None:
        self.detail = detail

    async def list_tasks(self, follower_account: str, page: PageRequest):
        """Return no summaries."""

        return Page(items=(), next_cursor=None)

    async def get_task(self, task_id: str) -> TaskDetailView | None:
        """Return the scripted detail."""

        return self.detail


class DiscoveryPort:
    """Supply distinct discovery status, candidate, and roster views."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    async def list_discovery_cycles(self, page: PageRequest):
        """Return no cycles."""

        self.calls.append("cycles")
        return Page(items=(), next_cursor=None)

    async def get_discovery_status(self) -> DiscoveryStatusView:
        """Return provider-safe status."""

        self.calls.append("status")
        return DiscoveryStatusView(
            state="idle",
            current_run_id=None,
            last_cycle_id="cycle-a",
            observed_at=NOW,
        )

    async def list_discovery_candidates(self, page: PageRequest) -> Page[DiscoveryCandidateView]:
        """Return one scored candidate."""

        self.calls.append("candidates")
        return Page(
            items=(
                DiscoveryCandidateView(
                    wallet="0x1111111111111111111111111111111111111111",
                    score="0.875",
                    accepted=True,
                    reason_code="accepted",
                    cycle_id="cycle-a",
                ),
            ),
            next_cursor=None,
        )

    async def list_discovery_roster(self, page: PageRequest) -> Page[DiscoveryRosterView]:
        """Return one active roster entry."""

        self.calls.append("roster")
        return Page(
            items=(
                DiscoveryRosterView(
                    wallet="0x1111111111111111111111111111111111111111",
                    tier="core",
                    active=True,
                    score="0.875",
                    updated_at=NOW,
                ),
            ),
            next_cursor=None,
        )


def task() -> CopyTask:
    """Return one domain task with exact nested financial configuration."""

    return CopyTask(
        task_id="task-a",
        name="Primary",
        leader_wallet="0x1111111111111111111111111111111111111111",
        follower_account="account-a",
        spend=TaskSpendPolicy(max_total=Decimal("125.50")),
    )


@pytest.mark.asyncio
async def test_system_status_and_capabilities_are_explicit_port_truth() -> None:
    """The core does not advertise features by inspecting object attributes."""

    port = SystemPort()
    service = SystemQueryService(port)

    status = await service.get_status()
    capabilities = await service.get_capabilities()

    assert status == SystemStatusView(system=port.system, readiness=port.readiness)
    assert capabilities.capabilities == ("task.read", "runtime.pause_entries")


@pytest.mark.asyncio
async def test_task_update_preserves_identity_and_paused_lifecycle_state() -> None:
    """Configuration updates cannot become a hidden resume or account move."""

    repository = InMemoryCopyTaskRepository()
    audit = AuditSink()
    service = TaskCommandService(
        task_service=CopyTaskService(repository),
        task_reader=repository,
        activation_guard=AllowingTaskActivation(),
        audit_sink=audit,
        clock=FixedClock(),
    )
    created = await service.create(TaskCreateCommand(task=task(), actor="operator-a"))
    incoming = replace(
        task(),
        name="Renamed",
        status=CopyTaskStatus.ACTIVE,
        leader_wallet="0x2222222222222222222222222222222222222222",
        follower_account="other-account",
    )

    updated = await service.update(
        TaskUpdateCommand(
            task=incoming,
            expected_version=created.version,
            actor="operator-a",
        )
    )

    assert updated.name == "Renamed"
    assert updated.status == CopyTaskStatus.PAUSED.value
    assert updated.leader_wallet == task().leader_wallet
    assert updated.follower_account == task().follower_account
    assert updated.version == 2
    assert audit.events[-1].action == "task.update"


@pytest.mark.asyncio
async def test_task_detail_keeps_nested_money_as_decimal_strings() -> None:
    """Detailed task configuration remains exact at the API boundary."""

    detail = TaskDetailView.from_domain(task())
    service = TaskQueryService(TaskPort(detail))

    result = await service.get("task-a")

    assert result.configuration.spend.max_total == "125.50"
    assert isinstance(result.configuration.spend.max_total, str)

    with pytest.raises(ControlPlaneError) as missing:
        await TaskQueryService(TaskPort(None)).get("missing")
    assert missing.value.reason is ControlPlaneReason.NOT_FOUND


@pytest.mark.asyncio
async def test_discovery_queries_keep_status_candidates_and_roster_separate() -> None:
    """Manual-run state and ranking projections remain side-effect-free reads."""

    port = DiscoveryPort()
    service = DiscoveryQueryService(port)
    page = PageRequest(limit=10)

    status = await service.get_status()
    candidates = await service.list_candidates(page)
    roster = await service.list_roster(page)

    assert status.state == "idle"
    assert candidates.items[0].score == "0.875"
    assert roster.items[0].active is True
    assert port.calls == ["status", "candidates", "roster"]
