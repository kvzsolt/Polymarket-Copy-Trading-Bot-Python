"""Narrow injected ports consumed by the aiohttp control-plane adapter."""

from __future__ import annotations

import secrets
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol

from services.control_plane.contracts import (
    ActivityView,
    BulkTaskCommand,
    BulkTaskResult,
    CapabilitiesView,
    DiscoveryCandidateView,
    DiscoveryCycleView,
    DiscoveryRosterView,
    DiscoveryRunCommand,
    DiscoveryRunReceipt,
    DiscoveryStatusView,
    EntryControlState,
    LockResetCommand,
    LockResetResult,
    OrderView,
    Page,
    PageRequest,
    PortfolioView,
    PositionView,
    ReentryLockView,
    ReservationView,
    RuntimeControlCommand,
    RuntimeView,
    SettingsDocument,
    SettingsPath,
    SettingsWriteCommand,
    SettingsWriteResult,
    SystemStatusView,
    SystemView,
    TaskCreateCommand,
    TaskDetailView,
    TaskDuplicateCommand,
    TaskUpdateCommand,
    TaskVersionCommand,
    TaskView,
)
from services.control_plane.contracts_reads import (
    AccountView,
    DetectionStatusView,
    FillView,
    LeaderView,
    PnlView,
    RiskStateView,
)
from services.copy_tasks.domain import CopyTask
from services.telemetry.hub import TelemetrySubscription


class CorrelationIdFactory(Protocol):
    """Generate opaque correlation IDs without trusting arbitrary headers."""

    def new(self) -> str:
        """Return one safe opaque ID."""

        ...


class SecureCorrelationIdFactory:
    """Generate cryptographically random correlation IDs."""

    def new(self) -> str:
        """Return a fresh unguessable request correlation identifier."""

        return secrets.token_hex(16)


class SystemQueryPort(Protocol):
    """Read safe process identity, status, and capability projections."""

    async def get(self) -> SystemView:
        """Return process identity and environment state."""

        ...

    async def get_status(self) -> SystemStatusView:
        """Return process identity paired with readiness state."""

        ...

    async def get_capabilities(self) -> CapabilitiesView:
        """Return capabilities backed by composed services."""

        ...


class TaskQueryPort(Protocol):
    """Read bounded copy-task summaries and details."""

    async def list(self, follower_account: str, page: PageRequest) -> Page[TaskView]:
        """Return one follower-scoped task page."""

        ...

    async def get(self, task_id: str) -> TaskDetailView:
        """Return one complete task detail."""

        ...


class TaskCommandPort(Protocol):
    """Mutate copy tasks through the canonical application service."""

    async def create(self, command: TaskCreateCommand) -> TaskView:
        """Create one paused task."""

        ...

    async def update(self, command: TaskUpdateCommand) -> TaskView:
        """Update one versioned task configuration."""

        ...

    async def pause(self, command: TaskVersionCommand) -> TaskView:
        """Pause one versioned task."""

        ...

    async def resume(self, command: TaskVersionCommand) -> TaskView:
        """Resume one safely bound versioned task."""

        ...

    async def duplicate(self, command: TaskDuplicateCommand) -> TaskView:
        """Duplicate one task into a paused task."""

        ...

    async def delete(self, command: TaskVersionCommand) -> TaskView:
        """Soft-delete one task without closing positions."""

        ...


class TaskBulkCommandPort(Protocol):
    """Apply a bounded all-or-none task mutation."""

    async def apply(self, command: BulkTaskCommand) -> BulkTaskResult:
        """Return the complete atomic bulk result."""

        ...


class TaskPayloadDecoder(Protocol):
    """Decode API task configuration without coupling HTTP to domain parsing."""

    def decode(
        self,
        payload: Mapping[str, object],
        *,
        task_id: str | None,
    ) -> CopyTask:
        """Return one validated domain task."""

        ...


class PositionQueryPort(Protocol):
    """Read follower-scoped durable position projections."""

    async def list(self, follower_account: str, page: PageRequest) -> Page[PositionView]:
        """Return one bounded open-position page."""

        ...

    async def get(self, follower_account: str, position_id: str) -> PositionView:
        """Return one open position."""

        ...


class OrderQueryPort(Protocol):
    """Read follower-scoped sanitized order projections."""

    async def list(self, follower_account: str, page: PageRequest) -> Page[OrderView]:
        """Return one bounded order page."""

        ...

    async def get(self, follower_account: str, order_id: str) -> OrderView:
        """Return one durable order projection."""

        ...


class ActivityQueryPort(Protocol):
    """Read structured activity and operator audit records."""

    async def list(
        self,
        follower_account: str | None,
        page: PageRequest,
    ) -> Page[ActivityView]:
        """Return one bounded activity page."""

        ...


class DiscoveryQueryPort(Protocol):
    """Read persisted sanitized discovery state."""

    async def list(self, page: PageRequest) -> Page[DiscoveryCycleView]:
        """Return one bounded discovery-cycle page."""

        ...

    async def get_status(self) -> DiscoveryStatusView:
        """Return the current discovery scheduler projection."""

        ...

    async def list_candidates(self, page: PageRequest) -> Page[DiscoveryCandidateView]:
        """Return one bounded evaluated-candidate page."""

        ...

    async def list_roster(self, page: PageRequest) -> Page[DiscoveryRosterView]:
        """Return one bounded active-roster page."""

        ...


class DiscoveryCommandPort(Protocol):
    """Request a bounded asynchronous discovery cycle."""

    async def request(self, command: DiscoveryRunCommand) -> DiscoveryRunReceipt:
        """Return immediate queue admission evidence."""

        ...


class ReentryLockQueryPort(Protocol):
    """Read follower-scoped reentry locks."""

    async def list(
        self,
        follower_account: str,
        page: PageRequest,
    ) -> Page[ReentryLockView]:
        """Return one bounded lock page."""

        ...


class ReentryLockCommandPort(Protocol):
    """Reset a reentry lock through its owning domain service."""

    async def reset(self, command: LockResetCommand) -> LockResetResult:
        """Return optimistic-concurrency reset evidence."""

        ...


class ReservationQueryPort(Protocol):
    """Read active and historical collateral reservations."""

    async def list(
        self,
        follower_account: str,
        page: PageRequest,
    ) -> Page[ReservationView]:
        """Return one bounded reservation page."""

        ...


class SettingsQueryPort(Protocol):
    """Read safe service-owned settings documents."""

    async def list(self) -> tuple[SettingsDocument, ...]:
        """Return all safe settings sections."""

        ...

    async def get(self, path: SettingsPath) -> SettingsDocument:
        """Return one settings section and its revision."""

        ...


class SettingsCommandPort(Protocol):
    """Persist one validated service-owned settings override."""

    async def write(self, command: SettingsWriteCommand) -> SettingsWriteResult:
        """Return atomic write and apply-semantics evidence."""

        ...


class PortfolioQueryPort(Protocol):
    """Read local cached follower portfolio state."""

    async def get(self, follower_account: str) -> PortfolioView:
        """Return one current exact-decimal portfolio snapshot."""

        ...


class RuntimeQueryPort(Protocol):
    """Read bounded worker and canonical safety state."""

    async def get(self) -> RuntimeView:
        """Return the current runtime projection."""

        ...


class RuntimeCommandPort(Protocol):
    """Change canonical runtime safety state."""

    async def pause_entries(self, command: RuntimeControlCommand) -> EntryControlState:
        """Pause new entries while preserving protective exits."""

        ...

    async def resume_entries(self, command: RuntimeControlCommand) -> EntryControlState:
        """Resume entries through the existing live gate."""

        ...

    async def full_stop(self, command: RuntimeControlCommand) -> EntryControlState:
        """Halt entries and protective exits and cancel resting orders."""

        ...


class AccountQueryPort(Protocol):
    """Read the follower accounts this process composed."""

    async def list(self) -> tuple[AccountView, ...]:
        """Return every composed follower account."""

        ...


class DetectionQueryPort(Protocol):
    """Read local detection-source health."""

    async def get(self) -> DetectionStatusView:
        """Return the current detection status projection."""

        ...


class FillQueryPort(Protocol):
    """Read follower-scoped durable fills."""

    async def list(self, follower_account: str, page: PageRequest) -> Page[FillView]:
        """Return one bounded newest-first fill page."""

        ...


class PnlQueryPort(Protocol):
    """Read session realized PnL and cache-only position valuation."""

    async def get(self, follower_account: str) -> PnlView:
        """Return one follower PnL projection."""

        ...


class RiskQueryPort(Protocol):
    """Read configured risk policy, durable risk state, and headroom."""

    async def get(self, follower_account: str) -> RiskStateView:
        """Return one follower risk projection."""

        ...


class LeaderQueryPort(Protocol):
    """Read the merged leader listing."""

    async def list(self) -> tuple[LeaderView, ...]:
        """Return one bounded merged leader listing."""

        ...


class MetricsQueryPort(Protocol):
    """Read the metrics half of the telemetry projection."""

    def snapshot(self) -> Mapping[str, object]:
        """Return counters, gauges, and histogram summaries."""

        ...


class TelemetryQueryPort(Protocol):
    """Read GUI-safe in-memory telemetry snapshots."""

    def snapshot(self, *, limit: int = 100) -> Mapping[str, object]:
        """Return a bounded JSON-compatible snapshot."""

        ...

    def metrics_snapshot(self) -> Mapping[str, object]:
        """Return the same metrics without the bounded event history."""

        ...


class TelemetryHubPort(Protocol):
    """Manage nonblocking bounded frontend telemetry subscribers."""

    def subscribe(self, *, capacity: int | None = None) -> TelemetrySubscription:
        """Return one bounded subscriber."""

        ...

    def unsubscribe(self, subscription: TelemetrySubscription) -> None:
        """Remove one subscriber idempotently."""

        ...


@dataclass(frozen=True, slots=True)
class ControlPlaneDependencies:
    """Optional application services; service presence defines route support.

    Command routes are registered individually only when their injected object
    exposes the corresponding method.  Unsupported commands therefore remain
    honest HTTP 404s instead of unsafe wrappers or placeholder 501 handlers.
    """

    system: SystemQueryPort | None = None
    accounts: AccountQueryPort | None = None
    detection: DetectionQueryPort | None = None
    fills: FillQueryPort | None = None
    pnl: PnlQueryPort | None = None
    risk: RiskQueryPort | None = None
    leaders: LeaderQueryPort | None = None
    metrics: MetricsQueryPort | None = None
    tasks: TaskQueryPort | None = None
    task_commands: TaskCommandPort | None = None
    task_bulk_commands: TaskBulkCommandPort | None = None
    task_decoder: TaskPayloadDecoder | None = None
    positions: PositionQueryPort | None = None
    orders: OrderQueryPort | None = None
    activity: ActivityQueryPort | None = None
    discovery: DiscoveryQueryPort | None = None
    discovery_commands: DiscoveryCommandPort | None = None
    locks: ReentryLockQueryPort | None = None
    lock_commands: ReentryLockCommandPort | None = None
    reservations: ReservationQueryPort | None = None
    settings: SettingsQueryPort | None = None
    settings_commands: SettingsCommandPort | None = None
    portfolio: PortfolioQueryPort | None = None
    runtime: RuntimeQueryPort | None = None
    runtime_commands: RuntimeCommandPort | None = None
    telemetry_query: TelemetryQueryPort | None = None
    telemetry_hub: TelemetryHubPort | None = None
    settings_environment: str | None = None

    def __post_init__(self) -> None:
        """Validate the optional current-environment route binding."""

        if self.settings_environment is not None:
            SettingsPath(environment=self.settings_environment, section="app")


__all__ = [
    "AccountQueryPort",
    "ActivityQueryPort",
    "ControlPlaneDependencies",
    "CorrelationIdFactory",
    "DiscoveryCommandPort",
    "DetectionQueryPort",
    "DiscoveryQueryPort",
    "FillQueryPort",
    "LeaderQueryPort",
    "MetricsQueryPort",
    "OrderQueryPort",
    "PnlQueryPort",
    "PortfolioQueryPort",
    "PositionQueryPort",
    "ReentryLockCommandPort",
    "ReentryLockQueryPort",
    "ReservationQueryPort",
    "RiskQueryPort",
    "RuntimeCommandPort",
    "RuntimeQueryPort",
    "SecureCorrelationIdFactory",
    "SettingsCommandPort",
    "SettingsQueryPort",
    "SystemQueryPort",
    "TaskBulkCommandPort",
    "TaskCommandPort",
    "TaskPayloadDecoder",
    "TaskQueryPort",
    "TelemetryHubPort",
    "TelemetryQueryPort",
]
