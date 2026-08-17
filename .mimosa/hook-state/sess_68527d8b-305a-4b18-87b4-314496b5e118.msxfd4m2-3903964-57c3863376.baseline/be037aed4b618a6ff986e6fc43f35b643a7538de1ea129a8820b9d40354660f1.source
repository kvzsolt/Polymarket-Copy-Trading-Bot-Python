"""Read-only application services over narrow control-plane query ports."""

from __future__ import annotations

from typing import Protocol

from services.control_plane.contracts import (
    ActivityView,
    CapabilitiesView,
    ControlPlaneError,
    ControlPlaneReason,
    DiscoveryCandidateView,
    DiscoveryCycleView,
    DiscoveryRosterView,
    DiscoveryStatusView,
    OrderView,
    Page,
    PageRequest,
    PortfolioView,
    PositionView,
    ReadinessView,
    ReentryLockView,
    ReservationView,
    RuntimeView,
    SystemStatusView,
    SystemView,
    TaskDetailView,
    TaskView,
)


class SystemQueryPort(Protocol):
    """Read seam for process identity and environment state."""

    async def get_system(self) -> SystemView:
        """Return the current system projection."""

        ...

    async def get_readiness(self) -> ReadinessView:
        """Return authoritative readiness evidence."""

        ...

    async def get_capabilities(self) -> CapabilitiesView:
        """Return explicitly composed capabilities."""

        ...


class TaskQueryPort(Protocol):
    """Read seam for paginated tasks."""

    async def list_tasks(self, follower_account: str, page: PageRequest) -> Page[TaskView]:
        """Return tasks in stable order."""

        ...

    async def get_task(self, task_id: str) -> TaskDetailView | None:
        """Return one task detail including soft-deleted state."""

        ...


class PositionQueryPort(Protocol):
    """Read seam for paginated open positions."""

    async def list_positions(self, follower_account: str, page: PageRequest) -> Page[PositionView]:
        """Return open positions in stable order."""

        ...

    async def get_position(
        self,
        follower_account: str,
        position_id: str,
    ) -> PositionView | None:
        """Return one follower-scoped position by outcome token identity."""

        ...


class OrderQueryPort(Protocol):
    """Read seam for sanitized order state."""

    async def list_orders(self, follower_account: str, page: PageRequest) -> Page[OrderView]:
        """Return orders without raw venue payloads."""

        ...

    async def get_order(
        self,
        follower_account: str,
        order_id: str,
    ) -> OrderView | None:
        """Return one follower-scoped order by durable idempotency identity."""

        ...


class ActivityQueryPort(Protocol):
    """Read seam for append-only control-plane activity."""

    async def list_activity(
        self, follower_account: str | None, page: PageRequest
    ) -> Page[ActivityView]:
        """Return activity in reverse chronological order."""

        ...


class DiscoveryQueryPort(Protocol):
    """Read seam for discovery cycle state."""

    async def list_discovery_cycles(self, page: PageRequest) -> Page[DiscoveryCycleView]:
        """Return provider-safe discovery projections."""

        ...

    async def get_discovery_status(self) -> DiscoveryStatusView:
        """Return current scheduler state."""

        ...

    async def list_discovery_candidates(self, page: PageRequest) -> Page[DiscoveryCandidateView]:
        """Return candidate projections."""

        ...

    async def list_discovery_roster(self, page: PageRequest) -> Page[DiscoveryRosterView]:
        """Return roster projections."""

        ...


class ReservationQueryPort(Protocol):
    """Read seam for task-budget reservations."""

    async def list_reservations(
        self, follower_account: str, page: PageRequest
    ) -> Page[ReservationView]:
        """Return reservations in stable order."""

        ...


class ReentryLockQueryPort(Protocol):
    """Read seam for protective re-entry locks."""

    async def list_reentry_locks(
        self, follower_account: str, page: PageRequest
    ) -> Page[ReentryLockView]:
        """Return lock projections with CAS revisions."""

        ...


class CachedPortfolioQueryPort(Protocol):
    """Local cache seam that must not perform venue or database I/O."""

    async def get_cached_portfolio(self, follower_account: str) -> PortfolioView | None:
        """Return one cached portfolio snapshot."""

        ...


class RuntimeQueryPort(Protocol):
    """Read seam for bounded local runtime state."""

    async def get_runtime(self) -> RuntimeView:
        """Return current runtime state."""

        ...


class SystemQueryService:
    """Query system identity through one narrow collaborator."""

    def __init__(self, port: SystemQueryPort) -> None:
        self._port = port

    async def get(self) -> SystemView:
        """Return the current system snapshot."""

        return await self._port.get_system()

    async def get_status(self) -> SystemStatusView:
        """Pair process identity with authoritative readiness state."""

        system = await self._port.get_system()
        readiness = await self._port.get_readiness()
        return SystemStatusView(system=system, readiness=readiness)

    async def get_capabilities(self) -> CapabilitiesView:
        """Return only capabilities explicitly supplied by composition."""

        return await self._port.get_capabilities()


class TaskQueryService:
    """Query versioned task summaries."""

    def __init__(self, port: TaskQueryPort) -> None:
        self._port = port

    async def list(self, follower_account: str, page: PageRequest) -> Page[TaskView]:
        """Return one bounded task page."""

        return await self._port.list_tasks(follower_account, page)

    async def get(self, task_id: str) -> TaskDetailView:
        """Return one task detail or a stable not-found error."""

        detail = await self._port.get_task(task_id)
        if detail is None:
            raise ControlPlaneError(ControlPlaneReason.NOT_FOUND)
        return detail


class PositionQueryService:
    """Query exact open-position projections."""

    def __init__(self, port: PositionQueryPort) -> None:
        self._port = port

    async def list(self, follower_account: str, page: PageRequest) -> Page[PositionView]:
        """Return one bounded position page."""

        return await self._port.list_positions(follower_account, page)

    async def get(self, follower_account: str, position_id: str) -> PositionView:
        """Return one scoped position or a stable not-found error."""

        position = await self._port.get_position(follower_account, position_id)
        if position is None:
            raise ControlPlaneError(ControlPlaneReason.NOT_FOUND)
        return position


class OrderQueryService:
    """Query sanitized order projections."""

    def __init__(self, port: OrderQueryPort) -> None:
        self._port = port

    async def list(self, follower_account: str, page: PageRequest) -> Page[OrderView]:
        """Return one bounded order page."""

        return await self._port.list_orders(follower_account, page)

    async def get(self, follower_account: str, order_id: str) -> OrderView:
        """Return one scoped order or a stable not-found error."""

        order = await self._port.get_order(follower_account, order_id)
        if order is None:
            raise ControlPlaneError(ControlPlaneReason.NOT_FOUND)
        return order


class ActivityQueryService:
    """Query append-only operator activity."""

    def __init__(self, port: ActivityQueryPort) -> None:
        self._port = port

    async def list(self, follower_account: str | None, page: PageRequest) -> Page[ActivityView]:
        """Return one bounded activity page."""

        return await self._port.list_activity(follower_account, page)


class DiscoveryQueryService:
    """Query provider-safe discovery cycle projections."""

    def __init__(self, port: DiscoveryQueryPort) -> None:
        self._port = port

    async def list(self, page: PageRequest) -> Page[DiscoveryCycleView]:
        """Return one bounded discovery page."""

        return await self._port.list_discovery_cycles(page)

    async def get_status(self) -> DiscoveryStatusView:
        """Return current discovery scheduler state."""

        return await self._port.get_discovery_status()

    async def list_candidates(self, page: PageRequest) -> Page[DiscoveryCandidateView]:
        """Return one bounded candidate page."""

        return await self._port.list_discovery_candidates(page)

    async def list_roster(self, page: PageRequest) -> Page[DiscoveryRosterView]:
        """Return one bounded roster page."""

        return await self._port.list_discovery_roster(page)


class ReservationQueryService:
    """Query exact task-budget reservation state."""

    def __init__(self, port: ReservationQueryPort) -> None:
        self._port = port

    async def list(self, follower_account: str, page: PageRequest) -> Page[ReservationView]:
        """Return one bounded reservation page."""

        return await self._port.list_reservations(follower_account, page)


class ReentryLockQueryService:
    """Query versioned protective lock state."""

    def __init__(self, port: ReentryLockQueryPort) -> None:
        self._port = port

    async def list(self, follower_account: str, page: PageRequest) -> Page[ReentryLockView]:
        """Return one bounded lock page."""

        return await self._port.list_reentry_locks(follower_account, page)


class PortfolioQueryService:
    """Serve cached portfolio truth without triggering external I/O."""

    def __init__(self, port: CachedPortfolioQueryPort) -> None:
        self._port = port

    async def get(self, follower_account: str) -> PortfolioView:
        """Return a present, non-stale cached portfolio snapshot."""

        snapshot = await self._port.get_cached_portfolio(follower_account)
        if snapshot is None:
            raise ControlPlaneError(ControlPlaneReason.PORTFOLIO_UNAVAILABLE)
        if snapshot.stale:
            raise ControlPlaneError(ControlPlaneReason.PORTFOLIO_STALE)
        return snapshot


class RuntimeQueryService:
    """Query bounded local runtime state."""

    def __init__(self, port: RuntimeQueryPort) -> None:
        self._port = port

    async def get(self) -> RuntimeView:
        """Return the current runtime snapshot."""

        return await self._port.get_runtime()


__all__ = [
    "ActivityQueryPort",
    "ActivityQueryService",
    "CachedPortfolioQueryPort",
    "DiscoveryQueryPort",
    "DiscoveryQueryService",
    "OrderQueryPort",
    "OrderQueryService",
    "PortfolioQueryService",
    "PositionQueryPort",
    "PositionQueryService",
    "ReentryLockQueryPort",
    "ReentryLockQueryService",
    "ReservationQueryPort",
    "ReservationQueryService",
    "RuntimeQueryPort",
    "RuntimeQueryService",
    "SystemQueryPort",
    "SystemQueryService",
    "TaskQueryPort",
    "TaskQueryService",
]
