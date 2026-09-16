"""Compose automatic membership with the existing discovery read contracts."""

from integrations.control_plane.contracts import AutoTraderControlPort, DiscoveryQueryPort
from services.control_plane.contracts import (
    DiscoveryCandidateView,
    DiscoveryCycleView,
    DiscoveryRosterView,
    DiscoveryStatusView,
    Page,
    PageRequest,
)


class AutomaticDiscoveryView:
    """Preserve cycle diagnostics while projecting the authoritative automatic roster."""

    def __init__(self, *, base: DiscoveryQueryPort, automatic: AutoTraderControlPort) -> None:
        """Inject existing read services; no provider calls occur in this adapter."""
        self._base = base
        self._automatic = automatic

    async def list(self, page: PageRequest) -> Page[DiscoveryCycleView]:
        """Read saved evaluation cycles."""
        return await self._base.list(page)

    async def get_status(self) -> DiscoveryStatusView:
        """Read saved provider diagnostics."""
        return await self._base.get_status()

    async def list_candidates(self, page: PageRequest) -> Page[DiscoveryCandidateView]:
        """Read the latest saved provider evidence."""
        return await self._base.list_candidates(page)

    async def list_roster(self, page: PageRequest) -> Page[DiscoveryRosterView]:
        """Read authoritative automatic membership instead of the legacy roster."""
        return await self._automatic.list_discovery_roster(page)


class AutomaticLeaderRosterView:
    """Adapt the paged automatic roster to the existing leader-list interface."""

    def __init__(self, automatic: AutoTraderControlPort) -> None:
        """Inject the authoritative scoped membership projection."""
        self._automatic = automatic

    async def list_leader_roster(self, limit: int) -> tuple[DiscoveryRosterView, ...]:
        """Read at most the requested number of persisted rows."""
        rows: list[DiscoveryRosterView] = []
        cursor = None
        while len(rows) < limit:
            page = await self._automatic.list_discovery_roster(
                PageRequest(limit=min(100, limit - len(rows)), cursor=cursor)
            )
            rows.extend(page.items)
            if not page.next_cursor or not page.items or page.next_cursor == cursor:
                break
            cursor = page.next_cursor
        return tuple(rows)
