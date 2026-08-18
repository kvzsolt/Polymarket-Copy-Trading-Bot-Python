"""Composition inputs for the optional extended control-plane reads.

The composition root owns which local runtime state an operator may observe.
This module carries that decision as one immutable bundle plus the two small
adapters the bundle needs: a read-only day-start-equity probe and a bounded
pending-confirmation stats probe. Nothing here performs venue or network I/O,
and every field is optional so an unwired process simply publishes fewer
routes instead of inventing data.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Protocol

from services.control_plane.contracts import AccountView, PendingConfirmationStatsView
from services.control_plane.queries_monitoring import (
    DetectionCountersSource,
    LeaderRosterPort,
    MetricsSource,
    ReadinessSource,
)
from services.control_plane.queries_trading import (
    CachedPortfolioPort,
    MarketQuoteSource,
    OpenPositionLike,
    OpenPositionsPort,
    PnlTrackerPort,
)
from utils.dayEquity import peek_day_start_equity


class PendingConfirmationTrackerLike(Protocol):
    """Read bounded state from the provisional-copy confirmation tracker."""

    @property
    def tracked_count(self) -> int:
        """Return how many provisional copies await a verdict."""

        ...

    @property
    def running(self) -> bool:
        """Return whether the confirmation loop is active."""

        ...

    @property
    def deadline_seconds(self) -> float:
        """Return the undecided-copy reporting deadline."""

        ...

    @property
    def max_tracked(self) -> int:
        """Return the ceiling on concurrently tracked copies."""

        ...


class PendingConfirmationStatsProbe:
    """Project one pending-copy tracker into an immutable stats view."""

    def __init__(self, tracker: PendingConfirmationTrackerLike | None = None) -> None:
        """Store the optional tracker.

        Args:
            tracker: Started confirmation tracker, or ``None`` when the
                selected detection mode composes none.
        """

        self._tracker = tracker

    def attach(self, tracker: PendingConfirmationTrackerLike | None) -> None:
        """Bind the tracker that composition creates after this probe.

        The control plane starts before the detection runtime, so the tracker
        does not exist yet when the read bundle is built. Binding it later
        keeps composition order unchanged and keeps the probe honest until
        then: an unattached probe reports ``None``, not zero tracked copies.

        Args:
            tracker: Started confirmation tracker, or ``None`` to detach.
        """

        self._tracker = tracker

    def __call__(self) -> PendingConfirmationStatsView | None:
        """Return current tracker stats, or ``None`` without a tracker."""

        tracker = self._tracker
        if tracker is None:
            return None
        return PendingConfirmationStatsView(
            tracked=int(tracker.tracked_count),
            running=bool(tracker.running),
            deadline_seconds=float(tracker.deadline_seconds),
            max_tracked=int(tracker.max_tracked),
        )


class FollowerScopedPositions:
    """Route one open-position read to that follower's own repository."""

    def __init__(self, repositories: Mapping[str, OpenPositionsPort]) -> None:
        """Store one durable position reader per follower account.

        Args:
            repositories: Position reader keyed by follower account.
        """

        self._repositories = dict(repositories)

    async def list_open_positions(self, user_id: str) -> Sequence[OpenPositionLike]:
        """Return the follower's open positions, or nothing when unknown."""

        repository = self._repositories.get(user_id)
        if repository is None:
            return ()
        return await repository.list_open_positions(user_id)


class DayStartEquityProbe:
    """Read today's recorded day-start equity without recording one."""

    def __call__(self) -> Decimal | None:
        """Return the recorded day-start equity, or ``None``."""

        try:
            return peek_day_start_equity()
        except Exception:
            return None


@dataclass(frozen=True, slots=True)
class ControlPlaneRuntimeReads:
    """Local runtime state the composition root exposes to read routes.

    Attributes:
        accounts: One projection per composed follower account.
        detection_counters: Bounded local detection counter source.
        pending_stats: Optional provisional-copy tracker probe.
        pnl_trackers: Session realized ledger per follower account.
        positions: Durable open-position reader shared by followers.
        market_stores: Cached WSS quote source per follower account.
        monitored_leaders: Late-bound reader of monitored leader wallets.
        configured_leaders: Operator-configured leader wallets.
        leaders_repo: Optional durable discovery roster reader.
        portfolio: Optional cached portfolio snapshot reader.
        readiness: Canonical process readiness truth.
        metrics: Process metrics registry.
        day_start_equity: Read-only day-start equity probe.
    """

    accounts: tuple[AccountView, ...] = ()
    detection_counters: DetectionCountersSource | None = None
    pending_stats: Callable[[], PendingConfirmationStatsView | None] | None = None
    pnl_trackers: Mapping[str, PnlTrackerPort] = field(default_factory=dict)
    positions: OpenPositionsPort | None = None
    market_stores: Mapping[str, MarketQuoteSource] = field(default_factory=dict)
    monitored_leaders: Callable[[], Iterable[str]] | None = None
    configured_leaders: tuple[str, ...] = ()
    leaders_repo: LeaderRosterPort | None = None
    portfolio: CachedPortfolioPort | None = None
    readiness: ReadinessSource | None = None
    metrics: MetricsSource | None = None
    day_start_equity: Callable[[], Decimal | None] = field(default_factory=DayStartEquityProbe)

    @property
    def detection_supported(self) -> bool:
        """Whether detection health can be answered from local state."""

        return (
            self.detection_counters is not None
            and self.readiness is not None
            and self.metrics is not None
        )

    @property
    def pnl_supported(self) -> bool:
        """Whether at least one follower has a composed session ledger."""

        return bool(self.pnl_trackers) and self.positions is not None

    @property
    def leaders_supported(self) -> bool:
        """Whether leader identities can be merged from local counters."""

        return self.detection_counters is not None


__all__ = [
    "ControlPlaneRuntimeReads",
    "DayStartEquityProbe",
    "FollowerScopedPositions",
    "PendingConfirmationStatsProbe",
    "PendingConfirmationTrackerLike",
]
