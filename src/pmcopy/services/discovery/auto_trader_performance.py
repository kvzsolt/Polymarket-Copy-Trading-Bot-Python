"""Bounded recent performance using the canonical closed-episode accounting rules."""

from decimal import Decimal
from datetime import datetime
from typing import Protocol

from pmcopy.domain.auto_trader import AutoTraderScope, CopyPerformance

from pmcopy.services.trading.leader_performance import (
    ClosedEpisodeAccumulator,
    ClosedEpisodeStats,
    PerformanceFill,
)


class RecentEpisodeAccumulator:
    """Extend episode accounting with a bounded recent window and closure watermark."""

    def __init__(self) -> None:
        """Start with no samples and no fabricated profit evidence."""
        self._accumulator = ClosedEpisodeAccumulator()
        self._recent: tuple[Decimal, ...] = ()
        self._watermark = "initial"

    def apply(self, fill: PerformanceFill) -> None:
        """Advance the watermark only for a completed attributed position episode."""
        before = self._accumulator.stats
        self._accumulator.apply(fill)
        after = self._accumulator.stats
        if after.closed_episodes > before.closed_episodes:
            self._recent = (*self._recent, after.realized_pnl - before.realized_pnl)[-20:]
            self._watermark = fill.fill_id

    @property
    def stats(self) -> ClosedEpisodeStats:
        """Expose the canonical source accounting totals."""
        return self._accumulator.stats

    @property
    def recent_pnls(self) -> tuple[Decimal, ...]:
        """Return at most twenty completed episode results."""
        return self._recent

    @property
    def closed_watermark(self) -> str:
        """Return the fill identity which most recently closed an episode."""
        return self._watermark


class LiveCopyPerformanceReader(Protocol):
    """Source-isolated copy performance read port."""

    async def read(self, scope: AutoTraderScope, wallet: str, now: datetime) -> CopyPerformance:
        """Read bounded closed-episode evidence for a live follower."""
        ...
