"""Maintain bounded exact-decimal portfolio snapshots for GUI queries.

Background refresh owns I/O. Query methods only read the in-memory cache, so
browser polling cannot add venue latency or acquire execution locks.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Protocol

from services.control_plane.contracts import PortfolioView

logger = logging.getLogger(__name__)


class Clock(Protocol):
    """Provide aware wall-clock time for snapshot age."""

    def now(self) -> datetime:
        """Return the current aware instant."""

        ...


class Sleeper(Protocol):
    """Suspend background refresh through an injected timing boundary."""

    async def sleep(self, seconds: float) -> None:
        """Suspend for the requested non-negative interval."""

        ...


class PortfolioSnapshotSource(Protocol):
    """Read one follower snapshot away from the GUI request path."""

    async def read(self) -> PortfolioAccountSnapshot:
        """Return one authoritative account snapshot."""

        ...


class AsyncioSleeper:
    """Production event-loop sleeper."""

    async def sleep(self, seconds: float) -> None:
        """Suspend the current task without blocking the event loop."""

        await asyncio.sleep(seconds)


@dataclass(frozen=True, slots=True)
class PortfolioAccountSnapshot:
    """Exact account inputs with active cost and total inventory valuation.

    Settled inventory contributes to equity but is excluded from active
    unrealized PnL. Missing redemption value keeps total equity unavailable.
    """

    follower_account: str
    aliases: tuple[str, ...]
    cash_usdc: Decimal
    reserved_usdc: Decimal
    positions_cost_usdc: Decimal | None
    position_count: int
    marked_inventory_usdc: Decimal | None = None
    settled_inventory_usdc: Decimal | None = Decimal(0)

    def __post_init__(self) -> None:
        """Validate identities and non-negative exact amounts."""

        if not str(self.follower_account).strip():
            raise ValueError("follower account is required")
        if not isinstance(self.aliases, tuple):
            raise TypeError("portfolio aliases must be a tuple")
        for value in (
            self.cash_usdc,
            self.reserved_usdc,
        ):
            if not isinstance(value, Decimal) or not value.is_finite() or value < 0:
                raise ValueError("portfolio amounts must be non-negative finite decimals")
        for valuation in (
            self.positions_cost_usdc,
            self.marked_inventory_usdc,
            self.settled_inventory_usdc,
        ):
            if valuation is not None and (
                not isinstance(valuation, Decimal) or not valuation.is_finite() or valuation < 0
            ):
                raise ValueError("Portfolio valuations must be finite non-negative decimals")
        if isinstance(self.position_count, bool) or self.position_count < 0:
            raise ValueError("position count must be a non-negative integer")


@dataclass(frozen=True, slots=True)
class PortfolioRefreshResult:
    """Low-cardinality result of one bounded background refresh."""

    updated_count: int
    failed_count: int


class PortfolioSnapshotCache:
    """Store current portfolio snapshots under canonical and alias identities."""

    def __init__(self, *, clock: Clock, stale_after: timedelta) -> None:
        """Store deterministic age policy and initialize an empty cache.

        Args:
            clock: Aware wall-clock source.
            stale_after: Positive maximum age for a usable snapshot.

        Raises:
            ValueError: If the stale interval is not positive.
        """

        if stale_after <= timedelta(0):
            raise ValueError("portfolio stale interval must be positive")
        self._clock = clock
        self._stale_after = stale_after
        self._snapshots: dict[str, tuple[PortfolioAccountSnapshot, datetime]] = {}

    def update(self, snapshot: PortfolioAccountSnapshot) -> None:
        """Atomically publish one validated account snapshot under its aliases."""

        observed_at = self._clock.now()
        if observed_at.tzinfo is None or observed_at.utcoffset() is None:
            raise ValueError("portfolio clock must return aware time")
        record = (snapshot, observed_at)
        identities = (snapshot.follower_account, *snapshot.aliases)
        for identity in identities:
            normalized = str(identity).strip().lower()
            if normalized:
                self._snapshots[normalized] = record

    async def get_cached_portfolio(self, follower_account: str) -> PortfolioView | None:
        """Return one local snapshot without performing I/O."""

        record = self._snapshots.get(str(follower_account).strip().lower())
        if record is None:
            return None
        snapshot, observed_at = record
        stale = self._clock.now() - observed_at > self._stale_after
        copyable = max(Decimal("0"), snapshot.cash_usdc - snapshot.reserved_usdc)
        marked_value = snapshot.marked_inventory_usdc
        equity = None if marked_value is None else snapshot.cash_usdc + marked_value
        unrealized = (
            None
            if marked_value is None
            or snapshot.positions_cost_usdc is None
            or snapshot.settled_inventory_usdc is None
            else marked_value - snapshot.settled_inventory_usdc - snapshot.positions_cost_usdc
        )
        return PortfolioView(
            follower_account=snapshot.follower_account,
            cash_usdc=format(snapshot.cash_usdc, "f"),
            reserved_usdc=format(snapshot.reserved_usdc, "f"),
            positions_value_usdc=None if marked_value is None else format(marked_value, "f"),
            equity_usdc=None if equity is None else format(equity, "f"),
            positions_cost_usdc=(
                None
                if snapshot.positions_cost_usdc is None
                else format(snapshot.positions_cost_usdc, "f")
            ),
            observed_at=observed_at,
            stale=stale,
            copyable_balance_usdc=format(copyable, "f"),
            position_count=snapshot.position_count,
            valuation_basis="unavailable" if marked_value is None else "market_mark",
            unrealized_pnl_usdc=None if unrealized is None else format(unrealized, "f"),
        )


class PortfolioRefreshService:
    """Refresh at most 64 follower snapshots on one background task."""

    _MAX_SOURCES = 64

    def __init__(
        self,
        *,
        sources: tuple[PortfolioSnapshotSource, ...],
        cache: PortfolioSnapshotCache,
        refresh_interval_seconds: float,
        sleeper: Sleeper | None = None,
    ) -> None:
        """Store bounded sources and lifecycle dependencies.

        Args:
            sources: One source per follower account.
            cache: Shared local query cache.
            refresh_interval_seconds: Positive cadence between refreshes.
            sleeper: Optional deterministic sleep seam.
        """

        if not isinstance(sources, tuple):
            raise TypeError("portfolio sources must be a tuple")
        if len(sources) > self._MAX_SOURCES:
            raise ValueError("portfolio refresh supports at most 64 followers")
        if refresh_interval_seconds <= 0:
            raise ValueError("portfolio refresh interval must be positive")
        self._sources = sources
        self._cache = cache
        self._refresh_interval = float(refresh_interval_seconds)
        self._sleeper = sleeper or AsyncioSleeper()
        self._task: asyncio.Task[None] | None = None
        self._started = False
        self._closed = False

    async def start(self) -> None:
        """Start the sole periodic refresh task exactly once."""

        if self._closed:
            raise RuntimeError("closed portfolio refresh cannot be restarted")
        if self._started:
            return
        self._started = True
        self._task = asyncio.create_task(
            self._run(),
            name="control-plane-portfolio-refresh",
        )

    async def refresh_once(self) -> PortfolioRefreshResult:
        """Refresh every follower concurrently and contain per-source failure."""

        results = await asyncio.gather(
            *(source.read() for source in self._sources),
            return_exceptions=True,
        )
        updated = 0
        failed = 0
        for result in results:
            if isinstance(result, BaseException):
                failed += 1
                logger.warning(
                    "portfolio_refresh_source_failed",
                    extra={"error_type": type(result).__name__},
                )
                continue
            try:
                self._cache.update(result)
            except Exception as exc:
                failed += 1
                logger.warning(
                    "portfolio_refresh_snapshot_rejected",
                    extra={"error_type": type(exc).__name__},
                )
            else:
                updated += 1
        return PortfolioRefreshResult(updated_count=updated, failed_count=failed)

    async def close(self) -> None:
        """Cancel periodic refresh idempotently."""

        if self._closed:
            return
        self._closed = True
        task = self._task
        self._task = None
        self._started = False
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def _run(self) -> None:
        """Refresh forever at the injected non-blocking cadence."""

        while True:
            await self.refresh_once()
            await self._sleeper.sleep(self._refresh_interval)


__all__ = [
    "AsyncioSleeper",
    "Clock",
    "PortfolioAccountSnapshot",
    "PortfolioRefreshResult",
    "PortfolioRefreshService",
    "PortfolioSnapshotCache",
    "PortfolioSnapshotSource",
    "Sleeper",
]
