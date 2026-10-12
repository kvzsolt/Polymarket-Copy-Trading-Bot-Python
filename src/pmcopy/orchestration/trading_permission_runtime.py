"""Refresh durable trading controls outside the order submission hot path."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Mapping
from typing import Protocol

from pmcopy.services.trading_permission import TradingPausedError


class TradingPermissionRefreshPort(Protocol):
    """Refresh one process-local trading-control cache."""

    async def refresh(self) -> object | None:
        """Load and cache current durable control state."""

        ...


class PermissionRefreshSleeper(Protocol):
    """Wait between off-path refresh passes."""

    def sleep(self, delay_seconds: float) -> Awaitable[None]:
        """Return an awaitable that completes after the requested delay."""

        ...


class AsyncioPermissionRefreshSleeper:
    """Production refresh sleeper backed by the asyncio event loop."""

    def sleep(self, delay_seconds: float) -> Awaitable[None]:
        """Return an asyncio sleep awaitable."""

        return asyncio.sleep(delay_seconds)


class TradingPermissionRuntime:
    """Initialize and periodically refresh cached trading-control state."""

    def __init__(
        self,
        *,
        permissions: Mapping[str, TradingPermissionRefreshPort],
        sleeper: PermissionRefreshSleeper | None = None,
    ) -> None:
        """Initialize the runtime from per-user permission owners.

        Args:
            permissions: Stable runtime-user to permission-cache mapping.
            sleeper: Injectable periodic wait adapter.
        """

        self._permissions = dict(permissions)
        self._sleeper = sleeper or AsyncioPermissionRefreshSleeper()

    async def initialize(self, *, blocked_user_ids: set[str]) -> tuple[str, ...]:
        """Load controls only after each user's startup reconciliation.

        Args:
            blocked_user_ids: Users whose exposure reconciliation failed.

        Returns:
            Sorted user identifiers whose control state could not be loaded.
        """

        failures: list[str] = []
        for user_id, permission in self._permissions.items():
            if user_id in blocked_user_ids:
                continue
            try:
                await permission.refresh()
            except TradingPausedError:
                failures.append(user_id)
        return tuple(sorted(failures))

    async def refresh_once(self) -> None:
        """Refresh every cache and fail closed if any refresh fails.

        Raises:
            TradingPausedError: If one or more live controls are unavailable.
        """

        failures = 0
        for permission in self._permissions.values():
            try:
                await permission.refresh()
            except TradingPausedError:
                failures += 1
        if failures:
            raise TradingPausedError("one or more trading controls are unavailable")

    async def run(self, *, interval_seconds: float) -> None:
        """Refresh controls continuously outside the order path.

        Args:
            interval_seconds: Positive configured refresh interval.

        Raises:
            ValueError: If the interval is not positive.
            TradingPausedError: If a refresh pass fails.
        """

        if interval_seconds <= 0:
            raise ValueError("trading-control refresh interval must be positive")
        while True:
            await self.refresh_once()
            await self._sleeper.sleep(interval_seconds)


__all__ = [
    "AsyncioPermissionRefreshSleeper",
    "PermissionRefreshSleeper",
    "TradingPermissionRefreshPort",
    "TradingPermissionRuntime",
]
