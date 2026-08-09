"""Durable post-protective-exit market re-entry lock domain.

A stop or target trigger alone is not sufficient. The lock is created only
when authoritative fill evidence proves that the protected allocation is fully
closed. This prevents repeated BUY->protective SELL churn while retaining all
risk-reducing and reconciliation paths.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Protocol


class ProtectiveExitReason(StrEnum):
    """Exit reasons that may create a market re-entry lock."""

    STOP_LOSS = "stop_loss"
    TAKE_PROFIT = "take_profit"
    TRAILING_STOP = "trailing_stop"
    LAYERED_STOP_LOSS = "layered_stop_loss"
    LAYERED_TAKE_PROFIT = "layered_take_profit"


@dataclass(frozen=True, slots=True)
class MarketReentryKey:
    """Identity scope for one task/leader/market lock."""

    follower_account: str
    task_id: str
    leader_wallet: str
    market_id: str

    def __post_init__(self) -> None:
        for name in ("follower_account", "task_id", "leader_wallet", "market_id"):
            if not str(getattr(self, name)).strip():
                raise ValueError(f"{name} is required")


@dataclass(frozen=True, slots=True)
class ProtectiveExitFill:
    """Authoritative or provisional protective-exit evidence."""

    key: MarketReentryKey
    reason: ProtectiveExitReason
    authoritative: bool
    filled_shares: Decimal
    remaining_protected_shares: Decimal
    occurred_at: datetime
    fill_id: str

    def __post_init__(self) -> None:
        if self.occurred_at.tzinfo is None:
            raise ValueError("occurred_at must be timezone-aware")
        if not str(self.fill_id).strip():
            raise ValueError("fill_id is required")
        for name in ("filled_shares", "remaining_protected_shares"):
            try:
                value = Decimal(str(getattr(self, name)))
            except (InvalidOperation, TypeError, ValueError) as exc:
                raise ValueError(f"{name} must be decimal-like") from exc
            if not value.is_finite() or value < 0:
                raise ValueError(f"{name} must be finite and non-negative")
            object.__setattr__(self, name, value)


@dataclass(frozen=True, slots=True)
class MarketReentryLock:
    """Persisted lock produced by a terminal protective fill."""

    key: MarketReentryKey
    reason: ProtectiveExitReason
    fill_id: str
    locked_at: datetime


@dataclass(frozen=True, slots=True)
class MarketReentryLockResult:
    """Idempotent result of processing protective fill evidence."""

    locked: bool
    created: bool
    reason: str


class MarketReentryLockRepository(Protocol):
    """Narrow persistence port for market re-entry locks."""

    async def get(self, key: MarketReentryKey) -> MarketReentryLock | None:
        """Return the active lock for ``key`` when present."""

    async def create_if_absent(self, lock: MarketReentryLock) -> bool:
        """Persist ``lock`` and return whether it was newly created."""

    async def delete(self, key: MarketReentryKey) -> bool:
        """Delete one lock and return whether a row existed."""

    async def delete_market(self, market_id: str) -> int:
        """Delete every active lock for a resolved market."""


class InMemoryMarketReentryLockRepository:
    """Deterministic repository used by unit tests and paper composition."""

    def __init__(self) -> None:
        self._locks: dict[MarketReentryKey, MarketReentryLock] = {}
        self._lock = asyncio.Lock()

    async def get(self, key: MarketReentryKey) -> MarketReentryLock | None:
        """Return one active lock."""

        async with self._lock:
            return self._locks.get(key)

    async def create_if_absent(self, lock: MarketReentryLock) -> bool:
        """Create one lock atomically and idempotently."""

        async with self._lock:
            if lock.key in self._locks:
                return False
            self._locks[lock.key] = lock
            return True

    async def delete(self, key: MarketReentryKey) -> bool:
        """Delete one lock."""

        async with self._lock:
            return self._locks.pop(key, None) is not None

    async def delete_market(self, market_id: str) -> int:
        """Delete locks for a resolved market."""

        async with self._lock:
            keys = [key for key in self._locks if key.market_id == market_id]
            for key in keys:
                del self._locks[key]
            return len(keys)


class MarketReentryLockService:
    """Apply terminal-fill semantics around a lock repository."""

    def __init__(self, repository: MarketReentryLockRepository) -> None:
        self._repository = repository

    async def is_locked(self, key: MarketReentryKey) -> bool:
        """Return whether a new BUY must be rejected before side effects."""

        return await self._repository.get(key) is not None

    async def record_protective_exit(
        self,
        fill: ProtectiveExitFill,
    ) -> MarketReentryLockResult:
        """Create a lock only for an authoritative terminal protective fill.

        Args:
            fill: Authoritative or provisional protective-exit evidence.

        Returns:
            Idempotent lock result. ``locked`` is True when the market is
            currently locked; ``created`` is True only when this call inserted
            a new lock row.
        """

        if not fill.authoritative:
            return MarketReentryLockResult(False, False, "exit_not_authoritative")
        if fill.filled_shares <= 0:
            return MarketReentryLockResult(False, False, "exit_has_no_fill")
        if fill.remaining_protected_shares > 0:
            return MarketReentryLockResult(
                False,
                False,
                "protected_allocation_still_open",
            )
        lock = MarketReentryLock(
            key=fill.key,
            reason=fill.reason,
            fill_id=fill.fill_id,
            locked_at=fill.occurred_at,
        )
        created = await self._repository.create_if_absent(lock)
        return MarketReentryLockResult(
            True,
            created,
            "market_locked_after_protective_exit",
        )

    async def release_manually(self, key: MarketReentryKey) -> bool:
        """Release one lock through an explicit operator action."""

        return await self._repository.delete(key)

    async def release_for_market_resolution(self, market_id: str) -> int:
        """Release all locks after authoritative market resolution."""

        return await self._repository.delete_market(market_id)


__all__ = [
    "InMemoryMarketReentryLockRepository",
    "MarketReentryKey",
    "MarketReentryLock",
    "MarketReentryLockRepository",
    "MarketReentryLockResult",
    "MarketReentryLockService",
    "ProtectiveExitFill",
    "ProtectiveExitReason",
]