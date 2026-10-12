"""PostgreSQL adapter for post-protective-exit market re-entry locks."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Protocol

from pmcopy.services.copy_tasks.reentry_lock import (
    MarketReentryKey,
    MarketReentryLock,
    ProtectiveExitReason,
)


class DatabasePool(Protocol):
    """Small async database surface required by this repository."""

    async def fetchval(self, query: str, *args: object) -> object | None:
        """Return the first column of the first row."""

    async def fetchrow(
        self,
        query: str,
        *args: object,
    ) -> Mapping[str, object] | None:
        """Return one database row."""


class PostgresMarketReentryLockRepository:
    """Persist task/leader/market locks atomically and idempotently."""

    def __init__(self, pool: DatabasePool) -> None:
        self._pool = pool

    async def get(self, key: MarketReentryKey) -> MarketReentryLock | None:
        """Return one active lock."""

        row = await self._pool.fetchrow(
            """
            SELECT follower_account, task_id, leader_wallet, market_id,
                   reason, fill_id, locked_at
              FROM market_reentry_locks
             WHERE follower_account = $1
               AND task_id = $2
               AND leader_wallet = $3
               AND market_id = $4
            """,
            key.follower_account,
            key.task_id,
            key.leader_wallet.casefold(),
            key.market_id,
        )
        if row is None:
            return None
        locked_at = row.get("locked_at")
        if not isinstance(locked_at, datetime):
            raise ValueError("market re-entry locked_at must be a datetime")
        reason = row.get("reason")
        fill_id = row.get("fill_id")
        if not isinstance(reason, str) or not isinstance(fill_id, str):
            raise ValueError("market re-entry lock reason/fill_id is invalid")
        return MarketReentryLock(
            key=MarketReentryKey(
                follower_account=self._text(row, "follower_account"),
                task_id=self._text(row, "task_id"),
                leader_wallet=self._text(row, "leader_wallet"),
                market_id=self._text(row, "market_id"),
            ),
            reason=ProtectiveExitReason(reason),
            fill_id=fill_id,
            locked_at=locked_at,
        )

    async def create_if_absent(self, lock: MarketReentryLock) -> bool:
        """Create one lock and return whether the row was inserted."""

        inserted = await self._pool.fetchval(
            """
            INSERT INTO market_reentry_locks
                (follower_account, task_id, leader_wallet, market_id,
                 reason, fill_id, locked_at, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
            ON CONFLICT DO NOTHING
            RETURNING fill_id
            """,
            lock.key.follower_account,
            lock.key.task_id,
            lock.key.leader_wallet.casefold(),
            lock.key.market_id,
            lock.reason.value,
            lock.fill_id,
            lock.locked_at,
        )
        return inserted is not None

    async def delete(self, key: MarketReentryKey) -> bool:
        """Release one lock through an explicit operator action."""

        count = await self._pool.fetchval(
            """
            WITH deleted AS (
                DELETE FROM market_reentry_locks
                 WHERE follower_account = $1
                   AND task_id = $2
                   AND leader_wallet = $3
                   AND market_id = $4
                RETURNING 1
            )
            SELECT COUNT(*) FROM deleted
            """,
            key.follower_account,
            key.task_id,
            key.leader_wallet.casefold(),
            key.market_id,
        )
        return self._non_negative_count(count) > 0

    async def delete_market(self, market_id: str) -> int:
        """Release all locks after authoritative market resolution."""

        count = await self._pool.fetchval(
            """
            WITH deleted AS (
                DELETE FROM market_reentry_locks
                 WHERE market_id = $1
                RETURNING 1
            )
            SELECT COUNT(*) FROM deleted
            """,
            str(market_id),
        )
        return self._non_negative_count(count)

    @staticmethod
    def _non_negative_count(value: object) -> int:
        if isinstance(value, bool) or not isinstance(value, int):
            return 0
        return max(0, value)

    @staticmethod
    def _text(row: Mapping[str, object], name: str) -> str:
        value = row.get(name)
        if not isinstance(value, str) or not value:
            raise ValueError(f"market re-entry {name} must be non-empty text")
        return value


__all__ = ["PostgresMarketReentryLockRepository"]
