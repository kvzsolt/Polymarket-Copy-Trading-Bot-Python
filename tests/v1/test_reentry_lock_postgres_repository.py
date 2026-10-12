"""Contract tests for durable post-protective-exit locks."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from pmcopy.services.copy_tasks.reentry_lock import (
    MarketReentryKey,
    MarketReentryLock,
    ProtectiveExitReason,
)
from pmcopy.storage.postgres.market_reentry_lock_repository import (
    PostgresMarketReentryLockRepository,
)


class FakePool:
    """Minimal asyncpg-like fake used by repository contracts."""

    def __init__(self) -> None:
        self.fetchval_results: list[object] = []
        self.fetchrow_results: list[object] = []
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    async def fetchval(self, sql: str, *args: object) -> object:
        self.calls.append((sql, args))
        return self.fetchval_results.pop(0)

    async def fetchrow(self, sql: str, *args: object) -> object:
        self.calls.append((sql, args))
        return self.fetchrow_results.pop(0)


def key() -> MarketReentryKey:
    """Return one deterministic identity scope."""

    return MarketReentryKey(
        follower_account="account-a",
        task_id="task-a",
        leader_wallet="0x1111111111111111111111111111111111111111",
        market_id="condition-a",
    )


@pytest.mark.asyncio
async def test_create_if_absent_is_atomic_and_idempotent() -> None:
    pool = FakePool()
    pool.fetchval_results = ["fill-a", None]
    repository = PostgresMarketReentryLockRepository(pool)
    lock = MarketReentryLock(
        key=key(),
        reason=ProtectiveExitReason.TAKE_PROFIT,
        fill_id="fill-a",
        locked_at=datetime(2026, 8, 8, tzinfo=UTC),
    )

    assert await repository.create_if_absent(lock) is True
    assert await repository.create_if_absent(lock) is False
    assert "ON CONFLICT DO NOTHING" in pool.calls[0][0]


@pytest.mark.asyncio
async def test_get_round_trips_reason_and_timestamp() -> None:
    pool = FakePool()
    when = datetime(2026, 8, 8, 1, 2, 3, tzinfo=UTC)
    pool.fetchrow_results = [
        {
            "follower_account": "account-a",
            "task_id": "task-a",
            "leader_wallet": "0x1111111111111111111111111111111111111111",
            "market_id": "condition-a",
            "reason": "stop_loss",
            "fill_id": "fill-a",
            "locked_at": when,
        }
    ]
    repository = PostgresMarketReentryLockRepository(pool)

    result = await repository.get(key())

    assert result is not None
    assert result.reason is ProtectiveExitReason.STOP_LOSS
    assert result.locked_at == when


@pytest.mark.asyncio
async def test_manual_release_and_market_resolution_release_return_counts() -> None:
    pool = FakePool()
    pool.fetchval_results = [1, 3]
    repository = PostgresMarketReentryLockRepository(pool)

    assert await repository.delete(key()) is True
    assert await repository.delete_market("condition-a") == 3
    assert "DELETE FROM market_reentry_locks" in pool.calls[0][0]
    assert "market_id = $1" in pool.calls[1][0]
