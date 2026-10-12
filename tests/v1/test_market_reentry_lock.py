"""Contracts for the post-SL/TP market re-entry lock."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from pmcopy.services.copy_tasks.reentry_lock import (
    InMemoryMarketReentryLockRepository,
    MarketReentryKey,
    MarketReentryLockService,
    ProtectiveExitFill,
    ProtectiveExitReason,
)


_KEY = MarketReentryKey(
    "account",
    "task",
    "0x1111111111111111111111111111111111111111",
    "market",
)


@pytest.mark.asyncio
async def test_trigger_or_failed_exit_does_not_create_lock() -> None:
    repo = InMemoryMarketReentryLockRepository()
    service = MarketReentryLockService(repo)

    result = await service.record_protective_exit(
        ProtectiveExitFill(
            key=_KEY,
            reason=ProtectiveExitReason.STOP_LOSS,
            authoritative=False,
            filled_shares=Decimal("10"),
            remaining_protected_shares=Decimal("0"),
            occurred_at=datetime.now(UTC),
            fill_id="fill-1",
        )
    )
    assert result.locked is False
    assert await service.is_locked(_KEY) is False


@pytest.mark.asyncio
async def test_authoritative_terminal_stop_or_take_profit_locks_durably_and_idempotently() -> None:
    repo = InMemoryMarketReentryLockRepository()
    service = MarketReentryLockService(repo)
    fill = ProtectiveExitFill(
        key=_KEY,
        reason=ProtectiveExitReason.TAKE_PROFIT,
        authoritative=True,
        filled_shares=Decimal("10"),
        remaining_protected_shares=Decimal("0"),
        occurred_at=datetime(2026, 8, 8, tzinfo=UTC),
        fill_id="fill-2",
    )

    first = await service.record_protective_exit(fill)
    second = await service.record_protective_exit(fill)
    assert first.locked is True
    assert second.locked is True
    assert second.created is False
    assert await service.is_locked(_KEY) is True


@pytest.mark.asyncio
async def test_partial_exit_does_not_lock_and_manual_or_resolution_unlocks() -> None:
    repo = InMemoryMarketReentryLockRepository()
    service = MarketReentryLockService(repo)
    partial = ProtectiveExitFill(
        key=_KEY,
        reason=ProtectiveExitReason.TRAILING_STOP,
        authoritative=True,
        filled_shares=Decimal("4"),
        remaining_protected_shares=Decimal("6"),
        occurred_at=datetime(2026, 8, 8, tzinfo=UTC),
        fill_id="fill-3",
    )
    assert (await service.record_protective_exit(partial)).locked is False

    terminal = ProtectiveExitFill(
        key=_KEY,
        reason=ProtectiveExitReason.TRAILING_STOP,
        authoritative=True,
        filled_shares=Decimal("6"),
        remaining_protected_shares=Decimal("0"),
        occurred_at=datetime(2026, 8, 8, tzinfo=UTC),
        fill_id="fill-4",
    )
    await service.record_protective_exit(terminal)
    assert await service.release_for_market_resolution("market") == 1
    assert await service.is_locked(_KEY) is False


@pytest.mark.asyncio
async def test_manual_unlock_releases_one_lock() -> None:
    repo = InMemoryMarketReentryLockRepository()
    service = MarketReentryLockService(repo)
    fill = ProtectiveExitFill(
        key=_KEY,
        reason=ProtectiveExitReason.STOP_LOSS,
        authoritative=True,
        filled_shares=Decimal("10"),
        remaining_protected_shares=Decimal("0"),
        occurred_at=datetime(2026, 8, 8, tzinfo=UTC),
        fill_id="fill-5",
    )
    await service.record_protective_exit(fill)
    assert await service.release_manually(_KEY) is True
    assert await service.is_locked(_KEY) is False
    assert await service.release_manually(_KEY) is False


@pytest.mark.asyncio
async def test_zero_fill_does_not_create_lock() -> None:
    repo = InMemoryMarketReentryLockRepository()
    service = MarketReentryLockService(repo)
    result = await service.record_protective_exit(
        ProtectiveExitFill(
            key=_KEY,
            reason=ProtectiveExitReason.STOP_LOSS,
            authoritative=True,
            filled_shares=Decimal("0"),
            remaining_protected_shares=Decimal("0"),
            occurred_at=datetime(2026, 8, 8, tzinfo=UTC),
            fill_id="fill-6",
        )
    )
    assert result.locked is False
    assert result.reason == "exit_has_no_fill"


def test_market_reentry_key_rejects_empty_fields() -> None:
    import pytest as _pytest

    with _pytest.raises(ValueError, match="task_id"):
        MarketReentryKey("account", "", "0x" + "11" * 20, "market")
