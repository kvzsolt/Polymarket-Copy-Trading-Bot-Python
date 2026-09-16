"""Real PostgreSQL recovery preserves absence and atomic ledger accounting."""

import asyncio
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from services.execution.fill_accounting import FillAccountingConflict
from storage.postgres.fills_repo import FillRecord, FillsRepo
from storage.postgres.positions_repo import PositionRecord, PositionsRepo
from tests.test_fills_repo_dedup_key import _pg_pool

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


class TestRecoveredSellPostgres:
    """Verify rollback and serialization with the real repository transaction."""

    async def test_repeated_recovery_keeps_one_fill_and_zero_inventory(self) -> None:
        """A replay requires fresh ledger totals and never creates a position."""
        async with _pg_pool() as pool:
            fills, positions = FillsRepo(pool), PositionsRepo(pool)
            record = self._record()
            fill_id, position = await fills.append_recovered_sell(record, positions)
            assert fill_id > 0 and position is None
            with pytest.raises(FillAccountingConflict):
                await fills.append_recovered_sell(record, positions)
            assert await positions.get_position("user", "condition", token_id="123") is None
            assert len(await fills.list_fills_for_user("user")) == 1
            accounted = await fills.accounted_order_fill("user", "order")
            assert accounted.shares == Decimal("11")
            assert accounted.collateral == Decimal("2.53")

    async def test_positive_position_rejects_recovery_without_inserting_fill(self) -> None:
        """A concurrent inventory arrival invalidates the earlier absence proof."""
        async with _pg_pool() as pool:
            fills, positions = FillsRepo(pool), PositionsRepo(pool)
            await positions.upsert_position(
                PositionRecord("user", "condition", "123", None, Decimal("2"), Decimal("0.2"))
            )
            with pytest.raises(ValueError):
                await fills.append_recovered_sell(self._record(), positions)
            assert await fills.list_fills_for_user("user") == []
            position = await positions.get_position("user", "condition", token_id="123")
            assert position is not None and position.size_tokens == Decimal("2")

    async def test_projection_failure_rolls_back_new_ledger_entry(self) -> None:
        """The task projection and historical fill remain one transaction."""
        async with _pg_pool() as pool:
            projection = SimpleNamespace(apply=AsyncMock(side_effect=RuntimeError("projection")))
            fills = FillsRepo(pool, task_projection=projection)
            with pytest.raises(RuntimeError, match="projection"):
                await fills.append_recovered_sell(self._record(), PositionsRepo(pool))
            assert await fills.list_fills_for_user("user") == []

    async def test_concurrent_recoveries_insert_only_one_ledger_entry(self) -> None:
        """Order accounting locks serialize even when the position row is absent."""
        async with _pg_pool() as pool:
            fills, positions = FillsRepo(pool), PositionsRepo(pool)
            results = await asyncio.gather(
                fills.append_recovered_sell(self._record(), positions),
                fills.append_recovered_sell(self._record(), positions),
                return_exceptions=True,
            )
            assert sum(isinstance(result, FillAccountingConflict) for result in results) == 1
            assert len(await fills.list_fills_for_user("user")) == 1

    @staticmethod
    def _record() -> FillRecord:
        return FillRecord(
            user_id="user",
            leader_id=None,
            order_id="order",
            condition_id="condition",
            token_id="123",
            side="SELL",
            size_tokens=Decimal("11"),
            price=Decimal("0.23"),
            fill_ts=1234,
            dedup_key="11",
            accounted_shares=Decimal(0),
        )
