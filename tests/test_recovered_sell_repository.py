"""Recovered SELL ledger writes retain atomic locks and skip position arithmetic."""

from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from pmcopy.services.execution.fill_accounting import FillAccountingConflict
from pmcopy.storage.postgres.fills_repo import FillRecord, FillsRepo


@pytest.fixture
def recovery_repository():
    """Expose deterministic transaction and locking boundaries."""
    connection = AsyncMock()
    connection.transaction = Mock(return_value=AsyncMock())
    connection.fetchval.side_effect = [Decimal(0), 7]
    connection_scope = AsyncMock()
    connection_scope.__aenter__.return_value = connection
    pool = Mock()
    pool.acquire.return_value = connection_scope
    positions = SimpleNamespace(
        _assert_absent_position_in_conn=AsyncMock(), _apply_fill_in_conn=AsyncMock()
    )
    projection = SimpleNamespace(apply=AsyncMock())
    record = FillRecord(
        user_id="user-1",
        leader_id="leader",
        order_id="order-1",
        condition_id="condition",
        token_id="123",
        side="SELL",
        size_tokens=Decimal("11"),
        price=Decimal("0.23"),
        fill_ts=1234,
        dedup_key="11",
        accounted_shares=Decimal(0),
    )
    return FillsRepo(pool, task_projection=projection), connection, positions, projection, record


class TestRecoveredSellRepository:
    """Keep recovery limited to missing historical SELL accounting."""

    @pytest.mark.asyncio
    async def test_records_fill_and_projection_without_reapplying_position(
        self, recovery_repository
    ):
        """The same transaction protects absence and projects the inserted ledger row."""
        repo, connection, positions, projection, record = recovery_repository
        assert await repo.append_recovered_sell(record, positions) == (7, None)
        positions._assert_absent_position_in_conn.assert_awaited_once_with(
            connection, user_id="user-1", condition_id="condition", token_id="123"
        )
        positions._apply_fill_in_conn.assert_not_awaited()
        projection.apply.assert_awaited_once_with(connection, 7, record)
        assert connection.fetchval.await_count == 2

    @pytest.mark.asyncio
    async def test_concurrent_position_change_blocks_before_ledger_write(self, recovery_repository):
        """An inventory row appearing after the venue read prevents a stale recovery."""
        repo, connection, positions, projection, record = recovery_repository
        positions._assert_absent_position_in_conn.side_effect = ValueError("Position changed")
        with pytest.raises(ValueError, match="Position changed"):
            await repo.append_recovered_sell(record, positions)
        connection.fetchval.assert_not_awaited()
        projection.apply.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_concurrent_ledger_change_requires_a_fresh_accounting_read(
        self, recovery_repository
    ):
        """Existing fill-accounting serialization also protects recovered writes."""
        repo, connection, positions, projection, record = recovery_repository
        connection.fetchval.side_effect = [Decimal("11")]
        with pytest.raises(FillAccountingConflict):
            await repo.append_recovered_sell(record, positions)
        assert connection.fetchval.await_count == 1
        projection.apply.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_duplicate_fill_does_not_repeat_task_projection(self, recovery_repository):
        """Stable cumulative dedup remains authoritative when insertion conflicts."""
        repo, connection, positions, projection, record = recovery_repository
        connection.fetchval.side_effect = [Decimal(0), None]
        assert await repo.append_recovered_sell(record, positions) == (0, None)
        positions._apply_fill_in_conn.assert_not_awaited()
        projection.apply.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "change", [{"side": "BUY"}, {"dedup_key": None}, {"accounted_shares": None}]
    )
    async def test_unqualified_fill_cannot_skip_position_application(
        self, recovery_repository, change
    ):
        """Recovery cannot be used as a generic unchecked ledger-only write."""
        repo, connection, positions, _, record = recovery_repository
        with pytest.raises(ValueError):
            await repo.append_recovered_sell(replace(record, **change), positions)
        connection.fetchval.assert_not_awaited()
