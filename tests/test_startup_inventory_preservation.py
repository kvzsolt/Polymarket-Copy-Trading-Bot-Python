"""Early startup inspection preserves cost basis until canonical reconciliation."""

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from domain.position_lifecycle import PositionLifecycle
from orchestrators.startup_health import (
    PositionSyncResult,
    _position_sync_check,
    reconcile_persisted_positions_to_live,
)
from storage.postgres.positions_repo import PositionRecord


class StartupInventory:
    """Expose a persisted SELL cost basis and detect every attempted deletion."""

    def __init__(self) -> None:
        self.repo = MagicMock()
        record = PositionRecord(
            user_id="user",
            condition_id="condition",
            token_id="123",
            leader_id="leader",
            size_tokens=Decimal("11"),
            avg_entry_price=Decimal("0.4"),
            lifecycle=PositionLifecycle.UNKNOWN,
        )
        self.repo.list_open_positions = AsyncMock(return_value=[record])
        self.repo.list_held_positions = AsyncMock(return_value=[record])
        self.repo.delete_position = AsyncMock()
        self.repo.delete_position_if_unchanged = AsyncMock(return_value=True)


@pytest.mark.asyncio
async def test_early_empty_snapshot_keeps_position_for_sell_fill_recovery() -> None:
    """An earlier venue zero cannot erase the basis needed by a recovered SELL."""
    inventory = StartupInventory()
    result = await reconcile_persisted_positions_to_live(
        inventory.repo, user_id="user", live_positions=[]
    )
    assert result.persisted_before == result.persisted_after == 1
    assert result.pruned_positions == 0
    assert result.deferred_positions == 1
    assert not result.fetch_failed
    assert result.pruned_token_ids == ()
    inventory.repo.delete_position.assert_not_awaited()
    inventory.repo.delete_position_if_unchanged.assert_not_awaited()


@pytest.mark.asyncio
async def test_startup_inspection_reads_complete_holdings() -> None:
    """The early inspection must retain settled inventory in its accounting view."""
    inventory = StartupInventory()
    inventory.repo.list_open_positions.return_value = []
    result = await reconcile_persisted_positions_to_live(
        inventory.repo,
        user_id="user",
        live_positions=[{"conditionId": "condition", "asset": "123", "size": "11"}],
    )
    assert result.persisted_before == result.persisted_after == 1
    assert result.deferred_positions == 0
    inventory.repo.list_held_positions.assert_awaited_once_with("user")
    inventory.repo.list_open_positions.assert_not_awaited()


def test_deferred_startup_inventory_is_not_reported_as_matching() -> None:
    """The health report distinguishes preserved pending alignment from convergence."""
    result = PositionSyncResult(
        persisted_before=1,
        persisted_after=1,
        live_count=0,
        pruned_positions=0,
        pruned_condition_ids=(),
        pruned_token_ids=(),
        deferred_positions=1,
    )
    check = _position_sync_check(result)
    assert check.status == "warning"
    assert "canonical" in check.message
    assert check.details["deferred_positions"] == 1
