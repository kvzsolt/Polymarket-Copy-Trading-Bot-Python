"""Manual inventory synchronization preserves lifecycle and verified absence."""

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from pmcopy.domain.position_lifecycle import PositionLifecycle
from pmcopy.services.positions.manual_sync_service import ManualSyncService
from pmcopy.storage.postgres.positions_repo import PositionRecord


class SyncScenario:
    """Compose manual synchronization with deterministic storage and venue I/O."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.positions = MagicMock()
        self.positions.list_open_positions = AsyncMock(return_value=[])
        self.positions.list_held_positions = AsyncMock(return_value=[])
        self.positions.delete_position = AsyncMock()
        self.positions.delete_position_if_unchanged = AsyncMock(return_value=True)
        self.positions.set_position_size = AsyncMock(return_value=True)
        self.history = MagicMock(delete_many=AsyncMock(), insert_many=AsyncMock())
        self.market_store = MagicMock()
        self.fetcher = AsyncMock(return_value=[])
        self.service = ManualSyncService(
            funder_address="wallet",
            user_id="user",
            positions_repo=self.positions,
            market_store=self.market_store,
            positions_fetcher=self.fetcher,
        )
        monkeypatch.setattr(self.service, "_sync_manual_trades", AsyncMock())
        monkeypatch.setattr(
            "pmcopy.services.positions.manual_sync_service.get_user_position_model",
            lambda wallet: self.history,
        )
        monkeypatch.setattr(
            "pmcopy.services.positions.manual_sync_service.print_manual_sync_discrepancy", MagicMock()
        )

    def hold(self, size: str = "5", *, settled: bool = False) -> None:
        """Set the local inventory expected before a complete venue read."""
        record = PositionRecord(
            user_id="user",
            condition_id="condition",
            token_id="token",
            leader_id=None,
            size_tokens=Decimal(size),
            avg_entry_price=Decimal("0.4"),
            lifecycle=PositionLifecycle.SETTLED if settled else PositionLifecycle.ACTIVE,
        )
        self.positions.list_held_positions.return_value = [record]
        self.positions.list_open_positions.return_value = [] if settled else [record]

    def venue(self, *, redeemable: object = False, size: str = "5") -> dict[str, object]:
        """Return a normalized zero-mark venue holding with explicit lifecycle evidence."""
        return {
            "asset": "token",
            "conditionId": "condition",
            "size": size,
            "avgPrice": "0.4",
            "curPrice": "0",
            "currentValue": "0",
            "redeemable": redeemable,
        }


@pytest.mark.asyncio
async def test_complete_empty_snapshot_prunes_all_held_inventory_with_guard(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A validated empty holdings list is authoritative even for settled local rows."""
    scenario = SyncScenario(monkeypatch)
    scenario.hold(settled=True)
    assert await scenario.service.sync_now(reason="test")
    scenario.positions.list_held_positions.assert_awaited_once_with("user")
    scenario.positions.delete_position_if_unchanged.assert_awaited_once_with(
        "user", "condition", token_id="token", expected_size=Decimal("5")
    )
    scenario.positions.delete_position.assert_not_awaited()
    scenario.market_store.evict.assert_called_once_with("token")


@pytest.mark.asyncio
@pytest.mark.parametrize("redeemable", [False, True])
async def test_zero_marked_holdings_keep_shares_and_explicit_lifecycle(
    monkeypatch: pytest.MonkeyPatch, redeemable: bool
) -> None:
    """Zero market value never erases shares or replaces settlement evidence."""
    scenario = SyncScenario(monkeypatch)
    scenario.hold(settled=redeemable)
    scenario.fetcher.return_value = [scenario.venue(redeemable=redeemable)]
    assert await scenario.service.sync_now(reason="test")
    written = scenario.positions.set_position_size.await_args.kwargs
    assert written["size_tokens"] == Decimal("5")
    assert written["expected_size"] == Decimal("5")
    assert written["lifecycle"] is (
        PositionLifecycle.SETTLED if redeemable else PositionLifecycle.ACTIVE
    )
    assert written["settlement_price"] == (Decimal("0") if redeemable else None)
    scenario.positions.delete_position_if_unchanged.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("size", ["0", "5"])
async def test_malformed_lifecycle_anywhere_prevents_all_snapshot_mutations(
    monkeypatch: pytest.MonkeyPatch, size: str
) -> None:
    """A malformed later row cannot partially publish or erase the current snapshot."""
    scenario = SyncScenario(monkeypatch)
    scenario.hold()
    invalid = scenario.venue(redeemable="true", size=size)
    invalid.update(asset="other-token", conditionId="other-condition")
    scenario.fetcher.return_value = [scenario.venue(), invalid]
    assert not await scenario.service.sync_now(reason="test")
    scenario.positions.set_position_size.assert_not_awaited()
    scenario.positions.delete_position_if_unchanged.assert_not_awaited()
    scenario.history.delete_many.assert_not_awaited()
    scenario.history.insert_many.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("present", [False, True])
async def test_compare_and_set_deferral_cannot_report_success(
    monkeypatch: pytest.MonkeyPatch, present: bool
) -> None:
    """A failed expected-state check leaves readiness false for either mutation."""
    scenario = SyncScenario(monkeypatch)
    scenario.hold()
    scenario.positions.set_position_size.return_value = False
    scenario.positions.delete_position_if_unchanged.return_value = False
    scenario.fetcher.return_value = [scenario.venue(size="3")] if present else []
    assert not await scenario.service.sync_now(reason="test")


@pytest.mark.asyncio
async def test_fill_during_snapshot_read_cannot_be_overwritten(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Expected local inventory is captured before the potentially stale venue request."""
    scenario = SyncScenario(monkeypatch)
    scenario.hold()

    async def fetch_during_fill(wallet: str) -> list[dict[str, object]]:
        """Return a stale snapshot while a fill commits to local storage."""
        scenario.hold("7")
        return [scenario.venue(size="3")]

    async def align_if_unchanged(**kwargs: object) -> bool:
        """Reject the earlier expected inventory after a concurrent fill."""
        return kwargs["expected_size"] == Decimal("7")

    scenario.fetcher.side_effect = fetch_during_fill
    scenario.positions.set_position_size.side_effect = align_if_unchanged
    assert not await scenario.service.sync_now(reason="test")
    assert scenario.positions.set_position_size.await_args.kwargs["expected_size"] == Decimal("5")


@pytest.mark.asyncio
@pytest.mark.parametrize("write_success", [False, True])
async def test_runtime_refresh_callback_runs_only_after_successful_alignment(
    monkeypatch: pytest.MonkeyPatch, write_success: bool
) -> None:
    """Runtime hydration and readiness run only after storage accepts the snapshot."""
    scenario = SyncScenario(monkeypatch)
    scenario.hold()
    scenario.fetcher.return_value = [scenario.venue()]
    scenario.positions.set_position_size.return_value = write_success
    refreshed = AsyncMock()
    service = ManualSyncService(
        funder_address="wallet",
        user_id="user",
        positions_repo=scenario.positions,
        market_store=scenario.market_store,
        positions_fetcher=scenario.fetcher,
        on_positions_synced=refreshed,
    )
    monkeypatch.setattr(service, "_sync_manual_trades", AsyncMock())
    assert await service.sync_now(reason="test") is write_success
    assert refreshed.await_count == int(write_success)
