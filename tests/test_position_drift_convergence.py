"""Regression coverage for verified inventory convergence and scope recovery."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from pmcopy.config.app_config import PositionDriftConfig
from pmcopy.domain.position_drift import PositionDriftAction, PositionDriftStatus
from pmcopy.services.position_drift_service import PositionDriftService


@dataclass(frozen=True)
class HeldPosition:
    """Represent exact inventory read from the persistence boundary."""

    condition_id: str = "condition"
    token_id: str = "token"
    size_tokens: Decimal = Decimal("5")


@dataclass(frozen=True)
class OwnedReservation:
    """Represent an in-flight order's durable inventory ownership."""

    id: str = "reservation"


class FixedClock:
    """Provide a deterministic current instant and explicit advancement."""

    def __init__(self) -> None:
        self._instant = datetime(2026, 9, 16, tzinfo=UTC)

    def now(self) -> datetime:
        """Return the controlled current instant."""
        return self._instant

    def advance(self, seconds: int) -> None:
        """Advance without depending on the operating system clock."""
        self._instant += timedelta(seconds=seconds)


class InventoryRepository:
    """Expose inventory and record guarded versus unconditional deletion."""

    def __init__(self) -> None:
        self.list_open_positions = AsyncMock(return_value=[HeldPosition()])
        self.list_held_positions = AsyncMock(return_value=[HeldPosition()])
        self.delete_position = AsyncMock()
        self.delete_position_if_unchanged = AsyncMock(return_value=True)

    def set_positions(self, positions: list[HeldPosition]) -> None:
        """Change subsequent repository reads as a completed external update."""
        self.list_open_positions.return_value = positions
        self.list_held_positions.return_value = positions


class ReservationRepository:
    """Expose controllable durable ownership reads."""

    def __init__(self) -> None:
        self.list_active_for_scope = AsyncMock(return_value=[])


class DriftScenario:
    """Compose the service with deterministic storage and venue seams."""

    def __init__(self) -> None:
        self.positions = InventoryRepository()
        self.reservations = ReservationRepository()
        self.fetcher = AsyncMock(return_value=[])
        self.service = PositionDriftService(
            positions_repo=self.positions,
            reservations_repo=self.reservations,
            live_positions_fetcher=self.fetcher,
            config=PositionDriftConfig(allow_auto_prune_zero=True),
            clock=FixedClock(),
        )

    def set_venue_size(self, size: str) -> None:
        """Set a complete venue snapshot for the controlled token."""
        self.fetcher.return_value = [{"asset": "token", "conditionId": "condition", "size": size}]

    def is_paused(self) -> bool:
        """Report whether the execution scope remains inhibited."""
        return self.service.is_scope_paused(
            user_id="user", condition_id="condition", token_id="token"
        )


@pytest.mark.asyncio
async def test_successful_guarded_prune_is_reconciled_and_ready() -> None:
    """A confirmed durable prune satisfies drift readiness immediately."""
    scenario = DriftScenario()
    report = await scenario.service.scan_user(user_id="user", wallet="wallet")
    assert report.clean
    assert report.records[0].is_clean
    assert report.records[0].action is PositionDriftAction.PRUNE_LOCAL_ZERO_POSITION
    assert not scenario.is_paused()
    scenario.positions.delete_position_if_unchanged.assert_awaited_once_with(
        "user", "condition", token_id="token", expected_size=Decimal("5")
    )
    scenario.positions.delete_position.assert_not_awaited()


@pytest.mark.asyncio
async def test_matching_snapshot_clears_only_the_converged_scope() -> None:
    """Fresh matching inventory releases a previous mismatch pause."""
    scenario = DriftScenario()
    scenario.set_venue_size("3")
    first = await scenario.service.scan_user(user_id="user", wallet="wallet")
    assert not first.clean
    assert scenario.is_paused()
    scenario.positions.set_positions([HeldPosition(size_tokens=Decimal("3"))])
    second = await scenario.service.scan_user(user_id="user", wallet="wallet")
    assert second.clean
    assert not second.paused_scopes
    assert not scenario.is_paused()


@pytest.mark.asyncio
async def test_matching_scope_does_not_clear_another_unresolved_scope() -> None:
    """Convergence releases only the scope independently verified to match."""
    scenario = DriftScenario()
    other = HeldPosition(condition_id="other-condition", token_id="other-token")
    scenario.positions.set_positions([HeldPosition(), other])
    scenario.fetcher.return_value = [
        {"asset": "token", "conditionId": "condition", "size": "3"},
        {"asset": "other-token", "conditionId": "other-condition", "size": "3"},
    ]
    await scenario.service.scan_user(user_id="user", wallet="wallet")
    scenario.positions.set_positions([HeldPosition(size_tokens=Decimal("3")), other])
    report = await scenario.service.scan_user(user_id="user", wallet="wallet")
    assert not report.clean
    assert not scenario.is_paused()
    assert report.paused_scopes == ("user|other-condition|other-token",)
    assert scenario.service.is_scope_paused(
        user_id="user", condition_id="other-condition", token_id="other-token"
    )


@pytest.mark.asyncio
async def test_guarded_prune_clears_a_previous_mismatch_pause() -> None:
    """Durable pruning releases the previous pause while preserving observed sizes."""
    scenario = DriftScenario()
    scenario.set_venue_size("3")
    await scenario.service.scan_user(user_id="user", wallet="wallet")
    assert scenario.is_paused()
    scenario.fetcher.return_value = []
    report = await scenario.service.scan_user(user_id="user", wallet="wallet")
    assert report.clean
    assert not report.paused_scopes
    assert not scenario.is_paused()
    assert report.records[0].local_size == Decimal("5")
    assert report.records[0].status is PositionDriftStatus.RECONCILED


@pytest.mark.asyncio
async def test_scope_removed_from_both_snapshots_clears_after_ownership_check() -> None:
    """A fully closed scope converges even though it is absent from both lists."""
    scenario = DriftScenario()
    scenario.set_venue_size("3")
    await scenario.service.scan_user(user_id="user", wallet="wallet")
    scenario.positions.set_positions([])
    scenario.fetcher.return_value = []
    report = await scenario.service.scan_user(user_id="user", wallet="wallet")
    assert report.clean
    assert not report.paused_scopes
    assert not scenario.is_paused()


@pytest.mark.asyncio
async def test_removed_scope_with_active_ownership_remains_unresolved() -> None:
    """Absence alone cannot release inventory still owned by an order."""
    scenario = DriftScenario()
    scenario.set_venue_size("3")
    await scenario.service.scan_user(user_id="user", wallet="wallet")
    scenario.positions.set_positions([])
    scenario.fetcher.return_value = []
    scenario.reservations.list_active_for_scope.return_value = [OwnedReservation()]
    report = await scenario.service.scan_user(user_id="user", wallet="wallet")
    assert not report.clean
    assert scenario.is_paused()
    assert report.records[0].status is PositionDriftStatus.RESERVATION_ORPHANED


@pytest.mark.asyncio
async def test_failed_prune_cannot_claim_success_or_start_holdoff(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Persistence errors preserve the pause and do not expose raw messages."""
    scenario = DriftScenario()
    failure = RuntimeError("SYNTHETIC_PRIVATE_DATABASE_ERROR")
    scenario.positions.delete_position.side_effect = failure
    scenario.positions.delete_position_if_unchanged.side_effect = failure
    report = await scenario.service.scan_user(user_id="user", wallet="wallet")
    assert not report.clean
    assert report.records[0].status is PositionDriftStatus.UNRESOLVED
    assert report.records[0].action is not PositionDriftAction.PRUNE_LOCAL_ZERO_POSITION
    assert scenario.is_paused()
    assert not scenario.service.was_recently_pruned(
        user_id="user", condition_id="condition", token_id="token"
    )
    assert "SYNTHETIC_PRIVATE_DATABASE_ERROR" not in repr(report)
    assert "SYNTHETIC_PRIVATE_DATABASE_ERROR" not in caplog.text


@pytest.mark.asyncio
async def test_concurrent_position_change_prevents_prune_success() -> None:
    """A rejected compare-and-delete remains unresolved for the next sweep."""
    scenario = DriftScenario()
    scenario.positions.delete_position_if_unchanged.return_value = False
    report = await scenario.service.scan_user(user_id="user", wallet="wallet")
    assert not report.clean
    assert report.records[0].status is PositionDriftStatus.UNRESOLVED
    assert scenario.is_paused()
    assert not scenario.service.was_recently_pruned(
        user_id="user", condition_id="condition", token_id="token"
    )
    scenario.positions.delete_position.assert_not_awaited()


@pytest.mark.asyncio
async def test_failed_ownership_check_keeps_account_and_previous_scope_paused() -> None:
    """An unavailable ownership read cannot release a previously paused scope."""
    scenario = DriftScenario()
    scenario.set_venue_size("3")
    await scenario.service.scan_user(user_id="user", wallet="wallet")
    scenario.positions.set_positions([])
    scenario.fetcher.return_value = []
    scenario.reservations.list_active_for_scope.side_effect = RuntimeError("unavailable")
    report = await scenario.service.scan_user(user_id="user", wallet="wallet")
    assert not report.clean
    assert "user||" in report.paused_scopes
    assert "user|condition|token" in report.paused_scopes
    assert scenario.service.is_scope_paused(user_id="user", condition_id="other", token_id="other")


@pytest.mark.asyncio
async def test_drift_compares_all_held_inventory_including_settled_holdings() -> None:
    """Settlement exclusion from active risk must not hide inventory from drift."""
    scenario = DriftScenario()
    scenario.positions.list_open_positions.return_value = []
    scenario.set_venue_size("5")
    report = await scenario.service.scan_user(user_id="user", wallet="wallet")
    assert report.clean
    assert len(report.records) == 1
    assert report.records[0].local_size == Decimal("5")
    scenario.positions.list_held_positions.assert_awaited_once_with("user")
    scenario.positions.list_open_positions.assert_not_awaited()


@pytest.mark.asyncio
async def test_clock_controls_report_time_and_prune_holdoff() -> None:
    """Time-sensitive behavior has a deterministic constructor-injected clock."""
    scenario = DriftScenario()
    clock = FixedClock()
    service = PositionDriftService(
        positions_repo=scenario.positions,
        reservations_repo=scenario.reservations,
        live_positions_fetcher=scenario.fetcher,
        config=PositionDriftConfig(allow_auto_prune_zero=True),
        clock=clock,
    )
    report = await service.scan_user(user_id="user", wallet="wallet")
    assert report.generated_at == int(clock.now().timestamp())
    assert service.was_recently_pruned(user_id="user", condition_id="condition", token_id="token")
    clock.advance(61)
    assert not service.was_recently_pruned(
        user_id="user", condition_id="condition", token_id="token"
    )


@pytest.mark.asyncio
async def test_fill_during_venue_fetch_prevents_pruning_the_new_inventory() -> None:
    """Expected inventory must predate the venue read to detect an overlapping fill."""
    scenario = DriftScenario()

    async def fetch_during_fill(wallet: str) -> list[dict[str, str]]:
        """Return a stale venue zero while a BUY fill commits locally."""
        scenario.positions.set_positions([HeldPosition(size_tokens=Decimal("7"))])
        return []

    async def guarded_delete(
        user_id: str, condition_id: str, *, token_id: str, expected_size: Decimal
    ) -> bool:
        """Apply the repository's compare-before-delete contract."""
        current = scenario.positions.list_held_positions.return_value[0].size_tokens
        return expected_size == current

    scenario.fetcher.side_effect = fetch_during_fill
    scenario.positions.delete_position_if_unchanged.side_effect = guarded_delete
    report = await scenario.service.scan_user(user_id="user", wallet="wallet")
    assert not report.clean
    assert scenario.is_paused()
    assert report.records[0].status is PositionDriftStatus.UNRESOLVED
    scenario.positions.delete_position_if_unchanged.assert_awaited_once_with(
        "user", "condition", token_id="token", expected_size=Decimal("5")
    )
