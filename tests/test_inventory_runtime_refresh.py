"""Only a successfully synchronized inventory may refresh monitoring state."""

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from pmcopy.services.healthServer import ReadinessTracker

import pytest


@pytest.mark.asyncio
async def test_refresh_removes_settled_tracking_and_updates_drift_readiness():
    from pmcopy.orchestrators.account_inventory_startup import InventoryRuntimeRefresh

    manager = Mock()
    repo = SimpleNamespace(
        list_open_positions=AsyncMock(
            return_value=[SimpleNamespace(condition_id="active", token_id="yes")]
        )
    )
    drift = SimpleNamespace(scan_user=AsyncMock(return_value=SimpleNamespace(clean=True)))
    readiness = ReadinessTracker()
    refresh = InventoryRuntimeRefresh(
        user_id="user", wallet="wallet", positions=repo, manager=manager, drift=drift
    )
    refresh.bind_readiness(readiness)
    await refresh.refresh()
    manager.retain_active_positions.assert_called_once_with({("active", "yes")})
    assert readiness.snapshot()["drift_ok"] is True


@pytest.mark.asyncio
async def test_multiple_accounts_do_not_clear_another_accounts_drift_failure():
    from pmcopy.orchestrators.account_inventory_startup import InventoryRuntimeRefresh

    readiness = ReadinessTracker()
    updates = []
    for user, clean in [("one", False), ("two", True)]:
        refresh = InventoryRuntimeRefresh(
            user_id=user,
            wallet=user,
            positions=SimpleNamespace(list_open_positions=AsyncMock(return_value=[])),
            manager=Mock(),
            drift=SimpleNamespace(scan_user=AsyncMock(return_value=SimpleNamespace(clean=clean))),
        )
        refresh.bind_readiness(readiness)
        updates.append(refresh)
    for refresh in updates:
        await refresh.refresh()
    assert readiness.snapshot()["drift_ok"] is False


def test_manager_removes_inactive_tracking_without_recording_a_trade():
    from pmcopy.services.positionManager import PositionManager

    manager = PositionManager()
    for condition in ("active", "settled"):
        manager.track_buy(
            condition_id=condition,
            asset="yes",
            entry_price=Decimal("0.4"),
            size_tokens=Decimal("10"),
            usdc_cost=Decimal("4"),
        )
    active = manager.get_position("active", "yes")
    manager.retain_active_positions({("active", "yes")})
    assert manager.get_position("active", "yes") is active
    assert manager.get_position("settled", "yes") is None
    assert manager.closed_positions == []


@pytest.mark.asyncio
async def test_refresh_reads_active_inventory_after_drift_prunes_a_stale_row():
    from pmcopy.orchestrators.account_inventory_startup import InventoryRuntimeRefresh

    rows = [SimpleNamespace(condition_id="stale", token_id="yes")]

    async def positions(user_id):
        return list(rows)

    async def scan(**kwargs):
        rows.clear()
        return SimpleNamespace(clean=True)

    manager = Mock()
    refresh = InventoryRuntimeRefresh(
        user_id="user",
        wallet="wallet",
        positions=SimpleNamespace(list_open_positions=positions),
        manager=manager,
        drift=SimpleNamespace(scan_user=scan),
    )
    await refresh.refresh()
    manager.retain_active_positions.assert_called_once_with(set())


def test_registering_an_unverified_account_invalidates_drift_readiness():
    readiness = ReadinessTracker()
    readiness.register_drift_account("one")
    readiness.update_account_drift("one", True)
    readiness.register_drift_account("two")
    assert readiness.snapshot()["drift_ok"] is False


@pytest.mark.asyncio
async def test_startup_scan_updates_the_same_account_truth_as_periodic_refresh():
    from pmcopy.orchestrators.runtime_bootstrap import startup_drift_check

    readiness = ReadinessTracker()
    for user in ("one", "two"):
        readiness.register_drift_account(user)
        readiness.update_account_drift(user, True)
    await startup_drift_check(
        runtime_users=[
            SimpleNamespace(user_id="one", wallet=SimpleNamespace(funder_address="wallet"))
        ],
        user_states={
            "one": SimpleNamespace(
                position_drift_service=SimpleNamespace(
                    scan_user=AsyncMock(return_value=SimpleNamespace(clean=False, paused_scopes=()))
                )
            )
        },
        config=SimpleNamespace(
            reconciliation=SimpleNamespace(position_drift=SimpleNamespace(check_on_startup=True))
        ),
        readiness=readiness,
    )
    readiness.update_account_drift("two", True)
    assert readiness.snapshot()["drift_ok"] is False
