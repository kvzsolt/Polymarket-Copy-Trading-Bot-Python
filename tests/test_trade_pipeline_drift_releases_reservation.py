"""Regression: insufficient_balance SELL routes through reconcile_position_drift
so the active SELL reservation is released.

Pre-fix: trade_pipeline's insufficient_balance branch called the local
``_sync_sell_position_from_live_holdings`` helper which updated the
local position row but never touched the reservation store. Any active
SELL reservation for the scope remained ``active`` forever, blocking
every future SELL on the same scope via the balance gate.

Post-fix: routes the same branch through
``ReconciliationService.reconcile_position_drift``, which owns the
position update AND the reservation release on the deleted path.

Test pins: when drift_result.action == "deleted" and a reservation_service
is wired, ``release_terminal`` is invoked for the SELL reservation.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from services.execution.reconciliation_service import ReconciliationService


def _record(side: str = "SELL"):
    return MagicMock(
        id="res-1",
        side=MagicMock(value=side),
    )


@pytest.mark.asyncio
async def test_drift_deleted_path_releases_sell_reservation():
    """live_size=0 → delete local position + release the SELL reservation."""
    svc = ReconciliationService.__new__(ReconciliationService)
    svc._audit = lambda *a, **kw: None  # silence audit emission

    positions_repo = MagicMock()
    positions_repo.get_position = AsyncMock(
        return_value=MagicMock(
            size_tokens=Decimal("5"),
            avg_entry_price=Decimal("0.5"),
        )
    )
    positions_repo.delete_position = AsyncMock(return_value=None)
    positions_repo.upsert_position = AsyncMock(return_value=None)
    svc.positions_repo = positions_repo

    reservation_service = MagicMock()
    reservation_service.release_terminal = AsyncMock(return_value=None)
    reservations_repo = MagicMock()
    reservations_repo.list_active_for_scope = AsyncMock(return_value=[_record(side="SELL")])
    reservation_service.reservations_repo = reservations_repo

    async def live_fetcher():
        return Decimal("0")  # zero live — drift "deleted" branch

    result = await svc.reconcile_position_drift(
        user_id="u",
        condition_id="c",
        token_id="t",
        leader_id="L",
        live_position_fetcher=live_fetcher,
        reservation_service=reservation_service,
    )

    assert result["action"] == "deleted"
    assert result["reconciled"] is True
    positions_repo.delete_position.assert_called_once_with("u", "c", token_id="t")
    reservation_service.release_terminal.assert_called_once_with(
        "res-1", reason="manual_position_drift_deleted"
    )


@pytest.mark.asyncio
async def test_drift_deleted_path_skips_non_sell_reservation():
    """BUY reservations on the scope must not be released by SELL drift."""
    svc = ReconciliationService.__new__(ReconciliationService)
    svc._audit = lambda *a, **kw: None

    positions_repo = MagicMock()
    positions_repo.get_position = AsyncMock(
        return_value=MagicMock(
            size_tokens=Decimal("3"),
            avg_entry_price=Decimal("0.5"),
        )
    )
    positions_repo.delete_position = AsyncMock(return_value=None)
    positions_repo.upsert_position = AsyncMock(return_value=None)
    svc.positions_repo = positions_repo

    reservation_service = MagicMock()
    reservation_service.release_terminal = AsyncMock(return_value=None)
    reservations_repo = MagicMock()
    # Only a BUY reservation — must NOT be released by a SELL-drift path.
    reservations_repo.list_active_for_scope = AsyncMock(return_value=[_record(side="BUY")])
    reservation_service.reservations_repo = reservations_repo

    async def live_fetcher():
        return Decimal("0")

    await svc.reconcile_position_drift(
        user_id="u",
        condition_id="c",
        token_id="t",
        leader_id="L",
        live_position_fetcher=live_fetcher,
        reservation_service=reservation_service,
    )

    reservation_service.release_terminal.assert_not_called()
