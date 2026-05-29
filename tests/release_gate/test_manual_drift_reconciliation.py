"""Release-gate: manual/off-bot drift reconciliation (Issue #75 item 11).

When a SELL fails with ``insufficient_balance`` the reconciler must:
* fetch live holdings;
* delete the local position if live size is zero;
* shrink it if live size is smaller;
* emit ``manual_position_drift_reconciled`` so operators can audit drift.
"""
from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from services.execution.reconciliation_service import ReconciliationService


def _make_service(local_size: Decimal | None) -> tuple[ReconciliationService, MagicMock, list]:
    audit_events: list[tuple[str, dict]] = []
    audit_logger = SimpleNamespace(
        log_event=lambda event, payload: audit_events.append((event, payload))
    )
    positions_repo = MagicMock()
    if local_size is None:
        positions_repo.get_position = AsyncMock(return_value=None)
    else:
        positions_repo.get_position = AsyncMock(
            return_value=SimpleNamespace(
                size_tokens=local_size,
                avg_entry_price=Decimal("0.50"),
            )
        )
    positions_repo.delete_position = AsyncMock(return_value=None)
    positions_repo.upsert_position = AsyncMock(return_value=None)
    svc = ReconciliationService(
        orders_repo=MagicMock(),
        fills_repo=MagicMock(),
        positions_repo=positions_repo,
        audit_logger=audit_logger,
    )
    return svc, positions_repo, audit_events


@pytest.mark.asyncio
async def test_drift_deletes_local_when_live_size_zero() -> None:
    svc, positions_repo, audit_events = _make_service(Decimal("5"))
    fetcher = AsyncMock(return_value=Decimal("0"))
    result = await svc.reconcile_position_drift(
        user_id="user-1",
        condition_id="0xcond",
        token_id="0xtok",
        leader_id="leader-1",
        live_position_fetcher=fetcher,
    )
    assert result["action"] == "deleted"
    assert result["reconciled"] is True
    positions_repo.delete_position.assert_called_once()
    assert any(name == "manual_position_drift_reconciled" for name, _ in audit_events)


@pytest.mark.asyncio
async def test_drift_shrinks_local_when_live_size_smaller() -> None:
    svc, positions_repo, audit_events = _make_service(Decimal("5"))
    fetcher = AsyncMock(return_value=Decimal("2"))
    result = await svc.reconcile_position_drift(
        user_id="user-1",
        condition_id="0xcond",
        token_id="0xtok",
        leader_id="leader-1",
        live_position_fetcher=fetcher,
    )
    assert result["action"] == "shrunk"
    assert result["reconciled"] is True
    positions_repo.upsert_position.assert_called_once()
    assert any(name == "manual_position_drift_reconciled" for name, _ in audit_events)


@pytest.mark.asyncio
async def test_drift_noop_when_local_matches_live() -> None:
    svc, _, _ = _make_service(Decimal("5"))
    fetcher = AsyncMock(return_value=Decimal("5"))
    result = await svc.reconcile_position_drift(
        user_id="user-1",
        condition_id="0xcond",
        token_id="0xtok",
        leader_id="leader-1",
        live_position_fetcher=fetcher,
    )
    assert result["action"] == "no_drift"
    assert result["reconciled"] is False


@pytest.mark.asyncio
async def test_drift_handles_fetch_failure() -> None:
    svc, _, audit_events = _make_service(Decimal("5"))
    fetcher = AsyncMock(side_effect=RuntimeError("upstream down"))
    result = await svc.reconcile_position_drift(
        user_id="user-1",
        condition_id="0xcond",
        token_id="0xtok",
        leader_id="leader-1",
        live_position_fetcher=fetcher,
    )
    assert result["action"] == "fetch_failed"
    assert result["reconciled"] is False
    assert any(name == "manual_position_drift_fetch_failed" for name, _ in audit_events)
