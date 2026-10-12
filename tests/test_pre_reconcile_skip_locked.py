"""Risk-loop pre-reconcile must not queue behind held position-scope locks.

Live failure (2026-07-04 02:17): ``Risk pre-reconcile TIMEOUT ... after 12.0s``
fired right after ``manual_sync_run`` and again seconds after a new BUY was
executing. Both of those hold the per-scope ``PositionCoordinator`` lock while
doing venue round-trips, so the risk loop's pre-reconcile — which acquires the
SAME scope lock per unconfirmed order — parked behind them until the outer
``asyncio.wait_for`` ceiling cancelled the whole batch.

Fix: the risk-loop entrypoint passes ``skip_locked_scopes=True`` so both
reconcilers do a non-blocking ``is_locked`` check and skip records whose scope
is actively being converged by another consumer (the lock holder applies the
fill, or the standalone reconciler / next tick picks the row up again).
Default behaviour (``False``) is unchanged for the ``execute_trades`` path,
which relies on blocking to order BUY visibility ahead of SELL signals.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pmcopy.domain.position_key import PositionKey
from pmcopy.services.positionCoordinator import PositionCoordinator

_TOKEN_ID = "1234567890"  # 10 digits — passes is_valid_clob_token_id


def _trade_context(coordinator: PositionCoordinator) -> SimpleNamespace:
    record = {
        "user_id": "u1",
        "condition_id": "c1",
        "asset": _TOKEN_ID,
        "order_id": "o1",
    }
    orders_repo = SimpleNamespace(
        get_unconfirmed_buys=AsyncMock(return_value=[record]),
        get_unconfirmed_sells=AsyncMock(return_value=[]),
    )
    return SimpleNamespace(
        user_id="u1",
        execution_service=object(),
        orders_repo=orders_repo,
        position_coordinator=coordinator,
        config=None,
        extra={},
    )


def _trade_scope() -> PositionKey:
    return PositionKey(user_id="u1", condition_id="c1", token_id=_TOKEN_ID)


def _risk_context(coordinator: PositionCoordinator) -> SimpleNamespace:
    record = {
        "order_id": "or1",
        "request": {
            "signal_id": "risk:sl:c1:x",
            "token_id": _TOKEN_ID,
            "condition_id": "c1",
        },
    }
    orders_repo = SimpleNamespace(
        get_unconfirmed_order_submissions=AsyncMock(return_value=[record]),
    )
    return SimpleNamespace(
        user_id="u1",
        execution_service=object(),
        orders_repo=orders_repo,
        position_coordinator=coordinator,
        pnl_tracker=MagicMock(),
        positions_repo=SimpleNamespace(get_position=AsyncMock(return_value=None)),
        config=None,
        extra={},
    )


@pytest.mark.asyncio
async def test_trade_reconcile_skips_locked_scope_when_flagged():
    from pmcopy.orchestrators.trade_pipeline import reconcile_submitted_unconfirmed

    coordinator = PositionCoordinator()
    context = _trade_context(coordinator)

    reconcile_one = AsyncMock(return_value=True)
    with patch("pmcopy.orchestrators.trade_pipeline._reconcile_one", reconcile_one):
        async with coordinator.acquire(_trade_scope()):
            reconciled = await asyncio.wait_for(
                reconcile_submitted_unconfirmed(context, skip_locked_scopes=True),
                timeout=1.0,
            )

    assert reconciled == 0
    reconcile_one.assert_not_awaited()


@pytest.mark.asyncio
async def test_trade_reconcile_processes_unlocked_scope_when_flagged():
    from pmcopy.orchestrators.trade_pipeline import reconcile_submitted_unconfirmed

    coordinator = PositionCoordinator()
    context = _trade_context(coordinator)

    reconcile_one = AsyncMock(return_value=True)
    with patch("pmcopy.orchestrators.trade_pipeline._reconcile_one", reconcile_one):
        reconciled = await asyncio.wait_for(
            reconcile_submitted_unconfirmed(context, skip_locked_scopes=True),
            timeout=1.0,
        )

    assert reconciled == 1
    reconcile_one.assert_awaited()


@pytest.mark.asyncio
async def test_trade_reconcile_default_still_blocks_until_lock_released():
    """execute_trades path (default flag) must keep waiting on the lock —
    BUY-visible-before-SELL ordering depends on it."""
    from pmcopy.orchestrators.trade_pipeline import reconcile_submitted_unconfirmed

    coordinator = PositionCoordinator()
    context = _trade_context(coordinator)

    reconcile_one = AsyncMock(return_value=True)
    with patch("pmcopy.orchestrators.trade_pipeline._reconcile_one", reconcile_one):
        release = asyncio.Event()

        async def _hold_lock() -> None:
            async with coordinator.acquire(_trade_scope()):
                await release.wait()

        holder = asyncio.create_task(_hold_lock())
        await asyncio.sleep(0)  # holder acquires the lock

        task = asyncio.create_task(reconcile_submitted_unconfirmed(context))
        await asyncio.sleep(0.05)
        assert not task.done(), "default path must block on the held scope lock"
        reconcile_one.assert_not_awaited()

        release.set()
        reconciled = await asyncio.wait_for(task, timeout=1.0)
        await holder

    assert reconciled == 1
    reconcile_one.assert_awaited()


@pytest.mark.asyncio
async def test_risk_reconcile_skips_locked_scope_when_flagged():
    from pmcopy.services.risk.risk_monitor import reconcile_unconfirmed_risk_orders

    coordinator = PositionCoordinator()
    context = _risk_context(coordinator)

    reconciler = MagicMock()
    reconciler.reconcile_risk_record = AsyncMock(return_value=True)
    with patch(
        "pmcopy.orchestrators.trade_pipeline.get_reconciliation_service",
        return_value=reconciler,
    ):
        async with coordinator.acquire(_trade_scope()):
            reconciled = await asyncio.wait_for(
                reconcile_unconfirmed_risk_orders(context, skip_locked_scopes=True),
                timeout=1.0,
            )

    assert reconciled == 0
    reconciler.reconcile_risk_record.assert_not_awaited()


@pytest.mark.asyncio
async def test_risk_reconcile_processes_unlocked_scope_when_flagged():
    from pmcopy.services.risk.risk_monitor import reconcile_unconfirmed_risk_orders

    coordinator = PositionCoordinator()
    context = _risk_context(coordinator)

    reconciler = MagicMock()
    reconciler.reconcile_risk_record = AsyncMock(return_value=True)
    with patch(
        "pmcopy.orchestrators.trade_pipeline.get_reconciliation_service",
        return_value=reconciler,
    ):
        reconciled = await asyncio.wait_for(
            reconcile_unconfirmed_risk_orders(context, skip_locked_scopes=True),
            timeout=1.0,
        )

    assert reconciled == 1
    reconciler.reconcile_risk_record.assert_awaited()


@pytest.mark.asyncio
async def test_risk_loop_pre_reconcile_passes_skip_locked():
    """The risk-loop entrypoint opts in to non-blocking behaviour for BOTH
    reconcilers — that is the actual production fix."""
    from pmcopy.services.risk.risk_monitor import _pre_reconcile_for_context

    context = SimpleNamespace(user_id="u1", execution_service=object(), extra={})

    trade_rec = AsyncMock(return_value=0)
    risk_rec = AsyncMock(return_value=0)
    with (
        patch("pmcopy.orchestrators.trade_pipeline.reconcile_submitted_unconfirmed", trade_rec),
        patch("pmcopy.services.risk.risk_monitor.reconcile_unconfirmed_risk_orders", risk_rec),
    ):
        await _pre_reconcile_for_context(context)

    trade_rec.assert_awaited_once()
    assert trade_rec.await_args.kwargs.get("skip_locked_scopes") is True
    risk_rec.assert_awaited_once()
    assert risk_rec.await_args.kwargs.get("skip_locked_scopes") is True
