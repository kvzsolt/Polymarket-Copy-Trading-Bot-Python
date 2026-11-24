"""Unit tests for the runtime reservation reaper and phantom-prune helpers.

These cover the deadlock fix where a SELL reservation stranded mid-session (a
forced-exit FAK that returned ``submitted_unconfirmed`` and was kept active for
reconciliation) was never reaped during the periodic risk loop — only at
startup — so the loop spun forever on ``risk_exit_already_reserved`` and the
phantom position was never pruned.

The helpers are intentionally context-shape-light (they only read
``execution_coordinator`` / ``extra`` / ``user_id``), so a SimpleNamespace stand
-in is sufficient and we avoid the heavyweight Postgres integration harness.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from pmcopy.services.risk.risk_monitor import (
    _attempt_phantom_prune_on_no_fill,
    _reap_expired_reservations_for_context,
    _should_attempt_no_fill_prune,
)


def _ctx(*, coordinator=None, extra=None, user_id="0xUSER"):
    return SimpleNamespace(
        execution_coordinator=coordinator,
        extra=extra if extra is not None else {},
        user_id=user_id,
    )


# ── reaper ──────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_reap_releases_expired_via_coordinator_reservation_service():
    reservation_service = MagicMock()
    reservation_service.release_expired_active = AsyncMock(return_value=["r1", "r2"])
    coordinator = SimpleNamespace(reservation_service=reservation_service)

    count = await _reap_expired_reservations_for_context(_ctx(coordinator=coordinator))

    assert count == 2
    reservation_service.release_expired_active.assert_awaited_once()


@pytest.mark.asyncio
async def test_reap_resolves_reservation_service_from_extra():
    reservation_service = MagicMock()
    reservation_service.release_expired_active = AsyncMock(return_value=[])
    ctx = _ctx(coordinator=None, extra={"reservation_service": reservation_service})

    count = await _reap_expired_reservations_for_context(ctx)

    assert count == 0
    reservation_service.release_expired_active.assert_awaited_once()


@pytest.mark.asyncio
async def test_reap_noop_when_no_reservation_service():
    count = await _reap_expired_reservations_for_context(_ctx())
    assert count == 0


@pytest.mark.asyncio
async def test_reap_swallows_reaper_exceptions():
    reservation_service = MagicMock()
    reservation_service.release_expired_active = AsyncMock(side_effect=RuntimeError("db down"))
    coordinator = SimpleNamespace(reservation_service=reservation_service)

    # Must never propagate — a reaper failure cannot break the risk loop.
    count = await _reap_expired_reservations_for_context(_ctx(coordinator=coordinator))
    assert count == 0


# ── no-fill prune throttle ───────────────────────────────────────────────────


def test_should_attempt_no_fill_prune_throttles_within_interval():
    ctx = _ctx()
    kw = dict(condition_id="cond-1", token_id="tok-1", interval_seconds=30.0)

    assert _should_attempt_no_fill_prune(ctx, now=1000.0, **kw) is True  # first
    assert _should_attempt_no_fill_prune(ctx, now=1010.0, **kw) is False  # 10s later
    assert _should_attempt_no_fill_prune(ctx, now=1031.0, **kw) is True  # >30s later


def test_should_attempt_no_fill_prune_is_per_scope():
    ctx = _ctx()
    a = dict(condition_id="cond-A", token_id="tok-A", interval_seconds=30.0)
    b = dict(condition_id="cond-B", token_id="tok-B", interval_seconds=30.0)

    assert _should_attempt_no_fill_prune(ctx, now=1000.0, **a) is True
    # Different scope is not throttled by the first scope's attempt.
    assert _should_attempt_no_fill_prune(ctx, now=1000.0, **b) is True
    assert _should_attempt_no_fill_prune(ctx, now=1001.0, **a) is False


# ── phantom prune ────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_attempt_phantom_prune_returns_drift_result(monkeypatch):
    import pmcopy.orchestrators.pipeline.trade_pipeline as tp

    drift_reconciler = MagicMock()
    drift_reconciler.reconcile_position_drift = AsyncMock(
        return_value={"reconciled": True, "action": "deleted", "live_size": 0}
    )
    monkeypatch.setattr(tp, "get_reconciliation_service", lambda _ctx: drift_reconciler)

    result = await _attempt_phantom_prune_on_no_fill(
        _ctx(),
        condition_id="cond-1",
        token_id="tok-1",
        leader_id="0xLEAD",
    )

    assert result == {"reconciled": True, "action": "deleted", "live_size": 0}
    drift_reconciler.reconcile_position_drift.assert_awaited_once()


@pytest.mark.asyncio
async def test_attempt_phantom_prune_empty_on_error(monkeypatch):
    import pmcopy.orchestrators.pipeline.trade_pipeline as tp

    def _boom(_ctx):
        raise RuntimeError("no reconciler wired")

    monkeypatch.setattr(tp, "get_reconciliation_service", _boom)

    result = await _attempt_phantom_prune_on_no_fill(
        _ctx(),
        condition_id="cond-1",
        token_id="tok-1",
        leader_id=None,
    )
    assert result == {}
