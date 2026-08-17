"""Regression: ExecutionCoordinator.execute_intent fails closed in live mode
when position_coordinator is missing.

Pre-fix: when ``position_coordinator is None`` the V2 path silently
downgraded to no-lock execution via ``_execute_intent_locked``. In live
mode two concurrent same-scope intents could both pass through
``reserve_for_intent`` and both POST to the venue, violating the
CLAUDE.md non-negotiable: "Same-position actions must serialize through
a coordinator."

The fix mirrors the PositionScope-import fail-closed branch directly
below: live-mode raises ``RuntimeError``; paper / test mode keeps the
no-lock fallback so unit tests can construct a coordinator without
wiring a PositionActionCoordinator.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from services.execution.coordinator import ExecutionCoordinator
from services.execution.reservation_service import ReservationService
from storage.postgres.fills_repo import FillsRepo
from storage.postgres.orders_repo import OrdersRepo
from storage.postgres.positions_repo import PositionsRepo


def _live_exec_service():
    cfg = SimpleNamespace(execution=SimpleNamespace(paper_trade_only=False))
    return SimpleNamespace(_config=cfg)


def _paper_exec_service():
    cfg = SimpleNamespace(execution=SimpleNamespace(paper_trade_only=True))
    return SimpleNamespace(_config=cfg)


def _coord(execution_service):
    # TST-03: spec=<class> so attribute typos surface as ``AttributeError``
    # at test time instead of silently returning a child MagicMock. None
    # of the awaitable repo methods are called in these tests (the live
    # branch fails closed earlier; the paper branch monkey-patches
    # ``_execute_intent_locked``) so plain spec is sufficient — no
    # AsyncMock needed here.
    return ExecutionCoordinator(
        reservation_service=MagicMock(spec=ReservationService),
        orders_repo=MagicMock(spec=OrdersRepo),
        fills_repo=MagicMock(spec=FillsRepo),
        positions_repo=MagicMock(spec=PositionsRepo),
        execution_service=execution_service,
        position_coordinator=None,  # the bug surface
    )


def _intent():
    return MagicMock(
        key=MagicMock(
            user_id="u",
            condition_id="c",
            token_id="1" * 64,
            side=MagicMock(value="BUY"),
            stable_key=lambda: "stable",
        )
    )


@pytest.mark.asyncio
async def test_live_mode_raises_when_position_coordinator_none():
    coord = _coord(_live_exec_service())
    with pytest.raises(RuntimeError, match=r"position_coordinator is None.*live mode"):
        await coord.execute_intent(_intent())


@pytest.mark.asyncio
async def test_paper_mode_keeps_no_lock_fallback(monkeypatch):
    """Paper / test mode should NOT raise — unit tests rely on the fallback."""
    coord = _coord(_paper_exec_service())

    sentinel = object()

    async def fake_locked(intent):
        return sentinel

    monkeypatch.setattr(coord, "_execute_intent_locked", fake_locked)

    result = await coord.execute_intent(_intent())
    assert result is sentinel
