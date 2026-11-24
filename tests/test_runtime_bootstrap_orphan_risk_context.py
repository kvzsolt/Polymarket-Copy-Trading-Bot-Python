"""P1-i: a user holding open copied positions must be risk-monitored even when
rotation/discovery leaves them with no enabled follow on any active leader.

Risk contexts were built only from (user x active-leader enabled follow). If the
active-leader set is empty, or a multi-user follow no longer intersects the
active leaders, that user's open positions went unmonitored and the protective
exits (stop-loss / trailing) never ran. ``build_context_maps`` now also builds a
risk-only context for any user with open positions, added to
``risk_contexts_by_user`` ONLY (never ``contexts_by_leader``) so it drives
protective exits without enabling new copy entries.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import pmcopy.orchestrators.bootstrap.runtime_bootstrap as rb


def _runtime_user(user_id="0xUser"):
    return SimpleNamespace(user_id=user_id, follows={"0xLeader": SimpleNamespace(enabled=True)})


def _user_state(open_positions):
    return SimpleNamespace(
        positions_repo=SimpleNamespace(list_open_positions=AsyncMock(return_value=open_positions))
    )


def _pos():
    return SimpleNamespace(token_id="t", condition_id="c", size_tokens=Decimal("5"))


@pytest.mark.asyncio
async def test_orphan_open_position_gets_risk_context_with_empty_active_leaders(monkeypatch):
    stub_ctx = SimpleNamespace(user_id="0xUser")
    monkeypatch.setattr(rb, "build_trading_context", AsyncMock(return_value=stub_ctx))

    ru = _runtime_user()
    states = {"0xUser": _user_state([_pos()])}

    contexts_by_leader, risk_by_user = await rb.build_context_maps(
        [ru], states, [], SimpleNamespace()
    )

    assert "0xUser" in risk_by_user, "open position must get a risk context"
    assert risk_by_user["0xUser"] is stub_ctx
    # No active leaders -> no trade-execution contexts (only protective exits).
    assert contexts_by_leader == {}


@pytest.mark.asyncio
async def test_no_open_positions_no_risk_context(monkeypatch):
    btc = AsyncMock()
    monkeypatch.setattr(rb, "build_trading_context", btc)

    ru = _runtime_user()
    states = {"0xUser": _user_state([])}

    _, risk_by_user = await rb.build_context_maps([ru], states, [], SimpleNamespace())

    assert risk_by_user == {}
    btc.assert_not_called()


@pytest.mark.asyncio
async def test_user_already_in_risk_contexts_not_rebuilt(monkeypatch):
    # User follows an ACTIVE leader -> gets a context the normal way; the
    # orphan pass must not build a second context or even query positions.
    active_ctx = SimpleNamespace(user_id="0xUser")
    btc = AsyncMock(return_value=active_ctx)
    monkeypatch.setattr(rb, "build_trading_context", btc)

    ru = _runtime_user()
    list_open = AsyncMock(return_value=[_pos()])
    states = {
        "0xUser": SimpleNamespace(positions_repo=SimpleNamespace(list_open_positions=list_open))
    }

    _, risk_by_user = await rb.build_context_maps([ru], states, ["0xLeader"], SimpleNamespace())

    assert risk_by_user["0xUser"] is active_ctx
    assert btc.await_count == 1  # built once via the active-leader path only
    list_open.assert_not_called()  # orphan pass skipped (already present)
