"""A04 — one account-owned coordination state per follower account.

The per-user ``ExecutionCoordinator`` must own exactly ONE
``PositionCoordinator`` (per-scope lock map + intent registry), constructed in
``build_user_state`` and injected into every per-leader ``TradingContext`` for
that account. Before this fix each ``build_trading_context`` call built a NEW
``TradingContext`` (whose dataclass default factory minted a fresh
coordinator) and then re-pointed the SHARED coordinator's
``position_coordinator`` attribute at it — so the last-built leader binding
won the shared coordinator while earlier bindings kept private instances.
Copy locks (``trade_pipeline``), protective locks (``risk_monitor``), and the
execution funnel then serialized on DIFFERENT lock maps, breaking mutual
exclusion between copy and protective paths on the same token, and intent
recovery seeded one registry PER BINDING instead of one per account.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pmcopy.config.env import SignatureType

import pmcopy.config.app_config as app_config_mod
import pmcopy.orchestrators.runtime_bootstrap as runtime_bootstrap
from pmcopy.orchestrators.runtime_bootstrap import (
    build_context_maps,
    build_trading_context,
    build_user_state,
)
from pmcopy.services.execution import intent_recovery as _intent_recovery_module
from pmcopy.services.intentRegistry import IntentSide, Reservation
from pmcopy.services.multiUserConfig import UserFollowConfig
from pmcopy.services.positionActionCoordinator import PositionScope
from pmcopy.services.positionCoordinator import PositionCoordinator
from tests.config_helpers import load_test_config

# Captured at import time, before the autouse fixture patches the module
# attribute, so the account-scoping test exercises the real function.
_real_recover_intents_from_db = _intent_recovery_module.recover_intents_from_db

_VALID_PRIVATE_KEY = "0x" + "a" * 64
_VALID_FUNDER = "0x" + "b" * 40
_VALID_USER = "0x" + "c" * 40

LEADER_1 = "0x" + "1" * 40
LEADER_2 = "0x" + "2" * 40


def _config(**overrides):
    env = {
        "PRIVATE_KEY": _VALID_PRIVATE_KEY,
        "FUNDER_ADDRESS": _VALID_FUNDER,
        "USER_ADDRESS": _VALID_USER,
        "RPC_URL": "https://polygon-rpc.example.com",
        "CHAIN_ID": "137",
    }
    env.update(overrides)
    with patch.object(app_config_mod, "_runtime_python_supported", return_value=True):
        return load_test_config(env)


def _follow(user_id: str, leader_id: str) -> UserFollowConfig:
    return UserFollowConfig(
        user_id=user_id,
        leader_id=leader_id,
        enabled=True,
        allocation_pct=0.05,
        max_trade_usdc=50.0,
        max_market_exposure_usdc=300.0,
    )


def _build_account_state(user_id: str = "user-1") -> object:
    runtime_user = SimpleNamespace(
        user_id=user_id,
        wallet=SimpleNamespace(funder_address=_VALID_FUNDER),
    )
    with patch.dict("os.environ", {}, clear=False):
        return build_user_state(
            runtime_user,
            client=object(),
            pool=MagicMock(),
            config=_config(),
            clob_gateway=object(),
        )


@pytest.fixture(autouse=True)
def _quiet_bootstrap(monkeypatch):
    """Keep the bootstrap off live DB / inventory paths for these wiring tests."""
    monkeypatch.setattr(
        "pmcopy.orchestrators.runtime_bootstrap.rehydrate_position_manager",
        AsyncMock(return_value=None),
    )
    async def _no_recovery(*args, **kwargs):
        return 0

    monkeypatch.setattr(
        "pmcopy.services.execution.intent_recovery.recover_intents_from_db", _no_recovery
    )


# ---------------------------------------------------------------------------
# Ownership: constructed once per account, born wired into the coordinator
# ---------------------------------------------------------------------------


def test_build_user_state_owns_one_account_coordinator_born_into_execution_coordinator():
    state = _build_account_state()

    assert isinstance(state.position_coordinator, PositionCoordinator)
    # The shared per-user ExecutionCoordinator must be BORN with the account
    # coordinator — no post-construction rewiring in build_trading_context.
    assert state.execution_coordinator.position_coordinator is state.position_coordinator


@pytest.mark.asyncio
async def test_every_leader_binding_shares_the_account_coordinator():
    state = _build_account_state()

    ctx1 = await build_trading_context(state, _follow("user-1", LEADER_1), _config())
    ctx2 = await build_trading_context(state, _follow("user-1", LEADER_2), _config())

    # Same account-owned instance end-to-end: per-binding contexts, the shared
    # execution coordinator, and the runtime state all observe ONE coordinator.
    assert ctx1.position_coordinator is state.position_coordinator
    assert ctx2.position_coordinator is state.position_coordinator
    assert ctx1.position_coordinator is ctx2.position_coordinator
    assert ctx1.execution_coordinator is state.execution_coordinator
    assert ctx1.execution_coordinator.position_coordinator is state.position_coordinator
    assert ctx2.execution_coordinator.position_coordinator is state.position_coordinator


@pytest.mark.asyncio
async def test_risk_context_shares_the_account_coordinator():
    state = _build_account_state()
    runtime_user = SimpleNamespace(
        user_id="user-1",
        wallet=SimpleNamespace(funder_address=_VALID_FUNDER),
        follows={LEADER_1: _follow("user-1", LEADER_1), LEADER_2: _follow("user-1", LEADER_2)},
    )

    contexts_by_leader, risk_contexts_by_user = await build_context_maps(
        [runtime_user], {"user-1": state}, [LEADER_1, LEADER_2], _config()
    )

    assert len(contexts_by_leader[LEADER_1]) == 1
    assert len(contexts_by_leader[LEADER_2]) == 1
    risk_context = risk_contexts_by_user["user-1"]
    # The risk context (first user context) must use the SAME account-owned
    # coordinator — protective exits serialize against copy entries.
    assert risk_context is contexts_by_leader[LEADER_1][0]
    assert risk_context.position_coordinator is state.position_coordinator
    assert contexts_by_leader[LEADER_2][0].position_coordinator is state.position_coordinator


# ---------------------------------------------------------------------------
# Mutual exclusion: copy + protective paths on the SAME token share ONE lock map
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_same_token_scopes_serialize_across_leader_bindings_and_coordinator():
    state = _build_account_state()
    ctx1 = await build_trading_context(state, _follow("user-1", LEADER_1), _config())
    ctx2 = await build_trading_context(state, _follow("user-1", LEADER_2), _config())

    scope = PositionScope(user_id="user-1", condition_id="cond-1", token_id="token-1")
    async with ctx1.position_coordinator.acquire(scope):
        # A copy entry through binding 1 holds the account-owned lock; the
        # second binding (e.g. a protective exit routed through another
        # context) must observe the SAME locked scope.
        assert ctx2.position_coordinator.is_locked(scope)
        assert state.execution_coordinator.position_coordinator.is_locked(scope)
    assert not ctx2.position_coordinator.is_locked(scope)


@pytest.mark.asyncio
async def test_intent_opened_through_one_binding_blocks_the_shared_registry():
    state = _build_account_state()
    ctx1 = await build_trading_context(state, _follow("user-1", LEADER_1), _config())
    ctx2 = await build_trading_context(state, _follow("user-1", LEADER_2), _config())

    scope = PositionScope(user_id="user-1", condition_id="cond-1", token_id="token-1")
    await ctx1.position_coordinator.begin_intent(
        scope, IntentSide.BUY, Reservation(quote_usdc=Decimal("5")), intent_id="intent-1"
    )

    # One registry per account: the duplicate-intent block from binding 1 is
    # visible through binding 2 — and through the execution coordinator.
    assert ctx2.position_coordinator.open_intent_for(scope) is not None
    assert (
        state.execution_coordinator.position_coordinator.open_intent_for(scope) is not None
    )
    with pytest.raises(Exception):
        await ctx2.position_coordinator.begin_intent(
            scope, IntentSide.SELL, Reservation(token_shares=Decimal("1")), intent_id="intent-2"
        )


@pytest.mark.asyncio
async def test_account_budget_contention_runs_through_one_reservation_service():
    state = _build_account_state()
    ctx1 = await build_trading_context(state, _follow("user-1", LEADER_1), _config())
    ctx2 = await build_trading_context(state, _follow("user-1", LEADER_2), _config())

    # The reservation service (BUY quote / SELL share budget holds) is owned
    # by the account runtime: every binding contends on the same budget.
    assert ctx1.extra["reservation_service"] is ctx2.extra["reservation_service"]
    assert ctx1.extra["reservations_repo"] is ctx2.extra["reservations_repo"]
    assert ctx1.execution_coordinator is ctx2.execution_coordinator


# ---------------------------------------------------------------------------
# Restart: open intents restore ONE account-owned registry, once per account
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_intent_recovery_seeds_one_registry_once_per_account(monkeypatch):
    state = _build_account_state()
    calls: list = []

    async def _recorder(registry, reservations_repo, **kwargs):
        calls.append((registry, kwargs))
        return 2

    monkeypatch.setattr(
        "pmcopy.services.execution.intent_recovery.recover_intents_from_db", _recorder
    )

    ctx1 = await build_trading_context(state, _follow("user-1", LEADER_1), _config())
    ctx2 = await build_trading_context(state, _follow("user-1", LEADER_2), _config())

    # Once per ACCOUNT — intents from all leaders land in the one registry,
    # never one seeded registry per binding.
    assert len(calls) == 1, "intent recovery must run once per account, not per binding"
    assert calls[0][0] is state.position_coordinator.intent_registry
    assert calls[0][0] is ctx1.position_coordinator.intent_registry
    assert calls[0][0] is ctx2.position_coordinator.intent_registry
    assert calls[0][1].get("account_user_id") == "user-1"


def _reservation_row(user_id: str, reservation_id: str, condition_id: str = "cond-1"):
    return SimpleNamespace(
        id=reservation_id,
        order_id=None,
        venue_order_id=None,
        user_id=user_id,
        condition_id=condition_id,
        token_id="token-1",
        side="BUY",
        reserved_usdc=Decimal("5"),
        reserved_size=Decimal("0"),
        created_at=datetime(2026, 10, 11, 12, tzinfo=UTC),
    )


class _FakeReservationsRepo:
    def __init__(self, rows):
        self._rows = rows

    async def list_active(self):
        return list(self._rows)


@pytest.mark.asyncio
async def test_intent_recovery_scopes_restored_intents_to_the_account():
    rows = [
        _reservation_row("user-1", "res-a1"),
        _reservation_row("user-1", "res-a2", condition_id="cond-2"),
        _reservation_row("user-2", "res-b1"),
    ]
    registry = PositionCoordinator().intent_registry

    restored = await _real_recover_intents_from_db(
        registry,
        _FakeReservationsRepo(rows),
        account_user_id="user-1",
    )

    assert restored == 2
    assert registry.open_intent_count() == 2
    assert registry.open_intent_for(
        PositionScope(user_id="user-1", condition_id="cond-1", token_id="token-1")
    )
    # Another account's ownership must never enter this account's registry.
    assert not registry.open_intent_for(
        PositionScope(user_id="user-2", condition_id="cond-1", token_id="token-1")
    )


# ---------------------------------------------------------------------------
# Protection continuity: empty follows stop new entries but keep protection,
# through the SAME account-owned coordinator.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_empty_follows_stop_new_entries_but_keep_protection_on_account_coordinator(
    monkeypatch,
):
    state = _build_account_state()
    runtime_user = SimpleNamespace(
        user_id="user-1",
        wallet=SimpleNamespace(funder_address=_VALID_FUNDER),
        follows={},  # explicit empty whitelist: nothing new is copied
    )
    open_position = SimpleNamespace(token_id="token-1", condition_id="cond-1", size_tokens=Decimal("5"))
    monkeypatch.setattr(
        state.positions_repo,
        "list_open_positions",
        AsyncMock(return_value=[open_position]),
    )

    contexts_by_leader, risk_contexts_by_user = await build_context_maps(
        [runtime_user], {"user-1": state}, [LEADER_1], _config()
    )

    # No copy contexts for any leader — the empty whitelist adds no entries.
    assert contexts_by_leader[LEADER_1] == []
    # But the risk-only context exists for the open position and shares the
    # account-owned coordinator, so protective exits still serialize.
    risk_context = risk_contexts_by_user["user-1"]
    assert risk_context is not None
    assert risk_context.position_coordinator is state.position_coordinator
