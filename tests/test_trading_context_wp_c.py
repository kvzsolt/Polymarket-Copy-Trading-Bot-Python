"""WP-C regression tests: typed ``TradingContext`` and account-scoped runtime state."""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pmcopy.application.runtime_state import (
    AccountExecutionRuntimeState,
    AccountRiskRuntimeState,
    ContextExtraBridge,
    resolve_enable_balance_tiered_policy,
)
from pmcopy.application.trading_context import TradingContext
from pmcopy.config.app_config import load_config
from pmcopy.orchestrators.runtime_bootstrap import (
    build_trading_context,
    build_user_state,
    default_follow,
)
from pmcopy.orchestrators.trade_pipeline import (
    _position_pct_cap_balance_usdc,
    _position_pct_cap_snapshot_is_fresh,
    get_reconciliation_service,
    seed_position_pct_cap_snapshot,
)
from pmcopy.services.circuitBreaker import CircuitBreaker
from pmcopy.services.multiUserConfig import RuntimeUser

_FUNDER_A = "0x" + "a" * 40
_FUNDER_B = "0x" + "b" * 40
_LEADER_1 = "0x" + "1" * 40
_LEADER_2 = "0x" + "2" * 40


def _make_config(*, enable_tiered: bool = False):
    return load_config(
        {
            "database": {"dsn": "postgresql://test:test@localhost/test"},
            "wallet": {
                "funder_address": _FUNDER_A,
                "polygon_rpc_url": "https://polygon-mainnet.g.alchemy.com/v2/test-key",
            },
            "polymarket": {
                "rpc_url": "https://polygon-mainnet.g.alchemy.com/v2/test-key",
            },
            "detection": {
                "onchain": {
                    "alchemy_wss_url": "wss://polygon-mainnet.g.alchemy.com/v2/test-key",
                }
            },
            "execution": {"trading_mode": "paper", "slippage_tolerance": 1.75},
            "risk": {"enable_balance_tiered_policy": enable_tiered},
        }
    )


def _runtime_user(user_id: str, funder: str, cfg) -> RuntimeUser:
    wallet = replace(cfg.wallet, funder_address=funder)
    return RuntimeUser(
        user_id=user_id,
        wallet=wallet,
        follows={
            _LEADER_1: default_follow(user_id, _LEADER_1, cfg),
            _LEADER_2: default_follow(user_id, _LEADER_2, cfg),
        },
        follows_explicit=True,
    )


@pytest.mark.asyncio
async def test_multi_leader_contexts_share_account_execution_and_risk_state() -> None:
    """All bindings of one follower share reconciler, 24h cap snapshot, and regime."""
    cfg = _make_config()
    user_a = _runtime_user("alpha", _FUNDER_A, cfg)
    state_a = build_user_state(
        user_a,
        client=MagicMock(),
        pool=MagicMock(),
        config=cfg,
        clob_gateway=MagicMock(),
    )

    with patch(
        "pmcopy.orchestrators.runtime_bootstrap.rehydrate_position_manager",
        new=AsyncMock(return_value=None),
    ):
        ctx_1 = await build_trading_context(state_a, user_a.follows[_LEADER_1], cfg)
        first_callback = state_a.execution_coordinator.reservation_service.reconciler_callback
        ctx_2 = await build_trading_context(state_a, user_a.follows[_LEADER_2], cfg)
        second_callback = state_a.execution_coordinator.reservation_service.reconciler_callback

    # Per-leader bindings stay distinct, while account-scoped state is shared.
    assert ctx_1.leader_wallet == _LEADER_1
    assert ctx_2.leader_wallet == _LEADER_2
    assert ctx_1.slippage_tolerance_bps == 175
    assert ctx_1.account_execution_state is state_a.account_execution_state
    assert ctx_2.account_execution_state is state_a.account_execution_state
    assert ctx_1.risk_state is state_a.risk_state
    assert ctx_2.risk_state is state_a.risk_state
    assert ctx_1.balance_regime is ctx_2.balance_regime
    assert first_callback is second_callback

    # Reconciler is unified across all leader bindings of the account.
    reconciler_1 = get_reconciliation_service(ctx_1)
    reconciler_2 = get_reconciliation_service(ctx_2)
    assert reconciler_1 is reconciler_2

    # Seeding the 24h MAX_POSITION_PCT snapshot on leader 1 (or risk context)
    # immediately covers leader 2 without needing a second RPC balance fetch.
    seed_position_pct_cap_snapshot(ctx_1, Decimal("450.25"), timestamp=1_700_000_000.0)
    assert _position_pct_cap_snapshot_is_fresh(ctx_2, now=1_700_000_100.0) is True
    with patch(
        "pmcopy.orchestrators.trade_pipeline.get_my_balance",
        new=AsyncMock(side_effect=AssertionError("RPC should not be called when fresh")),
    ):
        # Use current wall-clock seed so _position_pct_cap_balance_usdc sees freshness.
        seed_position_pct_cap_snapshot(ctx_1, Decimal("450.25"))
        assert await _position_pct_cap_balance_usdc(ctx_2) == Decimal("450.25")


@pytest.mark.asyncio
async def test_separate_follower_accounts_keep_isolated_state_but_share_venue_breaker() -> None:
    """Two follower accounts share the injected venue breaker without leaking account state."""
    cfg = _make_config()
    shared_breaker = CircuitBreaker.for_venue_config(cfg)
    user_a = _runtime_user("alpha", _FUNDER_A, cfg)
    user_b = _runtime_user("beta", _FUNDER_B, cfg)

    state_a = build_user_state(
        user_a,
        client=MagicMock(),
        pool=MagicMock(),
        config=cfg,
        clob_gateway=MagicMock(),
        venue_circuit_breaker=shared_breaker,
    )
    state_b = build_user_state(
        user_b,
        client=MagicMock(),
        pool=MagicMock(),
        config=cfg,
        clob_gateway=MagicMock(),
        venue_circuit_breaker=shared_breaker,
    )

    assert state_a.execution_service.circuit_breaker is shared_breaker
    assert state_b.execution_service.circuit_breaker is shared_breaker

    with patch(
        "pmcopy.orchestrators.runtime_bootstrap.rehydrate_position_manager",
        new=AsyncMock(return_value=None),
    ):
        ctx_a = await build_trading_context(state_a, user_a.follows[_LEADER_1], cfg)
        ctx_b = await build_trading_context(state_b, user_b.follows[_LEADER_1], cfg)

    assert ctx_a.account_execution_state is not ctx_b.account_execution_state
    assert ctx_a.risk_state is not ctx_b.risk_state
    assert ctx_a.balance_regime is not ctx_b.balance_regime
    assert get_reconciliation_service(ctx_a) is not get_reconciliation_service(ctx_b)

    seed_position_pct_cap_snapshot(ctx_a, Decimal("999.00"))
    assert _position_pct_cap_snapshot_is_fresh(ctx_b) is False


def test_context_extra_bridge_and_enable_balance_tiered_policy_resolution() -> None:
    """Legacy ``extra`` keys sync bidirectionally with typed fields; config flag resolves."""
    cfg_enabled = _make_config(enable_tiered=True)
    cfg_disabled = _make_config(enable_tiered=False)

    ctx = TradingContext(
        user_id="u1",
        wallet_address=_FUNDER_A,
        client=MagicMock(),
        positions_repo=MagicMock(),
        fills_repo=MagicMock(),
        orders_repo=MagicMock(),
        risk_blocks_repo=MagicMock(),
        pnl_tracker=MagicMock(),
        risk_engine=MagicMock(),
        accounting_service=MagicMock(),
        position_manager=MagicMock(),
        config=cfg_enabled,
        extra={"slippage_tolerance_bps": 325, "leader_wallet": _LEADER_1},
    )

    assert isinstance(ctx.extra, dict)
    assert isinstance(ctx.extra, ContextExtraBridge)
    assert isinstance(ctx.account_execution_state, AccountExecutionRuntimeState)
    assert isinstance(ctx.risk_state, AccountRiskRuntimeState)
    assert ctx.slippage_tolerance_bps == 325
    assert ctx.leader_wallet == _LEADER_1
    assert resolve_enable_balance_tiered_policy(ctx) is True

    ctx.config = cfg_disabled
    assert resolve_enable_balance_tiered_policy(ctx) is False

    # Explicit extra override on a configless SimpleNamespace test double also works.
    double = SimpleNamespace(extra={"enable_balance_tiered_policy": "true"})
    assert resolve_enable_balance_tiered_policy(double) is True
