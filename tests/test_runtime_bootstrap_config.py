from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import ANY, patch

import pytest
import config.app_config as app_config_mod
from config.app_config import load_config
import orchestrators.runtime_bootstrap as runtime_bootstrap
from orchestrators.runtime_bootstrap import (
    build_trading_context,
    configured_leaders,
    default_follow,
    startup_reconcile_unconfirmed,
)
from services.multiUserConfig import load_runtime_users


_VALID_PRIVATE_KEY = "0x" + "a" * 64
_VALID_FUNDER = "0x" + "b" * 40
_VALID_USER = "0x" + "c" * 40


def _config(**overrides):
    env = {
        "PRIVATE_KEY": _VALID_PRIVATE_KEY,
        "FUNDER_ADDRESS": _VALID_FUNDER,
        "USER_ADDRESS": _VALID_USER,
        "RPC_URL": "https://polygon-rpc.example.com",
        "CHAIN_ID": "137",
        "PERCENTAGE": "7",
        "BUY_MIN_AMOUNT": "3",
        "BUY_MAX_AMOUNT": "42",
        "MAX_MARKET_EXPOSURE_USDC": "123",
        "SLIPPAGE_TOLERANCE": "1.5",
    }
    env.update(overrides)
    with patch.object(app_config_mod, "_runtime_python_supported", return_value=True):
        return load_config(env)


def test_configured_leaders_uses_typed_config_wallets():
    cfg = _config(LEADER_WALLETS="0x" + "d" * 40 + ",0x" + "e" * 40)
    assert configured_leaders(cfg) == ["0x" + "d" * 40, "0x" + "e" * 40]


def test_default_follow_uses_typed_config_thresholds():
    cfg = _config()
    follow = default_follow("user-1", "leader-1", cfg)
    assert follow.allocation_pct == 0.07
    assert follow.max_trade_usdc == 42.0
    assert follow.max_market_exposure_usdc == 123.0


def test_load_runtime_users_single_user_uses_typed_config_defaults():
    cfg = _config(LEADER_WALLETS="0x" + "d" * 40)
    users = load_runtime_users(["0x" + "d" * 40], cfg)
    assert len(users) == 1
    user = users[0]
    assert user.wallet.funder_address == _VALID_FUNDER
    assert user.follows["0x" + "d" * 40].allocation_pct == 0.07
    assert user.follows["0x" + "d" * 40].max_trade_usdc == 42.0


@pytest.mark.asyncio
async def test_startup_reconcile_unconfirmed_dedupes_user_contexts_and_rehydrates_once():
    ctx1 = SimpleNamespace(user_id="user-1", execution_service=object())
    ctx2 = SimpleNamespace(user_id="user-1", execution_service=object())
    ctx3 = SimpleNamespace(user_id="user-2", execution_service=None)

    with (
        patch("orchestrators.trade_pipeline.reconcile_submitted_unconfirmed", side_effect=[2, 99]) as reconcile,
        patch("orchestrators.runtime_bootstrap.reconcile_unconfirmed_risk_orders", return_value=0) as reconcile_risk,
        patch("orchestrators.runtime_bootstrap.rehydrate_position_manager") as rehydrate,
    ):
        result = await startup_reconcile_unconfirmed([ctx1, ctx2, ctx3])

    assert result == {"user-1": 2}
    reconcile.assert_called_once_with(ctx1)
    reconcile_risk.assert_called_once_with(ctx1)
    rehydrate.assert_called_once_with(ctx1)


@pytest.mark.asyncio
async def test_startup_reconcile_unconfirmed_rehydrates_on_risk_only_convergence():
    ctx = SimpleNamespace(user_id="user-1", execution_service=object())

    with (
        patch("orchestrators.trade_pipeline.reconcile_submitted_unconfirmed", return_value=0) as reconcile,
        patch("orchestrators.runtime_bootstrap.reconcile_unconfirmed_risk_orders", return_value=3) as reconcile_risk,
        patch("orchestrators.runtime_bootstrap.rehydrate_position_manager") as rehydrate,
    ):
        result = await startup_reconcile_unconfirmed([ctx])

    assert result == {"user-1": 3}
    reconcile.assert_called_once_with(ctx)
    reconcile_risk.assert_called_once_with(ctx)
    rehydrate.assert_called_once_with(ctx)


def test_build_trading_context_uses_typed_sizing_execution_values():
    cfg = _config()
    follow = default_follow("user-1", "leader-1", cfg)
    risk_blocks_repo = object()
    user_state = SimpleNamespace(
        user=SimpleNamespace(user_id="user-1", wallet=SimpleNamespace(funder_address=_VALID_FUNDER)),
        client=object(),
        positions_repo=object(),
        fills_repo=object(),
        orders_repo=object(),
        risk_blocks_repo=risk_blocks_repo,
        pnl_tracker=object(),
        risk_engine=object(),
        accounting_service=object(),
        position_manager=object(),
        execution_service=object(),
    )

    with patch("orchestrators.runtime_bootstrap.rehydrate_position_manager"):
        ctx = build_trading_context(user_state, follow, cfg, readiness=object())

    assert ctx.buy_min_amount == 3.0
    assert ctx.buy_max_amount == 42.0
    assert ctx.extra["slippage_tolerance_bps"] == 150
    assert ctx.entry_permission_service is not None
    assert ctx.entry_permission_service._repo is risk_blocks_repo
    assert ctx.config is cfg
    assert ctx.copy_decision_service._config is cfg


def test_build_trading_context_constructs_copy_decision_service_only_with_injected_config():
    cfg = _config()
    follow = default_follow("user-1", "leader-1", cfg)
    user_state = SimpleNamespace(
        user=SimpleNamespace(user_id="user-1", wallet=SimpleNamespace(funder_address=_VALID_FUNDER)),
        client=object(),
        positions_repo=object(),
        fills_repo=object(),
        orders_repo=object(),
        risk_blocks_repo=object(),
        pnl_tracker=object(),
        risk_engine=object(),
        accounting_service=object(),
        position_manager=object(),
        execution_service=object(),
    )

    with (
        patch("domain.trading_context.CopyDecisionService") as copy_decision_cls,
        patch("orchestrators.runtime_bootstrap.rehydrate_position_manager"),
    ):
        build_trading_context(user_state, follow, cfg, readiness=object())

    copy_decision_cls.assert_called_once_with(config=cfg, leader_scorer=ANY)


def test_build_user_state_injects_typed_config_into_position_manager():
    cfg = _config()
    runtime_user = SimpleNamespace(
        user_id="user-1",
        wallet=SimpleNamespace(funder_address=_VALID_FUNDER),
    )

    with patch("orchestrators.runtime_bootstrap.PositionManager") as position_manager_cls:
        position_manager_instance = position_manager_cls.return_value
        state = runtime_bootstrap.build_user_state(
            runtime_user,
            client=object(),
            db_path="/tmp/runtime-bootstrap-test.db",
            config=cfg,
        )

    position_manager_cls.assert_called_once_with(config=cfg)
    assert state.position_manager is position_manager_instance


def test_copy_notional_prefers_typed_config_over_env():
    cfg = _config(
        SIZING_MODE="fixed",
        FIXED_AMOUNT="88",
        TRADE_MULTIPLIER="9",
        TIERED_TRADE_MULTIPLIERS="0-100:2.0",
        ADAPTIVE_SIZING_BASIS="performance",
        ADAPTIVE_MIN_PERCENT="5",
        ADAPTIVE_MAX_PERCENT="95",
        ADAPTIVE_THRESHOLD_USDC="777",
    )
    from domain.trading_context import TradingContext

    ctx = TradingContext(
        user_id="user-1",
        wallet_address=_VALID_FUNDER,
        client=object(),
        positions_repo=object(),
        fills_repo=object(),
        orders_repo=object(),
        risk_blocks_repo=object(),
        pnl_tracker=object(),
        risk_engine=object(),
        accounting_service=object(),
        position_manager=object(),
        config=cfg,
        buy_min_amount=3.0,
        buy_max_amount=42.0,
    )

    with patch.dict(
        "os.environ",
        {
            "SIZING_MODE": "proportional",
            "FIXED_AMOUNT": "1",
            "TRADE_MULTIPLIER": "1",
            "TIERED_TRADE_MULTIPLIERS": "",
            "ADAPTIVE_SIZING_BASIS": "trade_size",
            "ADAPTIVE_MIN_PERCENT": "0",
            "ADAPTIVE_MAX_PERCENT": "0",
            "ADAPTIVE_THRESHOLD_USDC": "1",
        },
        clear=False,
    ):
        result = ctx.copy_notional(Decimal("200"))

    assert result.strategy == "fixed"
    assert float(result.base_notional_usdc) == pytest.approx(88.0)
    assert float(result.final_notional_usdc) == pytest.approx(42.0)
