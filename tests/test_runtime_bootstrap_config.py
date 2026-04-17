from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

from config.app_config import load_config
from orchestrators.runtime_bootstrap import (
    build_trading_context,
    configured_leaders,
    default_follow,
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


def test_build_trading_context_uses_typed_sizing_execution_values():
    cfg = _config()
    follow = default_follow("user-1", "leader-1", cfg)
    user_state = SimpleNamespace(
        user=SimpleNamespace(user_id="user-1", wallet=SimpleNamespace(funder_address=_VALID_FUNDER)),
        client=object(),
        positions_repo=object(),
        fills_repo=object(),
        orders_repo=object(),
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
