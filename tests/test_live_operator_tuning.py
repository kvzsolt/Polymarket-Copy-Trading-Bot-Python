"""Preserved operator profile and explicitly enabled moderate-policy arithmetic.

Profile assertions track the non-secret YAML present at task start. Algorithm
fixtures separately opt into the tiered policy and cash reserve, which the
current operator profile leaves disabled. The synthetic 350-unit bankroll is
an arithmetic fixture, not evidence of a live balance or deployment.
"""

from __future__ import annotations

from decimal import Decimal

from config.app_config import AppConfig, load_config
from services.balance_regime import BalanceRegime
from services.risk.risk_engine import headroom_for_buy

BANKROLL = Decimal("350")
PER_COPY_CAP = Decimal("17.5")
DAILY_LOSS_STOP = Decimal("35")
MAX_EXPOSURE = Decimal("210")


def _overrides() -> dict:
    """Minimal non-secret overrides that let both environments load."""
    return {
        "database": {"dsn": "postgresql://test:test@localhost/test"},
        "wallet": {
            "funder_address": "0x" + "1" * 40,
            "private_key": "0x" + "a" * 64,
            "polygon_rpc_url": "https://rpc.invalid",
        },
        "polymarket": {"rpc_url": "https://rpc.invalid"},
        "network": {"rpc_url": "https://rpc.invalid"},
        # live/detection.yml selects an on-chain mode, which fails closed
        # without the wss:// endpoint the operator keeps in secrets.env.
        "detection": {
            "onchain": {"pending_tx_ws_url": "wss://polygon.example.invalid/v2/redacted"}
        },
        # live/control_plane.yml enables the operator API, which fails closed
        # without the bearer credential from secrets.env.
        "control_plane": {"auth": {"token": "test-control-credential"}},
    }


def _paper_and_live() -> tuple[AppConfig, AppConfig]:
    return load_config(_overrides()), load_config(_overrides(), environment="live")


def test_packaged_moderate_350_targets() -> None:
    """Both environments preserve the operator's shared sizing and risk knobs."""
    for config in _paper_and_live():
        sizing = config.sizing
        assert sizing.buy_max_amount == 17.5
        # Unchanged neighbours: the clamp floor, the proportional percentage,
        # and the 24 h-snapshot position cap all stay as shipped.
        assert sizing.buy_min_amount == 5.0
        assert sizing.percentage == 5.0
        assert sizing.max_position_pct == 0.07

        risk = config.risk
        assert risk.enable_balance_tiered_policy is False
        assert risk.balance_policy_reserve_pct == 0.40
        assert risk.balance_policy_reserve_floor == 35.0
        assert risk.balance_policy_hard_cap_pct == 0.05
        assert risk.balance_policy_hard_cap_ceiling == 17.5
        assert risk.balance_policy_max_open_positions == 12
        assert risk.max_open_positions == 25
        assert risk.balance_policy_allow_partial_exits is True
        assert risk.max_leader_exposure_usdc == 140.0
        assert risk.max_market_exposure_usdc == 35.0
        assert risk.max_category_exposure_usdc == 210.0
        assert risk.max_daily_drawdown_pct == 10.0

        execution = config.execution
        assert execution.max_position_size_usdc == 20.0
        assert execution.min_balance_headroom_usdc == 0.0
        # Gross-spend semantics would starve re-entries; the daily ceiling
        # stays disabled and the sub-min SELL policy stays at its default.
        assert execution.max_daily_spend_usdc == 0.0
        assert execution.sub_min_sell_policy == "round_up"

        exits = config.protective_exits
        assert exits.enable_stop_loss is True
        assert exits.stop_loss_threshold == 0.08
        assert exits.stop_loss_grace_period_seconds == 300.0


def test_headroom_matches_targets() -> None:
    """An explicitly enabled moderate policy retains its original arithmetic."""
    overrides = _overrides()
    overrides["risk"] = {"enable_balance_tiered_policy": True}
    config = load_config(overrides)

    # Fully liquid start: reserve = max(35, 350*0.40) = 140, tradable = 210,
    # hard cap = min(210, 350*0.05, 17.5) = 17.5 -> the per-copy target.
    fresh = headroom_for_buy(
        equity_usdc=BANKROLL,
        free_cash_usdc=BANKROLL,
        open_positions_count=0,
        config=config,
    )
    assert fresh.allow is True
    assert fresh.reason == "ok"
    assert fresh.cap_notional_usdc == PER_COPY_CAP

    # Fully deployed: 210 in positions leaves 140 free, exactly the reserve.
    deployed = headroom_for_buy(
        equity_usdc=BANKROLL,
        free_cash_usdc=BANKROLL - MAX_EXPOSURE,
        open_positions_count=6,
        config=config,
    )
    assert deployed.allow is False
    assert deployed.reason == "no_tradable_cash_after_reserve"

    # floor(210 / 17.5) = 12 concurrent copies.
    saturated = headroom_for_buy(
        equity_usdc=BANKROLL,
        free_cash_usdc=BANKROLL,
        open_positions_count=12,
        config=config,
    )
    assert saturated.allow is False
    assert saturated.reason == "max_open_positions_reached"

    # 10 % of the day-start equity is the daily-loss BUY stop.
    stopped = headroom_for_buy(
        equity_usdc=BANKROLL,
        free_cash_usdc=BANKROLL,
        open_positions_count=0,
        daily_loss_usdc=DAILY_LOSS_STOP,
        day_start_equity_usdc=BANKROLL,
        config=config,
    )
    assert stopped.allow is False
    assert stopped.reason == "no_buy_headroom"

    # Per-leader cap: 140 = 40 % of the bankroll (8 copies of one leader).
    leader_capped = headroom_for_buy(
        equity_usdc=BANKROLL,
        free_cash_usdc=BANKROLL,
        open_positions_count=0,
        leader_exposure_usdc=Decimal("140"),
        config=config,
    )
    assert leader_capped.allow is False
    assert leader_capped.reason == "no_buy_headroom"

    # Per-market cap: 35 = two copies in the same condition.
    market_capped = headroom_for_buy(
        equity_usdc=BANKROLL,
        free_cash_usdc=BANKROLL,
        open_positions_count=0,
        market_exposure_usdc=Decimal("35"),
        config=config,
    )
    assert market_capped.allow is False
    assert market_capped.reason == "no_buy_headroom"


def test_stop_loss_threshold_below_min_share_price() -> None:
    """The salvage stop-loss floor must sit under the entry price floor.

    ``PositionManager`` treats ``stop_loss_threshold`` as an absolute price,
    not a percentage drawdown, so a threshold at or above
    ``execution.min_share_price`` would make every freshly opened position
    eligible for a stop-loss exit the moment its grace period expires.
    """
    config = load_config(_overrides())

    assert config.protective_exits.stop_loss_threshold < config.execution.min_share_price
    assert config.protective_exits.stop_loss_threshold == 0.08
    assert config.execution.min_share_price == 0.10


def test_balance_regime_mirror() -> None:
    """An explicitly configured reserve is enforced independently of the profile."""
    overrides = _overrides()
    overrides["execution"] = {"min_balance_headroom_usdc": 140.0}
    config = load_config(overrides)
    regime = BalanceRegime.from_config(config.execution)

    allowed = regime.check_buy(notional_usdc=PER_COPY_CAP, free_cash_usdc=BANKROLL)
    assert allowed.allow is True
    assert allowed.reason == "ok"

    # 150 free - 17.5 = 132.5 < the 140 headroom floor.
    blocked = regime.check_buy(notional_usdc=PER_COPY_CAP, free_cash_usdc=Decimal("150"))
    assert blocked.allow is False
    assert blocked.reason == "balance_regime_headroom_floor"
    assert blocked.policy_name == "min_balance_headroom_usdc"

    # The absolute per-BUY cap leaves rounding slack above the 17.5 target.
    oversized = regime.check_buy(notional_usdc=Decimal("20.01"), free_cash_usdc=BANKROLL)
    assert oversized.allow is False
    assert oversized.reason == "balance_regime_position_size_exceeded"
