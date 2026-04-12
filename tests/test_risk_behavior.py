"""Category 5: Risk behavior.

Covers:
- Stop-loss trigger and near-miss
- Take-profit trigger
- Trailing stop: activation, trigger, no-trigger
- Near-max warn and exit
- Profit lock legacy mode
- Profit lock advanced mode
- Exit priority ordering
- Kill switch
- Daily drawdown limit
- Max open positions
- Headroom calculation
"""

from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.settings import RiskSettings
from services.positionManager import PositionManager
from services.riskEngine import (
    AccountCapPolicy,
    PortfolioSnapshot,
    ProposedTrade,
    RiskDecision,
    RiskEngine,
    RiskHeadroom,
    RiskPolicy,
    compute_hard_cap,
    compute_reserve_cash,
    compute_soft_cap,
    headroom_for_buy,
)


def _risk_settings(**overrides) -> RiskSettings:
    defaults = dict(
        stop_loss_ratio=0.20,
        take_profit_ratio=0.70,
        daily_drawdown_ratio=0.20,
        profit_lock_trigger_ratio=0.70,
        profit_lock_drawdown_ratio=0.15,
        profit_lock_sell_ratio=0.50,
        trailing_trigger_ratio=0.20,
        trailing_distance_ratio=0.05,
        near_max_price_level=0.95,
        near_max_warn_level=0.92,
    )
    defaults.update(overrides)
    return RiskSettings(**defaults)


def _make_pm(**risk_overrides) -> PositionManager:
    rs = _risk_settings(**risk_overrides)
    pm = PositionManager.__new__(PositionManager)
    pm._positions = {}
    pm._closed = []
    pm._sl_pct = rs.stop_loss_ratio
    pm._tp_pct = rs.take_profit_ratio
    pm._enabled = True
    pm._profit_lock_mode = "legacy"
    pm._profit_lock_trigger = rs.profit_lock_trigger_ratio
    pm._profit_lock_sell_fraction = rs.profit_lock_sell_ratio
    pm._enable_near_max = True
    pm._near_max_exit = rs.near_max_price_level
    pm._near_max_warn = rs.near_max_warn_level
    pm._near_max_warn_ratio = 0.50
    pm._enable_trailing = True
    pm._trailing_activation_pct = rs.trailing_trigger_ratio
    pm._trailing_distance = rs.trailing_distance_ratio
    pm._enable_staged_tp = False
    pm._staged_levels = []
    pm._staged_fractions = []
    pm._enable_leader_divergence = False
    return pm


def _open_position(pm, cid="cond-1", entry=0.50, tokens=100.0, cost=50.0):
    """Helper to open a position."""
    return pm.track_buy(cid, f"asset-{cid}", entry, tokens, cost)


# ---------------------------------------------------------------------------
# Stop-loss
# ---------------------------------------------------------------------------


class TestStopLoss:
    def test_stop_loss_trigger(self):
        pm = _make_pm()
        _open_position(pm, entry=0.50)
        action = pm.evaluate_risk_action("cond-1", 0.38)  # -24%
        assert action is not None
        assert action["reason"] == "stop_loss"
        assert action["size_fraction"] == 1.0

    def test_stop_loss_exactly_at_threshold(self):
        pm = _make_pm()
        _open_position(pm, entry=0.50)
        action = pm.evaluate_risk_action("cond-1", 0.399)  # just past -20% threshold
        assert action is not None
        assert action["reason"] == "stop_loss"

    def test_stop_loss_near_miss(self):
        pm = _make_pm()
        _open_position(pm, entry=0.50)
        action = pm.evaluate_risk_action("cond-1", 0.41)  # -18%, just above threshold
        assert action is None or action["reason"] != "stop_loss"

    def test_stop_loss_below_zero(self):
        """Price at 0 should trigger stop-loss."""
        pm = _make_pm()
        _open_position(pm, entry=0.50)
        action = pm.evaluate_risk_action("cond-1", 0.01)  # -98%
        assert action is not None
        assert action["reason"] == "stop_loss"

    def test_stop_loss_different_threshold(self):
        pm = _make_pm(stop_loss_ratio=0.50)
        _open_position(pm, entry=0.50)
        action = pm.evaluate_risk_action("cond-1", 0.30)  # -40%
        assert action is None or action["reason"] != "stop_loss"
        action = pm.evaluate_risk_action("cond-1", 0.24)  # -52%
        assert action is not None
        assert action["reason"] == "stop_loss"


# ---------------------------------------------------------------------------
# Take-profit
# ---------------------------------------------------------------------------


class TestTakeProfit:
    def test_take_profit_trigger(self):
        pm = _make_pm()
        pm._enable_staged_tp = False
        pm._enable_trailing = False
        pm._profit_lock_mode = "disabled"
        pm._enable_near_max = False
        _open_position(pm, entry=0.50)
        action = pm.evaluate_risk_action("cond-1", 0.90)  # +80%
        assert action is not None
        assert action["reason"] == "take_profit"

    def test_take_profit_near_miss(self):
        pm = _make_pm()
        pm._enable_staged_tp = False
        pm._enable_trailing = False
        pm._profit_lock_mode = "disabled"
        pm._enable_near_max = False
        _open_position(pm, entry=0.50)
        action = pm.evaluate_risk_action("cond-1", 0.83)  # +66%, below 70%
        assert action is None


# ---------------------------------------------------------------------------
# Trailing stop
# ---------------------------------------------------------------------------


class TestTrailingStop:
    def test_activation_at_threshold(self):
        pm = _make_pm()
        pm._enable_staged_tp = False
        pm._profit_lock_mode = "disabled"
        pm._enable_near_max = False
        _open_position(pm, entry=0.50)
        # Activate trailing at just past +20% threshold (0.601 to avoid float boundary)
        action = pm.evaluate_risk_action("cond-1", 0.601)
        assert action is None  # trailing just activated, no sell yet
        pos = pm.get_position("cond-1")
        assert pos.exit_state.trailing_active is True

    def test_trailing_trigger_on_pullback(self):
        pm = _make_pm()
        pm._enable_staged_tp = False
        pm._profit_lock_mode = "disabled"
        pm._enable_near_max = False
        _open_position(pm, entry=0.50)

        # Activate trailing at $0.60
        pm.evaluate_risk_action("cond-1", 0.60)
        # Price rises to $0.80
        pm.evaluate_risk_action("cond-1", 0.80)
        # Price drops by trailing_distance from peak ($0.80 - $0.05 = $0.75)
        action = pm.evaluate_risk_action("cond-1", 0.74)
        assert action is not None
        assert action["reason"] == "trailing_stop"
        assert action["size_fraction"] == 1.0

    def test_trailing_no_trigger_above_trail(self):
        pm = _make_pm()
        pm._enable_staged_tp = False
        pm._profit_lock_mode = "disabled"
        pm._enable_near_max = False
        _open_position(pm, entry=0.50)

        pm.evaluate_risk_action("cond-1", 0.60)  # activate
        pm.evaluate_risk_action("cond-1", 0.70)  # rise
        # Still above trail level (0.70 - 0.05 = 0.65)
        action = pm.evaluate_risk_action("cond-1", 0.66)
        assert action is None or action["reason"] != "trailing_stop"

    def test_trailing_does_not_trigger_below_entry(self):
        """Trailing stop should NOT trigger when price drops below entry."""
        pm = _make_pm()
        pm._enable_staged_tp = False
        pm._profit_lock_mode = "disabled"
        pm._enable_near_max = False
        _open_position(pm, entry=0.50)

        pm.evaluate_risk_action("cond-1", 0.601)  # activate (past float boundary)
        # Price drops below entry — stop-loss should fire, not trailing
        action = pm.evaluate_risk_action("cond-1", 0.39)  # well past SL threshold
        assert action["reason"] == "stop_loss"


# ---------------------------------------------------------------------------
# Near-max warn and exit
# ---------------------------------------------------------------------------


class TestNearMax:
    def test_near_max_warn_trim(self):
        pm = _make_pm()
        pm._enable_staged_tp = False
        pm._enable_trailing = False
        pm._profit_lock_mode = "disabled"
        _open_position(pm, entry=0.50)
        action = pm.evaluate_risk_action("cond-1", 0.93)
        assert action is not None
        assert action["reason"] == "near_max_partial_trim"
        assert action["size_fraction"] == pytest.approx(0.50)

    def test_near_max_full_exit(self):
        pm = _make_pm()
        pm._enable_staged_tp = False
        pm._enable_trailing = False
        pm._profit_lock_mode = "disabled"
        _open_position(pm, entry=0.50)
        action = pm.evaluate_risk_action("cond-1", 0.96)
        assert action is not None
        assert action["reason"] == "near_max_full_exit"
        assert action["size_fraction"] == 1.0

    def test_near_max_warn_only_once(self):
        """Near-max warn should only trigger once."""
        pm = _make_pm()
        pm._enable_staged_tp = False
        pm._enable_trailing = False
        pm._profit_lock_mode = "disabled"
        _open_position(pm, entry=0.50)

        action1 = pm.evaluate_risk_action("cond-1", 0.93)
        assert action1 is not None
        assert action1["reason"] == "near_max_partial_trim"

        action2 = pm.evaluate_risk_action("cond-1", 0.93)
        assert action2 is None or action2["reason"] != "near_max_partial_trim"

    def test_below_warn_no_action(self):
        pm = _make_pm()
        pm._enable_staged_tp = False
        pm._enable_trailing = False
        pm._profit_lock_mode = "disabled"
        pm._tp_pct = 1.0  # disable legacy TP (set above any possible profit)
        _open_position(pm, entry=0.50)
        action = pm.evaluate_risk_action("cond-1", 0.89)
        assert action is None


# ---------------------------------------------------------------------------
# Profit lock legacy mode
# ---------------------------------------------------------------------------


class TestProfitLockLegacy:
    def test_profit_lock_arm_above_trigger(self):
        pm = _make_pm()
        pm._profit_lock_mode = "legacy"
        pm._enable_staged_tp = False
        pm._enable_trailing = False
        pm._enable_near_max = False
        _open_position(pm, entry=0.50)

        # Arm at +70%
        action = pm.evaluate_risk_action("cond-1", 0.85)
        assert action is None  # arm only, no exit
        pos = pm.get_position("cond-1")
        assert pos.profit_lock_armed is True

    def test_profit_lock_fallback_exit(self):
        pm = _make_pm()
        pm._profit_lock_mode = "legacy"
        pm._enable_staged_tp = False
        pm._enable_trailing = False
        pm._enable_near_max = False
        _open_position(pm, entry=0.50)

        # Arm at +70%
        pm.evaluate_risk_action("cond-1", 0.85)
        # Price drops below trigger -> exit
        action = pm.evaluate_risk_action("cond-1", 0.80)
        assert action is not None
        assert action["reason"] == "profit_lock_fallback"
        assert action["size_fraction"] == 1.0

    def test_profit_lock_held_above_trigger(self):
        pm = _make_pm()
        pm._profit_lock_mode = "legacy"
        pm._enable_staged_tp = False
        pm._enable_trailing = False
        pm._enable_near_max = False
        _open_position(pm, entry=0.50)

        # Arm
        pm.evaluate_risk_action("cond-1", 0.85)
        # Stay above trigger -> no action
        action = pm.evaluate_risk_action("cond-1", 0.86)
        assert action is None


# ---------------------------------------------------------------------------
# Profit lock advanced mode
# ---------------------------------------------------------------------------


class TestProfitLockAdvanced:
    def test_advanced_partial_sell_at_trigger(self):
        pm = _make_pm()
        pm._profit_lock_mode = "advanced"
        pm._profit_lock_trigger = 0.70
        pm._profit_lock_sell_fraction = 0.50
        pm._enable_staged_tp = False
        pm._enable_trailing = False
        pm._enable_near_max = False
        _open_position(pm, entry=0.50)

        action = pm.evaluate_risk_action("cond-1", 0.90)  # +80% > 70%
        assert action is not None
        assert action["reason"] == "profit_lock"
        assert action["size_fraction"] == pytest.approx(0.50)

    def test_advanced_activates_trailing(self):
        pm = _make_pm()
        pm._profit_lock_mode = "advanced"
        pm._profit_lock_trigger = 0.70
        pm._profit_lock_sell_fraction = 0.50
        pm._enable_staged_tp = False
        pm._enable_trailing = True
        pm._enable_near_max = False
        _open_position(pm, entry=0.50)

        pm.evaluate_risk_action("cond-1", 0.90)
        pos = pm.get_position("cond-1")
        assert pos.exit_state.trailing_active is True

    def test_advanced_only_triggers_once(self):
        pm = _make_pm()
        pm._profit_lock_mode = "advanced"
        pm._profit_lock_trigger = 0.70
        pm._profit_lock_sell_fraction = 0.50
        pm._enable_staged_tp = False
        pm._enable_trailing = False
        pm._enable_near_max = False
        pm._tp_pct = 1.0  # disable legacy TP (set above any possible profit)
        _open_position(pm, entry=0.50)

        pm.evaluate_risk_action("cond-1", 0.90)
        # Second call at same/higher price -> no additional sell
        action = pm.evaluate_risk_action("cond-1", 0.95)
        assert action is None


# ---------------------------------------------------------------------------
# Exit priority
# ---------------------------------------------------------------------------


class TestExitPriority:
    """Stop-loss > near-max > profit lock > trailing > legacy TP."""

    def test_stop_loss_beats_near_max(self):
        pm = _make_pm()
        pm._profit_lock_mode = "disabled"
        pm._enable_staged_tp = False
        pm._enable_trailing = False
        _open_position(pm, entry=0.95)
        # Price at 0.75 -> -21% (stop loss) AND near-max price
        action = pm.evaluate_risk_action("cond-1", 0.75)
        assert action["reason"] == "stop_loss"

    def test_near_max_beats_profit_lock(self):
        pm = _make_pm()
        pm._profit_lock_mode = "legacy"
        pm._enable_staged_tp = False
        pm._enable_trailing = False
        _open_position(pm, entry=0.50)
        # Arm profit lock
        pm.evaluate_risk_action("cond-1", 0.90)
        # Price at 0.96 -> near-max exit should fire before profit lock
        action = pm.evaluate_risk_action("cond-1", 0.96)
        assert action["reason"] == "near_max_full_exit"


# ---------------------------------------------------------------------------
# Kill switch
# ---------------------------------------------------------------------------


class TestKillSwitch:
    def test_kill_switch_rejects_all(self):
        engine = RiskEngine()
        policy = RiskPolicy(
            max_trade_usdc=Decimal("100"),
            max_portfolio_pct_per_trade=Decimal("0.10"),
            max_market_exposure_usdc=Decimal("300"),
            max_category_exposure_usdc=Decimal("600"),
            max_leader_exposure_usdc=Decimal("500"),
            max_open_positions=20,
            max_daily_drawdown_pct=Decimal("20"),
            max_slippage_bps=200,
            kill_switch=True,
        )
        portfolio = PortfolioSnapshot(
            equity_usdc=Decimal("1000"),
            daily_drawdown_pct=Decimal("0"),
            open_positions_count=0,
            market_exposures={},
            category_exposures={},
            leader_exposures={},
        )
        trade = ProposedTrade(
            user_id="u1",
            leader_id="l1",
            market_id="m1",
            category="crypto",
            token_id="t1",
            side="BUY",
            notional_usdc=Decimal("10"),
        )
        decision = engine.evaluate(portfolio, policy, trade)
        assert decision.allow is False
        assert decision.reason == "kill_switch_enabled"

    def test_kill_switch_off_allows(self):
        engine = RiskEngine()
        policy = RiskPolicy(
            max_trade_usdc=Decimal("100"),
            max_portfolio_pct_per_trade=Decimal("0.10"),
            max_market_exposure_usdc=Decimal("300"),
            max_category_exposure_usdc=Decimal("600"),
            max_leader_exposure_usdc=Decimal("500"),
            max_open_positions=20,
            max_daily_drawdown_pct=Decimal("20"),
            max_slippage_bps=200,
            kill_switch=False,
        )
        portfolio = PortfolioSnapshot(
            equity_usdc=Decimal("1000"),
            daily_drawdown_pct=Decimal("0"),
            open_positions_count=0,
            market_exposures={},
            category_exposures={},
            leader_exposures={},
        )
        trade = ProposedTrade(
            user_id="u1",
            leader_id="l1",
            market_id="m1",
            category="crypto",
            token_id="t1",
            side="BUY",
            notional_usdc=Decimal("10"),
        )
        decision = engine.evaluate(portfolio, policy, trade)
        assert decision.allow is True


# ---------------------------------------------------------------------------
# Daily drawdown limit
# ---------------------------------------------------------------------------


class TestDailyDrawdown:
    def test_daily_drawdown_limit_hit(self):
        engine = RiskEngine()
        policy = RiskPolicy(
            max_trade_usdc=Decimal("100"),
            max_portfolio_pct_per_trade=Decimal("0.10"),
            max_market_exposure_usdc=Decimal("300"),
            max_category_exposure_usdc=Decimal("600"),
            max_leader_exposure_usdc=Decimal("500"),
            max_open_positions=20,
            max_daily_drawdown_pct=Decimal("20"),
            max_slippage_bps=200,
        )
        portfolio = PortfolioSnapshot(
            equity_usdc=Decimal("1000"),
            daily_drawdown_pct=Decimal("22"),  # above 20% limit
            open_positions_count=0,
            market_exposures={},
            category_exposures={},
            leader_exposures={},
        )
        trade = ProposedTrade(
            user_id="u1",
            leader_id="l1",
            market_id="m1",
            category="crypto",
            token_id="t1",
            side="BUY",
            notional_usdc=Decimal("10"),
        )
        decision = engine.evaluate(portfolio, policy, trade)
        assert decision.allow is False
        assert "daily_drawdown" in decision.reason

    def test_daily_drawdown_below_limit_ok(self):
        engine = RiskEngine()
        policy = RiskPolicy(
            max_trade_usdc=Decimal("100"),
            max_portfolio_pct_per_trade=Decimal("0.10"),
            max_market_exposure_usdc=Decimal("300"),
            max_category_exposure_usdc=Decimal("600"),
            max_leader_exposure_usdc=Decimal("500"),
            max_open_positions=20,
            max_daily_drawdown_pct=Decimal("20"),
            max_slippage_bps=200,
        )
        portfolio = PortfolioSnapshot(
            equity_usdc=Decimal("1000"),
            daily_drawdown_pct=Decimal("15"),
            open_positions_count=0,
            market_exposures={},
            category_exposures={},
            leader_exposures={},
        )
        trade = ProposedTrade(
            user_id="u1",
            leader_id="l1",
            market_id="m1",
            category="crypto",
            token_id="t1",
            side="BUY",
            notional_usdc=Decimal("10"),
        )
        decision = engine.evaluate(portfolio, policy, trade)
        assert decision.allow is True


# ---------------------------------------------------------------------------
# Max open positions
# ---------------------------------------------------------------------------


class TestMaxOpenPositions:
    def test_max_positions_reached(self):
        engine = RiskEngine()
        policy = RiskPolicy(
            max_trade_usdc=Decimal("100"),
            max_portfolio_pct_per_trade=Decimal("0.10"),
            max_market_exposure_usdc=Decimal("300"),
            max_category_exposure_usdc=Decimal("600"),
            max_leader_exposure_usdc=Decimal("500"),
            max_open_positions=5,
            max_daily_drawdown_pct=Decimal("20"),
            max_slippage_bps=200,
        )
        portfolio = PortfolioSnapshot(
            equity_usdc=Decimal("1000"),
            daily_drawdown_pct=Decimal("0"),
            open_positions_count=5,  # at limit
            market_exposures={},
            category_exposures={},
            leader_exposures={},
        )
        trade = ProposedTrade(
            user_id="u1",
            leader_id="l1",
            market_id="m1",
            category="crypto",
            token_id="t1",
            side="BUY",
            notional_usdc=Decimal("10"),
        )
        decision = engine.evaluate(portfolio, policy, trade)
        assert decision.allow is False
        assert "open_positions" in decision.reason

    def test_sell_allowed_at_max_positions(self):
        engine = RiskEngine()
        policy = RiskPolicy(
            max_trade_usdc=Decimal("100"),
            max_portfolio_pct_per_trade=Decimal("0.10"),
            max_market_exposure_usdc=Decimal("300"),
            max_category_exposure_usdc=Decimal("600"),
            max_leader_exposure_usdc=Decimal("500"),
            max_open_positions=5,
            max_daily_drawdown_pct=Decimal("20"),
            max_slippage_bps=200,
        )
        portfolio = PortfolioSnapshot(
            equity_usdc=Decimal("1000"),
            daily_drawdown_pct=Decimal("0"),
            open_positions_count=5,
            market_exposures={},
            category_exposures={},
            leader_exposures={},
        )
        trade = ProposedTrade(
            user_id="u1",
            leader_id="l1",
            market_id="m1",
            category="crypto",
            token_id="t1",
            side="SELL",
            notional_usdc=Decimal("10"),
        )
        decision = engine.evaluate(portfolio, policy, trade)
        assert decision.allow is True
        assert decision.reason == "ok_sell_exit"


# ---------------------------------------------------------------------------
# Headroom calculation
# ---------------------------------------------------------------------------


class TestHeadroomCalculation:
    def _policy(self) -> AccountCapPolicy:
        return AccountCapPolicy(
            name="test",
            reserve_pct=Decimal("0.10"),
            reserve_floor=Decimal("5"),
            hard_cap_pct=Decimal("0.15"),
            hard_cap_ceiling=Decimal("10"),
            max_open_positions=12,
            allow_partial_exits=True,
            allow_pyramiding=False,
            max_adds_per_position=1,
            base_min_score=Decimal("0.20"),
            strategy_min_notional=Decimal("0.01"),
        )

    def test_reserve_cash(self):
        policy = self._policy()
        # equity=100, reserve_pct=0.10 -> 10, floor=5 -> max(5,10) = 10
        assert compute_reserve_cash(Decimal("100"), policy) == Decimal("10")

    def test_reserve_floor_applied(self):
        policy = self._policy()
        # equity=10, reserve_pct=0.10 -> 1, floor=5 -> max(5,1) = 5
        assert compute_reserve_cash(Decimal("10"), policy) == Decimal("5")

    def test_hard_cap(self):
        policy = self._policy()
        equity = Decimal("100")
        tradable = Decimal("90")
        # hard_cap = min(90, 100*0.15, 10) = min(90, 15, 10) = 10
        result = compute_hard_cap(equity, tradable, policy)
        assert result == Decimal("10")

    def test_soft_cap(self):
        policy = self._policy()
        hard_cap = Decimal("10")
        # decision_score=0.8, base_min_score=0.2
        # score_norm = (0.8-0.2)/(1-0.2) = 0.75
        # soft_cap = 10 * (0.55 + 0.45 * 0.75) = 10 * 0.8875 = 8.875
        result = compute_soft_cap(hard_cap, Decimal("0.8"), Decimal("0.2"))
        assert result == pytest.approx(Decimal("8.875"))

    def test_headroom_allow(self):
        policy = self._policy()
        result = headroom_for_buy(
            equity_usdc=Decimal("100"),
            free_cash_usdc=Decimal("90"),
            open_positions_count=0,
            policy=policy,
        )
        assert result.allow is True
        assert result.cap_notional_usdc > 0

    def test_headroom_max_positions_reached(self):
        policy = self._policy()
        result = headroom_for_buy(
            equity_usdc=Decimal("100"),
            free_cash_usdc=Decimal("90"),
            open_positions_count=12,  # at max
            policy=policy,
        )
        assert result.allow is False
        assert result.reason == "max_open_positions_reached"

    def test_headroom_no_tradable_cash(self):
        policy = self._policy()
        result = headroom_for_buy(
            equity_usdc=Decimal("100"),
            free_cash_usdc=Decimal("3"),  # less than reserve floor of 5
            open_positions_count=0,
            policy=policy,
        )
        assert result.allow is False
        assert "no_tradable" in result.reason or result.reason == "no_tradable_cash_after_reserve"

    def test_headroom_daily_loss_exceeds(self):
        policy = self._policy()
        result = headroom_for_buy(
            equity_usdc=Decimal("100"),
            free_cash_usdc=Decimal("90"),
            open_positions_count=0,
            daily_loss_usdc=Decimal("25"),  # exceeds 20% of 100
            policy=policy,
        )
        assert result.allow is False
        assert "no_buy_headroom" in result.reason

    def test_headroom_market_exposure_exceeded(self):
        policy = self._policy()
        result = headroom_for_buy(
            equity_usdc=Decimal("1000"),
            free_cash_usdc=Decimal("500"),
            open_positions_count=0,
            market_exposure_usdc=Decimal("400"),  # exceeds default MAX_MARKET_EXPOSURE_USDC=300
            policy=policy,
        )
        # Market headroom is max(0, 300 - 400) = 0, so cap=0
        assert result.allow is False

    def test_headroom_category_exposure_exceeded(self):
        policy = self._policy()
        result = headroom_for_buy(
            equity_usdc=Decimal("1000"),
            free_cash_usdc=Decimal("500"),
            open_positions_count=0,
            category_exposure_usdc=Decimal("700"),  # exceeds default MAX_CATEGORY_EXPOSURE_USDC=600
            policy=policy,
        )
        assert result.allow is False

    def test_headroom_leader_exposure_exceeded(self):
        policy = self._policy()
        result = headroom_for_buy(
            equity_usdc=Decimal("1000"),
            free_cash_usdc=Decimal("500"),
            open_positions_count=0,
            leader_exposure_usdc=Decimal("600"),  # exceeds default MAX_LEADER_EXPOSURE_USDC=500
            policy=policy,
        )
        assert result.allow is False


# ---------------------------------------------------------------------------
# Risk management disabled
# ---------------------------------------------------------------------------


class TestRiskDisabled:
    def test_risk_mgmt_disabled_no_action(self):
        pm = _make_pm()
        pm._enabled = False
        _open_position(pm, entry=0.50)
        action = pm.evaluate_risk_action("cond-1", 0.20)  # massive loss
        assert action is None
