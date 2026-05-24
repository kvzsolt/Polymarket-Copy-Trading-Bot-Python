"""Issue #8 tests — advanced exit-rule ordering and ratio semantics."""

from decimal import Decimal


import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.settings import RiskSettings
from services.positionManager import PositionManager
from services.riskEngine import (
    AccountCapPolicy,
    compute_hard_cap,
    compute_reserve_cash,
    compute_soft_cap,
    headroom_for_buy,
)


def _make_pm() -> PositionManager:
    """Create a PositionManager with test risk settings, bypassing ENV."""
    risk = RiskSettings(
        stop_loss_ratio=0.0,
        take_profit_ratio=0.0,
        daily_drawdown_ratio=0.20,
        profit_lock_trigger_ratio=0.70,
        profit_lock_drawdown_ratio=0.10,
        profit_lock_sell_ratio=0.30,
        trailing_trigger_ratio=0.20,
        trailing_distance_ratio=0.05,
        near_max_price_level=0.95,
        near_max_warn_level=0.90,
    )
    pm = PositionManager(risk_settings=risk)
    # Make deterministic regardless of process env mutation from other tests/CI
    pm._enabled = True
    # Override feature flags directly (bypass ENV)
    pm._enable_near_max = True
    pm._profit_lock_mode = "advanced"
    pm._enable_staged_tp = True
    pm._staged_levels = [0.30, 0.50]
    pm._staged_fractions = [0.25, 0.35]
    pm._enable_trailing = True
    pm._enable_leader_divergence = True
    pm.track_buy("cond-test", "asset-1", 0.40, 100.0, 40.0)
    return pm


# ---------------------------------------------------------------------------
# Exit priority ordering tests
# ---------------------------------------------------------------------------

class TestExitPriorityOrdering:
    def test_primitive_stop_loss_is_inactive(self):
        pm = _make_pm()
        pm._enable_near_max = False
        pm._profit_lock_mode = "disabled"
        pm._enable_staged_tp = False
        pm._enable_trailing = False
        pm._enable_leader_divergence = False
        result = pm.evaluate_risk_action("cond-test", 0.30)
        assert result is None

    def test_near_max_beats_profit_lock(self):
        pm = _make_pm()
        result = pm.evaluate_risk_action("cond-test", 0.96)
        assert result is not None
        assert result["reason"] == "near_max_full_exit"

    def test_no_trigger_when_below_thresholds(self):
        pm = _make_pm()
        result = pm.evaluate_risk_action("cond-test", 0.44)
        assert result is None

    def test_trailing_stop_activates_and_triggers(self):
        pm = _make_pm()
        # Push staged TP high so it doesn't fire
        pm._staged_levels = [99.0, 99.0]
        # Activate trailing at +25%
        pm.evaluate_risk_action("cond-test", 0.50)
        # Price drops: peak=0.50, trail trigger = 0.50-0.05=0.45
        result = pm.evaluate_risk_action("cond-test", 0.44)
        assert result is not None
        assert result["reason"] == "trailing_stop"

    def test_legacy_take_profit_is_inactive_when_staged_disabled(self):
        pm = _make_pm()
        pm._enable_staged_tp = False
        pm._enable_trailing = False
        pm._enable_near_max = False
        pm._profit_lock_mode = "disabled"
        result = pm.evaluate_risk_action("cond-test", 0.56)
        assert result is None

    def test_staged_tp_level_1_fires(self):
        pm = _make_pm()
        pm._profit_lock_mode = "disabled"  # disable profit lock so it doesn't fire first
        pm._staged_levels = [0.30, 0.50]
        pm._staged_fractions = [0.25, 0.35]
        result = pm.evaluate_risk_action("cond-test", 0.52)
        assert result is not None
        assert result["reason"] == "staged_tp_level_1"
        assert result["size_fraction"] == 0.25

    def test_staged_tp_level_2_after_level_1(self):
        pm = _make_pm()
        pm._profit_lock_mode = "disabled"
        pm._staged_levels = [0.30, 0.50]
        pm._staged_fractions = [0.25, 0.35]
        pm.evaluate_risk_action("cond-test", 0.52)  # trigger level 1
        result = pm.evaluate_risk_action("cond-test", 0.60)  # trigger level 2
        assert result is not None
        assert result["reason"] == "staged_tp_level_2"
        assert result["size_fraction"] == 0.35

    def test_profit_lock_before_staged_tp(self):
        pm = _make_pm()
        pm._profit_lock_trigger = 0.30
        pm._staged_levels = [0.30, 0.50]
        pm._staged_fractions = [0.25, 0.35]
        result = pm.evaluate_risk_action("cond-test", 0.52)
        assert result is not None
        assert result["reason"] == "profit_lock"


# ---------------------------------------------------------------------------
# Ratio semantics tests
# ---------------------------------------------------------------------------

class TestRatioSemantics:
    def test_headroom_ratio_drawdown(self):
        result = headroom_for_buy(
            equity_usdc=Decimal("1000"),
            free_cash_usdc=Decimal("800"),
            open_positions_count=0,
            daily_loss_usdc=Decimal("250"),
        )
        assert result.allow is False

    def test_reserve_cash_ratio(self):
        policy = AccountCapPolicy(
            name="test", reserve_pct=Decimal("0.10"),
            reserve_floor=Decimal("5"), hard_cap_pct=Decimal("0.15"),
            hard_cap_ceiling=Decimal("100"), max_open_positions=10,
            allow_partial_exits=True, allow_pyramiding=False,
            max_adds_per_position=1, base_min_score=Decimal("0.20"),
            strategy_min_notional=Decimal("1"),
        )
        reserve = compute_reserve_cash(Decimal("1000"), policy)
        assert reserve == Decimal("100")

    def test_hard_cap_ratio(self):
        policy = AccountCapPolicy(
            name="test", reserve_pct=Decimal("0.10"),
            reserve_floor=Decimal("5"), hard_cap_pct=Decimal("0.15"),
            hard_cap_ceiling=Decimal("100"), max_open_positions=10,
            allow_partial_exits=True, allow_pyramiding=False,
            max_adds_per_position=1, base_min_score=Decimal("0.20"),
            strategy_min_notional=Decimal("1"),
        )
        cap = compute_hard_cap(Decimal("1000"), Decimal("900"), policy)
        assert cap == Decimal("100")

    def test_soft_cap_scales_with_score(self):
        cap = compute_soft_cap(
            hard_cap=Decimal("100"),
            decision_score=Decimal("0.60"),
            base_min_score=Decimal("0.20"),
        )
        assert cap == Decimal("77.5")
