"""Tests for v3.1 features: advanced exits, trader discovery, leader scoring integration.

These tests mock out heavy dependencies (web3, eth-account) so they run in
environments without the full Polymarket runtime installed.
"""

import os
import sys
import types
import unittest
import importlib.abc
import importlib.machinery
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest


def _scorer_config() -> SimpleNamespace:
    return SimpleNamespace(
        sizing=SimpleNamespace(
            enable_trust_size_multipliers=True,
            trust_size_multiplier_min=0.50,
            trust_size_multiplier_max=1.25,
        ),
        discovery=SimpleNamespace(
            scoring=SimpleNamespace(
                runtime_default_roster_score=0.50,
                runtime_pnl_target_usdc=100.0,
                runtime_activity_target=8,
                runtime_score_floor=0.10,
                runtime_weights=SimpleNamespace(
                    roster_score=0.70,
                    win_rate=0.15,
                    pnl=0.10,
                    activity=0.05,
                ),
            )
        ),
    )


class _AutoMockModule(types.ModuleType):
    """Module stub that returns MagicMock for any attribute."""

    def __getattr__(self, name):
        if name.startswith("__") and name.endswith("__"):
            raise AttributeError(name)
        mock = MagicMock()
        setattr(self, name, mock)
        return mock


class _MockLoader(importlib.abc.Loader):
    def create_module(self, spec):
        mod = _AutoMockModule(spec.name)
        mod.__path__ = [spec.name]
        mod.__package__ = spec.name
        return mod

    def exec_module(self, module):
        pass


class _MockFinder(importlib.abc.MetaPathFinder):
    """Meta path finder used by this test module to stub heavy optional deps.

    IMPORTANT: this is installed only for the duration of this test module.
    Leaving it installed globally will break other tests by returning MagicMocks
    for real libraries.
    """

    _ALLOWED = frozenset(
        (
            # External / heavy deps we don't want to require in unit tests.
            "py_order_utils",
            "web3",
            "dotenv",
            "aiohttp",
            "motor",
            "colorama",
            "eth_account",
            "hexbytes",
        )
    )

    def find_spec(self, fullname, path, target=None):
        top = fullname.split(".")[0]
        if top in self._ALLOWED:
            return importlib.machinery.ModuleSpec(fullname, _MockLoader(), is_package=True)
        return None


# Install BEFORE any project imports (module-scoped, with teardown below)
_MOCK_FINDER = _MockFinder()


def setUpModule():  # noqa: N802 (unittest-style name)
    sys.meta_path.insert(0, _MOCK_FINDER)


def tearDownModule():  # noqa: N802
    # Ensure we don't contaminate other pytest modules.
    try:
        sys.meta_path.remove(_MOCK_FINDER)
    except ValueError:
        pass


sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))


# ═══════════════════════════════════════════════════════════════════════
# Position Exit System Tests
# ═══════════════════════════════════════════════════════════════════════


class TestPositionExitSystem(unittest.TestCase):
    """Test the advanced exit system in PositionManager."""

    def _make_pm(self, **env_overrides):
        """Create a fresh PositionManager with the given feature flag overrides.

        PositionManager no longer reads module-level ENV; it takes its config
        via constructor args.  We build a minimal namespace that maps the old
        ENV-style keys directly onto the private attributes the constructor
        would otherwise set.
        """
        defaults = {
            "ENABLE_RISK_MGMT": True,
            "ENABLE_PROFIT_LOCK": True,
            "PROFIT_LOCK_TRIGGER_PCT": 70,
            "PROFIT_LOCK_SELL_FRACTION": 0.50,
            "ENABLE_NEAR_MAX_EXIT": True,
            "NEAR_MAX_PRICE_EXIT": 0.95,
            "NEAR_MAX_PRICE_WARN": 0.92,
            "NEAR_MAX_PRICE_WARN_TRIM_RATIO": 0.50,
            "ENABLE_TRAILING_STOP": True,
            "TRAILING_ACTIVATION_PCT": 20,
            "TRAILING_DISTANCE": 0.05,
            "ENABLE_STAGED_TP": True,
            "STAGED_TP_LEVELS": "0.30,0.50",
            "STAGED_TP_FRACTIONS": "0.25,0.35",
            "ENABLE_LEADER_DIVERGENCE_EXIT": True,
        }
        defaults.update(env_overrides)

        import pmcopy.services.positionManager as pm_mod

        def _csv_floats(s):
            if not s:
                return []
            parts = [p.strip() for p in str(s).split(",") if p.strip()]
            return [float(p) for p in parts]

        def _staged_level_ratios(s):
            if not s:
                return []
            ratios = []
            for part in [p.strip() for p in str(s).split(",") if p.strip()]:
                raw = float(part)
                ratios.append(
                    raw / 100.0
                    if "." not in part and "e" not in part.lower() and raw > 1.0
                    else raw
                )
            return ratios

        def _pct_ratio(value):
            raw = float(value)
            return raw / 100.0 if raw > 1.0 else raw

        def _trailing_activation(value):
            if value is None:
                return True, 0.0
            raw = str(value).strip().lower()
            if raw in ("", "auto"):
                return True, 0.0
            return False, float(value) / 100.0

        pm = pm_mod.PositionManager()  # bare defaults branch

        # Override all private attrs to match the requested flags.
        pm._risk_master_enabled = bool(defaults["ENABLE_RISK_MGMT"])
        pm._enabled = bool(defaults["ENABLE_RISK_MGMT"])

        enable_profit_lock = bool(defaults["ENABLE_PROFIT_LOCK"])
        pm._profit_lock_mode = "advanced" if enable_profit_lock else "disabled"
        pm._profit_lock_trigger = float(defaults["PROFIT_LOCK_TRIGGER_PCT"]) / 100.0
        pm._profit_lock_sell_fraction = float(defaults["PROFIT_LOCK_SELL_FRACTION"])

        pm._enable_near_max = bool(defaults["ENABLE_NEAR_MAX_EXIT"])
        pm._near_max_exit = float(defaults["NEAR_MAX_PRICE_EXIT"])
        pm._near_max_warn = float(defaults["NEAR_MAX_PRICE_WARN"])
        pm._near_max_warn_ratio = float(defaults["NEAR_MAX_PRICE_WARN_TRIM_RATIO"])

        pm._enable_trailing = bool(defaults["ENABLE_TRAILING_STOP"])
        pm._trailing_activation_auto, pm._trailing_activation_pct = _trailing_activation(
            defaults["TRAILING_ACTIVATION_PCT"]
        )
        pm._trailing_distance = _pct_ratio(defaults["TRAILING_DISTANCE"])

        pm._enable_staged_tp = bool(defaults["ENABLE_STAGED_TP"])
        raw_fractions = _csv_floats(defaults["STAGED_TP_FRACTIONS"])
        pm._staged_levels = _staged_level_ratios(defaults["STAGED_TP_LEVELS"]) or [0.30, 0.50]
        pm._staged_fractions = raw_fractions or [0.25, 0.35]

        pm._enable_leader_divergence = bool(defaults["ENABLE_LEADER_DIVERGENCE_EXIT"])

        return pm

    def _add_position(self, pm, entry_price=0.50, size=100.0, cid="test_cid_001"):
        pm.track_buy(
            condition_id=cid,
            asset="token_abc",
            entry_price=entry_price,
            size_tokens=size,
            usdc_cost=entry_price * size,
            leader_wallet="0x1234567890abcdef1234567890abcdef12345678",
        )
        return cid

    # ── Near-Max Exit ──

    def test_near_max_full_exit_at_95c(self):
        pm = self._make_pm()
        cid = self._add_position(pm, entry_price=0.50)
        action = pm.evaluate_risk_action(cid, 0.95)
        assert action is not None
        assert action["reason"] == "near_max_full_exit"
        assert action["size_fraction"] == 1.0

    def test_near_max_full_exit_logs_once_then_throttles(self):
        """Steady-state NEAR-MAX exit logs INFO once, repeats drop to DEBUG.

        The risk loop re-evaluates every ~0.25s; without throttling a position
        parked at >= near-max would spam an identical INFO line every tick.
        Decision output must stay unchanged across all evaluations.
        """
        import logging
        import pmcopy.services.positionManager as pm_mod

        pm = self._make_pm()
        cid = self._add_position(pm, entry_price=0.50)

        with self.assertLogs(pm_mod.logger, level="INFO") as cap1:
            a1 = pm.evaluate_risk_action(cid, 0.99)
        assert a1["reason"] == "near_max_full_exit"
        info1 = [
            r
            for r in cap1.records
            if r.levelno == logging.INFO and "NEAR-MAX full exit" in r.getMessage()
        ]
        assert len(info1) == 1

        with self.assertLogs(pm_mod.logger, level="DEBUG") as cap2:
            a2 = pm.evaluate_risk_action(cid, 0.99)
            a3 = pm.evaluate_risk_action(cid, 0.99)
        assert a2["reason"] == "near_max_full_exit"
        assert a3["reason"] == "near_max_full_exit"
        msgs = [r for r in cap2.records if "NEAR-MAX full exit" in r.getMessage()]
        assert all(r.levelno == logging.DEBUG for r in msgs)
        assert not any(r.levelno == logging.INFO for r in msgs)
        assert len(msgs) >= 1

    def test_near_max_full_exit_relogs_after_price_drops_back(self):
        """A genuine re-entry (price falls below then re-crosses) logs INFO again."""
        import logging
        import pmcopy.services.positionManager as pm_mod

        pm = self._make_pm()
        cid = self._add_position(pm, entry_price=0.50)

        pm.evaluate_risk_action(cid, 0.99)  # first cross → INFO + flag set
        pm.evaluate_risk_action(cid, 0.80)  # below threshold → flag cleared
        with self.assertLogs(pm_mod.logger, level="INFO") as cap:
            again = pm.evaluate_risk_action(cid, 0.99)  # re-cross → INFO again
        assert again["reason"] == "near_max_full_exit"
        info = [
            r
            for r in cap.records
            if r.levelno == logging.INFO and "NEAR-MAX full exit" in r.getMessage()
        ]
        assert len(info) == 1

    def test_near_max_warn_at_92c(self):
        pm = self._make_pm()
        cid = self._add_position(pm, entry_price=0.50)
        action = pm.evaluate_risk_action(cid, 0.93)
        assert action is not None
        assert action["reason"] == "near_max_partial_trim"
        assert action["size_fraction"] == pytest.approx(0.50)

    def test_near_max_warn_fires_once(self):
        pm = self._make_pm()
        cid = self._add_position(pm, entry_price=0.50)
        action1 = pm.evaluate_risk_action(cid, 0.93)
        assert action1["reason"] == "near_max_partial_trim"
        pm.track_sell(cid, 0.93, 50.0, reason="near_max_partial_trim")
        action2 = pm.evaluate_risk_action(cid, 0.93)
        if action2:
            assert action2["reason"] != "near_max_partial_trim"

    def test_near_max_disabled(self):
        pm = self._make_pm(ENABLE_NEAR_MAX_EXIT=False)
        cid = self._add_position(pm, entry_price=0.50)
        action = pm.evaluate_risk_action(cid, 0.96)
        if action:
            assert "near_max" not in action["reason"]

    # ── Profit Lock ──

    def test_profit_lock_at_70pct(self):
        pm = self._make_pm()
        cid = self._add_position(pm, entry_price=0.50)
        # +80% = 0.90 (below near-max warn 0.92)
        action = pm.evaluate_risk_action(cid, 0.90)
        assert action is not None
        assert action["reason"] == "profit_lock"
        assert action["size_fraction"] == pytest.approx(0.50)

    def test_profit_lock_fires_once(self):
        pm = self._make_pm()
        cid = self._add_position(pm, entry_price=0.50)
        action1 = pm.evaluate_risk_action(cid, 0.90)
        assert action1["reason"] == "profit_lock"
        pm.track_sell(cid, 0.90, 50.0, reason="profit_lock")
        action2 = pm.evaluate_risk_action(cid, 0.90)
        if action2:
            assert action2["reason"] != "profit_lock"

    def test_profit_lock_disabled(self):
        pm = self._make_pm(ENABLE_PROFIT_LOCK=False)
        cid = self._add_position(pm, entry_price=0.50)
        action = pm.evaluate_risk_action(cid, 0.88)
        if action:
            assert action["reason"] != "profit_lock"

    # ── Staged Take-Profit ──

    def test_staged_tp_level_1(self):
        pm = self._make_pm(ENABLE_PROFIT_LOCK=False)
        cid = self._add_position(pm, entry_price=0.50)
        action = pm.evaluate_risk_action(cid, 0.66)  # +32%
        assert action is not None
        assert action["reason"] == "staged_tp_level_1"
        assert action["size_fraction"] == pytest.approx(0.25)

    def test_staged_tp_level_2(self):
        pm = self._make_pm(ENABLE_PROFIT_LOCK=False)
        cid = self._add_position(pm, entry_price=0.50)
        pm.evaluate_risk_action(cid, 0.66)  # trigger level 1
        pm.track_sell(cid, 0.66, 25.0, reason="staged_tp_level_1")
        action = pm.evaluate_risk_action(cid, 0.76)  # +52%
        assert action is not None
        assert action["reason"] == "staged_tp_level_2"
        assert action["size_fraction"] == pytest.approx(0.35)

    def test_staged_tp_disabled(self):
        pm = self._make_pm(ENABLE_STAGED_TP=False, ENABLE_PROFIT_LOCK=False)
        cid = self._add_position(pm, entry_price=0.50)
        action = pm.evaluate_risk_action(cid, 0.66)
        assert action is None

    def test_staged_tp_uses_typed_config_defaults(self):
        from tests.config_helpers import load_test_config
        from pmcopy.services.positionManager import PositionManager

        cfg = load_test_config(
            {
                "PRIVATE_KEY": "0x" + "a" * 64,
                "FUNDER_ADDRESS": "0x" + "b" * 40,
                "RPC_URL": "https://polygon-rpc.example.com",
                "CHAIN_ID": "137",
                "USER_ADDRESS": "0x" + "c" * 40,
                "ENABLE_RISK_MGMT": "true",
                "ENABLE_STAGED_TP": "true",
                "ENABLE_NEAR_MAX_EXIT": "false",
            }
        )
        pm = PositionManager(config=cfg)
        cid = self._add_position(pm, entry_price=0.50)

        action = pm.evaluate_risk_action(cid, 0.66)

        assert action is not None
        assert action["reason"] == "staged_tp_level_1"
        assert action["size_fraction"] == pytest.approx(0.25)

    def test_staged_tp_normalizes_whole_number_ratio_values(self):
        from tests.config_helpers import load_test_config
        from pmcopy.services.positionManager import PositionManager

        cfg = load_test_config(
            {
                "PRIVATE_KEY": "0x" + "a" * 64,
                "FUNDER_ADDRESS": "0x" + "b" * 40,
                "RPC_URL": "https://polygon-rpc.example.com",
                "CHAIN_ID": "137",
                "USER_ADDRESS": "0x" + "c" * 40,
                "ENABLE_RISK_MGMT": "true",
                "ENABLE_STAGED_TP": "true",
                "ENABLE_PROFIT_LOCK": "false",
                "STAGED_TP_LEVELS": "130",
                "STAGED_TP_FRACTIONS": "0.25",
                "ENABLE_NEAR_MAX_EXIT": "false",
            }
        )
        pm = PositionManager(config=cfg)
        cid = self._add_position(pm, entry_price=0.40)

        assert pm.evaluate_risk_action(cid, 0.91) is None
        action = pm.evaluate_risk_action(cid, 0.92)

        assert action is not None
        assert action["reason"] == "staged_tp_level_1"
        assert action["size_fraction"] == pytest.approx(0.25)

    def test_staged_tp_preserves_decimal_ratio_above_one(self):
        from tests.config_helpers import load_test_config
        from pmcopy.services.positionManager import PositionManager

        cfg = load_test_config(
            {
                "PRIVATE_KEY": "0x" + "a" * 64,
                "FUNDER_ADDRESS": "0x" + "b" * 40,
                "RPC_URL": "https://polygon-rpc.example.com",
                "CHAIN_ID": "137",
                "USER_ADDRESS": "0x" + "c" * 40,
                "ENABLE_RISK_MGMT": "true",
                "ENABLE_STAGED_TP": "true",
                "ENABLE_PROFIT_LOCK": "false",
                "STAGED_TP_LEVELS": "1.30",
                "STAGED_TP_FRACTIONS": "0.25",
                "ENABLE_NEAR_MAX_EXIT": "false",
            }
        )
        pm = PositionManager(config=cfg)
        cid = self._add_position(pm, entry_price=0.40)

        assert pm.evaluate_risk_action(cid, 0.91) is None
        action = pm.evaluate_risk_action(cid, 0.92)

        assert action is not None
        assert action["reason"] == "staged_tp_level_1"
        assert action["size_fraction"] == pytest.approx(0.25)

    def test_staged_tp_accepts_legacy_percentage_levels(self):
        from tests.config_helpers import load_test_config
        from pmcopy.services.positionManager import PositionManager

        cfg = load_test_config(
            {
                "PRIVATE_KEY": "0x" + "a" * 64,
                "FUNDER_ADDRESS": "0x" + "b" * 40,
                "RPC_URL": "https://polygon-rpc.example.com",
                "CHAIN_ID": "137",
                "USER_ADDRESS": "0x" + "c" * 40,
                "ENABLE_RISK_MGMT": "true",
                "ENABLE_STAGED_TP": "true",
                "STAGED_TP_LEVELS": "30,50",
                "STAGED_TP_FRACTIONS": "0.25,0.35",
                "ENABLE_NEAR_MAX_EXIT": "false",
            }
        )
        pm = PositionManager(config=cfg)
        cid = self._add_position(pm, entry_price=0.50)

        action = pm.evaluate_risk_action(cid, 0.66)

        assert action is not None
        assert action["reason"] == "staged_tp_level_1"
        assert action["size_fraction"] == pytest.approx(0.25)

    # ── Trailing Stop ──

    def test_trailing_activates(self):
        pm = self._make_pm(ENABLE_PROFIT_LOCK=False, ENABLE_STAGED_TP=False)
        cid = self._add_position(pm, entry_price=0.50)
        action = pm.evaluate_risk_action(cid, 0.62)  # +24%
        assert action is None
        pos = pm.get_position(cid)
        assert pos.exit_state.trailing_active

    def test_trailing_triggers_on_pullback(self):
        pm = self._make_pm(ENABLE_PROFIT_LOCK=False, ENABLE_STAGED_TP=False)
        cid = self._add_position(pm, entry_price=0.50)
        pm.evaluate_risk_action(cid, 0.70)  # peak
        action = pm.evaluate_risk_action(cid, 0.64)  # 0.70 * (1 - 5%) = 0.665 > 0.64
        assert action is not None
        assert action["reason"] == "trailing_stop"

    def test_trailing_no_trigger_above_trail(self):
        pm = self._make_pm(ENABLE_PROFIT_LOCK=False, ENABLE_STAGED_TP=False)
        cid = self._add_position(pm, entry_price=0.50)
        pm.evaluate_risk_action(cid, 0.70)
        action = pm.evaluate_risk_action(cid, 0.67)  # above 0.665 trail
        assert action is None

    def test_trailing_distance_accepts_whole_percentage(self):
        pm = self._make_pm(
            ENABLE_PROFIT_LOCK=False,
            ENABLE_STAGED_TP=False,
            TRAILING_DISTANCE=10,
        )
        cid = self._add_position(pm, entry_price=0.50)
        pm.evaluate_risk_action(cid, 0.70)
        assert pm.evaluate_risk_action(cid, 0.64) is None
        action = pm.evaluate_risk_action(cid, 0.63)  # 0.70 * (1 - 10%)
        assert action is not None
        assert action["reason"] == "trailing_stop"

    def test_dynamic_trailing_activates_at_30_pct_for_10_to_15_payout(self):
        pm = self._make_pm(
            ENABLE_PROFIT_LOCK=False,
            ENABLE_NEAR_MAX_EXIT=False,
            ENABLE_STAGED_TP=False,
            TRAILING_ACTIVATION_PCT="",
        )
        cid = self._add_position(
            pm,
            entry_price=Decimal("0.6666666666666666666666666667"),
            size=Decimal("15"),
        )

        assert pm.evaluate_risk_action(cid, Decimal("0.86")) is None
        assert not pm.get_position(cid).exit_state.trailing_active

        assert pm.evaluate_risk_action(cid, Decimal("0.867")) is None
        assert pm.get_position(cid).exit_state.trailing_active

    def test_dynamic_trailing_activates_at_30_usdc_value_for_10_to_40_payout(self):
        pm = self._make_pm(
            ENABLE_PROFIT_LOCK=False,
            ENABLE_NEAR_MAX_EXIT=False,
            ENABLE_STAGED_TP=False,
            TRAILING_ACTIVATION_PCT="auto",
        )
        cid = self._add_position(pm, entry_price=Decimal("0.25"), size=Decimal("40"))

        assert pm.evaluate_risk_action(cid, Decimal("0.725")) is None
        assert not pm.get_position(cid).exit_state.trailing_active

        assert pm.evaluate_risk_action(cid, Decimal("0.75")) is None
        assert pm.get_position(cid).exit_state.trailing_active

    def test_dynamic_trailing_keeps_configured_distance(self):
        pm = self._make_pm(
            ENABLE_PROFIT_LOCK=False,
            ENABLE_NEAR_MAX_EXIT=False,
            ENABLE_STAGED_TP=False,
            TRAILING_ACTIVATION_PCT="auto",
            TRAILING_DISTANCE=0.10,
        )
        cid = self._add_position(pm, entry_price=Decimal("0.25"), size=Decimal("40"))

        pm.evaluate_risk_action(cid, Decimal("0.75"))
        pm.evaluate_risk_action(cid, Decimal("0.90"))
        assert pm.evaluate_risk_action(cid, Decimal("0.82")) is None
        action = pm.evaluate_risk_action(cid, Decimal("0.80"))

        assert action is not None
        assert action["reason"] == "trailing_stop"

    def test_near_max_still_wins_before_dynamic_trailing(self):
        pm = self._make_pm(
            ENABLE_PROFIT_LOCK=False,
            ENABLE_NEAR_MAX_EXIT=True,
            NEAR_MAX_PRICE_WARN_TRIM_RATIO=0.0,
            ENABLE_STAGED_TP=False,
            TRAILING_ACTIVATION_PCT="auto",
        )
        cid = self._add_position(pm, entry_price=Decimal("0.25"), size=Decimal("40"))

        action = pm.evaluate_risk_action(cid, Decimal("0.95"))

        assert action is not None
        assert action["reason"] == "near_max_full_exit"

    def test_trailing_disabled(self):
        pm = self._make_pm(
            ENABLE_TRAILING_STOP=False, ENABLE_PROFIT_LOCK=False, ENABLE_STAGED_TP=False
        )
        cid = self._add_position(pm, entry_price=0.50)
        pm.evaluate_risk_action(cid, 0.70)
        action = pm.evaluate_risk_action(cid, 0.60)
        assert action is None

    # ── Leader Divergence ──

    def test_leader_divergence(self):
        pm = self._make_pm(
            ENABLE_PROFIT_LOCK=False, ENABLE_STAGED_TP=False, ENABLE_TRAILING_STOP=False
        )
        cid = self._add_position(pm, entry_price=0.50)
        pm.signal_leader_divergence(cid)
        action = pm.evaluate_risk_action(cid, 0.55)
        assert action is not None
        assert action["reason"] == "leader_divergence"
        assert action["size_fraction"] == pytest.approx(0.50)

    def test_leader_divergence_fires_once(self):
        pm = self._make_pm(
            ENABLE_PROFIT_LOCK=False, ENABLE_STAGED_TP=False, ENABLE_TRAILING_STOP=False
        )
        cid = self._add_position(pm, entry_price=0.50)
        pm.signal_leader_divergence(cid)
        action1 = pm.evaluate_risk_action(cid, 0.55)
        assert action1["reason"] == "leader_divergence"
        pm.track_sell(cid, 0.55, 50.0, reason="leader_divergence")
        action2 = pm.evaluate_risk_action(cid, 0.55)
        assert action2 is None

    # ── Priority ──

    def test_near_max_beats_profit_lock(self):
        pm = self._make_pm()
        cid = self._add_position(pm, entry_price=0.10)
        action = pm.evaluate_risk_action(cid, 0.96)
        assert action["reason"] == "near_max_full_exit"


# ═══════════════════════════════════════════════════════════════════════
# Leader Scoring + Discovery Integration Tests
# ═══════════════════════════════════════════════════════════════════════


class TestLeaderScoringIntegration(unittest.TestCase):
    def _mock_env(self):
        return {
            "LEADER_SCORE_BALANCE_TARGET_USDC": 500.0,
            "LEADER_SCORE_PNL_BONUS_CAP": 0.1,
            "LEADER_SCORE_ACTIVITY_TARGET": 8,
            "LEADER_SCORE_WEIGHT_BALANCE": 0.35,
            "LEADER_SCORE_WEIGHT_ANALYTICS": 0.45,
            "LEADER_SCORE_WEIGHT_ACTIVITY": 0.20,
            "LEADER_SCORE_FLOOR": 0.10,
            "TRUST_SIZE_MULTIPLIER_MIN": 0.50,
            "TRUST_SIZE_MULTIPLIER_MAX": 1.25,
        }

    def test_scoring_without_discovery(self):
        import pmcopy.services.leaderScoringService as ls_mod

        scorer = ls_mod.LeaderScoringService(config=_scorer_config())
        result = scorer.score_leader(
            "0x1234567890abcdef1234567890abcdef12345678",
            leader_balance=300.0,
            analytics={"win_rate": 65.0, "net_pnl": 50.0},
            recent_trade_count=5,
        )
        assert result.score > 0.0

    def test_trust_multiplier(self):
        import pmcopy.services.leaderScoringService as ls_mod

        scorer = ls_mod.LeaderScoringService(config=_scorer_config())
        assert scorer.trust_multiplier(0.0) == pytest.approx(0.50)
        assert scorer.trust_multiplier(1.0) == pytest.approx(1.25)
        assert scorer.trust_multiplier(0.5) == pytest.approx(0.875)


# ═══════════════════════════════════════════════════════════════════════
# Feature Flag Tests
# ═══════════════════════════════════════════════════════════════════════


class TestFeatureFlags(unittest.TestCase):
    def _make_pm(self, **overrides):
        defaults = {
            "ENABLE_RISK_MGMT": True,
            "ENABLE_PROFIT_LOCK": False,
            "PROFIT_LOCK_TRIGGER_PCT": 70,
            "PROFIT_LOCK_SELL_FRACTION": 0.50,
            "ENABLE_NEAR_MAX_EXIT": False,
            "NEAR_MAX_PRICE_EXIT": 0.95,
            "NEAR_MAX_PRICE_WARN": 0.92,
            "NEAR_MAX_PRICE_WARN_TRIM_RATIO": 0.50,
            "ENABLE_TRAILING_STOP": False,
            "TRAILING_ACTIVATION_PCT": 20,
            "TRAILING_DISTANCE": 0.05,
            "ENABLE_STAGED_TP": False,
            "STAGED_TP_LEVELS": "0.30,0.50",
            "STAGED_TP_FRACTIONS": "0.25,0.35",
            "ENABLE_LEADER_DIVERGENCE_EXIT": False,
        }
        defaults.update(overrides)

        import pmcopy.services.positionManager as pm_mod

        def _csv_floats(s):
            if not s:
                return []
            parts = [p.strip() for p in str(s).split(",") if p.strip()]
            return [float(p) for p in parts]

        def _staged_level_ratios(s):
            if not s:
                return []
            ratios = []
            for part in [p.strip() for p in str(s).split(",") if p.strip()]:
                raw = float(part)
                ratios.append(
                    raw / 100.0
                    if "." not in part and "e" not in part.lower() and raw > 1.0
                    else raw
                )
            return ratios

        def _pct_ratio(value):
            raw = float(value)
            return raw / 100.0 if raw > 1.0 else raw

        def _trailing_activation(value):
            if value is None:
                return True, 0.0
            raw = str(value).strip().lower()
            if raw in ("", "auto"):
                return True, 0.0
            return False, float(value) / 100.0

        pm = pm_mod.PositionManager()
        pm._risk_master_enabled = bool(defaults["ENABLE_RISK_MGMT"])
        pm._enabled = bool(defaults["ENABLE_RISK_MGMT"])
        pm._profit_lock_mode = "advanced" if defaults["ENABLE_PROFIT_LOCK"] else "disabled"
        pm._profit_lock_trigger = float(defaults["PROFIT_LOCK_TRIGGER_PCT"]) / 100.0
        pm._profit_lock_sell_fraction = float(defaults["PROFIT_LOCK_SELL_FRACTION"])
        pm._enable_near_max = bool(defaults["ENABLE_NEAR_MAX_EXIT"])
        pm._near_max_exit = float(defaults["NEAR_MAX_PRICE_EXIT"])
        pm._near_max_warn = float(defaults["NEAR_MAX_PRICE_WARN"])
        pm._near_max_warn_ratio = float(defaults["NEAR_MAX_PRICE_WARN_TRIM_RATIO"])
        pm._enable_trailing = bool(defaults["ENABLE_TRAILING_STOP"])
        pm._trailing_activation_auto, pm._trailing_activation_pct = _trailing_activation(
            defaults["TRAILING_ACTIVATION_PCT"]
        )
        pm._trailing_distance = _pct_ratio(defaults["TRAILING_DISTANCE"])
        pm._enable_staged_tp = bool(defaults["ENABLE_STAGED_TP"])
        raw_fractions = _csv_floats(defaults["STAGED_TP_FRACTIONS"])
        pm._staged_levels = _staged_level_ratios(defaults["STAGED_TP_LEVELS"]) or [0.30, 0.50]
        pm._staged_fractions = raw_fractions or [0.25, 0.35]
        pm._enable_leader_divergence = bool(defaults["ENABLE_LEADER_DIVERGENCE_EXIT"])
        return pm

    def test_all_optional_exits_disabled_means_no_action(self):
        pm = self._make_pm(
            ENABLE_PROFIT_LOCK=False,
            ENABLE_NEAR_MAX_EXIT=False,
            ENABLE_TRAILING_STOP=False,
            ENABLE_STAGED_TP=False,
            ENABLE_LEADER_DIVERGENCE_EXIT=False,
        )
        pm.track_buy("cid1", "tok", 0.50, 100, 50.0)
        assert pm.evaluate_risk_action("cid1", 0.39) is None
        assert pm.evaluate_risk_action("cid1", 0.81) is None

    def test_risk_fully_disabled(self):
        pm = self._make_pm(
            ENABLE_RISK_MGMT=False,
            ENABLE_PROFIT_LOCK=False,
            ENABLE_NEAR_MAX_EXIT=False,
            ENABLE_TRAILING_STOP=False,
            ENABLE_STAGED_TP=False,
            ENABLE_LEADER_DIVERGENCE_EXIT=False,
        )
        pm.track_buy("cid1", "tok", 0.50, 100, 50.0)
        assert pm.evaluate_risk_action("cid1", 0.01) is None

    def test_near_max_requires_global_risk_master(self):
        pm = self._make_pm(
            ENABLE_RISK_MGMT=False,
            ENABLE_NEAR_MAX_EXIT=True,
            ENABLE_PROFIT_LOCK=False,
            ENABLE_TRAILING_STOP=False,
            ENABLE_STAGED_TP=False,
            ENABLE_LEADER_DIVERGENCE_EXIT=False,
        )
        pm.track_buy("cid1", "tok", 0.50, 100, 50.0)
        assert pm.evaluate_risk_action("cid1", 0.96) is None

    def test_near_max_can_run_with_master_on_and_base_subengines_off(self):
        pm = self._make_pm(
            ENABLE_RISK_MGMT=True,
            ENABLE_NEAR_MAX_EXIT=True,
            ENABLE_PROFIT_LOCK=False,
            ENABLE_TRAILING_STOP=False,
            ENABLE_STAGED_TP=False,
            ENABLE_LEADER_DIVERGENCE_EXIT=False,
        )
        pm.track_buy("cid1", "tok", 0.50, 100, 50.0)
        action = pm.evaluate_risk_action("cid1", 0.96)
        assert action is not None
        assert action["reason"] == "near_max_full_exit"

    def test_profit_lock_requires_global_risk_master(self):
        pm = self._make_pm(
            ENABLE_RISK_MGMT=False,
            ENABLE_PROFIT_LOCK=True,
            ENABLE_NEAR_MAX_EXIT=False,
            ENABLE_TRAILING_STOP=False,
            ENABLE_STAGED_TP=False,
            ENABLE_LEADER_DIVERGENCE_EXIT=False,
        )
        pm.track_buy("cid1", "tok", 0.50, 100, 50.0)
        assert pm.evaluate_risk_action("cid1", 0.90) is None

    def test_profit_lock_can_run_with_master_on_and_base_subengines_off(self):
        pm = self._make_pm(
            ENABLE_RISK_MGMT=True,
            ENABLE_PROFIT_LOCK=True,
            ENABLE_NEAR_MAX_EXIT=False,
            ENABLE_TRAILING_STOP=False,
            ENABLE_STAGED_TP=False,
            ENABLE_LEADER_DIVERGENCE_EXIT=False,
        )
        pm.track_buy("cid1", "tok", 0.50, 100, 50.0)
        action = pm.evaluate_risk_action("cid1", 0.90)
        assert action is not None
        assert action["reason"] == "profit_lock"


if __name__ == "__main__":
    unittest.main()
