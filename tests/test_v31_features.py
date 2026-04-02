"""Tests for v3.1 features: advanced exits, trader discovery, leader scoring integration.

These tests mock out heavy dependencies (py_clob_client, web3) so they run in
environments without the full Polymarket SDK installed.
"""

import os
import sys
import types
import unittest
import importlib.abc
import importlib.machinery
from unittest.mock import MagicMock, patch


class _AutoMockModule(types.ModuleType):
    """Module stub that returns MagicMock for any attribute."""
    def __getattr__(self, name):
        if name.startswith('__') and name.endswith('__'):
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

    _ALLOWED = frozenset((
        # External / heavy deps we don't want to require in unit tests.
        "py_clob_client", "py_order_utils", "web3",
        "dotenv", "aiohttp", "motor", "colorama",
        "eth_account", "hexbytes",
    ))

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


sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# ═══════════════════════════════════════════════════════════════════════
# Position Exit System Tests
# ═══════════════════════════════════════════════════════════════════════


class TestPositionExitSystem(unittest.TestCase):
    """Test the advanced exit system in PositionManager."""

    def _make_pm(self, **env_overrides):
        """Create a fresh PositionManager with custom ENV."""
        defaults = {
            'ENABLE_RISK_MGMT': True,
            'STOP_LOSS_PCT': 20,
            'TAKE_PROFIT_PCT': 60,
            'ENABLE_PROFIT_LOCK': True,
            'PROFIT_LOCK_TRIGGER_PCT': 70,
            'PROFIT_LOCK_SELL_FRACTION': 0.50,
            'ENABLE_NEAR_MAX_EXIT': True,
            'NEAR_MAX_PRICE_EXIT': 0.95,
            'NEAR_MAX_PRICE_WARN': 0.92,
            'NEAR_MAX_PRICE_WARN_TRIM_RATIO': 0.50,
            'ENABLE_TRAILING_STOP': True,
            'TRAILING_ACTIVATION_PCT': 20,
            'TRAILING_DISTANCE': 0.05,
            'ENABLE_STAGED_TP': True,
            'STAGED_TP_LEVELS': '30,50',
            'STAGED_TP_FRACTIONS': '0.25,0.35',
            'ENABLE_LEADER_DIVERGENCE_EXIT': True,
        }
        defaults.update(env_overrides)

        import importlib
        import services.positionManager as pm_mod
        with patch.object(pm_mod, 'ENV', defaults):
            # Force a fresh instance
            return pm_mod.PositionManager()

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

    # ── Stop Loss ──

    def test_stop_loss_triggers(self):
        pm = self._make_pm()
        cid = self._add_position(pm, entry_price=0.50)
        action = pm.evaluate_risk_action(cid, 0.39)  # -22%
        self.assertIsNotNone(action)
        self.assertEqual(action["reason"], "stop_loss")
        self.assertEqual(action["size_fraction"], 1.0)

    def test_stop_loss_not_triggered(self):
        pm = self._make_pm()
        cid = self._add_position(pm, entry_price=0.50)
        action = pm.evaluate_risk_action(cid, 0.45)  # -10%
        self.assertIsNone(action)

    # ── Near-Max Exit ──

    def test_near_max_full_exit_at_95c(self):
        pm = self._make_pm()
        cid = self._add_position(pm, entry_price=0.50)
        action = pm.evaluate_risk_action(cid, 0.95)
        self.assertIsNotNone(action)
        self.assertEqual(action["reason"], "near_max_full_exit")
        self.assertEqual(action["size_fraction"], 1.0)

    def test_near_max_warn_at_92c(self):
        pm = self._make_pm()
        cid = self._add_position(pm, entry_price=0.50)
        action = pm.evaluate_risk_action(cid, 0.93)
        self.assertIsNotNone(action)
        self.assertEqual(action["reason"], "near_max_partial_trim")
        self.assertAlmostEqual(action["size_fraction"], 0.50)

    def test_near_max_warn_fires_once(self):
        pm = self._make_pm()
        cid = self._add_position(pm, entry_price=0.50)
        action1 = pm.evaluate_risk_action(cid, 0.93)
        self.assertEqual(action1["reason"], "near_max_partial_trim")
        pm.track_sell(cid, 0.93, 50.0, reason="near_max_partial_trim")
        action2 = pm.evaluate_risk_action(cid, 0.93)
        if action2:
            self.assertNotEqual(action2["reason"], "near_max_partial_trim")

    def test_near_max_disabled(self):
        pm = self._make_pm(ENABLE_NEAR_MAX_EXIT=False)
        cid = self._add_position(pm, entry_price=0.50)
        action = pm.evaluate_risk_action(cid, 0.96)
        if action:
            self.assertNotIn("near_max", action["reason"])

    # ── Profit Lock ──

    def test_profit_lock_at_70pct(self):
        pm = self._make_pm()
        cid = self._add_position(pm, entry_price=0.50)
        # +80% = 0.90 (below near-max warn 0.92)
        action = pm.evaluate_risk_action(cid, 0.90)
        self.assertIsNotNone(action)
        self.assertEqual(action["reason"], "profit_lock")
        self.assertAlmostEqual(action["size_fraction"], 0.50)

    def test_profit_lock_fires_once(self):
        pm = self._make_pm()
        cid = self._add_position(pm, entry_price=0.50)
        action1 = pm.evaluate_risk_action(cid, 0.90)
        self.assertEqual(action1["reason"], "profit_lock")
        pm.track_sell(cid, 0.90, 50.0, reason="profit_lock")
        action2 = pm.evaluate_risk_action(cid, 0.90)
        if action2:
            self.assertNotEqual(action2["reason"], "profit_lock")

    def test_profit_lock_disabled(self):
        pm = self._make_pm(ENABLE_PROFIT_LOCK=False)
        cid = self._add_position(pm, entry_price=0.50)
        action = pm.evaluate_risk_action(cid, 0.88)
        if action:
            self.assertNotEqual(action["reason"], "profit_lock")

    # ── Staged Take-Profit ──

    def test_staged_tp_level_1(self):
        pm = self._make_pm(ENABLE_PROFIT_LOCK=False)
        cid = self._add_position(pm, entry_price=0.50)
        action = pm.evaluate_risk_action(cid, 0.66)  # +32%
        self.assertIsNotNone(action)
        self.assertEqual(action["reason"], "staged_tp_level_1")
        self.assertAlmostEqual(action["size_fraction"], 0.25)

    def test_staged_tp_level_2(self):
        pm = self._make_pm(ENABLE_PROFIT_LOCK=False)
        cid = self._add_position(pm, entry_price=0.50)
        pm.evaluate_risk_action(cid, 0.66)  # trigger level 1
        pm.track_sell(cid, 0.66, 25.0, reason="staged_tp_level_1")
        action = pm.evaluate_risk_action(cid, 0.76)  # +52%
        self.assertIsNotNone(action)
        self.assertEqual(action["reason"], "staged_tp_level_2")
        self.assertAlmostEqual(action["size_fraction"], 0.35)

    def test_staged_tp_disabled(self):
        pm = self._make_pm(ENABLE_STAGED_TP=False, ENABLE_PROFIT_LOCK=False)
        cid = self._add_position(pm, entry_price=0.50)
        action = pm.evaluate_risk_action(cid, 0.66)
        self.assertIsNone(action)

    # ── Trailing Stop ──

    def test_trailing_activates(self):
        pm = self._make_pm(ENABLE_PROFIT_LOCK=False, ENABLE_STAGED_TP=False)
        cid = self._add_position(pm, entry_price=0.50)
        action = pm.evaluate_risk_action(cid, 0.62)  # +24%
        self.assertIsNone(action)
        pos = pm.get_position(cid)
        self.assertTrue(pos.exit_state.trailing_active)

    def test_trailing_triggers_on_pullback(self):
        pm = self._make_pm(ENABLE_PROFIT_LOCK=False, ENABLE_STAGED_TP=False)
        cid = self._add_position(pm, entry_price=0.50)
        pm.evaluate_risk_action(cid, 0.70)  # peak
        action = pm.evaluate_risk_action(cid, 0.64)  # 0.70 - 0.05 = 0.65 > 0.64
        self.assertIsNotNone(action)
        self.assertEqual(action["reason"], "trailing_stop")

    def test_trailing_no_trigger_above_trail(self):
        pm = self._make_pm(ENABLE_PROFIT_LOCK=False, ENABLE_STAGED_TP=False)
        cid = self._add_position(pm, entry_price=0.50)
        pm.evaluate_risk_action(cid, 0.70)
        action = pm.evaluate_risk_action(cid, 0.66)  # above 0.65 trail
        self.assertIsNone(action)

    def test_trailing_disabled(self):
        pm = self._make_pm(ENABLE_TRAILING_STOP=False, ENABLE_PROFIT_LOCK=False, ENABLE_STAGED_TP=False)
        cid = self._add_position(pm, entry_price=0.50)
        pm.evaluate_risk_action(cid, 0.70)
        action = pm.evaluate_risk_action(cid, 0.60)
        self.assertIsNone(action)

    # ── Leader Divergence ──

    def test_leader_divergence(self):
        pm = self._make_pm(ENABLE_PROFIT_LOCK=False, ENABLE_STAGED_TP=False, ENABLE_TRAILING_STOP=False)
        cid = self._add_position(pm, entry_price=0.50)
        pm.signal_leader_divergence(cid)
        action = pm.evaluate_risk_action(cid, 0.55)
        self.assertIsNotNone(action)
        self.assertEqual(action["reason"], "leader_divergence")
        self.assertAlmostEqual(action["size_fraction"], 0.50)

    def test_leader_divergence_fires_once(self):
        pm = self._make_pm(ENABLE_PROFIT_LOCK=False, ENABLE_STAGED_TP=False, ENABLE_TRAILING_STOP=False)
        cid = self._add_position(pm, entry_price=0.50)
        pm.signal_leader_divergence(cid)
        action1 = pm.evaluate_risk_action(cid, 0.55)
        self.assertEqual(action1["reason"], "leader_divergence")
        pm.track_sell(cid, 0.55, 50.0, reason="leader_divergence")
        action2 = pm.evaluate_risk_action(cid, 0.55)
        self.assertIsNone(action2)

    # ── Priority ──

    def test_stop_loss_beats_everything(self):
        pm = self._make_pm()
        cid = self._add_position(pm, entry_price=0.50)
        pm.signal_leader_divergence(cid)
        action = pm.evaluate_risk_action(cid, 0.39)
        self.assertEqual(action["reason"], "stop_loss")

    def test_near_max_beats_profit_lock(self):
        pm = self._make_pm()
        cid = self._add_position(pm, entry_price=0.10)
        action = pm.evaluate_risk_action(cid, 0.96)
        self.assertEqual(action["reason"], "near_max_full_exit")

    # ── Legacy TP ──

    def test_legacy_tp_when_staged_disabled(self):
        pm = self._make_pm(ENABLE_STAGED_TP=False, ENABLE_PROFIT_LOCK=False, ENABLE_TRAILING_STOP=False)
        cid = self._add_position(pm, entry_price=0.50)
        action = pm.evaluate_risk_action(cid, 0.81)  # +62%
        self.assertIsNotNone(action)
        self.assertEqual(action["reason"], "take_profit")


# ═══════════════════════════════════════════════════════════════════════
# Trader Discovery Scoring Tests
# ═══════════════════════════════════════════════════════════════════════


class TestTraderDiscoveryScoring(unittest.TestCase):

    def test_score_high_pnl(self):
        from services.traderDiscoveryService import TraderDiscoveryService, TraderProfile
        ds = TraderDiscoveryService()
        p = TraderProfile(
            wallet="0x1234567890abcdef1234567890abcdef12345678",
            overall_gain=5_000_000, win_amount=6_000_000, loss_amount=-1_000_000,
            win_rate=0.73, total_positions=200, active_positions=10,
            event_count=80, total_current_value=50_000,
        )
        score = ds._score_trader(p)
        self.assertGreater(score, 0.5)
        self.assertLessEqual(score, 1.0)

    def test_score_loser(self):
        from services.traderDiscoveryService import TraderDiscoveryService, TraderProfile
        ds = TraderDiscoveryService()
        p = TraderProfile(
            wallet="0xabcdefabcdefabcdefabcdefabcdefabcdefabcd",
            overall_gain=-500, win_amount=100, loss_amount=-600,
            win_rate=0.30, total_positions=5, event_count=3,
        )
        self.assertLess(ds._score_trader(p), 0.3)

    def test_score_empty(self):
        from services.traderDiscoveryService import TraderDiscoveryService, TraderProfile
        ds = TraderDiscoveryService()
        p = TraderProfile(wallet="0x0000000000000000000000000000000000000000")
        self.assertAlmostEqual(ds._score_trader(p), 0.0, places=1)

    def test_pct_win_rate_normalization(self):
        from services.traderDiscoveryService import TraderDiscoveryService, TraderProfile
        ds = TraderDiscoveryService()
        p = TraderProfile(
            wallet="0x1234567890abcdef1234567890abcdef12345678",
            overall_gain=100_000, win_amount=200_000, loss_amount=-100_000,
            win_rate=73.3, total_positions=50, event_count=30,
        )
        self.assertGreater(ds._score_trader(p), 0.3)


# ═══════════════════════════════════════════════════════════════════════
# Leader Scoring + Discovery Integration Tests
# ═══════════════════════════════════════════════════════════════════════


class TestLeaderScoringIntegration(unittest.TestCase):

    def _mock_env(self):
        return {
            'LEADER_SCORE_BALANCE_TARGET_USDC': 500.0,
            'LEADER_SCORE_PNL_BONUS_CAP': 0.1,
            'LEADER_SCORE_ACTIVITY_TARGET': 8,
            'LEADER_SCORE_WEIGHT_BALANCE': 0.35,
            'LEADER_SCORE_WEIGHT_ANALYTICS': 0.45,
            'LEADER_SCORE_WEIGHT_ACTIVITY': 0.20,
            'LEADER_SCORE_FLOOR': 0.10,
            'TRUST_SIZE_MULTIPLIER_MIN': 0.70,
            'TRUST_SIZE_MULTIPLIER_MAX': 1.25,
        }

    def test_scoring_without_discovery(self):
        import services.leaderScoringService as ls_mod
        with patch.object(ls_mod, 'ENV', self._mock_env()):
            scorer = ls_mod.LeaderScoringService(discovery_service=None)
            result = scorer.score_leader(
                "0x1234567890abcdef1234567890abcdef12345678",
                leader_balance=300.0,
                analytics={"win_rate": 65.0, "net_pnl": 50.0},
                recent_trade_count=5,
            )
            self.assertGreater(result.score, 0.0)
            self.assertEqual(result.discovery_score, 0.0)

    def test_scoring_with_discovery(self):
        from services.traderDiscoveryService import TraderProfile
        import services.leaderScoringService as ls_mod

        mock_profile = TraderProfile(
            wallet="0x1234567890abcdef1234567890abcdef12345678",
            overall_gain=2_000_000, win_rate=0.80, total_positions=150,
            event_count=60, active_positions=5, total_current_value=10_000,
        )
        mock_profile.composite_score = 0.75

        mock_disc = MagicMock()
        mock_disc.get_cached_profile.return_value = mock_profile

        with patch.object(ls_mod, 'ENV', self._mock_env()):
            scorer = ls_mod.LeaderScoringService(discovery_service=mock_disc)
            result = scorer.score_leader(
                "0x1234567890abcdef1234567890abcdef12345678",
                leader_balance=300.0,
            )
            self.assertGreater(result.discovery_score, 0.0)
            self.assertIn("discovery_pnl", result.details)

    def test_trust_multiplier(self):
        import services.leaderScoringService as ls_mod
        with patch.object(ls_mod, 'ENV', self._mock_env()):
            scorer = ls_mod.LeaderScoringService()
            self.assertAlmostEqual(scorer.trust_multiplier(0.0), 0.70)
            self.assertAlmostEqual(scorer.trust_multiplier(1.0), 1.25)
            self.assertAlmostEqual(scorer.trust_multiplier(0.5), 0.975)


# ═══════════════════════════════════════════════════════════════════════
# Feature Flag Tests
# ═══════════════════════════════════════════════════════════════════════


class TestFeatureFlags(unittest.TestCase):

    def _make_pm(self, **overrides):
        defaults = {
            'ENABLE_RISK_MGMT': True, 'STOP_LOSS_PCT': 20, 'TAKE_PROFIT_PCT': 60,
            'ENABLE_PROFIT_LOCK': False, 'PROFIT_LOCK_TRIGGER_PCT': 70, 'PROFIT_LOCK_SELL_FRACTION': 0.50,
            'ENABLE_NEAR_MAX_EXIT': False, 'NEAR_MAX_PRICE_EXIT': 0.95, 'NEAR_MAX_PRICE_WARN': 0.92,
            'NEAR_MAX_PRICE_WARN_TRIM_RATIO': 0.50,
            'ENABLE_TRAILING_STOP': False, 'TRAILING_ACTIVATION_PCT': 20, 'TRAILING_DISTANCE': 0.05,
            'ENABLE_STAGED_TP': False, 'STAGED_TP_LEVELS': '30,50', 'STAGED_TP_FRACTIONS': '0.25,0.35',
            'ENABLE_LEADER_DIVERGENCE_EXIT': False,
        }
        defaults.update(overrides)
        import services.positionManager as pm_mod
        with patch.object(pm_mod, 'ENV', defaults):
            return pm_mod.PositionManager()

    def test_all_disabled_sl_still_works(self):
        pm = self._make_pm()
        pm.track_buy("cid1", "tok", 0.50, 100, 50.0)
        self.assertIsNone(pm.evaluate_risk_action("cid1", 0.55))
        action = pm.evaluate_risk_action("cid1", 0.39)
        self.assertEqual(action["reason"], "stop_loss")

    def test_legacy_tp_when_all_disabled(self):
        pm = self._make_pm()
        pm.track_buy("cid1", "tok", 0.50, 100, 50.0)
        action = pm.evaluate_risk_action("cid1", 0.81)
        self.assertIsNotNone(action)
        self.assertEqual(action["reason"], "take_profit")

    def test_risk_fully_disabled(self):
        pm = self._make_pm(ENABLE_RISK_MGMT=False)
        pm.track_buy("cid1", "tok", 0.50, 100, 50.0)
        self.assertIsNone(pm.evaluate_risk_action("cid1", 0.01))


if __name__ == "__main__":
    unittest.main()
