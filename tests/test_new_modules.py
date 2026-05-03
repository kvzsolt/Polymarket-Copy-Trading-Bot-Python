"""Tests for new modules: PositionManager, PnLTracker, RiskMonitor, and config."""

from __future__ import annotations

import json
import sys
import tempfile
import types
from pathlib import Path
from unittest.mock import AsyncMock, patch, MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# Lightweight stubs for optional Polymarket SDK imports used by legacy modules.
if "py_clob_client" not in sys.modules:
    py_clob_client = types.ModuleType("py_clob_client")
    sys.modules["py_clob_client"] = py_clob_client
if "py_clob_client.clob_types" not in sys.modules:
    clob_types = types.ModuleType("py_clob_client.clob_types")
    clob_types.OrderType = type("OrderType", (), {"FOK": "FOK", "GTC": "GTC"})
    clob_types.OrderArgs = type("OrderArgs", (), {})
    clob_types.MarketOrderArgs = type("MarketOrderArgs", (), {})
    clob_types.PartialCreateOrderOptions = type("PartialCreateOrderOptions", (), {})
    sys.modules["py_clob_client.clob_types"] = clob_types
if "py_clob_client.client" not in sys.modules:
    client_mod = types.ModuleType("py_clob_client.client")
    client_mod.ClobClient = type("ClobClient", (), {})
    sys.modules["py_clob_client.client"] = client_mod
if "py_clob_client.order_builder" not in sys.modules:
    sys.modules["py_clob_client.order_builder"] = types.ModuleType("py_clob_client.order_builder")
if "py_clob_client.order_builder.constants" not in sys.modules:
    constants = types.ModuleType("py_clob_client.order_builder.constants")
    constants.BUY = "BUY"
    constants.SELL = "SELL"
    sys.modules["py_clob_client.order_builder.constants"] = constants

FAKE_TOKEN_ID = "71321045679252212594626385532706912750332728571942532223535764789812338637396"
FAKE_FUNDER = "0x0000000000000000000000000000000000000001"


# ---------------------------------------------------------------------------
# Config — _leader_wallets
# ---------------------------------------------------------------------------
class TestConfigLeaderWallets:
    def test_comma_separated(self):
        from config.env import _leader_wallets
        with patch.dict("os.environ", {"LEADER_WALLETS": "0xAAA,0xBBB,0xCCC", "USER_ADDRESS": ""}):
            result = _leader_wallets()
            assert result == ["0xAAA", "0xBBB", "0xCCC"]

    def test_single_wallet_fallback(self):
        from config.env import _leader_wallets
        with patch.dict("os.environ", {"LEADER_WALLETS": "", "USER_ADDRESS": "0xSINGLE"}):
            result = _leader_wallets()
            assert result == ["0xSINGLE"]

    def test_empty_both(self):
        from config.env import _leader_wallets
        with patch.dict("os.environ", {"LEADER_WALLETS": "", "USER_ADDRESS": ""}):
            result = _leader_wallets()
            assert result == []

    def test_whitespace_trimmed(self):
        from config.env import _leader_wallets
        with patch.dict("os.environ", {"LEADER_WALLETS": " 0xA , 0xB , ", "USER_ADDRESS": ""}):
            result = _leader_wallets()
            assert result == ["0xA", "0xB"]


# ---------------------------------------------------------------------------
# PositionManager
# ---------------------------------------------------------------------------
class TestPositionManager:
    def _make_pm(self, enabled=True, extra_env=None):
        # PositionManager now uses AppConfig via constructor injection.
        # Build an explicit mock config so the tests don't depend on bare-default
        # constructor semantics.
        config = MagicMock()
        config.risk.enable_risk_mgmt = enabled
        config.risk.profit_lock_trigger_pct = 70
        config.risk.profit_lock_sell_fraction = 0.5
        config.risk.near_max_price_exit = 0.97
        config.risk.near_max_price_warn = 0.95
        config.risk.near_max_price_warn_trim_ratio = 0.5
        config.risk.staged_tp_levels = "9999"
        config.risk.staged_tp_fractions = "0.25,0.35"
        config.features.enable_profit_lock = False
        config.features.enable_near_max_exit = False
        config.features.enable_trailing_stop = False
        config.features.enable_staged_tp = False
        if extra_env:
            if "NEAR_MAX_PRICE_WARN" in extra_env:
                config.risk.near_max_price_warn = extra_env["NEAR_MAX_PRICE_WARN"]
                config.features.enable_near_max_exit = True
            if "NEAR_MAX_PRICE_EXIT" in extra_env:
                config.risk.near_max_price_exit = extra_env["NEAR_MAX_PRICE_EXIT"]
                config.features.enable_near_max_exit = True
            if "NEAR_MAX_PRICE_WARN_TRIM_RATIO" in extra_env:
                config.risk.near_max_price_warn_trim_ratio = extra_env["NEAR_MAX_PRICE_WARN_TRIM_RATIO"]
            if "NEAR_MAX_PRICE_EXIT" in extra_env:
                config.risk.near_max_price_exit = extra_env["NEAR_MAX_PRICE_EXIT"]
                config.features.enable_near_max_exit = True
            if "PROFIT_LOCK_TRIGGER_PCT" in extra_env:
                config.risk.profit_lock_trigger_pct = extra_env["PROFIT_LOCK_TRIGGER_PCT"]
                config.features.enable_profit_lock = True
            if "STAGED_TP_LEVELS" in extra_env:
                # Very high staged TP level effectively disables legacy TP.
                config.risk.staged_tp_levels = extra_env["STAGED_TP_LEVELS"]
                config.features.enable_staged_tp = True
        from services.positionManager import PositionManager
        return PositionManager(config=config)

    def test_track_buy_creates_position(self):
        pm = self._make_pm()
        pos = pm.track_buy("c1", "a1", 0.5, 10.0, 5.0)
        assert pos.condition_id == "c1"
        assert pos.entry_price == 0.5
        assert pos.size_tokens == 10.0
        assert "c1::a1" in pm.open_positions

    def test_track_buy_averages_in(self):
        pm = self._make_pm()
        pm.track_buy("c1", "a1", 0.4, 10.0, 4.0)
        pm.track_buy("c1", "a1", 0.6, 10.0, 6.0)
        pos = pm.get_position("c1")
        assert pos.size_tokens == 20.0
        assert pos.usdc_cost == 10.0
        # Weighted avg: (0.4*10 + 0.6*10) / 20 = 0.5
        assert abs(pos.entry_price - 0.5) < 0.001

    def test_track_sell_full_close(self):
        pm = self._make_pm()
        pm.track_buy("c1", "a1", 0.5, 10.0, 5.0)
        result = pm.track_sell("c1", 0.7, 10.0, reason="leader_sell")
        assert result is not None
        assert result.exit_price == 0.7
        assert result.exit_reason == "leader_sell"
        assert result.realized_pnl == pytest.approx(2.0)  # (0.7-0.5)*10
        assert "c1::a1" not in pm.open_positions
        assert len(pm.closed_positions) == 1

    def test_track_sell_partial(self):
        pm = self._make_pm()
        pm.track_buy("c1", "a1", 0.5, 10.0, 5.0)
        result = pm.track_sell("c1", 0.6, 5.0)
        assert result is None  # Not fully closed
        pos = pm.get_position("c1")
        assert pos.size_tokens == 5.0
        assert pos.realized_pnl == pytest.approx(0.5)  # (0.6-0.5)*5

    def test_track_sell_unknown_position(self):
        pm = self._make_pm()
        assert pm.track_sell("nonexistent", 0.5, 1.0) is None

    def test_check_risk_triggers_no_basic_exit_when_optional_engines_disabled(self):
        pm = self._make_pm()
        pm.track_buy("c1", "a1", 1.0, 10.0, 10.0)
        # Basic hard stop-loss / legacy take-profit are retired.
        assert pm.check_risk_triggers("c1", 0.75) is None
        assert pm.check_risk_triggers("c1", 1.35) is None

    def test_check_risk_triggers_within_bounds(self):
        pm = self._make_pm()
        pm.track_buy("c1", "a1", 1.0, 10.0, 10.0)
        # Price up 10% — no trigger
        assert pm.check_risk_triggers("c1", 1.10) is None

    def test_check_risk_triggers_disabled(self):
        pm = self._make_pm(enabled=False)
        pm.track_buy("c1", "a1", 1.0, 10.0, 10.0)
        # Would be stop-loss but risk is disabled
        assert pm.check_risk_triggers("c1", 0.5) is None

    def test_check_risk_triggers_no_position(self):
        pm = self._make_pm()
        assert pm.check_risk_triggers("nonexistent", 0.5) is None

    def test_near_max_warn_trim_triggers_once(self):
        pm = self._make_pm(
            extra_env={
                "NEAR_MAX_PRICE_WARN": 0.95,
                "NEAR_MAX_PRICE_EXIT": 0.97,  # keep room for warn-trim stage
                "NEAR_MAX_PRICE_WARN_TRIM_RATIO": 0.50,
            },
        )
        pm.track_buy("c1", "a1", 0.5, 10.0, 5.0)
        action1 = pm.evaluate_risk_action("c1", 0.951)
        assert action1 is not None
        assert action1["reason"] == "near_max_partial_trim"
        action2 = pm.evaluate_risk_action("c1", 0.952)
        # Should not fire same trim repeatedly.
        assert not (action2 and action2.get("reason") == "near_max_partial_trim")

    def test_near_max_full_exit_defaults_to_95c(self):
        # _near_max_warn_ratio is hardcoded 0.50 in risk_settings path.
        # Set near_max_warn_level above 0.951 so warn_trim doesn't fire first;
        # near_max_exit=0.95 is the real target under test.
        from config.settings import RiskSettings
        rs = RiskSettings(
            stop_loss_ratio=0.20,
            take_profit_ratio=0.99,
            daily_drawdown_ratio=0.25,
            profit_lock_trigger_ratio=0.70,
            profit_lock_drawdown_ratio=0.05,
            profit_lock_sell_ratio=0.50,
            trailing_trigger_ratio=0.20,
            trailing_distance_ratio=0.05,
            near_max_price_level=0.95,
            near_max_warn_level=0.952,
        )
        from services.positionManager import PositionManager
        pm = PositionManager(risk_settings=rs)
        pm.track_buy("c1", "a1", 0.5, 10.0, 5.0)
        action = pm.evaluate_risk_action("c1", 0.951)
        assert action is not None
        assert action["reason"] == "near_max_full_exit"
        assert action["size_fraction"] == 1.0

    def test_profit_lock_arms_above_70_and_triggers_on_fallback(self):
        # risk_settings path gives _profit_lock_mode="legacy": arm on +70%,
        # exit on fallback below trigger. near_max disabled via level=1.0.
        from config.settings import RiskSettings
        rs = RiskSettings(
            stop_loss_ratio=0.20,
            take_profit_ratio=0.99,
            daily_drawdown_ratio=0.25,
            profit_lock_trigger_ratio=0.70,
            profit_lock_drawdown_ratio=0.05,
            profit_lock_sell_ratio=0.50,
            trailing_trigger_ratio=0.20,
            trailing_distance_ratio=0.05,
            near_max_price_level=1.0,
            near_max_warn_level=1.0,
        )
        from services.positionManager import PositionManager
        pm = PositionManager(risk_settings=rs)
        pm.track_buy("c1", "a1", 0.5, 10.0, 5.0)
        # Arm lock first (> +70%) but do not exit immediately.
        assert pm.evaluate_risk_action("c1", 0.90) is None
        pos = pm.get_position("c1")
        assert pos is not None and pos.profit_lock_armed is True
        # Staying above 70% should not trigger.
        assert pm.evaluate_risk_action("c1", 0.86) is None
        assert pos.profit_lock_held_logged is True
        # Fallback below 70% profit should trigger the exit.
        action = pm.evaluate_risk_action("c1", 0.84)
        assert action is not None
        assert action["reason"] == "profit_lock_fallback"
        assert action["size_fraction"] == 1.0

    def test_summary_empty(self):
        pm = self._make_pm()
        s = pm.summary()
        assert s["open_positions"] == 0
        assert s["closed_positions"] == 0
        assert s["win_rate"] == 0.0

    def test_same_condition_different_tokens_do_not_collapse(self):
        pm = self._make_pm()
        pm.track_buy("c1", "token-a", 0.5, 10.0, 5.0)
        pm.track_buy("c1", "token-b", 0.4, 20.0, 8.0)
        assert len(pm.open_positions) == 2
        assert pm.get_position("c1") is None

    def test_summary_with_trades(self):
        pm = self._make_pm()
        pm.track_buy("c1", "a1", 0.5, 10.0, 5.0)
        pm.track_buy("c2", "a2", 0.3, 20.0, 6.0)
        pm.track_sell("c1", 0.7, 10.0)  # Win: +$2
        pm.track_sell("c2", 0.2, 20.0)  # Loss: -$2
        s = pm.summary()
        assert s["open_positions"] == 0
        assert s["closed_positions"] == 2
        assert s["wins"] == 1
        assert s["losses"] == 1
        assert s["win_rate"] == 50.0

    def test_get_position_nonexistent(self):
        pm = self._make_pm()
        assert pm.get_position("xyz") is None


# ---------------------------------------------------------------------------
# PnLTracker
# ---------------------------------------------------------------------------
class TestPnLTracker:
    def _make_tracker(self):
        from services.pnlTracker import PnLTracker
        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False, mode="w") as f:
            path = f.name
        return PnLTracker(log_path=path), path

    def test_record_buy_updates_metrics(self):
        tracker, _ = self._make_tracker()
        tracker.record_buy("c1", 0.5, 10.0, 5.0)
        m = tracker.metrics()
        assert m["total_buys"] == 1
        assert m["total_invested"] == 5.0

    def test_record_sell_updates_metrics(self):
        tracker, _ = self._make_tracker()
        tracker.record_buy("c1", 0.5, 10.0, 5.0)
        tracker.record_sell("c1", 0.7, 10.0, 7.0, entry_price=0.5, realized_pnl=2.0, exit_reason="leader_sell")
        m = tracker.metrics()
        assert m["total_sells"] == 1
        assert m["wins"] == 1
        assert m["net_pnl"] == 2.0
        assert m["total_returned"] == 7.0

    def test_win_rate_calculation(self):
        tracker, _ = self._make_tracker()
        # 2 wins, 1 loss
        tracker.record_sell("c1", 0.6, 10.0, 6.0, realized_pnl=1.0)
        tracker.record_sell("c2", 0.7, 10.0, 7.0, realized_pnl=2.0)
        tracker.record_sell("c3", 0.3, 10.0, 3.0, realized_pnl=-2.0)
        m = tracker.metrics()
        assert m["wins"] == 2
        assert m["losses"] == 1
        assert m["win_rate"] == pytest.approx(66.7, abs=0.1)

    def test_jsonl_file_written(self):
        tracker, path = self._make_tracker()
        tracker.record_buy("c1", 0.5, 10.0, 5.0)
        with open(path) as f:
            lines = f.readlines()
        assert len(lines) == 1
        record = json.loads(lines[0])
        assert record["condition_id"] == "c1"
        assert record["side"] == "BUY"

    def test_max_drawdown_calculation(self):
        tracker, _ = self._make_tracker()
        tracker.record_buy("c1", 0.5, 10.0, 5.0)
        # Sell at profit: net = 7 - 5 = 2
        tracker.record_sell("c1", 0.7, 10.0, 7.0, realized_pnl=2.0)
        # Another buy/sell at loss: net = 7 + 3 - 5 - 10 = -5, drawdown from peak(2) = 7
        tracker.record_buy("c2", 0.5, 20.0, 10.0)
        tracker.record_sell("c2", 0.15, 20.0, 3.0, realized_pnl=-7.0)
        m = tracker.metrics()
        assert m["max_drawdown"] > 0

    def test_empty_metrics(self):
        tracker, _ = self._make_tracker()
        m = tracker.metrics()
        assert m["total_trades"] == 0
        assert m["win_rate"] == 0.0
        assert m["net_pnl"] == 0.0

    def test_print_summary_no_crash(self):
        tracker, _ = self._make_tracker()
        tracker.print_summary()  # Should not raise


# ---------------------------------------------------------------------------
# RiskMonitor — risk_monitor_loop
# ---------------------------------------------------------------------------
class TestRiskMonitor:
    @pytest.mark.asyncio
    async def test_risk_monitor_exits_when_global_risk_master_disabled(self):
        """When the global risk-management master is off, the loop should return immediately.

        We only assert on externally observable behavior (no venue calls). We deliberately
        do not assert that ``ExecutionService`` was constructed: the disabled-path is allowed
        to early-return before allocating any venue-bound services.
        """
        from services.riskMonitor import risk_monitor_loop
        from services.healthServer import ReadinessTracker
        mock_clob = AsyncMock()
        mock_config = MagicMock()
        mock_config.polling.risk_check_interval = 1
        mock_config.risk.enable_risk_mgmt = False
        await risk_monitor_loop(mock_clob, {}, None, None, ReadinessTracker(), config=mock_config)
        mock_clob.get_price.assert_not_called()

    @pytest.mark.asyncio
    async def test_risk_monitor_triggers_sell(self):
        """When a position triggers SL, the monitor should sell."""
        from services.riskMonitor import risk_monitor_loop
        from services.positionManager import PositionManager, TrackedPosition

        mock_clob = AsyncMock()
        mock_clob.get_price.return_value = {"price": "0.3", "size": "100"}

        mock_pm = MagicMock(spec=PositionManager)
        pos = TrackedPosition(
            condition_id="c1", asset=FAKE_TOKEN_ID, side="BUY",
            entry_price=0.5, size_tokens=10.0, usdc_cost=5.0,
        )
        mock_pm.open_positions = {"c1": pos}
        mock_pm.evaluate_risk_action.return_value = {"reason": "near_max_full_exit", "size_fraction": 1.0}

        mock_tracker = MagicMock()

        mock_config = MagicMock()
        mock_config.polling.risk_check_interval = 1
        mock_config.risk.enable_risk_mgmt = True
        mock_config.execution.slippage_tolerance = 2.0
        mock_config.execution.transaction_fee_bps = 0
        mock_config.wallet.funder_address = FAKE_FUNDER
        iteration_count = 0

        async def patched_sleep(n):
            nonlocal iteration_count
            iteration_count += 1
            if iteration_count >= 1:
                raise asyncio.CancelledError()

        import asyncio
        with patch("services.riskMonitor.asyncio.sleep", side_effect=patched_sleep), \
             patch("services.riskMonitor.ExecutionService") as mock_exec_cls:
            mock_exec = mock_exec_cls.return_value
            mock_exec.execute = AsyncMock(
                return_value=MagicMock(
                    accepted=True,
                    filled_amount=10.0,
                    avg_price=0.3,
                    status="filled",
                )
            )
            with pytest.raises(asyncio.CancelledError):
                await risk_monitor_loop(mock_clob, {}, mock_pm, mock_tracker, mock_tracker, config=mock_config)
        mock_exec.execute.assert_called_once()
        mock_pm.track_sell.assert_called_once()

    @pytest.mark.asyncio
    async def test_risk_monitor_runs_when_master_enabled_for_optional_exit_testing(self):
        """Optional exits can be tested in isolation as long as the global master stays on."""
        from services.riskMonitor import risk_monitor_loop
        from services.positionManager import PositionManager, TrackedPosition

        mock_clob = AsyncMock()
        mock_clob.get_price.return_value = {"price": "0.96", "size": "100"}

        mock_pm = MagicMock(spec=PositionManager)
        pos = TrackedPosition(
            condition_id="c1", asset=FAKE_TOKEN_ID, side="BUY",
            entry_price=0.5, size_tokens=10.0, usdc_cost=5.0,
        )
        mock_pm.open_positions = {"c1": pos}
        mock_pm.evaluate_risk_action.return_value = {"reason": "near_max_full_exit", "size_fraction": 1.0}

        mock_tracker = MagicMock()

        mock_config = MagicMock()
        mock_config.polling.risk_check_interval = 1
        mock_config.risk.enable_risk_mgmt = True
        mock_config.execution.slippage_tolerance = 2.0
        mock_config.execution.transaction_fee_bps = 0
        mock_config.wallet.funder_address = FAKE_FUNDER

        iteration_count = 0

        async def patched_sleep(n):
            nonlocal iteration_count
            iteration_count += 1
            if iteration_count >= 1:
                raise asyncio.CancelledError()

        import asyncio
        with patch("services.riskMonitor.asyncio.sleep", side_effect=patched_sleep), \
             patch("services.riskMonitor.ExecutionService") as mock_exec_cls:
            mock_exec = mock_exec_cls.return_value
            mock_exec.execute = AsyncMock(
                return_value=MagicMock(
                    accepted=True,
                    filled_amount=10.0,
                    avg_price=0.96,
                    status="filled",
                )
            )
            with pytest.raises(asyncio.CancelledError):
                await risk_monitor_loop(mock_clob, {}, mock_pm, mock_tracker, mock_tracker, config=mock_config)
        mock_exec.execute.assert_called_once()
        mock_pm.track_sell.assert_called_once()


# ---------------------------------------------------------------------------
# Config — helpers
# ---------------------------------------------------------------------------
class TestConfigHelpers:
    def test_float_helper(self):
        from config.env import _float
        with patch.dict("os.environ", {"TEST_FLOAT": "3.14"}):
            assert _float("TEST_FLOAT", "0") == pytest.approx(3.14)

    def test_float_helper_default(self):
        from config.env import _float
        with patch.dict("os.environ", {}, clear=False):
            result = _float("NONEXISTENT_KEY_XYZ_123", "42.5")
            assert result == pytest.approx(42.5)

    def test_int_helper(self):
        from config.env import _int
        with patch.dict("os.environ", {"TEST_INT": "7"}):
            assert _int("TEST_INT", "0") == 7

    def test_int_helper_default(self):
        from config.env import _int
        with patch.dict("os.environ", {}, clear=False):
            assert _int("NONEXISTENT_KEY_INT_XYZ", "99") == 99

    def test_app_config_allows_subsecond_risk_interval(self):
        from config.app_config import load_config
        with patch("config.app_config._runtime_python_supported", return_value=True):
            cfg = load_config({
                "PRIVATE_KEY": "0x" + "1" * 64,
                "FUNDER_ADDRESS": "0x" + "2" * 40,
                "RPC_URL": "https://polygon-rpc.example",
                "CHAIN_ID": "137",
                "SIGNATURE_TYPE": "2",
                "RISK_CHECK_INTERVAL": "0.25",
            })
        assert cfg.polling.risk_check_interval == pytest.approx(0.25)


# ---------------------------------------------------------------------------
# AutoRedeem — _is_market_resolved (removed in DI refactor, function no longer exists)
# ---------------------------------------------------------------------------
class TestAutoRedeemResolution:
    @pytest.mark.skip(reason="_is_market_resolved was removed in the DI refactor (PR #20)")
    def test_resolved_true(self):
        pass

    @pytest.mark.skip(reason="_is_market_resolved was removed in the DI refactor (PR #20)")
    def test_not_resolved(self):
        pass

    @pytest.mark.skip(reason="_is_market_resolved was removed in the DI refactor (PR #20)")
    def test_api_error_returns_none(self):
        pass

    @pytest.mark.skip(reason="_is_market_resolved was removed in the DI refactor (PR #20)")
    def test_none_market_returns_none(self):
        pass


# ---------------------------------------------------------------------------
# AutoRedeem — _get_winning_index_sets
# ---------------------------------------------------------------------------
class TestAutoRedeemIndexSets:
    @pytest.mark.skip(reason="_get_winning_index_sets was removed in the DI refactor (PR #20)")
    def test_returns_both_sets(self):
        pass


# ---------------------------------------------------------------------------
# AutoRedeem — auto_redeem_loop
# ---------------------------------------------------------------------------
class TestAutoRedeemLoop:
    @pytest.mark.asyncio
    async def test_disabled_returns_immediately(self):
        from services.autoRedeem import auto_redeem_loop
        mock_clob = AsyncMock()
        config = MagicMock()
        config.redemption.enable_auto_redeem = False
        config.polling.redeem_check_interval = 1
        config.network.rpc_url = "http://rpc"
        await auto_redeem_loop(mock_clob, config)
        assert True

    @pytest.mark.asyncio
    async def test_no_rpc_returns_immediately(self):
        from services.autoRedeem import auto_redeem_loop
        mock_clob = AsyncMock()
        config = MagicMock()
        config.redemption.enable_auto_redeem = True
        config.polling.redeem_check_interval = 1
        config.network.rpc_url = ""
        await auto_redeem_loop(mock_clob, config)
        assert True

    @pytest.mark.asyncio
    async def test_loop_checks_resolved_and_redeems(self):
        from services.autoRedeem import auto_redeem_loop
        from services.redemptionService import RedeemablePosition

        iteration_count = 0

        async def patched_sleep(n):
            nonlocal iteration_count
            iteration_count += 1
            if iteration_count >= 1:
                raise asyncio.CancelledError()

        import asyncio
        redeemables = [
            RedeemablePosition(
                user_id="0x1111111111111111111111111111111111111111",
                token_id="a1",
                condition_id="0xabc123def456abc123def456abc123def456abc123def456abc123def456abc1",
                outcome_index=0,
                size=10.0,
                cur_price=0.99,
                current_value=9.9,
                title="Test",
                outcome="Yes",
            )
        ]
        
        config = MagicMock()
        config.redemption.enable_auto_redeem = True
        config.polling.redeem_check_interval = 1
        config.network.rpc_url = "http://localhost:8545"
        config.wallet.funder_address = "0x1111111111111111111111111111111111111111"
        config.database.db_path = "data/test.db"

        with patch("services.autoRedeem.RedemptionService") as mock_service_cls, \
             patch("services.autoRedeem.PositionsRepo") as mock_repo_cls, \
             patch("services.autoRedeem.asyncio.sleep", side_effect=patched_sleep):
            svc = mock_service_cls.return_value
            repo = mock_repo_cls.return_value
            svc.scan_redeemables = AsyncMock(return_value=redeemables)
            svc.redeem_condition = AsyncMock(return_value=MagicMock(submitted=True, status="confirmed", tx_hash="0xtx", error=None))
            await auto_redeem_loop(MagicMock(), config)
        svc.redeem_condition.assert_called_once()
        repo.delete_position.assert_called_once_with(
            "0x1111111111111111111111111111111111111111",
            "0xabc123def456abc123def456abc123def456abc123def456abc123def456abc1",
            token_id="a1",
        )


# ---------------------------------------------------------------------------
# AutoRedeem — _build_redeem_tx
# ---------------------------------------------------------------------------
class TestBuildRedeemTx:
    @pytest.mark.skip(reason="_build_redeem_tx was removed in the DI refactor (PR #20)")
    def test_builds_valid_tx(self):
        pass

    @pytest.mark.skip(reason="_build_redeem_tx was removed in the DI refactor (PR #20)")
    def test_invalid_address_returns_none(self):
        pass


# ---------------------------------------------------------------------------
# Proportional sell — verify sell ratio matches leader
# ---------------------------------------------------------------------------
