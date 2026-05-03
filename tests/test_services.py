from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch, MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.tradeMonitor import _normalize_activities
from services.copyDecisionService import CopyDecision
from legacy.tradeExecutor import (
    _calc_buy_amount,
    _apply_balance_cap,
    _min_valid_buy_notional_usdc,
    _maybe_boost_tiny_leader_buy_cap,
)
from utils.getMyBalance import _get_balance_sync, get_my_balance


# ---------------------------------------------------------------------------
# TradeMonitor — _normalize_activities
# ---------------------------------------------------------------------------
class TestTradeMonitorNormalize:
    def test_normalize_list(self):
        data = [{"a": 1}, {"b": 2}]
        assert _normalize_activities(data) == data

    def test_normalize_dict_with_data(self):
        assert _normalize_activities({"data": [{"a": 1}]}) == [{"a": 1}]

    def test_normalize_dict_with_activities(self):
        assert _normalize_activities({"activities": [{"a": 1}]}) == [{"a": 1}]

    def test_normalize_dict_with_results(self):
        assert _normalize_activities({"results": [{"a": 1}]}) == [{"a": 1}]

    def test_normalize_none(self):
        assert _normalize_activities(None) == []

    def test_normalize_empty_list(self):
        assert _normalize_activities([]) == []

    def test_normalize_non_dict_non_list(self):
        assert _normalize_activities("string") == []

    def test_normalize_dict_with_empty_known_keys(self):
        """Dict with known keys but empty lists should return empty list."""
        assert _normalize_activities({"data": []}) == []
        assert _normalize_activities({"activities": []}) == []
        assert _normalize_activities({"results": []}) == []

    def test_normalize_dict_without_known_keys(self):
        """Dict without data/activities/results falls through to empty."""
        assert _normalize_activities({"unknown": [1, 2]}) == []

    def test_normalize_integer_input(self):
        assert _normalize_activities(42) == []


# ---------------------------------------------------------------------------
# TradeExecutor — _calc_buy_amount
# ---------------------------------------------------------------------------
class TestTradeExecutorCalcBuyAmount:
    def test_basic_calculation(self):
        with patch("legacy.tradeExecutor.ENV", {"SIZING_MODE": "proportional", "FIXED_AMOUNT": 10}):
            assert _calc_buy_amount({"usdcSize": 100}, 0.05, 0.5, 50) == 5.0

    def test_clamp_to_max(self):
        with patch("legacy.tradeExecutor.ENV", {"SIZING_MODE": "proportional", "FIXED_AMOUNT": 10}):
            assert _calc_buy_amount({"usdcSize": 10000}, 0.05, 0.5, 50) == 50

    def test_clamp_to_min(self):
        with patch("legacy.tradeExecutor.ENV", {"SIZING_MODE": "proportional", "FIXED_AMOUNT": 10}):
            assert _calc_buy_amount({"usdcSize": 1}, 0.05, 5.0, 100) == 5.0

    def test_fallback_to_size_times_price(self):
        trade = {"usdcSize": 0, "size": 10, "price": 0.5}
        with patch("legacy.tradeExecutor.ENV", {"SIZING_MODE": "proportional", "FIXED_AMOUNT": 10}):
            # 10 * 0.5 * 0.10 = 0.5, but $1 floor applies
            assert _calc_buy_amount(trade, 0.10, 0.1, 100) == 1.0

    def test_zero_usdc_zero_size(self):
        trade = {"usdcSize": 0, "size": 0, "price": 0}
        with patch("legacy.tradeExecutor.ENV", {"SIZING_MODE": "proportional", "FIXED_AMOUNT": 10}):
            # 0 * ratio = 0, clamped to buy_min=0.5, but $1 floor applies
            assert _calc_buy_amount(trade, 0.05, 0.5, 100) == 1.0

    def test_none_usdc_size_falls_back(self):
        """When usdcSize is None, should fall back to size * price."""
        trade = {"usdcSize": None, "size": 20, "price": 0.5}
        with patch("legacy.tradeExecutor.ENV", {"SIZING_MODE": "proportional", "FIXED_AMOUNT": 10}):
            # 20*0.5*0.10 = 1.0
            assert _calc_buy_amount(trade, 0.10, 0.1, 100) == 1.0

    def test_missing_keys_returns_dollar_floor(self):
        """Empty trade dict should clamp to $1 floor."""
        with patch("legacy.tradeExecutor.ENV", {"SIZING_MODE": "proportional", "FIXED_AMOUNT": 10}):
            assert _calc_buy_amount({}, 0.05, 0.5, 100) == 1.0

    def test_min_amount_below_one_gets_floored(self):
        """buy_min < $1 should be floored to $1."""
        with patch("legacy.tradeExecutor.ENV", {"SIZING_MODE": "proportional", "FIXED_AMOUNT": 10}):
            assert _calc_buy_amount({"usdcSize": 1}, 0.05, 0.5, 100) == 1.0

    def test_exact_boundary_no_clamp(self):
        """proxy_amount exactly at max should return max."""
        with patch("legacy.tradeExecutor.ENV", {"SIZING_MODE": "proportional", "FIXED_AMOUNT": 10}):
            assert _calc_buy_amount({"usdcSize": 1000}, 0.05, 0.5, 50) == 50

    def test_ratio_of_one(self):
        """100% copy ratio should mirror leader amount."""
        with patch("legacy.tradeExecutor.ENV", {"SIZING_MODE": "proportional", "FIXED_AMOUNT": 10}):
            assert _calc_buy_amount({"usdcSize": 25}, 1.0, 1.0, 100) == 25.0

    def test_string_usdc_size_coerced(self):
        """String numeric values should be coerced via float()."""
        with patch("legacy.tradeExecutor.ENV", {"SIZING_MODE": "proportional", "FIXED_AMOUNT": 10}):
            assert _calc_buy_amount({"usdcSize": "200"}, 0.05, 0.5, 50) == 10.0

    def test_fixed_sizing_mode(self):
        """Fixed mode ignores leader trade size and uses FIXED_AMOUNT."""
        with patch("legacy.tradeExecutor.ENV", {"SIZING_MODE": "fixed", "FIXED_AMOUNT": 25}):
            result = _calc_buy_amount({"usdcSize": 1000}, 0.05, 1, 100)
            assert result == 25.0

    def test_fixed_sizing_mode_clamped_to_max(self):
        """Fixed amount exceeding max should be clamped."""
        with patch("legacy.tradeExecutor.ENV", {"SIZING_MODE": "fixed", "FIXED_AMOUNT": 200}):
            result = _calc_buy_amount({"usdcSize": 1000}, 0.05, 1, 100)
            assert result == 100

    def test_adaptive_sizing_mode(self):
        """Adaptive mode scales ratio by win rate."""
        mock_pm = MagicMock()
        mock_pm.summary.return_value = {"win_rate": 80}
        with patch("legacy.tradeExecutor.ENV", {"SIZING_MODE": "adaptive", "FIXED_AMOUNT": 10}), \
             patch("legacy.tradeExecutor.get_position_manager", return_value=mock_pm):
            # leader=100, ratio=0.10, win_rate=80% → multiplier = 0.5 + 0.8*1.5 = 1.7
            # 100 * 0.10 * 1.7 = 17.0
            result = _calc_buy_amount({"usdcSize": 100}, 0.10, 1, 200)
            assert result == 17.0


# ---------------------------------------------------------------------------
# TradeExecutor — _apply_balance_cap
# ---------------------------------------------------------------------------
class TestApplyBalanceCap:
    def test_caps_at_max_position_pct(self):
        with patch("legacy.tradeExecutor.ENV", {"MAX_POSITION_PCT": 0.25}):
            assert _apply_balance_cap(50, 100) == 25.0

    def test_amount_below_cap_unchanged(self):
        with patch("legacy.tradeExecutor.ENV", {"MAX_POSITION_PCT": 0.25}):
            assert _apply_balance_cap(10, 100) == 10.0

    def test_caps_at_balance_when_low(self):
        with patch("legacy.tradeExecutor.ENV", {"MAX_POSITION_PCT": 0.25}):
            assert _apply_balance_cap(50, 20) == 5.0

    def test_tiny_balance_allows_full(self):
        with patch("legacy.tradeExecutor.ENV", {"MAX_POSITION_PCT": 0.25}):
            # max_allowed = 2*0.25 = 0.5 < 1.0, so falls back to full balance
            assert _apply_balance_cap(1.5, 2.0) == 1.5


class TestTinyLeaderBoost:
    def test_min_valid_buy_notional_prefers_five_shares_when_larger(self):
        # Default SLIPPAGE_TOLERANCE=2% => worst_price=0.408, 5 tokens => 2.04 USDC
        assert _min_valid_buy_notional_usdc(live_price=0.40, market_min_size=0.0) == 2.04

    def test_min_valid_buy_notional_respects_market_min_size(self):
        # Default SLIPPAGE_TOLERANCE=2% => worst_price=0.204, 8 tokens => 1.632 USDC
        assert _min_valid_buy_notional_usdc(live_price=0.20, market_min_size=8.0) == 1.6320000000000001

    def test_min_valid_buy_notional_slippage_zero_matches_quote(self):
        with patch.dict("legacy.tradeExecutor.ENV", {"SLIPPAGE_TOLERANCE": 0.0}, clear=False):
            assert _min_valid_buy_notional_usdc(live_price=0.40, market_min_size=0.0) == 2.0

    def test_boosts_tiny_leader_buy_when_confidence_high(self):
        with patch.dict("legacy.tradeExecutor.ENV", {"MAX_POSITION_PCT": 0.25}, clear=False):
            boosted = _maybe_boost_tiny_leader_buy_cap(
                leader_notional=0.40,
                min_leader_trade_notional=1.0,
                decision_score=0.90,
                raw_amount=1.0,
                shadow_cash=100.0,
                live_price=0.50,
                market_min_size=0.0,
                high_confidence_score=0.75,
            )
        # Default SLIPPAGE_TOLERANCE=2% => worst_price=0.51, 5 tokens => 2.55 USDC
        assert boosted == 2.55

    def test_does_not_boost_tiny_leader_buy_when_confidence_low(self):
        with patch("legacy.tradeExecutor.ENV", {"MAX_POSITION_PCT": 0.25}):
            boosted = _maybe_boost_tiny_leader_buy_cap(
                leader_notional=0.40,
                min_leader_trade_notional=1.0,
                decision_score=0.50,
                raw_amount=1.0,
                shadow_cash=100.0,
                live_price=0.50,
                market_min_size=0.0,
                high_confidence_score=0.75,
            )
        assert boosted == 1.0


# ---------------------------------------------------------------------------
# getMyBalance
# ---------------------------------------------------------------------------
class TestGetMyBalance:
    def test_sync_balance_invalid_address_returns_none(self):
        assert _get_balance_sync("not-an-address") is None

    def test_sync_balance_prefers_collateral_contract_over_legacy_usdc(self):
        fake_contract = MagicMock()
        fake_contract.functions.balanceOf.return_value.call.return_value = 12_500_000
        fake_w3 = MagicMock()
        fake_w3.eth.get_code.return_value = b"\x01"
        fake_w3.eth.contract.return_value = fake_contract

        with patch.dict(
            "utils.getMyBalance.ENV",
            {
                "RPC_URL": "http://rpc",
                "USDC_CONTRACT_ADDRESS": "0x" + "a" * 40,
                "PUSD_CONTRACT_ADDRESS": "0x" + "b" * 40,
                "COLLATERAL_CONTRACT_ADDRESS": "0x" + "c" * 40,
            },
            clear=False,
        ), patch("utils.getMyBalance.Web3") as mock_web3:
            mock_web3.HTTPProvider.return_value = object()
            mock_web3.return_value = fake_w3
            mock_web3.to_checksum_address.side_effect = lambda v: f"chk:{v}"

            assert _get_balance_sync("0x1111111111111111111111111111111111111111") == 12.5
            contract_address = fake_w3.eth.contract.call_args.kwargs["address"]
            assert contract_address == "chk:0x" + "c" * 40

    @pytest.mark.asyncio
    async def test_async_balance_returns_value(self):
        with patch("utils.getMyBalance._get_balance_sync", return_value=12.5):
            assert await get_my_balance("0x1111111111111111111111111111111111111111") == 12.5

    @pytest.mark.asyncio
    async def test_async_balance_handles_sync_failure(self):
        with patch("utils.getMyBalance._get_balance_sync", side_effect=RuntimeError("rpc down")):
            assert await get_my_balance("0x1111111111111111111111111111111111111111") is None


# ---------------------------------------------------------------------------
# TradeExecutor — execute_trades (direct call, no queue)
# ---------------------------------------------------------------------------
class TestTradeExecutorExecuteTrades:
    @pytest.mark.asyncio
    async def test_execute_trades_calls_post_order(self):
        """execute_trades should call execution service for each trade."""
        from legacy.tradeExecutor import execute_trades
        mock_clob = AsyncMock()
        mock_clob.get_min_order_size.return_value = 0.0
        mock_clob.get_price.return_value = {"price": "0.5", "size": "100"}
        mock_exec = MagicMock()
        mock_exec.execute = AsyncMock(
            return_value=MagicMock(
                accepted=True,
                filled_amount=Decimal("10"),
                avg_price=Decimal("0.5"),
                status="filled",
                order_id="o1",
                error=None,
            )
        )

        trade = {
            "conditionId": "c1", "asset": "a1", "transactionHash": "0x1",
            "side": "BUY", "usdcSize": 100, "size": 10, "price": 0.5,
        }
        bot_params = {
            "leaderWallet": "0xleader",
            "tradeRate": 5,
            "buyMinAmount": 1,
            "buyMaxAmount": 50,
            "positionThreshold": 0,
        }
        with patch("legacy.tradeExecutor.get_my_balance", new_callable=AsyncMock, return_value=100.0), \
             patch("legacy.tradeExecutor.fetch_data", new_callable=AsyncMock, return_value=[]), \
             patch("legacy.tradeExecutor._get_execution_service", return_value=mock_exec), \
             patch.dict("legacy.tradeExecutor.ENV", {"FUNDER_ADDRESS": "0x" + "a" * 40}, clear=False):
            await execute_trades(mock_clob, bot_params, [trade])
        mock_exec.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_execute_trades_skips_buy_below_min_share_price(self):
        """BUY should be skipped when live quote is below configured MIN_SHARE_PRICE."""
        from legacy.tradeExecutor import execute_trades
        mock_clob = AsyncMock()
        mock_clob.get_price.return_value = {"price": "0.50", "size": "100"}
        mock_exec = MagicMock()
        mock_exec.execute = AsyncMock()

        trade = {
            "conditionId": "c1", "asset": "a1", "transactionHash": "0x1",
            "side": "BUY", "usdcSize": 100, "size": 10, "price": 0.5,
        }
        bot_params = {
            "leaderWallet": "0xleader",
            "tradeRate": 5,
            "buyMinAmount": 1,
            "buyMaxAmount": 50,
            "positionThreshold": 0,
        }
        with patch("legacy.tradeExecutor.get_my_balance", new_callable=AsyncMock, return_value=100.0), \
             patch("legacy.tradeExecutor.fetch_data", new_callable=AsyncMock, return_value=[]), \
             patch("legacy.tradeExecutor._get_execution_service", return_value=mock_exec), \
             patch.dict("legacy.tradeExecutor.ENV", {"MIN_SHARE_PRICE": 0.60, "FUNDER_ADDRESS": "0x" + "a" * 40}, clear=False):
            await execute_trades(mock_clob, bot_params, [trade])

        mock_exec.execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_execute_trades_empty_list_no_crash(self):
        """Passing an empty trade list should not crash."""
        from legacy.tradeExecutor import execute_trades
        mock_clob = AsyncMock()
        bot_params = {
            "leaderWallet": "0xleader",
            "tradeRate": 5,
            "buyMinAmount": 1,
            "buyMaxAmount": 50,
            "positionThreshold": 0,
        }
        with patch("legacy.tradeExecutor.get_my_balance", new_callable=AsyncMock, return_value=100.0), \
             patch("legacy.tradeExecutor.fetch_data", new_callable=AsyncMock, return_value=[]), \
             patch("legacy.tradeExecutor._get_execution_service", return_value=MagicMock(execute=AsyncMock())):
            await execute_trades(mock_clob, bot_params, [])

    @pytest.mark.asyncio
    async def test_execute_trades_skips_unknown_side(self):
        """Trades with unknown side should be skipped."""
        from legacy.tradeExecutor import execute_trades
        mock_clob = AsyncMock()
        trade = {
            "conditionId": "c1", "asset": "a1", "transactionHash": "0x1",
            "side": "UNKNOWN", "usdcSize": 10,
        }
        bot_params = {
            "leaderWallet": "0xleader",
            "tradeRate": 5,
            "buyMinAmount": 1,
            "buyMaxAmount": 50,
            "positionThreshold": 0,
        }
        with patch("legacy.tradeExecutor.get_my_balance", new_callable=AsyncMock, return_value=100.0), \
             patch("legacy.tradeExecutor.fetch_data", new_callable=AsyncMock, return_value=[]), \
             patch.dict("legacy.tradeExecutor.ENV", {"FUNDER_ADDRESS": "0x" + "a" * 40}, clear=False), \
             patch("legacy.tradeExecutor._get_execution_service", return_value=MagicMock(execute=AsyncMock())) as mock_exec:
            await execute_trades(mock_clob, bot_params, [trade])
        mock_exec.return_value.execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_execute_trades_skips_when_balance_lookup_fails(self):
        """Missing FUNDER_ADDRESS should raise before orders are posted."""
        from legacy.tradeExecutor import execute_trades
        mock_clob = AsyncMock()
        trade = {
            "conditionId": "c1", "asset": "a1", "transactionHash": "0x1",
            "side": "BUY", "usdcSize": 10,
        }
        bot_params = {
            "leaderWallet": "0xleader",
            "tradeRate": 5,
            "buyMinAmount": 1,
            "buyMaxAmount": 50,
            "positionThreshold": 0,
        }
        with patch.dict("legacy.tradeExecutor.ENV", {"FUNDER_ADDRESS": ""}, clear=False), \
             patch("legacy.tradeExecutor.get_my_balance", new_callable=AsyncMock, return_value=100.0), \
             patch("legacy.tradeExecutor.fetch_data", new_callable=AsyncMock, return_value=[]), \
             patch("legacy.tradeExecutor._get_execution_service", return_value=MagicMock(execute=AsyncMock())):
            with pytest.raises(ValueError, match="FUNDER_ADDRESS"):
                await execute_trades(mock_clob, bot_params, [trade])

    @pytest.mark.asyncio
    async def test_execute_trades_does_not_log_executed_for_duplicate_or_skip(self):
        """Skipped/duplicate actions must not be logged as EXECUTED."""
        from legacy.tradeExecutor import execute_trades
        mock_clob = AsyncMock()
        mock_clob.get_price.return_value = {"price": "0.50", "size": "100"}
        mock_exec = MagicMock()
        mock_exec.market_meta_service = MagicMock(
            get=AsyncMock(
                return_value=SimpleNamespace(min_order_size=Decimal("0.00001"))
            )
        )
        mock_exec.execute = AsyncMock(
            return_value=MagicMock(
                accepted=True,
                filled_amount=Decimal("0"),
                avg_price=Decimal("0.5"),
                status="duplicate_ignored",
                order_id="dup-order",
                error=None,
            )
        )

        decision = CopyDecision(
            allow=True,
            skip_code="ok",
            reason="approved",
            side="BUY",
            decision_score=0.9,
            leader_score=0.9,
            trade_score=0.9,
            live_price=0.5,
            reference_price=0.5,
            target_notional_usdc=2.5,
            target_size_tokens=5.0,
            price_drift_pct=0.0,
            edge_decay_pct=0.0,
        )

        trade = {
            "conditionId": "c1",
            "asset": "a1",
            "transactionHash": "0xleaderhash",
            "side": "BUY",
            "usdcSize": 10,
            "size": 20,
            "price": 0.5,
        }
        bot_params = {
            "leaderWallet": "0xleader",
            "tradeRate": 5,
            "buyMinAmount": 1,
            "buyMaxAmount": 50,
            "positionThreshold": 0,
        }
        with patch("legacy.tradeExecutor.get_my_balance", new_callable=AsyncMock, return_value=100.0), \
             patch("legacy.tradeExecutor.fetch_data", new_callable=AsyncMock, return_value=[]), \
             patch("legacy.tradeExecutor._get_execution_service", return_value=mock_exec), \
             patch("legacy.tradeExecutor._copy_decision.decide_buy", return_value=decision), \
             patch("legacy.tradeExecutor.log_trade_execution") as log_exec, \
             patch("legacy.tradeExecutor.log_trade_skip") as log_skip, \
             patch.dict("legacy.tradeExecutor.ENV", {"FUNDER_ADDRESS": "0x" + "a" * 40}, clear=False):
            await execute_trades(mock_clob, bot_params, [trade])

        log_exec.assert_not_called()
        assert log_skip.called

    @pytest.mark.asyncio
    async def test_execute_trades_logs_copy_order_id_not_leader_tx(self):
        """Execution logs should include copy order id and not fake leader tx as copy tx."""
        from legacy.tradeExecutor import execute_trades
        mock_clob = AsyncMock()
        mock_clob.get_price.return_value = {"price": "0.50", "size": "100"}
        mock_exec = MagicMock()
        mock_exec.market_meta_service = MagicMock(
            get=AsyncMock(
                return_value=SimpleNamespace(min_order_size=Decimal("0.00001"))
            )
        )
        mock_exec.execute = AsyncMock(
            return_value=MagicMock(
                accepted=True,
                filled_amount=Decimal("5"),
                avg_price=Decimal("0.5"),
                status="filled",
                order_id="copy-order-123",
                error=None,
            )
        )
        decision = CopyDecision(
            allow=True,
            skip_code="ok",
            reason="approved",
            side="BUY",
            decision_score=0.9,
            leader_score=0.9,
            trade_score=0.9,
            live_price=0.5,
            reference_price=0.5,
            target_notional_usdc=2.5,
            target_size_tokens=5.0,
            price_drift_pct=0.0,
            edge_decay_pct=0.0,
        )
        trade = {
            "conditionId": "c1",
            "asset": "a1",
            "transactionHash": "0xleaderhash",
            "side": "BUY",
            "usdcSize": 10,
            "size": 20,
            "price": 0.5,
        }
        bot_params = {
            "leaderWallet": "0xleader",
            "tradeRate": 5,
            "buyMinAmount": 1,
            "buyMaxAmount": 50,
            "positionThreshold": 0,
        }
        with patch("legacy.tradeExecutor.get_my_balance", new_callable=AsyncMock, return_value=100.0), \
             patch("legacy.tradeExecutor.fetch_data", new_callable=AsyncMock, return_value=[]), \
             patch("legacy.tradeExecutor._get_execution_service", return_value=mock_exec), \
             patch("legacy.tradeExecutor._copy_decision.decide_buy", return_value=decision), \
             patch("legacy.tradeExecutor.log_trade_execution") as log_exec, \
             patch.dict("legacy.tradeExecutor.ENV", {"FUNDER_ADDRESS": "0x" + "a" * 40}, clear=False):
            await execute_trades(mock_clob, bot_params, [trade])

        assert log_exec.called
        kwargs = log_exec.call_args.kwargs
        assert kwargs["meta"]["copyOrderId"] == "copy-order-123"
        assert kwargs["meta"]["leaderTx"] == "0xleaderhash"
        assert "tx_hash" not in kwargs or kwargs["tx_hash"] is None



# ---------------------------------------------------------------------------
# PostOrder — helper functions
# ---------------------------------------------------------------------------
class TestFetchData:
    @pytest.mark.asyncio
    async def test_fetch_data_raises_on_error(self):
        from utils.fetchData import fetch_data
        with pytest.raises(Exception):
            await fetch_data("http://localhost:1/nonexistent")
