from __future__ import annotations

import sys
import time
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.copyDecisionService import CopyDecisionService
from services.leaderScoringService import LeaderScore
from services.tradeAnalyticsService import TradeAnalyticsService
from services.traderDiscoveryService import RankedLeader, TraderDiscoveryService


def _leader(score: float = 0.9) -> LeaderScore:
    return LeaderScore(
        wallet="0x" + ("1" * 40),
        score=score,
        balance_score=score,
        analytics_score=score,
        activity_score=score,
        sample_size=20,
        details={},
    )


class TestCopyDecisionService:
    def test_buy_enforces_min_5_shares(self):
        svc = CopyDecisionService()
        trade = {"price": 0.40, "timestamp": time.time(), "usdcSize": 20}
        decision = svc.decide_buy(
            trade=trade,
            leader_score=_leader(),
            live_price=0.40,
            base_notional_usdc=0.50,
            max_notional_usdc=10.0,
            market_min_size=0.0,
        )
        assert decision.allow is True
        assert decision.target_size_tokens == pytest.approx(5.0)
        assert decision.target_notional_usdc == pytest.approx(2.0)

    def test_buy_enforces_min_1_notional(self):
        svc = CopyDecisionService()
        trade = {"price": 0.08, "timestamp": time.time(), "usdcSize": 10}
        decision = svc.decide_buy(
            trade=trade,
            leader_score=_leader(),
            live_price=0.08,
            base_notional_usdc=0.20,
            max_notional_usdc=10.0,
            market_min_size=0.0,
        )
        assert decision.allow is True
        assert decision.target_notional_usdc >= 1.0
        assert decision.target_size_tokens >= 12.5

    def test_buy_market_min_size_overrides_min_5_shares(self):
        svc = CopyDecisionService()
        trade = {"price": 0.20, "timestamp": time.time(), "usdcSize": 10}
        decision = svc.decide_buy(
            trade=trade,
            leader_score=_leader(),
            live_price=0.20,
            base_notional_usdc=1.00,
            max_notional_usdc=10.0,
            market_min_size=8.0,
        )
        assert decision.allow is True
        assert decision.target_size_tokens == pytest.approx(8.0)
        assert decision.target_notional_usdc == pytest.approx(1.6)

    def test_buy_skips_if_cap_cannot_satisfy_minimum_executable_size(self):
        svc = CopyDecisionService()
        trade = {"price": 0.40, "timestamp": time.time(), "usdcSize": 10}
        decision = svc.decide_buy(
            trade=trade,
            leader_score=_leader(),
            live_price=0.40,
            base_notional_usdc=0.20,
            max_notional_usdc=0.20,
            market_min_size=0.0,
        )
        assert decision.allow is False
        assert decision.skip_code == "risk_cap_too_low_for_min_order"

    def test_buy_decision_gate_can_be_disabled(self):
        svc = CopyDecisionService()
        # Gate is disabled by default (_decision_gate_enabled = False).
        # Even with a near-resolution stale signal and low leader score,
        # the gate should be bypassed and the trade allowed.
        trade = {"price": 0.96, "timestamp": time.time() - 60, "usdcSize": 10}
        decision = svc.decide_buy(
            trade=trade,
            leader_score=_leader(score=0.2),
            live_price=0.96,
            base_notional_usdc=2.0,
            max_notional_usdc=10.0,
            market_min_size=0.0,
        )
        assert decision.allow is True
        assert decision.skip_code == "ok"
        assert decision.reason == "decision gate disabled by config"
        assert decision.details.get("decision_gate_enabled") is False

    def test_sell_decision_gate_can_be_disabled(self):
        svc = CopyDecisionService()
        trade = {"price": 0.40, "timestamp": time.time(), "usdcSize": 10}
        decision = svc.decide_sell(
            trade=trade,
            sell_tokens=2.0,
            live_price=0.40,
            market_min_size=0.0,
            max_sell_tokens=2.0,
        )
        assert decision.allow is True
        assert decision.skip_code == "ok"
        assert decision.target_size_tokens == pytest.approx(2.0)
        assert decision.reason == "decision gate disabled by config"

    def test_sell_decision_gate_disabled_still_honors_market_min_size(self):
        svc = CopyDecisionService()
        trade = {"price": 0.40, "timestamp": time.time(), "usdcSize": 10}
        decision = svc.decide_sell(
            trade=trade,
            sell_tokens=2.0,
            live_price=0.40,
            market_min_size=5.0,
            max_sell_tokens=10.0,
        )
        assert decision.allow is True
        assert decision.target_size_tokens == pytest.approx(5.0)
        assert decision.reason == "decision gate disabled by config"


class TestTraderDiscoveryService:
    @pytest.mark.asyncio
    async def test_resolve_leaders_disabled_returns_configured(self):
        svc = TraderDiscoveryService()
        configured = ["0x" + ("1" * 40)]
        with patch.dict("services.traderDiscoveryService.ENV", {"AUTO_DISCOVER_LEADERS": False}, clear=False):
            resolved = await svc.resolve_leaders(configured)
        assert resolved == configured

    @pytest.mark.asyncio
    async def test_discovery_sort_is_deterministic_by_score_then_rank(self):
        svc = TraderDiscoveryService()
        # Two traders with same computed score; expect smaller rank first.
        rows = {
            "data": [
                {
                    "trader": "0x" + ("2" * 40),
                    "trader_name": "B",
                    "overall_gain": 2000000,
                    "win_amount": 2000000,
                    "loss_amount": -0,
                    "win_rate": 0.80,
                    "total_positions": 200,
                    "active_positions": 10,
                    "event_ct": 80,
                    "rank": 5,
                    "total_current_value": 50000,
                    "trader_tags": "",
                },
                {
                    "trader": "0x" + ("3" * 40),
                    "trader_name": "A",
                    "overall_gain": 2000000,
                    "win_amount": 2000000,
                    "loss_amount": -0,
                    "win_rate": 0.80,
                    "total_positions": 200,
                    "active_positions": 10,
                    "event_ct": 80,
                    "rank": 1,
                    "total_current_value": 50000,
                    "trader_tags": "",
                },
            ]
        }
        with patch("services.traderDiscoveryService._fetch_json", return_value=rows), \
             patch.dict(
                 "services.traderDiscoveryService.ENV",
                 {"MIN_LEADER_PNL": 0, "MIN_LEADER_WIN_RATE": 0, "MIN_LEADER_POSITIONS": 0, "DISCOVERY_MAX_RESULTS": 50},
                 clear=False,
             ):
            cands = await svc.discover_candidates(max_results=2)
        assert len(cands) == 2
        assert cands[0].rank == 1
        assert cands[1].rank == 5

    @pytest.mark.asyncio
    async def test_resolve_leaders_adds_ranked_candidates(self):
        svc = TraderDiscoveryService()
        configured = ["0x" + ("1" * 40)]
        discovered = [
            RankedLeader(wallet="0x" + ("2" * 40), score=0.80, recent_trade_count=10, recent_notional_usdc=200),
            RankedLeader(wallet="0x" + ("3" * 40), score=0.20, recent_trade_count=10, recent_notional_usdc=200),
        ]
        with patch.dict(
            "services.traderDiscoveryService.ENV",
            {
                "AUTO_DISCOVER_LEADERS": True,
                "AUTO_DISCOVER_MIN_SCORE": 0.45,
                "AUTO_DISCOVER_TOP_N": 5,
            },
            clear=False,
        ):
            with patch.object(svc, "discover_ranked_leaders", new=AsyncMock(return_value=discovered)):
                resolved = await svc.resolve_leaders(configured)
        assert configured[0] in resolved
        assert discovered[0].wallet in resolved
        assert discovered[1].wallet not in resolved


class TestTradeAnalyticsService:
    @pytest.mark.asyncio
    async def test_records_decision_and_execution_metrics(self):
        svc = TradeAnalyticsService()
        dummy_decision = type(
            "Decision",
            (),
            {
                "allow": True,
                "skip_code": "ok",
                "decision_score": 0.7,
                "leader_score": 0.8,
                "trade_score": 0.75,
            },
        )()
        trade = {"side": "BUY", "price": 0.50, "conditionId": "cond1", "asset": "asset1"}

        await svc.record_decision(decision=dummy_decision, trade=trade)
        await svc.record_execution(
            decision=dummy_decision,
            trade=trade,
            fill_price=0.52,
            result="executed",
        )
        snap = await svc.snapshot()
        assert snap["total_signals"] == 1
        assert snap["approved_signals"] == 1
        assert snap["executed_trades"] == 1
        assert snap["avg_quality_score"] > 0
        assert snap["avg_slippage_bps"] > 0
