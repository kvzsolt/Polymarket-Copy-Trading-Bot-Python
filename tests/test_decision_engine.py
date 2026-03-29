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
        with patch.dict("services.copyDecisionService.ENV", {"COPY_DECISION_MIN_SCORE": 0.0}, clear=False):
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
        with patch.dict("services.copyDecisionService.ENV", {"COPY_DECISION_MIN_SCORE": 0.0}, clear=False):
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
        with patch.dict("services.copyDecisionService.ENV", {"COPY_DECISION_MIN_SCORE": 0.0}, clear=False):
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
        with patch.dict("services.copyDecisionService.ENV", {"COPY_DECISION_MIN_SCORE": 0.0}, clear=False):
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


class TestTraderDiscoveryService:
    @pytest.mark.asyncio
    async def test_resolve_leaders_disabled_returns_configured(self):
        svc = TraderDiscoveryService()
        configured = ["0x" + ("1" * 40)]
        with patch.dict("services.traderDiscoveryService.ENV", {"AUTO_DISCOVER_LEADERS": False}, clear=False):
            resolved = await svc.resolve_leaders(configured)
        assert resolved == configured

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

