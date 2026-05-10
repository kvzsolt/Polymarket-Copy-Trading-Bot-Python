from __future__ import annotations

import sys
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
import os

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.copyDecisionService import CopyDecisionService
from services.leaderScoringService import LeaderScore
from services.tradeAnalyticsService import TradeAnalyticsService
from services.traderDiscoveryService import RankedLeader, TraderDiscoveryService


def _test_config() -> SimpleNamespace:
    return SimpleNamespace(
        execution=SimpleNamespace(
            enable_copy_decision_gate=False,
            slippage_tolerance=2.0,
            enable_near_resolution_gate=False,
            near_resolution_gate_price=0.95,
            near_resolution_max_age_seconds=300,
        ),
        leader=SimpleNamespace(
            copy_decision_min_score=0.0,
            leader_score_balance_target_usdc=500.0,
            leader_score_pnl_bonus_cap=0.1,
            leader_score_activity_target=8,
            leader_score_weight_balance=0.35,
            leader_score_weight_analytics=0.45,
            leader_score_weight_activity=0.20,
            leader_score_floor=0.10,
            copy_max_signal_age_seconds=120.0,
            copy_max_price_drift_pct=6.0,
            copy_max_edge_decay_pct=0.1,
            copy_liquidity_target_usdc=500.0,
        ),
        sizing=SimpleNamespace(
            enable_trust_size_multipliers=True,
            trust_size_multiplier_min=0.50,
            trust_size_multiplier_max=1.25,
        ),
    )


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
    def test_buy_passes_when_no_venue_metadata(self):
        """With no market metadata (market_min_size=0), no venue floor applies → BUY passes."""
        svc = CopyDecisionService(config=_test_config())
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
        assert decision.skip_code == "ok"

    def test_buy_passes_when_tiny_order_no_venue_floor(self):
        """With no market metadata (market_min_size=0), no venue floor → BUY passes."""
        svc = CopyDecisionService(config=_test_config())
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
        assert decision.skip_code == "ok"

    def test_buy_upsizes_to_venue_min_when_cap_allows(self):
        """BUY below venue min → upsize to venue min when cap allows."""
        svc = CopyDecisionService(config=_test_config())
        trade = {"price": 0.20, "timestamp": time.time(), "usdcSize": 10}
        decision = svc.decide_buy(
            trade=trade,
            leader_score=_leader(),
            live_price=0.20,
            base_notional_usdc=1.00,
            max_notional_usdc=10.0,
            market_min_size=8.0,
        )
        # 1.00 / 0.20 = 5 tokens < 8 min → upsize to 8 tokens
        # 8 * 0.20 * 1.02 = 1.632 USDC, cap=10 → fits
        assert decision.allow is True
        assert decision.target_size_tokens >= 8.0 - 1e-6

    def test_buy_skips_if_cap_cannot_satisfy_minimum_executable_size(self):
        svc = CopyDecisionService(config=_test_config())
        trade = {"price": 0.84, "timestamp": time.time(), "usdcSize": 10}
        decision = svc.decide_buy(
            trade=trade,
            leader_score=_leader(),
            live_price=0.84,
            base_notional_usdc=10.0,
            max_notional_usdc=4.25,
            market_min_size=5.0,
        )
        assert decision.allow is False
        assert decision.skip_code == "risk_cap_too_low_for_min_order"

    def test_buy_returns_worst_case_notional_when_already_above_min_size(self):
        svc = CopyDecisionService(config=_test_config())
        trade = {"price": 0.84, "timestamp": time.time(), "usdcSize": 5.88}
        decision = svc.decide_buy(
            trade=trade,
            leader_score=_leader(),
            live_price=0.84,
            base_notional_usdc=5.88,
            max_notional_usdc=8.0,
            market_min_size=5.0,
        )
        assert decision.allow is True
        assert decision.target_size_tokens == pytest.approx(7.0)
        assert decision.target_notional_usdc == pytest.approx(5.9976, rel=1e-4)
        assert decision.details["live_notional_usdc"] == pytest.approx(5.88)
        assert decision.details["required_worst_notional_usdc"] == pytest.approx(5.9976, rel=1e-4)

    def test_buy_decision_gate_can_be_disabled(self):
        svc = CopyDecisionService(config=_test_config())
        # Gate is disabled by default (_decision_gate_enabled = False).
        # Even with a near-resolution stale signal and low leader score,
        # the gate should be bypassed when the trade is already executable.
        trade = {"price": 0.96, "timestamp": time.time() - 60, "usdcSize": 10}
        decision = svc.decide_buy(
            trade=trade,
            leader_score=_leader(score=0.2),
            live_price=0.96,
            base_notional_usdc=6.0,
            max_notional_usdc=10.0,
            market_min_size=0.0,
        )
        assert decision.allow is True
        assert decision.skip_code == "ok"
        assert decision.reason == "decision gate disabled by config"
        assert decision.details.get("decision_gate_enabled") is False

    def test_sell_tiny_order_bumps_to_venue_minimum_when_position_can_cover_it(self):
        trade = {"price": 0.40, "timestamp": time.time(), "usdcSize": 10}
        with patch.dict(
            os.environ,
            {
                "ENABLE_EXECUTABLE_ORDER_GUARD": "true",
                "COPY_MIN_SHARES": "99",
                "COPY_MIN_NOTIONAL_USDC": "99",
            },
            clear=False,
        ):
            svc = CopyDecisionService(config=_test_config())
            decision = svc.decide_sell(
                trade=trade,
                sell_tokens=1.0,
                live_price=0.40,
                market_min_size=5.0,
                max_sell_tokens=10.0,
            )

        assert decision.allow is True
        assert decision.skip_code == "ok"
        assert decision.target_size_tokens == pytest.approx(5.0)
        assert decision.reason == "decision gate disabled by config"

    def test_sell_skips_when_full_position_cannot_satisfy_venue_minimum(self):
        trade = {"price": 0.40, "timestamp": time.time(), "usdcSize": 10}
        with patch.dict(
            os.environ,
            {
                "ENABLE_EXECUTABLE_ORDER_GUARD": "true",
                "COPY_MIN_SHARES": "99",
                "COPY_MIN_NOTIONAL_USDC": "99",
            },
            clear=False,
        ):
            svc = CopyDecisionService(config=_test_config())
            decision = svc.decide_sell(
                trade=trade,
                sell_tokens=1.0,
                live_price=0.40,
                market_min_size=5.0,
                max_sell_tokens=4.0,
            )

        assert decision.allow is False
        assert decision.skip_code == "sell_held_below_venue_min"
        assert decision.target_size_tokens == pytest.approx(1.0)

    def test_sell_keeps_requested_size_when_it_already_clears_minimum(self):
        trade = {"price": 0.40, "timestamp": time.time(), "usdcSize": 10}
        with patch.dict(
            os.environ,
            {
                "ENABLE_EXECUTABLE_ORDER_GUARD": "true",
                "COPY_MIN_SHARES": "99",
                "COPY_MIN_NOTIONAL_USDC": "99",
            },
            clear=False,
        ):
            svc = CopyDecisionService(config=_test_config())
            decision = svc.decide_sell(
                trade=trade,
                sell_tokens=7.0,
                live_price=0.40,
                market_min_size=5.0,
                max_sell_tokens=10.0,
            )

        assert decision.allow is True
        assert decision.target_size_tokens == pytest.approx(7.0)
        assert decision.reason == "decision gate disabled by config"

    def test_sell_decision_gate_can_be_disabled(self):
        svc = CopyDecisionService(config=_test_config())
        trade = {"price": 0.40, "timestamp": time.time(), "usdcSize": 10}
        decision = svc.decide_sell(
            trade=trade,
            sell_tokens=5.0,
            live_price=0.40,
            market_min_size=0.0,
            max_sell_tokens=5.0,
        )
        assert decision.allow is True
        assert decision.skip_code == "ok"
        assert decision.target_size_tokens == pytest.approx(5.0)
        assert decision.reason == "decision gate disabled by config"

    def test_sell_decision_gate_disabled_still_honors_market_min_size(self):
        svc = CopyDecisionService(config=_test_config())
        trade = {"price": 0.40, "timestamp": time.time(), "usdcSize": 10}
        decision = svc.decide_sell(
            trade=trade,
            sell_tokens=2.0,
            live_price=0.40,
            market_min_size=5.0,
            max_sell_tokens=10.0,
        )
        # Decision gate disabled but guard enabled → upsize to venue min
        assert decision.allow is True
        assert decision.target_size_tokens == pytest.approx(5.0)
        assert decision.sizing is not None
        assert decision.sizing.upsized is True


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
        # Patch instance attrs directly — they're set in __init__ from ENV
        # and don't re-read ENV at resolve time.
        svc._auto_discover = True
        svc._auto_discover_min_score = 0.45
        svc._auto_discover_top_n = 5
        configured = ["0x" + ("1" * 40)]
        discovered = [
            RankedLeader(wallet="0x" + ("2" * 40), score=0.80, recent_trade_count=10, recent_notional_usdc=200),
            RankedLeader(wallet="0x" + ("3" * 40), score=0.20, recent_trade_count=10, recent_notional_usdc=200),
        ]
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


class TestCopyDecisionServicePartialHeadroom:
    """Tests for issue #37: MAX_POSITION_PCT should downsize BUY to remaining cap headroom."""

    def test_buy_downsized_to_partial_headroom(self):
        """17% exposure, 25% cap → 8% headroom → BUY should be downsized to fit, not denied."""
        # equity=100 USDC, cap=0.25 → max cap notional=25
        # existing market exposure = 17 → remaining = 8 USDC headroom
        # desired trade = 15 USDC, cap constrains to 8 USDC
        # min_order at 0.50 price = 5 shares * 0.50 = 2.50 USDC notional
        # → should downsize to ~8 USDC (fits within cap and above min)
        svc = CopyDecisionService(config=_test_config())
        trade = {"price": 0.50, "timestamp": time.time(), "usdcSize": 15.0}
        decision = svc.decide_buy(
            trade=trade,
            leader_score=_leader(),
            live_price=0.50,
            base_notional_usdc=15.0,
            max_notional_usdc=8.0,  # cap headroom = 8 USDC
            market_min_size=5.0,
        )
        assert decision.allow is True, f"Expected allow=True, got skip_code={decision.skip_code}"
        # final notional must be <= cap headroom
        assert decision.target_notional_usdc <= 8.0 + 1e-6
        # final order must be above minimum
        assert decision.target_size_tokens >= 5.0 - 1e-6

    def test_buy_hard_denied_when_zero_headroom(self):
        """Zero remaining cap headroom → hard deny."""
        svc = CopyDecisionService(config=_test_config())
        trade = {"price": 0.50, "timestamp": time.time(), "usdcSize": 15.0}
        decision = svc.decide_buy(
            trade=trade,
            leader_score=_leader(),
            live_price=0.50,
            base_notional_usdc=15.0,
            max_notional_usdc=0.0,  # zero headroom
            market_min_size=5.0,
        )
        assert decision.allow is False
        assert decision.skip_code in ("insufficient_notional", "buy_cap_blocks_upsize_to_venue_min")

    def test_buy_hard_denied_when_headroom_below_min_order(self):
        """Cap headroom exists but is below the minimum executable order size → hard deny."""
        # headroom = 1 USDC, min order = 5 shares * 0.50 = 2.50 USDC → can't fill minimum
        svc = CopyDecisionService(config=_test_config())
        trade = {"price": 0.50, "timestamp": time.time(), "usdcSize": 15.0}
        decision = svc.decide_buy(
            trade=trade,
            leader_score=_leader(),
            live_price=0.50,
            base_notional_usdc=15.0,
            max_notional_usdc=1.0,  # below min order notional
            market_min_size=5.0,
        )
        assert decision.allow is False
        assert decision.skip_code == "buy_cap_blocks_upsize_to_venue_min"

    def test_buy_not_capped_when_headroom_covers_full_order(self):
        """When cap headroom > desired order size, full order goes through unchanged."""
        svc = CopyDecisionService(config=_test_config())
        trade = {"price": 0.50, "timestamp": time.time(), "usdcSize": 10.0}
        decision = svc.decide_buy(
            trade=trade,
            leader_score=_leader(),
            live_price=0.50,
            base_notional_usdc=10.0,
            max_notional_usdc=20.0,  # plenty of headroom
            market_min_size=5.0,
        )
        assert decision.allow is True
        assert decision.target_size_tokens == pytest.approx(20.0, rel=0.05)


class TestPhase3MetadataSizing:
    """Phase 3: metadata-driven min-executable order sizing (issue #38)."""

    def test_buy_upsizes_to_venue_min_when_allowed(self):
        """BUY below venue min → upsize when balance/headroom allow."""
        svc = CopyDecisionService(config=_test_config())
        trade = {"price": 0.50, "timestamp": time.time(), "usdcSize": 5}
        decision = svc.decide_buy(
            trade=trade,
            leader_score=_leader(),
            live_price=0.50,
            base_notional_usdc=1.00,  # 2 tokens, below min
            max_notional_usdc=10.0,
            market_min_size=5.0,
        )
        assert decision.allow is True
        assert decision.target_size_tokens >= 5.0 - 1e-6
        assert decision.sizing is not None
        assert decision.sizing.accept is True
        assert decision.sizing.upsized is True

    def test_buy_skips_when_headroom_insufficient(self):
        """BUY below venue min, cap too low → skip with risk_cap_too_low."""
        svc = CopyDecisionService(config=_test_config())
        trade = {"price": 0.50, "timestamp": time.time(), "usdcSize": 5}
        decision = svc.decide_buy(
            trade=trade,
            leader_score=_leader(),
            live_price=0.50,
            base_notional_usdc=10.0,
            max_notional_usdc=1.0,  # cap = 1 USDC, min needs ~2.55
            market_min_size=5.0,
        )
        assert decision.allow is False
        assert decision.skip_code == "buy_cap_blocks_upsize_to_venue_min"

    def test_sell_skips_when_held_below_venue_min(self):
        """SELL below venue min, held position too small → skip."""
        svc = CopyDecisionService(config=_test_config())
        trade = {"price": 0.50, "timestamp": time.time(), "usdcSize": 5}
        decision = svc.decide_sell(
            trade=trade,
            sell_tokens=2.0,
            live_price=0.50,
            market_min_size=5.0,
            max_sell_tokens=3.0,  # held < venue min
        )
        assert decision.allow is False
        assert decision.skip_code == "sell_held_below_venue_min"
        assert decision.sizing is not None
        assert decision.sizing.accept is False

    def test_sell_upsizes_when_held_supports_min(self):
        """SELL below venue min, held position >= min → upsize."""
        svc = CopyDecisionService(config=_test_config())
        trade = {"price": 0.50, "timestamp": time.time(), "usdcSize": 5}
        decision = svc.decide_sell(
            trade=trade,
            sell_tokens=2.0,
            live_price=0.50,
            market_min_size=5.0,
            max_sell_tokens=10.0,  # enough held
        )
        assert decision.allow is True
        assert decision.target_size_tokens >= 5.0 - 1e-6
        assert decision.sizing is not None
        assert decision.sizing.upsized is True


class TestEffectiveIntentKey:
    """Phase 3: idempotency key recomputed on effective adjusted request."""

    def test_key_changes_when_effective_size_changes(self):
        from decimal import Decimal
        from services.copyDecisionService import effective_intent_key

        key_before = effective_intent_key(
            user="user1",
            condition_id="cond1",
            token_id="token1",
            side="BUY",
            effective_size=Decimal("2.0"),
            effective_price=Decimal("0.50"),
            tick_size=Decimal("0.01"),
            intent_id="intent-1",
        )
        key_after = effective_intent_key(
            user="user1",
            condition_id="cond1",
            token_id="token1",
            side="BUY",
            effective_size=Decimal("5.0"),  # upsized
            effective_price=Decimal("0.50"),
            tick_size=Decimal("0.01"),
            intent_id="intent-1",
        )
        assert key_before != key_after
        assert len(key_before) == 64  # SHA256 hex

    def test_key_deterministic(self):
        from decimal import Decimal
        from services.copyDecisionService import effective_intent_key

        key1 = effective_intent_key(
            user="user1",
            condition_id="cond1",
            token_id="token1",
            side="BUY",
            effective_size=Decimal("5.0"),
            effective_price=Decimal("0.50"),
            tick_size=Decimal("0.01"),
            intent_id="intent-1",
        )
        key2 = effective_intent_key(
            user="user1",
            condition_id="cond1",
            token_id="token1",
            side="BUY",
            effective_size=Decimal("5.0"),
            effective_price=Decimal("0.50"),
            tick_size=Decimal("0.01"),
            intent_id="intent-1",
        )
        assert key1 == key2
