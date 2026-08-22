from __future__ import annotations

import sys
import time
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
import os

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.copyDecisionService import CopyDecisionService
from services.leaderScoringService import LeaderScore


def _test_config() -> SimpleNamespace:
    return SimpleNamespace(
        execution=SimpleNamespace(
            enable_copy_decision_gate=False,
            slippage_tolerance=2.0,
            enable_near_resolution_gate=False,
            near_resolution_gate_price=0.95,
            near_resolution_max_age_seconds=300,
        ),
        copy_trading=SimpleNamespace(
            copy_decision_min_score=0.0,
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


def _leader(score: float = 0.9) -> LeaderScore:
    return LeaderScore(
        wallet="0x" + ("1" * 40),
        score=score,
        roster_score=score,
        runtime_score=score,
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
        """BUY below the dynamic share minimum upsizes when cap allows."""
        # live_price=0.20 => $1 floor is 5 shares, but market minimum is 8.
        # base_notional=1.00 => desired=5, so the order upsizes to 8 shares.
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
        assert decision.allow is True
        assert decision.target_size_tokens == pytest.approx(8.0)
        assert decision.sizing is not None
        assert decision.sizing.accept is True
        assert decision.sizing.upsized is True

    def test_buy_minimum_uses_copied_price_without_slippage_reserve(self):
        """Fund exactly five shares when the copied-price cap permits them."""

        svc = CopyDecisionService(config=_test_config())
        trade = {"price": 0.70, "timestamp": time.time(), "usdcSize": 10}

        decision = svc.decide_buy(
            trade=trade,
            leader_score=_leader(),
            live_price=0.70,
            base_notional_usdc=0.50,
            max_notional_usdc=3.50,
            market_min_size=5.0,
            tick_size=0.01,
        )

        assert decision.allow is True
        assert decision.target_size_tokens == pytest.approx(5.0)
        assert decision.target_notional_usdc == pytest.approx(3.50)
        assert decision.sizing is not None
        assert decision.sizing.effective_price == Decimal("0.70")

    def test_buy_skips_if_cap_cannot_satisfy_minimum_executable_size(self):
        """Skip when the risk cap cannot cover the dynamic share minimum."""
        svc = CopyDecisionService(config=_test_config())
        trade = {"price": 0.84, "timestamp": time.time(), "usdcSize": 10}
        decision = svc.decide_buy(
            trade=trade,
            leader_score=_leader(),
            live_price=0.84,
            base_notional_usdc=10.0,
            max_notional_usdc=4.19,
            market_min_size=5.0,
        )
        # Five shares cost 4.20 at the copied price, above the 4.19 cap, so
        # execution must fail closed instead of shrinking below five shares.
        assert decision.allow is False
        assert decision.skip_code == "buy_cap_blocks_upsize_to_venue_min"
        assert decision.sizing is not None
        assert decision.sizing.accept is False

    def test_buy_returns_copied_price_notional_when_already_above_min_size(self):
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
        assert decision.target_notional_usdc == pytest.approx(5.88, rel=1e-4)
        assert decision.details["live_notional_usdc"] == pytest.approx(5.88)
        assert decision.details["required_worst_notional_usdc"] == pytest.approx(5.88, rel=1e-4)

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
        # live_price=0.40 => $1 floor is 2.5 shares; market minimum is 5.
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
        # live_price=0.40 => taker_min = ceil(1/0.40) = 3 tokens
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
                market_min_size=5.0,  # ignored for taker
                max_sell_tokens=2.0,  # held < taker min (3)
            )

        assert decision.allow is False
        assert decision.skip_code == "sell_held_below_venue_min"
        assert decision.target_size_tokens == pytest.approx(0.0)

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
        # The dynamic five-share floor exceeds the $1 (2.5-share) floor.
        assert decision.allow is True
        assert decision.target_size_tokens == pytest.approx(5.0)
        assert decision.sizing is not None
        assert decision.sizing.upsized is True


class TestCopyDecisionServicePartialHeadroom:
    """Tests for issue #37: MAX_POSITION_PCT should downsize BUY to remaining cap headroom."""

    def test_buy_downsized_to_partial_headroom(self):
        """17% exposure, 25% cap → 8% headroom → BUY should be downsized to fit, not denied."""
        # equity=100 USDC, cap=0.25 → max cap notional=25
        # existing market exposure = 17 → remaining = 8 USDC headroom
        # desired trade = 15 USDC, cap constrains to 8 USDC
        # live_price=0.50 => taker_min = ceil(1/0.50) = 2 tokens → worst_notional ~ 1.02
        # 8 USDC headroom > 1.02 => fits minimum => allow
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
        # final order must be above taker minimum (2 tokens at 0.50 = $1 notional)
        assert decision.target_size_tokens >= 2.0 - 1e-6

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
        # live_price=0.50 => taker_min = ceil(1/0.50) = 2 tokens
        # worst_notional = 2 * 0.50 * 1.02 = ~1.02
        # headroom = 1 USDC < ~1.02 => can't fill minimum
        svc = CopyDecisionService(config=_test_config())
        trade = {"price": 0.50, "timestamp": time.time(), "usdcSize": 15.0}
        decision = svc.decide_buy(
            trade=trade,
            leader_score=_leader(),
            live_price=0.50,
            base_notional_usdc=15.0,
            max_notional_usdc=1.0,  # below min executable notional
            market_min_size=5.0,
        )
        assert decision.allow is False
        assert decision.skip_code in (
            "buy_cap_blocks_upsize_to_venue_min",
            "risk_cap_too_low_for_min_order",
        )

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
        """BUY below the five-share market minimum upsizes with headroom."""
        svc = CopyDecisionService(config=_test_config())
        trade = {"price": 0.50, "timestamp": time.time(), "usdcSize": 5}
        decision = svc.decide_buy(
            trade=trade,
            leader_score=_leader(),
            live_price=0.50,
            base_notional_usdc=1.00,
            max_notional_usdc=10.0,
            market_min_size=5.0,
        )
        assert decision.allow is True
        assert decision.target_size_tokens == pytest.approx(5.0, rel=1e-4)
        assert decision.sizing is not None
        assert decision.sizing.accept is True
        assert decision.sizing.upsized is True

    def test_buy_skips_when_headroom_insufficient(self):
        """BUY below taker min, cap too low → skip."""
        # live_price=0.50 => taker_min = 2 tokens => worst_notional ~ 1.02
        # cap = 1 USDC < 1.02 => can't fill
        svc = CopyDecisionService(config=_test_config())
        trade = {"price": 0.50, "timestamp": time.time(), "usdcSize": 5}
        decision = svc.decide_buy(
            trade=trade,
            leader_score=_leader(),
            live_price=0.50,
            base_notional_usdc=10.0,
            max_notional_usdc=1.0,  # cap = 1 USDC, taker min needs ~1.02
            market_min_size=5.0,
        )
        assert decision.allow is False
        assert decision.skip_code in (
            "buy_cap_blocks_upsize_to_venue_min",
            "risk_cap_too_low_for_min_order",
        )

    def test_sell_skips_when_held_below_venue_min(self):
        """SELL below taker min, held position too small → skip."""
        # live_price=0.50 => taker_min = ceil(1/0.50) = 2 tokens
        svc = CopyDecisionService(config=_test_config())
        trade = {"price": 0.50, "timestamp": time.time(), "usdcSize": 5}
        decision = svc.decide_sell(
            trade=trade,
            sell_tokens=2.0,
            live_price=0.50,
            market_min_size=5.0,
            max_sell_tokens=1.0,  # held < taker min (2)
        )
        assert decision.allow is False
        assert decision.skip_code == "sell_held_below_venue_min"
        assert decision.sizing is not None
        assert decision.sizing.accept is False

    def test_sell_upsizes_when_held_supports_min(self):
        """SELL below the dynamic minimum upsizes when holdings cover it."""
        svc = CopyDecisionService(config=_test_config())
        trade = {"price": 0.50, "timestamp": time.time(), "usdcSize": 5}
        decision = svc.decide_sell(
            trade=trade,
            sell_tokens=1.0,
            live_price=0.50,
            market_min_size=5.0,
            max_sell_tokens=10.0,  # enough held
        )
        assert decision.allow is True
        assert decision.target_size_tokens == pytest.approx(5.0, rel=1e-4)
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
