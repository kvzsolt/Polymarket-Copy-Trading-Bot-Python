"""Tests for TradeScoringService — near-resolution gate and scoring logic."""
from __future__ import annotations

import time

import pytest

from config.env import ENV, rehydrate_env
from services.tradeScoringService import TradeScoringService


@pytest.fixture(autouse=True)
def _reset_env(monkeypatch):
    monkeypatch.setenv("COPY_MAX_SIGNAL_AGE_SECONDS", "120")
    monkeypatch.setenv("COPY_MAX_PRICE_DRIFT_PCT", "6")
    monkeypatch.setenv("COPY_MAX_EDGE_DECAY_PCT", "70")
    monkeypatch.setenv("COPY_LIQUIDITY_TARGET_USDC", "50")
    monkeypatch.setenv("ENABLE_NEAR_RESOLUTION_GATE", "true")
    monkeypatch.setenv("NEAR_RESOLUTION_GATE_PRICE", "0.95")
    monkeypatch.setenv("NEAR_RESOLUTION_MAX_AGE_SECONDS", "15")
    rehydrate_env()


def _trade(*, price: float = 0.50, usdc_size: float = 100.0, age_seconds: float = 5.0) -> dict:
    ts = int(time.time() - age_seconds)
    return {
        "price": price,
        "usdcSize": usdc_size,
        "timestamp": ts,
    }


class TestNearResolutionGate:
    def setup_method(self):
        self.svc = TradeScoringService()

    def test_near_resolution_fresh_signal_allowed(self):
        """High price but very fresh signal — should NOT be hard-failed."""
        trade = _trade(price=0.97, age_seconds=5.0)
        result = self.svc.score_buy_trade(trade, live_price=0.97, leader_score=0.8)
        assert result.hard_fail_reason != "near_resolution_too_stale"

    def test_near_resolution_stale_signal_rejected(self):
        """High live_price with stale signal — should be hard-failed."""
        trade = _trade(price=0.96, age_seconds=60.0)
        result = self.svc.score_buy_trade(trade, live_price=0.96, leader_score=0.8)
        assert result.hard_fail_reason == "near_resolution_too_stale"

    def test_below_resolution_threshold_not_gated(self):
        """Signal at live_price=0.90 should NOT trigger near-resolution gate."""
        trade = _trade(price=0.90, age_seconds=60.0)
        result = self.svc.score_buy_trade(trade, live_price=0.90, leader_score=0.8)
        assert result.hard_fail_reason != "near_resolution_too_stale"

    def test_exactly_at_threshold_stale_rejected(self, monkeypatch):
        """Signal at exactly NEAR_RESOLUTION_GATE_PRICE with stale age — rejected."""
        monkeypatch.setenv("NEAR_RESOLUTION_GATE_PRICE", "0.90")
        rehydrate_env()
        trade = _trade(price=0.90, age_seconds=30.0)
        result = TradeScoringService().score_buy_trade(trade, live_price=0.90, leader_score=0.8)
        assert result.hard_fail_reason == "near_resolution_too_stale"

    def test_gate_disabled_when_zero(self, monkeypatch):
        """NEAR_RESOLUTION_GATE_PRICE=0 disables the gate."""
        monkeypatch.setenv("NEAR_RESOLUTION_GATE_PRICE", "0")
        rehydrate_env()
        trade = _trade(price=0.99, age_seconds=60.0)
        result = TradeScoringService().score_buy_trade(trade, live_price=0.99, leader_score=0.8)
        assert result.hard_fail_reason != "near_resolution_too_stale"

    def test_gate_disabled_by_explicit_toggle(self, monkeypatch):
        """ENABLE_NEAR_RESOLUTION_GATE=false disables gate regardless of threshold."""
        monkeypatch.setenv("ENABLE_NEAR_RESOLUTION_GATE", "false")
        monkeypatch.setenv("NEAR_RESOLUTION_GATE_PRICE", "0.95")
        monkeypatch.setenv("NEAR_RESOLUTION_MAX_AGE_SECONDS", "15")
        rehydrate_env()
        trade = _trade(price=0.99, age_seconds=60.0)
        result = TradeScoringService().score_buy_trade(trade, live_price=0.99, leader_score=0.8)
        assert result.hard_fail_reason != "near_resolution_too_stale"

    def test_age_check_not_triggered_without_timestamp(self):
        """Trade with no timestamp (ts=0) should not trigger near-resolution gate."""
        trade = {"price": 0.97, "usdcSize": 100.0, "timestamp": 0}
        result = self.svc.score_buy_trade(trade, live_price=0.97, leader_score=0.8)
        assert result.hard_fail_reason != "near_resolution_too_stale"

    def test_normal_trade_not_affected(self):
        """Normal mid-range trade should not be affected by near-resolution gate."""
        trade = _trade(price=0.50, age_seconds=30.0)
        result = self.svc.score_buy_trade(trade, live_price=0.50, leader_score=0.8)
        assert result.hard_fail_reason is None


class TestTradeScoringBasics:
    def setup_method(self):
        self.svc = TradeScoringService()

    def test_invalid_price_hard_fail(self):
        trade = {"price": 0, "usdcSize": 0, "timestamp": int(time.time())}
        result = self.svc.score_buy_trade(trade, live_price=0.5, leader_score=0.8)
        assert result.hard_fail_reason == "invalid_reference_or_live_price"

    def test_stale_signal_hard_fail(self):
        trade = _trade(price=0.50, age_seconds=200.0)
        result = self.svc.score_buy_trade(trade, live_price=0.50, leader_score=0.8)
        assert result.hard_fail_reason == "signal_too_old"

    def test_price_drift_hard_fail(self, monkeypatch):
        monkeypatch.setenv("COPY_MAX_PRICE_DRIFT_PCT", "5")
        rehydrate_env()
        trade = _trade(price=0.50, age_seconds=5.0)
        result = TradeScoringService().score_buy_trade(trade, live_price=0.60, leader_score=0.8)
        assert result.hard_fail_reason == "price_drift_exceeded"

    def test_edge_decay_hard_fail(self, monkeypatch):
        monkeypatch.setenv("COPY_MAX_EDGE_DECAY_PCT", "10")
        monkeypatch.setenv("COPY_MAX_PRICE_DRIFT_PCT", "100")  # disable drift gate
        rehydrate_env()
        # ref_price=0.50 → initial_edge=0.50; live_price=0.90 → live_edge=0.10
        # decay = (0.50 - 0.10) / 0.50 = 0.80 → 80% > 10%
        trade = _trade(price=0.50, age_seconds=5.0)
        result = TradeScoringService().score_buy_trade(trade, live_price=0.90, leader_score=0.8)
        assert result.hard_fail_reason == "edge_decay_exceeded"

    def test_score_in_range(self):
        trade = _trade(price=0.50, age_seconds=5.0)
        result = self.svc.score_buy_trade(trade, live_price=0.51, leader_score=0.8)
        assert 0.0 <= result.score <= 1.0
        assert result.hard_fail_reason is None
