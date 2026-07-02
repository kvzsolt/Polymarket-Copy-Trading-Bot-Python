"""Tests for ``services/strategy_profile/strategy_classifier.py`` (PR3)."""
from __future__ import annotations

from models.strategy_profile import StrategyArchetype
from services.strategy_profile.strategy_classifier import classify_profile


def test_too_few_trades_is_noisy_uncopyable() -> None:
    features = {"trade_count_30d": 3.0, "market_concentration_hhi": 0.3}
    archetype, copyable, _ = classify_profile(features)
    assert archetype == StrategyArchetype.NOISY_UNCOPYABLE
    assert copyable is False


def test_fast_crypto_scalper() -> None:
    features = {
        "trade_count_30d": 50.0,
        "trade_count_7d": 20.0,
        "avg_hold_time_minutes": 10.0,
        "crypto_fast_signal_ratio": 0.6,
        "grouped_signal_ratio": 0.5,
        "market_concentration_hhi": 0.2,
    }
    archetype, copyable, _ = classify_profile(features)
    assert archetype == StrategyArchetype.FAST_CRYPTO_SCALPER
    assert copyable is True


def test_low_price_lottery() -> None:
    features = {
        "trade_count_30d": 30.0,
        "low_price_entry_ratio": 0.80,
        "market_concentration_hhi": 0.3,
        "avg_hold_time_minutes": 120.0,
        "crypto_fast_signal_ratio": 0.1,
        "grouped_signal_ratio": 0.1,
    }
    archetype, copyable, _ = classify_profile(features)
    assert archetype == StrategyArchetype.LOW_PRICE_LOTTERY
    assert copyable is True


def test_near_certainty_sweeper() -> None:
    features = {
        "trade_count_30d": 25.0,
        "near_certainty_entry_ratio": 0.70,
        "near_max_exit_ratio": 0.50,
        "market_concentration_hhi": 0.3,
        "avg_hold_time_minutes": 120.0,
        "crypto_fast_signal_ratio": 0.1,
        "grouped_signal_ratio": 0.1,
        "low_price_entry_ratio": 0.0,
    }
    archetype, copyable, _ = classify_profile(features)
    assert archetype == StrategyArchetype.NEAR_CERTAINTY_SWEEPER
    assert copyable is True


def test_event_specialist() -> None:
    features = {
        "trade_count_30d": 15.0,
        "trade_count_7d": 3.0,
        "market_concentration_hhi": 0.45,
        "avg_hold_time_minutes": 180.0,
        "crypto_fast_signal_ratio": 0.1,
        "grouped_signal_ratio": 0.1,
        "low_price_entry_ratio": 0.2,
        "near_certainty_entry_ratio": 0.2,
        "near_max_exit_ratio": 0.1,
        "win_rate_7d": 0.3,
        "roi_7d": -0.1,
    }
    archetype, copyable, _ = classify_profile(features)
    assert archetype == StrategyArchetype.EVENT_SPECIALIST
    assert copyable is True


def test_balanced_swing() -> None:
    features = {
        "trade_count_30d": 20.0,
        "trade_count_7d": 5.0,
        "market_concentration_hhi": 0.5,
        "avg_hold_time_minutes": 240.0,
        "crypto_fast_signal_ratio": 0.1,
        "grouped_signal_ratio": 0.1,
        "low_price_entry_ratio": 0.2,
        "near_certainty_entry_ratio": 0.2,
        "near_max_exit_ratio": 0.1,
        "win_rate_7d": 0.55,
        "roi_7d": 0.05,
    }
    archetype, copyable, _ = classify_profile(features)
    assert archetype == StrategyArchetype.BALANCED_SWING
    assert copyable is True


def test_extreme_concentration_low_count_is_noisy() -> None:
    features = {
        "trade_count_30d": 10.0,
        "market_concentration_hhi": 0.9,
    }
    archetype, copyable, _ = classify_profile(features)
    assert archetype == StrategyArchetype.NOISY_UNCOPYABLE
    assert copyable is False


def test_no_clear_signature_falls_back_to_noisy() -> None:
    features = {
        "trade_count_30d": 20.0,
        "market_concentration_hhi": 0.5,
        "avg_hold_time_minutes": 45.0,  # too short for EVENT_SPECIALIST (>60)
        "crypto_fast_signal_ratio": 0.2,
        "grouped_signal_ratio": 0.2,
        "low_price_entry_ratio": 0.3,
        "near_certainty_entry_ratio": 0.3,
        "near_max_exit_ratio": 0.2,
        "win_rate_7d": 0.30,
        "roi_7d": -0.20,
    }
    archetype, copyable, _ = classify_profile(features)
    assert archetype == StrategyArchetype.NOISY_UNCOPYABLE
    assert copyable is False