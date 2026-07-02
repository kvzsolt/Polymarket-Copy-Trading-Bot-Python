"""Tests for ``services/strategy_profile/strategy_env_tuner.py`` (PR3)."""
from __future__ import annotations

import time

from models.strategy_profile import StrategyArchetype, StrategyProfile
from services.strategy_profile.strategy_env_tuner import tune_strategy


def _profile(archetype: StrategyArchetype) -> StrategyProfile:
    return StrategyProfile(
        wallet="0xabc",
        lookback_days=30,
        features={},
        archetype=archetype,
        copyable=True,
        computed_at=int(time.time()),
    )


def test_fast_crypto_scalper_patches() -> None:
    patches = tune_strategy(_profile(StrategyArchetype.FAST_CRYPTO_SCALPER))
    keys = {p.key for p in patches}
    assert "ENABLE_ONCHAIN_PENDING_DETECTION" in keys
    assert "PENDING_TX_MAX_SIGNAL_AGE_MS" in keys
    assert "COPY_MAX_SIGNAL_AGE_SECONDS" in keys
    assert "BUY_MAX_AMOUNT" in keys
    assert "MARKET_DATA_PRIMARY_SOURCE" in keys
    # Every patch has a reason + confidence in [0, 1].
    for p in patches:
        assert p.reason
        assert 0.0 <= p.confidence <= 1.0
        assert p.new_value


def test_low_price_lottery_patches() -> None:
    patches = tune_strategy(_profile(StrategyArchetype.LOW_PRICE_LOTTERY))
    keys = {p.key for p in patches}
    assert "MIN_SHARE_PRICE" in keys
    assert "MAX_SHARE_PRICE" in keys
    assert "ENABLE_NEAR_MAX_EXIT" in keys
    assert "ENABLE_STOP_LOSS" in keys
    # Lottery never proposes enabling pending detection.
    assert "ENABLE_ONCHAIN_PENDING_DETECTION" not in keys


def test_near_certainty_sweeper_patches() -> None:
    patches = tune_strategy(_profile(StrategyArchetype.NEAR_CERTAINTY_SWEEPER))
    keys = {p.key for p in patches}
    assert "MIN_SHARE_PRICE" in keys
    assert "ENABLE_NEAR_RESOLUTION_GATE" in keys
    assert "NEAR_MAX_PRICE_EXIT" in keys
    assert "BUY_MAX_AMOUNT" in keys


def test_event_specialist_patches() -> None:
    patches = tune_strategy(_profile(StrategyArchetype.EVENT_SPECIALIST))
    assert len(patches) >= 1
    keys = {p.key for p in patches}
    assert "COPY_MAX_SIGNAL_AGE_SECONDS" in keys


def test_balanced_swing_patches() -> None:
    patches = tune_strategy(_profile(StrategyArchetype.BALANCED_SWING))
    assert len(patches) >= 1


def test_noisy_uncopyable_returns_empty() -> None:
    patches = tune_strategy(_profile(StrategyArchetype.NOISY_UNCOPYABLE))
    assert patches == []


def test_tuner_never_produces_protected_keys() -> None:
    """No archetype's patch list may include a protected key."""
    from services.strategy_profile.env_patch_writer import is_protected_key

    for archetype in StrategyArchetype:
        patches = tune_strategy(_profile(archetype))
        for p in patches:
            assert not is_protected_key(p.key), (
                f"archetype {archetype} produced a protected-key patch: {p.key}"
            )