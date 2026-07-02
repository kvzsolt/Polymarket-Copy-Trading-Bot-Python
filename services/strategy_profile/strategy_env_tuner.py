"""Archetype -> EnvPatch list (PR3).

Produces a list of :class:`EnvPatch` proposals tuned to the classified
archetype. Dry-run by default; the caller (``env_patch_writer``) only mutates
the env file when ``--apply`` is passed.

Per the deep-research report's example patches. Do not overfit: the
``NOISY_UNCOPYABLE`` archetype produces an empty patch list and a "do not
auto-tune" recommendation.
"""
from __future__ import annotations

from models.env_patch import EnvPatch
from models.strategy_profile import StrategyArchetype, StrategyProfile

_HIGH = 0.9
_MED = 0.7
_LOW = 0.5


def tune_strategy(profile: StrategyProfile) -> list[EnvPatch]:
    """Return the env-patch list for ``profile.archetype``."""
    archetype = profile.archetype
    if archetype == StrategyArchetype.FAST_CRYPTO_SCALPER:
        return _fast_crypto_scalper(profile)
    if archetype == StrategyArchetype.LOW_PRICE_LOTTERY:
        return _low_price_lottery(profile)
    if archetype == StrategyArchetype.NEAR_CERTAINTY_SWEEPER:
        return _near_certainty_sweeper(profile)
    if archetype == StrategyArchetype.EVENT_SPECIALIST:
        return _event_specialist(profile)
    if archetype == StrategyArchetype.BALANCED_SWING:
        return _balanced_swing(profile)
    # NOISY_UNCOPYABLE -> no patches; recommend not auto-tuning.
    return []


def _patch(key: str, new_value: str, reason: str, confidence: float) -> EnvPatch:
    return EnvPatch(
        key=key,
        old_value=None,  # populated by the writer when --apply runs
        new_value=new_value,
        reason=reason,
        confidence=confidence,
    )


def _fast_crypto_scalper(profile: StrategyProfile) -> list[EnvPatch]:
    return [
        _patch("ENABLE_ONCHAIN_PENDING_DETECTION", "true",
               "fast scalpers benefit from pending-tx detection (sub-block latency)", _HIGH),
        _patch("PENDING_TX_MAX_SIGNAL_AGE_MS", "500",
               "tight pending-signal age window for fast copy", _HIGH),
        _patch("COPY_MAX_SIGNAL_AGE_SECONDS", "2",
               "fast scalpers require sub-2s copy latency", _HIGH),
        _patch("COPY_MAX_PRICE_DRIFT_PCT", "3",
               "narrow price-drift tolerance for scalper entries", _MED),
        _patch("BUY_MAX_AMOUNT", "3",
               "smaller clip size to match scalper notional distribution", _MED),
        _patch("MAX_POSITION_PCT", "0.05",
               "tight per-position cap for fast-turnover strategy", _MED),
        _patch("MARKET_DATA_PRIMARY_SOURCE", "rest_snapshot",
               "REST snapshot is lowest-latency for scalper execution", _HIGH),
        _patch("MARKET_DATA_REST_SNAPSHOT_MAX_AGE_MS", "300",
               "sub-300ms snapshot freshness for scalper entries", _MED),
    ]


def _low_price_lottery(profile: StrategyProfile) -> list[EnvPatch]:
    return [
        _patch("MIN_SHARE_PRICE", "0.03",
               "match lottery-style low-price entries", _MED),
        _patch("MAX_SHARE_PRICE", "0.35",
               "upper bound on lottery-style entries", _MED),
        _patch("BUY_MAX_AMOUNT", "2",
               "smaller clip for high-volume low-price entries", _MED),
        _patch("MAX_POSITION_PCT", "0.03",
               "tight cap on low-price inventory", _MED),
        _patch("MAX_OPEN_POSITIONS", "80",
               "allow many concurrent low-price positions", _LOW),
        _patch("ENABLE_STOP_LOSS", "false",
               "lottery entries are binary-outcome; stop-loss misfires", _LOW),
        _patch("ENABLE_NEAR_MAX_EXIT", "true",
               "exit near certainty when lottery pays off", _MED),
    ]


def _near_certainty_sweeper(profile: StrategyProfile) -> list[EnvPatch]:
    return [
        _patch("MIN_SHARE_PRICE", "0.75",
               "sweeper entries are high-probability", _MED),
        _patch("MAX_SHARE_PRICE", "0.95",
               "upper bound on near-certainty entries", _MED),
        _patch("ENABLE_NEAR_RESOLUTION_GATE", "true",
               "gate on near-resolution markets only", _MED),
        _patch("NEAR_RESOLUTION_GATE_PRICE", "0.95",
               "resolution-gate price threshold", _MED),
        _patch("ENABLE_NEAR_MAX_EXIT", "true",
               "exit at near-max prices", _MED),
        _patch("NEAR_MAX_PRICE_WARN", "0.95",
               "warn threshold for near-max exit", _LOW),
        _patch("NEAR_MAX_PRICE_EXIT", "0.99",
               "exit threshold for near-max sweepers", _LOW),
        _patch("BUY_MAX_AMOUNT", "6",
               "larger clip for high-probability entries", _MED),
    ]


def _event_specialist(profile: StrategyProfile) -> list[EnvPatch]:
    return [
        _patch("COPY_MAX_SIGNAL_AGE_SECONDS", "10",
               "event specialists trade on multi-hour windows; relax age gate", _LOW),
        _patch("COPY_MAX_PRICE_DRIFT_PCT", "5",
               "wider drift tolerance for event-driven entries", _LOW),
        _patch("BUY_MAX_AMOUNT", "8",
               "moderate clip for event-specialist notional", _LOW),
        _patch("MAX_POSITION_PCT", "0.10",
               "moderate per-position cap", _LOW),
    ]


def _balanced_swing(profile: StrategyProfile) -> list[EnvPatch]:
    return [
        _patch("COPY_MAX_SIGNAL_AGE_SECONDS", "5",
               "balanced swing trades tolerate moderate copy latency", _LOW),
        _patch("COPY_MAX_PRICE_DRIFT_PCT", "4",
               "moderate drift tolerance", _LOW),
        _patch("BUY_MAX_AMOUNT", "6",
               "moderate clip size", _LOW),
        _patch("MAX_POSITION_PCT", "0.08",
               "moderate per-position cap", _LOW),
    ]