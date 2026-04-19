"""Typed, validated settings built from the ENV snapshot.

This module is a legacy compatibility layer for older tests and env snapshots.
The live runtime uses :mod:`config.app_config` and the advanced exit engines
there; the primitive stop-loss / single take-profit fields remain here only so
older snapshots can still load.

Usage::

    from config.settings import load_settings

    settings = load_settings()          # uses ENV
    settings.risk.stop_loss_ratio       # legacy compat only
    settings.execution.slippage_tolerance_ratio  # 0.02 (was SLIPPAGE_TOLERANCE=2.0)
"""

from __future__ import annotations

import os
import warnings
from dataclasses import dataclass

from config.env import ENV, EnvValidationError
from config.validation import (
    validate_bps,
    validate_non_negative_float,
    validate_price_level,
    validate_ratio,
)


# ---------------------------------------------------------------------------
# Migration shim: old env name -> (new field, conversion)
# ---------------------------------------------------------------------------

_DIV100 = lambda v: v / 100.0   # noqa: E731
_IDENTITY = lambda v: v         # noqa: E731

# Each entry: (old_env_name, new_field_name, conversion_for_old_value)
_LEGACY_MAPPINGS: list[tuple[str, str, object]] = [
    # Risk compatibility fields.
    ("STOP_LOSS_PCT",              "stop_loss_ratio",              _DIV100),
    ("TAKE_PROFIT_PCT",            "take_profit_ratio",             _DIV100),
    ("MAX_DAILY_DRAWDOWN_PCT",     "daily_drawdown_ratio",         _DIV100),
    ("PROFIT_LOCK_TRIGGER_PCT",    "profit_lock_trigger_ratio",    _DIV100),
    ("PROFIT_LOCK_DRAWDOWN_PCT",   "profit_lock_drawdown_ratio",   _DIV100),
    ("PROFIT_LOCK_SELL_FRACTION",  "profit_lock_sell_ratio",       _IDENTITY),
    ("TRAILING_ACTIVATION_PCT",    "trailing_trigger_ratio",       _DIV100),
    ("TRAILING_DISTANCE",          "trailing_distance_ratio",      _IDENTITY),
    ("NEAR_MAX_PRICE_EXIT",        "near_max_price_level",         _IDENTITY),
    ("NEAR_MAX_PRICE_WARN",        "near_max_warn_level",          _IDENTITY),
    # Sizing
    ("MAX_POSITION_PCT",           "max_position_ratio",           _IDENTITY),
    ("RISK_BUFFER_PCT",            "risk_buffer_ratio",            _IDENTITY),
    ("PERCENTAGE",                 "copy_notional_multiplier",     _DIV100),
    # Execution / decision
    ("SLIPPAGE_TOLERANCE",         "slippage_tolerance_ratio",     _DIV100),
    ("COPY_MAX_PRICE_DRIFT_PCT",   "copy_max_price_drift_ratio",   _DIV100),
    ("COPY_MAX_EDGE_DECAY_PCT",    "copy_max_edge_decay_ratio",    _DIV100),
    ("TRANSACTION_FEE_BPS",        "txn_fee_bps",                  _IDENTITY),
]


# Defaults (all in new-name units, matching runtime.env defaults).
# The legacy primitive exit ratios stay here for compatibility, but they are
# not consumed by the live risk engine.
_DEFAULTS: dict[str, float] = {
    "stop_loss_ratio": 0.20,
    "take_profit_ratio": 0.70,
    "daily_drawdown_ratio": 0.20,
    "profit_lock_trigger_ratio": 0.70,
    "profit_lock_drawdown_ratio": 0.15,
    "profit_lock_sell_ratio": 0.50,
    "trailing_trigger_ratio": 0.20,
    "trailing_distance_ratio": 0.05,
    "near_max_price_level": 0.95,
    "near_max_warn_level": 0.92,
    "max_position_ratio": 0.45,
    "risk_buffer_ratio": 0.10,
    "copy_notional_multiplier": 0.06,
    "slippage_tolerance_ratio": 0.02,
    "copy_max_price_drift_ratio": 0.15,
    "copy_max_edge_decay_ratio": 0.90,
    "txn_fee_bps": 20,
}


def _build_lookup(env: dict, errors: list[str], *, allow_env_fallback: bool = True) -> dict[str, float]:
    """Build a new_name -> value lookup from *env* using legacy mappings.

    New-name keys in *env* take precedence over old-name keys.
    When *allow_env_fallback* is True (default), also checks ``os.environ``
    for new-name keys that may not exist in the passed snapshot.
    """
    result: dict[str, float] = {}
    for old_name, new_name, convert in _LEGACY_MAPPINGS:
        # New name in env takes precedence (already in correct units)
        raw = env.get(new_name)
        if raw is None and allow_env_fallback:
            # Fallback: check UPPERCASE variant in os.environ only
            raw = os.environ.get(new_name) or os.environ.get(new_name.upper())
        if raw not in (None, ""):
            try:
                result[new_name] = float(raw)
            except ValueError:
                errors.append(f"{new_name}={raw!r} is not a valid number")
            continue
        # Fall back to old name with conversion
        raw = env.get(old_name)
        if raw is None and allow_env_fallback:
            raw = os.environ.get(old_name)
        if raw not in (None, ""):
            try:
                result[new_name] = float(convert(float(raw)))
            except ValueError:
                errors.append(f"{old_name}={raw!r} is not a valid number")
            else:
                warnings.warn(
                    f"Legacy env var '{old_name}' is deprecated; use '{new_name}' instead",
                    DeprecationWarning,
                    stacklevel=3,
                )
    return result


def _get(lookup: dict[str, float], name: str) -> float:
    """Get a value from *lookup*, falling back to ``_DEFAULTS``."""
    if name in lookup:
        return lookup[name]
    return _DEFAULTS[name]


# ---------------------------------------------------------------------------
# Typed settings dataclasses
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class RiskSettings:
    """Risk settings used by legacy compatibility tests."""
    stop_loss_ratio: float          # legacy compat only; not used by live exits
    take_profit_ratio: float        # legacy compat only; not used by live exits
    daily_drawdown_ratio: float     # max daily drawdown as fraction
    profit_lock_trigger_ratio: float
    profit_lock_drawdown_ratio: float
    profit_lock_sell_ratio: float
    trailing_trigger_ratio: float   # profit % to activate trailing stop
    trailing_distance_ratio: float  # trailing distance (share price units)
    near_max_price_level: float     # full-exit price level
    near_max_warn_level: float      # warn/trim price level


@dataclass(frozen=True)
class SizingSettings:
    """Position sizing parameters."""
    max_position_ratio: float       # max fraction of balance per trade
    risk_buffer_ratio: float        # buffer ratio for risk calculations
    copy_notional_multiplier: float # multiplier for copy trade sizing


@dataclass(frozen=True)
class ExecutionSettings:
    """Execution quality and decision gate parameters."""
    slippage_tolerance_ratio: float     # max slippage as ratio [0,1]
    copy_max_price_drift_ratio: float   # max price drift from reference
    copy_max_edge_decay_ratio: float    # max edge decay
    txn_fee_bps: int                    # transaction fee in basis points


@dataclass(frozen=True)
class AppSettings:
    """Top-level container for all typed settings."""
    risk: RiskSettings
    sizing: SizingSettings
    execution: ExecutionSettings


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------

def load_settings(env: dict | None = None) -> AppSettings:
    """Build validated ``AppSettings`` from the ENV snapshot.

    Args:
        env: Override dict (defaults to the module-level ``ENV``).
             Useful for testing.

    Raises:
        EnvValidationError: On any invalid value.
    """
    if env is None:
        env = ENV
        allow_env_fallback = True
    else:
        allow_env_fallback = False

    errors: list[str] = []
    lookup = _build_lookup(env, errors, allow_env_fallback=allow_env_fallback)

    def _safe(validator, name: str, default: float):
        val = _get(lookup, name)
        try:
            return validator(val, name)
        except EnvValidationError as exc:
            errors.append(str(exc))
            return default

    risk = RiskSettings(
        stop_loss_ratio=_safe(validate_ratio, "stop_loss_ratio", 0.20),
        take_profit_ratio=_safe(validate_ratio, "take_profit_ratio", 0.70),
        daily_drawdown_ratio=_safe(validate_ratio, "daily_drawdown_ratio", 0.20),
        profit_lock_trigger_ratio=_safe(validate_ratio, "profit_lock_trigger_ratio", 0.70),
        profit_lock_drawdown_ratio=_safe(validate_ratio, "profit_lock_drawdown_ratio", 0.15),
        profit_lock_sell_ratio=_safe(validate_ratio, "profit_lock_sell_ratio", 0.50),
        trailing_trigger_ratio=_safe(validate_ratio, "trailing_trigger_ratio", 0.20),
        trailing_distance_ratio=_safe(validate_non_negative_float, "trailing_distance_ratio", 0.05),
        near_max_price_level=_safe(validate_price_level, "near_max_price_level", 0.95),
        near_max_warn_level=_safe(validate_price_level, "near_max_warn_level", 0.92),
    )

    sizing = SizingSettings(
        max_position_ratio=_safe(validate_ratio, "max_position_ratio", 0.45),
        risk_buffer_ratio=_safe(validate_non_negative_float, "risk_buffer_ratio", 0.10),
        copy_notional_multiplier=_safe(validate_non_negative_float, "copy_notional_multiplier", 0.06),
    )

    execution = ExecutionSettings(
        slippage_tolerance_ratio=_safe(validate_ratio, "slippage_tolerance_ratio", 0.02),
        copy_max_price_drift_ratio=_safe(validate_non_negative_float, "copy_max_price_drift_ratio", 0.15),
        copy_max_edge_decay_ratio=_safe(validate_non_negative_float, "copy_max_edge_decay_ratio", 0.90),
        txn_fee_bps=_safe(lambda v, n: validate_bps(int(v), n), "txn_fee_bps", 20),
    )

    # Cross-field validation for the live advanced exits only.
    if risk.near_max_warn_level > risk.near_max_price_level:
        errors.append(
            f"near_max_warn_level ({risk.near_max_warn_level}) must be <= "
            f"near_max_price_level ({risk.near_max_price_level})"
        )

    if errors:
        raise EnvValidationError(
            "Invalid settings:\n- " + "\n- ".join(errors)
        )

    return AppSettings(risk=risk, sizing=sizing, execution=execution)


# ---------------------------------------------------------------------------
# Cached loader (computed once per process, call clear_settings_cache() to reset)
# ---------------------------------------------------------------------------

_cached_settings: AppSettings | None = None


def load_settings_cached(env: dict | None = None) -> AppSettings:
    """Return cached settings, building on first call.

    Use ``clear_settings_cache()`` to force a rebuild.
    """
    global _cached_settings
    if _cached_settings is None:
        _cached_settings = load_settings(env)
    return _cached_settings


def clear_settings_cache() -> None:
    """Clear the cached settings so the next call rebuilds from env."""
    global _cached_settings
    _cached_settings = None
