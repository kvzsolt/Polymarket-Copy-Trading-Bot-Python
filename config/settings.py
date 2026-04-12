"""Typed, validated settings built from the ENV snapshot.

Usage::

    from config.settings import load_settings

    settings = load_settings()          # uses ENV
    settings.risk.stop_loss_ratio       # 0.20  (was STOP_LOSS_PCT=20)
    settings.execution.slippage_tolerance_ratio  # 0.02  (was SLIPPAGE_TOLERANCE=2.0)
"""

from __future__ import annotations

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
    # Risk
    ("STOP_LOSS_PCT",              "stop_loss_ratio",              _DIV100),
    ("TAKE_PROFIT_PCT",            "tp_price",                     _DIV100),
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


# Defaults (all in new-name units, matching runtime.env defaults)
_DEFAULTS: dict[str, float] = {
    "stop_loss_ratio": 0.20,
    "tp_price": 0.70,
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


def _build_lookup(env: dict) -> dict[str, float]:
    """Build a new_name -> value lookup from *env* using legacy mappings.

    New-name keys in *env* take precedence over old-name keys.
    """
    result: dict[str, float] = {}
    for old_name, new_name, convert in _LEGACY_MAPPINGS:
        # New name in env takes precedence (already in correct units)
        if new_name in env:
            raw = env[new_name]
            if raw not in (None, ""):
                result[new_name] = float(raw)
                continue
        # Fall back to old name with conversion
        if old_name in env:
            raw = env[old_name]
            if raw not in (None, ""):
                result[new_name] = float(convert(float(raw)))
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
    """Risk management ratios (all in [0,1] unless noted)."""
    stop_loss_ratio: float          # fraction loss to trigger stop
    tp_price: float                 # take-profit price level [0,1]
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

    lookup = _build_lookup(env)

    errors: list[str] = []

    def _safe(validator, name: str, default: float):
        val = _get(lookup, name)
        try:
            return validator(val, name)
        except EnvValidationError as exc:
            errors.append(str(exc))
            return default

    risk = RiskSettings(
        stop_loss_ratio=_safe(validate_ratio, "stop_loss_ratio", 0.20),
        tp_price=_safe(validate_non_negative_float, "tp_price", 0.70),
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

    if errors:
        raise EnvValidationError(
            "Invalid settings:\n- " + "\n- ".join(errors)
        )

    return AppSettings(risk=risk, sizing=sizing, execution=execution)
