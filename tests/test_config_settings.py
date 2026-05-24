"""Tests for config.settings — typed settings, migration shim, and validation."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.env import ENV, EnvValidationError
from config.settings import (
    AppSettings,
    ExecutionSettings,
    RiskSettings,
    SizingSettings,
    load_settings,
)
from config.validation import (
    validate_bps,
    validate_price_level,
    validate_ratio,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fresh_env(**overrides: Any) -> dict:
    """Build a minimal ENV-like dict with overrides on top of defaults."""
    env = dict(ENV)  # snapshot current defaults
    env.update(overrides)
    return env


# ---------------------------------------------------------------------------
# load_settings() returns valid typed settings from default ENV
# ---------------------------------------------------------------------------

class TestLoadSettingsDefaults:
    def test_returns_app_settings(self):
        settings = load_settings()
        assert isinstance(settings, AppSettings)
        assert isinstance(settings.risk, RiskSettings)
        assert isinstance(settings.sizing, SizingSettings)
        assert isinstance(settings.execution, ExecutionSettings)

    def test_default_risk_values_are_ratios(self):
        settings = load_settings()
        # All ratio fields must be in [0, 1]
        assert 0 <= settings.risk.stop_loss_ratio <= 1
        assert 0 <= settings.risk.take_profit_ratio <= 1
        assert 0 <= settings.risk.daily_drawdown_ratio <= 1
        assert 0 <= settings.risk.profit_lock_trigger_ratio <= 1
        assert 0 <= settings.risk.trailing_trigger_ratio <= 1
        assert 0 <= settings.risk.near_max_price_level <= 1
        assert 0 <= settings.risk.near_max_warn_level <= 1

    def test_default_execution_values(self):
        settings = load_settings()
        assert 0 <= settings.execution.slippage_tolerance_ratio <= 1
        assert settings.execution.txn_fee_bps >= 0

    def test_frozen_dataclasses(self):
        settings = load_settings()
        with pytest.raises(AttributeError):
            settings.risk.stop_loss_ratio = 0.5  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Old env names are migrated correctly
# ---------------------------------------------------------------------------

class TestOldNameMigration:
    def test_stop_loss_pct_migrated(self):
        env = _fresh_env(STOP_LOSS_PCT=25)
        settings = load_settings(env)
        assert settings.risk.stop_loss_ratio == pytest.approx(0.25)

    def test_take_profit_pct_migrated(self):
        env = _fresh_env(TAKE_PROFIT_PCT=70)
        settings = load_settings(env)
        assert settings.risk.take_profit_ratio == pytest.approx(0.70)

    def test_slippage_tolerance_migrated(self):
        env = _fresh_env(SLIPPAGE_TOLERANCE=2.0)
        settings = load_settings(env)
        assert settings.execution.slippage_tolerance_ratio == pytest.approx(0.02)

    def test_max_position_pct_migrated(self):
        env = _fresh_env(MAX_POSITION_PCT=0.45)
        settings = load_settings(env)
        assert settings.sizing.max_position_ratio == pytest.approx(0.45)

    def test_risk_buffer_pct_migrated(self):
        env = _fresh_env(RISK_BUFFER_PCT=0.10)
        settings = load_settings(env)
        assert settings.sizing.risk_buffer_ratio == pytest.approx(0.10)

    def test_daily_drawdown_migrated(self):
        env = _fresh_env(MAX_DAILY_DRAWDOWN_PCT=20)
        settings = load_settings(env)
        assert settings.risk.daily_drawdown_ratio == pytest.approx(0.20)

    def test_profit_lock_trigger_migrated(self):
        env = _fresh_env(PROFIT_LOCK_TRIGGER_PCT=70)
        settings = load_settings(env)
        assert settings.risk.profit_lock_trigger_ratio == pytest.approx(0.70)

    def test_profit_lock_drawdown_migrated(self):
        env = _fresh_env(PROFIT_LOCK_DRAWDOWN_PCT=15)
        settings = load_settings(env)
        assert settings.risk.profit_lock_drawdown_ratio == pytest.approx(0.15)

    def test_profit_lock_sell_fraction_migrated(self):
        env = _fresh_env(PROFIT_LOCK_SELL_FRACTION=0.50)
        settings = load_settings(env)
        assert settings.risk.profit_lock_sell_ratio == pytest.approx(0.50)

    def test_trailing_activation_migrated(self):
        env = _fresh_env(TRAILING_ACTIVATION_PCT=20)
        settings = load_settings(env)
        assert settings.risk.trailing_trigger_ratio == pytest.approx(0.20)

    def test_trailing_distance_migrated(self):
        env = _fresh_env(TRAILING_DISTANCE=0.05)
        settings = load_settings(env)
        assert settings.risk.trailing_distance_ratio == pytest.approx(0.05)

    def test_near_max_price_exit_migrated(self):
        env = _fresh_env(NEAR_MAX_PRICE_EXIT=0.95, NEAR_MAX_PRICE_WARN=0.90)
        settings = load_settings(env)
        assert settings.risk.near_max_price_level == pytest.approx(0.95)

    def test_near_max_price_warn_migrated(self):
        env = _fresh_env(NEAR_MAX_PRICE_WARN=0.92)
        settings = load_settings(env)
        assert settings.risk.near_max_warn_level == pytest.approx(0.92)

    def test_copy_max_price_drift_migrated(self):
        env = _fresh_env(COPY_MAX_PRICE_DRIFT_PCT=15)
        settings = load_settings(env)
        assert settings.execution.copy_max_price_drift_ratio == pytest.approx(0.15)

    def test_copy_max_edge_decay_migrated(self):
        env = _fresh_env(COPY_MAX_EDGE_DECAY_PCT=90)
        settings = load_settings(env)
        assert settings.execution.copy_max_edge_decay_ratio == pytest.approx(0.90)

    def test_transaction_fee_bps_migrated(self):
        env = _fresh_env(TRANSACTION_FEE_BPS=20)
        settings = load_settings(env)
        assert settings.execution.txn_fee_bps == 20

    def test_percentage_migrated_to_multiplier(self):
        env = _fresh_env(PERCENTAGE=6)
        settings = load_settings(env)
        assert settings.sizing.copy_notional_multiplier == pytest.approx(0.06)


# ---------------------------------------------------------------------------
# New names take precedence over old names
# ---------------------------------------------------------------------------

class TestNewNamePrecedence:
    def test_new_stop_loss_overrides_old(self):
        env = _fresh_env(STOP_LOSS_PCT=25, stop_loss_ratio=0.10)
        settings = load_settings(env)
        assert settings.risk.stop_loss_ratio == pytest.approx(0.10)

    def test_new_slippage_overrides_old(self):
        env = _fresh_env(SLIPPAGE_TOLERANCE=5.0, slippage_tolerance_ratio=0.01)
        settings = load_settings(env)
        assert settings.execution.slippage_tolerance_ratio == pytest.approx(0.01)

    def test_new_max_position_overrides_old(self):
        env = _fresh_env(MAX_POSITION_PCT=0.45, max_position_ratio=0.30)
        settings = load_settings(env)
        assert settings.sizing.max_position_ratio == pytest.approx(0.30)

    def test_new_trailing_overrides_old(self):
        env = _fresh_env(TRAILING_ACTIVATION_PCT=20, trailing_trigger_ratio=0.10)
        settings = load_settings(env)
        assert settings.risk.trailing_trigger_ratio == pytest.approx(0.10)

    def test_new_txn_fee_bps_overrides_old(self):
        env = _fresh_env(TRANSACTION_FEE_BPS=20, txn_fee_bps=15)
        settings = load_settings(env)
        assert settings.execution.txn_fee_bps == 15

    def test_new_copy_multiplier_overrides_old(self):
        env = _fresh_env(PERCENTAGE=6, copy_notional_multiplier=0.10)
        settings = load_settings(env)
        assert settings.sizing.copy_notional_multiplier == pytest.approx(0.10)


# ---------------------------------------------------------------------------
# Invalid values raise EnvValidationError
# ---------------------------------------------------------------------------

class TestValidationErrors:
    def test_ratio_above_one_raises(self):
        env = _fresh_env(stop_loss_ratio=1.5)
        with pytest.raises(EnvValidationError, match="stop_loss_ratio"):
            load_settings(env)

    def test_ratio_below_zero_raises(self):
        env = _fresh_env(stop_loss_ratio=-0.1)
        with pytest.raises(EnvValidationError, match="stop_loss_ratio"):
            load_settings(env)

    def test_price_level_above_one_raises(self):
        env = _fresh_env(near_max_price_level=1.2)
        with pytest.raises(EnvValidationError, match="near_max_price_level"):
            load_settings(env)

    def test_negative_bps_raises(self):
        env = _fresh_env(txn_fee_bps=-5)
        with pytest.raises(EnvValidationError, match="txn_fee_bps"):
            load_settings(env)

    def test_negative_trailing_distance_raises(self):
        env = _fresh_env(trailing_distance_ratio=-0.01)
        with pytest.raises(EnvValidationError, match="trailing_distance_ratio"):
            load_settings(env)

    def test_slippage_above_one_raises(self):
        env = _fresh_env(slippage_tolerance_ratio=1.5)
        with pytest.raises(EnvValidationError, match="slippage_tolerance_ratio"):
            load_settings(env)


# ---------------------------------------------------------------------------
# Validator unit tests
# ---------------------------------------------------------------------------

class TestValidators:
    def test_validate_ratio_accepts_zero(self):
        assert validate_ratio(0.0, "field") == 0.0

    def test_validate_ratio_accepts_one(self):
        assert validate_ratio(1.0, "field") == 1.0

    def test_validate_ratio_rejects_over_one(self):
        with pytest.raises(EnvValidationError):
            validate_ratio(1.01, "field")

    def test_validate_ratio_rejects_negative(self):
        with pytest.raises(EnvValidationError):
            validate_ratio(-0.01, "field")

    def test_validate_price_level_boundaries(self):
        assert validate_price_level(0.0, "field") == 0.0
        assert validate_price_level(1.0, "field") == 1.0
        with pytest.raises(EnvValidationError):
            validate_price_level(1.1, "field")
        with pytest.raises(EnvValidationError):
            validate_price_level(-0.1, "field")

    def test_validate_bps_accepts_zero(self):
        assert validate_bps(0, "field") == 0

    def test_validate_bps_rejects_negative(self):
        with pytest.raises(EnvValidationError):
            validate_bps(-1, "field")


# ---------------------------------------------------------------------------
# Settings with empty env (all defaults)
# ---------------------------------------------------------------------------

class TestEmptyEnvDefaults:
    def test_load_with_empty_env_uses_defaults(self):
        settings = load_settings(env={})
        # Should not raise, all defaults are valid
        assert settings.risk.stop_loss_ratio == pytest.approx(0.20)
        assert settings.risk.take_profit_ratio == pytest.approx(0.70)
        assert settings.risk.daily_drawdown_ratio == pytest.approx(0.20)
        assert settings.risk.trailing_trigger_ratio == pytest.approx(0.20)
        assert settings.risk.trailing_distance_ratio == pytest.approx(0.05)
        assert settings.risk.near_max_price_level == pytest.approx(0.95)
        assert settings.risk.near_max_warn_level == pytest.approx(0.92)
        assert settings.sizing.max_position_ratio == pytest.approx(0.45)
        assert settings.sizing.risk_buffer_ratio == pytest.approx(0.10)
        assert settings.sizing.copy_notional_multiplier == pytest.approx(0.06)
        assert settings.execution.slippage_tolerance_ratio == pytest.approx(0.02)
        assert settings.execution.txn_fee_bps == 20
        assert settings.execution.copy_max_price_drift_ratio == pytest.approx(0.15)
        assert settings.execution.copy_max_edge_decay_ratio == pytest.approx(0.90)
