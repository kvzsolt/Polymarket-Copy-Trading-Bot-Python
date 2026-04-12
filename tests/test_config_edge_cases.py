"""Category 1: Config parsing/validation edge cases.

Covers:
- Missing required fields -> safe defaults
- Cross-field validation errors
- Invalid ratio values (>1.0, <0.0)
- Non-numeric values -> EnvValidationError
- Zero values for critical ratios
- Boundary values (exactly 0.0, exactly 1.0)
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.env import ENV, EnvValidationError
from config.settings import load_settings
from config.validation import (
    validate_bps,
    validate_non_negative_float,
    validate_price_level,
    validate_positive_float,
    validate_ratio,
    validate_usdc,
)


def _fresh_env(**overrides: str) -> dict:
    """Build a minimal ENV-like dict with overrides."""
    env = dict(ENV)
    env.update(overrides)
    return env


def _get_field(settings, field: str):
    """Get a field from the appropriate settings group."""
    for group in (settings.risk, settings.sizing, settings.execution):
        if hasattr(group, field):
            return getattr(group, field)
    raise AttributeError(f"No settings group has field '{field}'")


# ---------------------------------------------------------------------------
# Missing required fields -> safe defaults
# ---------------------------------------------------------------------------


class TestMissingFieldsDefaults:
    """When a field is absent from env, the default is used."""

    def test_missing_stop_loss_uses_default(self):
        env = dict(ENV)
        env.pop("stop_loss_ratio", None)
        env.pop("STOP_LOSS_PCT", None)
        settings = load_settings(env)
        assert settings.risk.stop_loss_ratio == pytest.approx(0.20)

    def test_missing_take_profit_uses_default(self):
        env = dict(ENV)
        env.pop("take_profit_ratio", None)
        env.pop("TAKE_PROFIT_PCT", None)
        settings = load_settings(env)
        assert settings.risk.take_profit_ratio == pytest.approx(0.70)

    def test_missing_trailing_distance_uses_default(self):
        env = dict(ENV)
        env.pop("trailing_distance_ratio", None)
        env.pop("TRAILING_DISTANCE", None)
        settings = load_settings(env)
        assert settings.risk.trailing_distance_ratio == pytest.approx(0.05)

    def test_empty_env_all_defaults(self):
        settings = load_settings(env={})
        assert settings.risk.stop_loss_ratio == pytest.approx(0.20)
        assert settings.risk.take_profit_ratio == pytest.approx(0.70)
        assert settings.sizing.max_position_ratio == pytest.approx(0.45)
        assert settings.execution.txn_fee_bps == 20

    def test_blank_string_treated_as_missing(self):
        env = _fresh_env(stop_loss_ratio="")
        settings = load_settings(env)
        # Empty string should fall through to default
        assert settings.risk.stop_loss_ratio == pytest.approx(0.20)


# ---------------------------------------------------------------------------
# Cross-field validation
# ---------------------------------------------------------------------------


class TestCrossFieldValidation:
    def test_near_max_warn_gt_exit_raises(self):
        env = _fresh_env(
            near_max_warn_level=0.97,
            near_max_price_level=0.95,
        )
        with pytest.raises(EnvValidationError, match="near_max_warn_level"):
            load_settings(env)

    def test_near_max_warn_equal_exit_ok(self):
        env = _fresh_env(
            near_max_warn_level=0.95,
            near_max_price_level=0.95,
        )
        settings = load_settings(env)
        assert settings.risk.near_max_warn_level == pytest.approx(0.95)

    def test_near_max_warn_lt_exit_ok(self):
        env = _fresh_env(
            near_max_warn_level=0.90,
            near_max_price_level=0.95,
        )
        settings = load_settings(env)
        assert settings.risk.near_max_warn_level == pytest.approx(0.90)

    def test_profit_lock_trigger_lte_stop_loss_raises(self):
        env = _fresh_env(
            profit_lock_trigger_ratio=0.10,
            stop_loss_ratio=0.20,
        )
        with pytest.raises(EnvValidationError, match="profit_lock_trigger_ratio"):
            load_settings(env)

    def test_profit_lock_trigger_equal_stop_loss_raises(self):
        env = _fresh_env(
            profit_lock_trigger_ratio=0.20,
            stop_loss_ratio=0.20,
        )
        with pytest.raises(EnvValidationError, match="profit_lock_trigger_ratio"):
            load_settings(env)

    def test_profit_lock_trigger_gt_stop_loss_ok(self):
        env = _fresh_env(
            profit_lock_trigger_ratio=0.30,
            stop_loss_ratio=0.20,
        )
        settings = load_settings(env)
        assert settings.risk.profit_lock_trigger_ratio == pytest.approx(0.30)

    def test_multiple_errors_reported_together(self):
        env = _fresh_env(
            stop_loss_ratio=2.0,
            near_max_warn_level=0.99,
            near_max_price_level=0.90,
        )
        with pytest.raises(EnvValidationError) as exc_info:
            load_settings(env)
        msg = str(exc_info.value)
        # Both the invalid stop_loss AND the cross-field error should appear
        assert "stop_loss_ratio" in msg


# ---------------------------------------------------------------------------
# Invalid ratio values
# ---------------------------------------------------------------------------


class TestInvalidRatioValues:
    @pytest.mark.parametrize("field,value", [
        ("stop_loss_ratio", 1.5),
        ("stop_loss_ratio", -0.1),
        ("take_profit_ratio", 2.0),
        ("take_profit_ratio", -0.5),
        ("daily_drawdown_ratio", -0.01),
        ("daily_drawdown_ratio", 1.01),
        ("slippage_tolerance_ratio", -0.1),
        ("slippage_tolerance_ratio", 1.5),
        ("profit_lock_trigger_ratio", -0.1),
        ("profit_lock_sell_ratio", 1.2),
        ("trailing_trigger_ratio", -1.0),
        ("near_max_price_level", 1.5),
        ("near_max_warn_level", -0.1),
        ("max_position_ratio", 2.0),
    ])
    def test_invalid_ratio_raises(self, field, value):
        env = _fresh_env(**{field: value})
        with pytest.raises(EnvValidationError):
            load_settings(env)


# ---------------------------------------------------------------------------
# Non-numeric values
# ---------------------------------------------------------------------------


class TestNonNumericValues:
    def test_stop_loss_non_numeric_raises(self):
        env = _fresh_env(stop_loss_ratio="abc")
        with pytest.raises(EnvValidationError, match="not a valid number"):
            load_settings(env)

    def test_take_profit_non_numeric_raises(self):
        env = _fresh_env(take_profit_ratio="xyz")
        with pytest.raises(EnvValidationError, match="not a valid number"):
            load_settings(env)

    def test_txn_fee_bps_non_numeric_raises(self):
        env = _fresh_env(txn_fee_bps="not_a_number")
        with pytest.raises(EnvValidationError, match="not a valid number"):
            load_settings(env)

    def test_trailing_distance_non_numeric_raises(self):
        env = _fresh_env(trailing_distance_ratio="notnumeric")
        with pytest.raises(EnvValidationError, match="not a valid number"):
            load_settings(env)


# ---------------------------------------------------------------------------
# Zero values for critical ratios
# ---------------------------------------------------------------------------


class TestZeroValues:
    def test_stop_loss_zero_is_valid(self):
        """stop_loss=0.0 means 'no stop-loss' — ratio validator allows [0,1]."""
        env = _fresh_env(stop_loss_ratio=0.0)
        settings = load_settings(env)
        assert settings.risk.stop_loss_ratio == 0.0

    def test_take_profit_zero_is_valid(self):
        env = _fresh_env(take_profit_ratio=0.0)
        settings = load_settings(env)
        assert settings.risk.take_profit_ratio == 0.0

    def test_trailing_distance_zero_is_valid(self):
        """trailing_distance uses validate_non_negative_float, so 0 is allowed."""
        env = _fresh_env(trailing_distance_ratio=0.0)
        settings = load_settings(env)
        assert settings.risk.trailing_distance_ratio == 0.0

    def test_risk_buffer_zero_is_valid(self):
        env = _fresh_env(risk_buffer_ratio=0.0)
        settings = load_settings(env)
        assert settings.sizing.risk_buffer_ratio == 0.0


# ---------------------------------------------------------------------------
# Boundary values (exactly 0.0, exactly 1.0)
# ---------------------------------------------------------------------------


class TestBoundaryValues:
    @pytest.mark.parametrize("field", [
        "stop_loss_ratio",
        "take_profit_ratio",
        "daily_drawdown_ratio",
        "profit_lock_drawdown_ratio",
        "profit_lock_sell_ratio",
        "trailing_trigger_ratio",
        "slippage_tolerance_ratio",
    ])
    def test_ratio_exactly_zero(self, field):
        env = _fresh_env(**{field: 0.0})
        settings = load_settings(env)
        val = _get_field(settings, field)
        assert val == 0.0

    def test_sizing_ratio_exactly_zero(self):
        env = _fresh_env(max_position_ratio=0.0)
        settings = load_settings(env)
        assert settings.sizing.max_position_ratio == 0.0

    def test_profit_lock_trigger_zero_with_stop_loss_zero(self):
        """profit_lock_trigger_ratio=0 requires stop_loss_ratio=0 too (cross-field)."""
        env = _fresh_env(profit_lock_trigger_ratio=0.0, stop_loss_ratio=0.0)
        # Cross-field check: profit_lock_trigger <= stop_loss -> error. Both 0 -> equal -> raises.
        with pytest.raises(EnvValidationError, match="profit_lock_trigger_ratio"):
            load_settings(env)

    @pytest.mark.parametrize("field", [
        "take_profit_ratio",
        "daily_drawdown_ratio",
        "profit_lock_drawdown_ratio",
        "profit_lock_sell_ratio",
        "trailing_trigger_ratio",
        "slippage_tolerance_ratio",
    ])
    def test_ratio_exactly_one(self, field):
        env = _fresh_env(**{field: 1.0})
        settings = load_settings(env)
        val = _get_field(settings, field)
        assert val == 1.0

    def test_stop_loss_one_requires_profit_lock_above(self):
        """stop_loss_ratio=1.0 needs profit_lock_trigger > 1.0 which is invalid.
        So stop_loss=1.0 with default profit_lock raises cross-field error."""
        env = _fresh_env(stop_loss_ratio=1.0)
        with pytest.raises(EnvValidationError, match="profit_lock_trigger_ratio"):
            load_settings(env)

    def test_stop_loss_zero_works(self):
        env = _fresh_env(stop_loss_ratio=0.0)
        settings = load_settings(env)
        assert settings.risk.stop_loss_ratio == 0.0

    def test_sizing_ratio_exactly_one(self):
        env = _fresh_env(max_position_ratio=1.0)
        settings = load_settings(env)
        assert settings.sizing.max_position_ratio == 1.0

    def test_profit_lock_trigger_one(self):
        """profit_lock_trigger_ratio=1.0 with stop_loss at default 0.20 -> valid."""
        env = _fresh_env(profit_lock_trigger_ratio=1.0)
        settings = load_settings(env)
        assert settings.risk.profit_lock_trigger_ratio == 1.0

    def test_price_level_boundaries(self):
        """Price levels also in [0,1]."""
        env = _fresh_env(near_max_price_level=0.0, near_max_warn_level=0.0)
        settings = load_settings(env)
        assert settings.risk.near_max_price_level == 0.0
        assert settings.risk.near_max_warn_level == 0.0

        env = _fresh_env(near_max_price_level=1.0, near_max_warn_level=1.0)
        settings = load_settings(env)
        assert settings.risk.near_max_price_level == 1.0
        assert settings.risk.near_max_warn_level == 1.0

    def test_bps_zero_is_valid(self):
        env = _fresh_env(txn_fee_bps=0)
        settings = load_settings(env)
        assert settings.execution.txn_fee_bps == 0

    def test_bps_large_value_ok(self):
        """bps just needs >= 0, no upper bound."""
        env = _fresh_env(txn_fee_bps=10000)
        settings = load_settings(env)
        assert settings.execution.txn_fee_bps == 10000


# ---------------------------------------------------------------------------
# Validator unit tests (additional edge cases)
# ---------------------------------------------------------------------------


class TestValidatorEdgeCases:
    def test_validate_usdc_positive_required(self):
        with pytest.raises(EnvValidationError, match="must be > 0"):
            validate_usdc(0.0, "field")
        with pytest.raises(EnvValidationError, match="must be > 0"):
            validate_usdc(-1.0, "field")

    def test_validate_usdc_allow_zero(self):
        assert validate_usdc(0.0, "field", allow_zero=True) == 0.0
        with pytest.raises(EnvValidationError):
            validate_usdc(-1.0, "field", allow_zero=True)

    def test_validate_positive_float_rejects_zero(self):
        with pytest.raises(EnvValidationError, match="must be > 0"):
            validate_positive_float(0.0, "field")

    def test_validate_non_negative_float_accepts_zero(self):
        assert validate_non_negative_float(0.0, "field") == 0.0

    def test_validate_non_negative_float_rejects_negative(self):
        with pytest.raises(EnvValidationError):
            validate_non_negative_float(-0.001, "field")

    def test_validate_ratio_error_includes_field_name(self):
        with pytest.raises(EnvValidationError, match="my_field"):
            validate_ratio(1.5, "my_field")

    def test_validate_price_level_error_includes_field_name(self):
        with pytest.raises(EnvValidationError, match="price_lvl"):
            validate_price_level(2.0, "price_lvl")
