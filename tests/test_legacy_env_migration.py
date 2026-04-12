"""Category 2: Legacy env migration.

Covers:
- New-name keys take precedence over old-name keys
- Old-name keys get /100 conversion
- DeprecationWarning emitted when using old names
- Identity conversions work correctly
- Missing both old and new -> default value used
- Mix of old and new names -> correct behavior
"""

from __future__ import annotations

import sys
import warnings
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.env import ENV, EnvValidationError
from config.settings import _DEFAULTS, _LEGACY_MAPPINGS, load_settings


def _fresh_env(**overrides: str) -> dict:
    """Build a minimal ENV-like dict with overrides."""
    env = dict(ENV)
    env.update(overrides)
    return env


# ---------------------------------------------------------------------------
# New-name keys take precedence over old-name keys
# ---------------------------------------------------------------------------


class TestNewNamePrecedence:
    """When both old and new names are present, new-name wins."""

    def test_stop_loss_new_over_old(self):
        env = _fresh_env(STOP_LOSS_PCT=25, stop_loss_ratio=0.15)
        settings = load_settings(env)
        assert settings.risk.stop_loss_ratio == pytest.approx(0.15)

    def test_take_profit_new_over_old(self):
        env = _fresh_env(TAKE_PROFIT_PCT=60, take_profit_ratio=0.80)
        settings = load_settings(env)
        assert settings.risk.take_profit_ratio == pytest.approx(0.80)

    def test_trailing_trigger_new_over_old(self):
        env = _fresh_env(TRAILING_ACTIVATION_PCT=30, trailing_trigger_ratio=0.15)
        settings = load_settings(env)
        assert settings.risk.trailing_trigger_ratio == pytest.approx(0.15)

    def test_slippage_new_over_old(self):
        env = _fresh_env(SLIPPAGE_TOLERANCE=5.0, slippage_tolerance_ratio=0.03)
        settings = load_settings(env)
        assert settings.execution.slippage_tolerance_ratio == pytest.approx(0.03)

    def test_max_position_new_over_old(self):
        env = _fresh_env(MAX_POSITION_PCT=0.50, max_position_ratio=0.25)
        settings = load_settings(env)
        assert settings.sizing.max_position_ratio == pytest.approx(0.25)

    def test_txn_fee_bps_new_over_old(self):
        env = _fresh_env(TRANSACTION_FEE_BPS=30, txn_fee_bps=10)
        settings = load_settings(env)
        assert settings.execution.txn_fee_bps == 10

    def test_copy_multiplier_new_over_old(self):
        env = _fresh_env(PERCENTAGE=10, copy_notional_multiplier=0.08)
        settings = load_settings(env)
        assert settings.sizing.copy_notional_multiplier == pytest.approx(0.08)


# ---------------------------------------------------------------------------
# Old-name keys get /100 conversion (DIV100 mappings)
# ---------------------------------------------------------------------------


class TestDiv100Conversion:
    """Old-name percentage values (0-100) are divided by 100."""

    @pytest.mark.parametrize("old_val,expected", [
        (20, 0.20),
        (50, 0.50),
        (1, 0.01),
        (5, 0.05),
    ])
    def test_stop_loss_pct_conversion(self, old_val, expected):
        env = _fresh_env(STOP_LOSS_PCT=old_val)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            settings = load_settings(env)
        assert settings.risk.stop_loss_ratio == pytest.approx(expected)

    @pytest.mark.parametrize("old_val,expected", [
        (60, 0.60),
        (70, 0.70),
        (90, 0.90),
    ])
    def test_take_profit_pct_conversion(self, old_val, expected):
        env = _fresh_env(TAKE_PROFIT_PCT=old_val)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            settings = load_settings(env)
        assert settings.risk.take_profit_ratio == pytest.approx(expected)

    @pytest.mark.parametrize("old_val,expected", [
        (20, 0.20),
        (15, 0.15),
        (50, 0.50),
    ])
    def test_daily_drawdown_pct_conversion(self, old_val, expected):
        env = _fresh_env(MAX_DAILY_DRAWDOWN_PCT=old_val)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            settings = load_settings(env)
        assert settings.risk.daily_drawdown_ratio == pytest.approx(expected)

    @pytest.mark.parametrize("old_val,expected", [
        (70, 0.70),
        (30, 0.30),
    ])
    def test_profit_lock_trigger_pct_conversion(self, old_val, expected):
        env = _fresh_env(PROFIT_LOCK_TRIGGER_PCT=old_val)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            settings = load_settings(env)
        assert settings.risk.profit_lock_trigger_ratio == pytest.approx(expected)

    @pytest.mark.parametrize("old_val,expected", [
        (15, 0.15),
        (10, 0.10),
    ])
    def test_profit_lock_drawdown_pct_conversion(self, old_val, expected):
        env = _fresh_env(PROFIT_LOCK_DRAWDOWN_PCT=old_val)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            settings = load_settings(env)
        assert settings.risk.profit_lock_drawdown_ratio == pytest.approx(expected)

    @pytest.mark.parametrize("old_val,expected", [
        (2.0, 0.02),
        (5.0, 0.05),
        (1.0, 0.01),
    ])
    def test_slippage_tolerance_conversion(self, old_val, expected):
        env = _fresh_env(SLIPPAGE_TOLERANCE=old_val)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            settings = load_settings(env)
        assert settings.execution.slippage_tolerance_ratio == pytest.approx(expected)

    @pytest.mark.parametrize("old_val,expected", [
        (6, 0.06),
        (10, 0.10),
        (1, 0.01),
    ])
    def test_percentage_conversion(self, old_val, expected):
        env = _fresh_env(PERCENTAGE=old_val)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            settings = load_settings(env)
        assert settings.sizing.copy_notional_multiplier == pytest.approx(expected)

    @pytest.mark.parametrize("old_val,expected", [
        (15, 0.15),
        (20, 0.20),
    ])
    def test_trailing_activation_pct_conversion(self, old_val, expected):
        env = _fresh_env(TRAILING_ACTIVATION_PCT=old_val)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            settings = load_settings(env)
        assert settings.risk.trailing_trigger_ratio == pytest.approx(expected)


# ---------------------------------------------------------------------------
# DeprecationWarning emitted when using old names
# ---------------------------------------------------------------------------


class TestDeprecationWarning:
    def test_stop_loss_old_name_warns(self):
        env = _fresh_env(STOP_LOSS_PCT=20)
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            load_settings(env)
        dep_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
        assert any("STOP_LOSS_PCT" in str(x.message) for x in dep_warnings)

    def test_take_profit_old_name_warns(self):
        env = _fresh_env(TAKE_PROFIT_PCT=70)
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            load_settings(env)
        dep_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
        assert any("TAKE_PROFIT_PCT" in str(x.message) for x in dep_warnings)

    def test_slippage_old_name_warns(self):
        env = _fresh_env(SLIPPAGE_TOLERANCE=2.0)
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            load_settings(env)
        dep_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
        assert any("SLIPPAGE_TOLERANCE" in str(x.message) for x in dep_warnings)

    def test_new_name_does_not_warn(self):
        env = _fresh_env(stop_loss_ratio=0.20)
        # Clear any old-name keys that might be in the base env
        for old_name, _, _ in _LEGACY_MAPPINGS:
            env.pop(old_name, None)
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            load_settings(env)
        dep_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
        assert len(dep_warnings) == 0


# ---------------------------------------------------------------------------
# Identity conversions (no /100)
# ---------------------------------------------------------------------------


class TestIdentityConversions:
    """Old-name keys that use _IDENTITY conversion (no /100)."""

    def test_trailing_distance_identity(self):
        env = _fresh_env(TRAILING_DISTANCE=0.07)
        settings = load_settings(env)
        assert settings.risk.trailing_distance_ratio == pytest.approx(0.07)

    def test_near_max_price_exit_identity(self):
        env = _fresh_env(NEAR_MAX_PRICE_EXIT=0.97)
        settings = load_settings(env)
        assert settings.risk.near_max_price_level == pytest.approx(0.97)

    def test_near_max_price_warn_identity(self):
        env = _fresh_env(NEAR_MAX_PRICE_WARN=0.90)
        settings = load_settings(env)
        assert settings.risk.near_max_warn_level == pytest.approx(0.90)

    def test_max_position_pct_identity(self):
        env = _fresh_env(MAX_POSITION_PCT=0.35)
        settings = load_settings(env)
        assert settings.sizing.max_position_ratio == pytest.approx(0.35)

    def test_risk_buffer_pct_identity(self):
        env = _fresh_env(RISK_BUFFER_PCT=0.15)
        settings = load_settings(env)
        assert settings.sizing.risk_buffer_ratio == pytest.approx(0.15)

    def test_profit_lock_sell_fraction_identity(self):
        env = _fresh_env(PROFIT_LOCK_SELL_FRACTION=0.60)
        settings = load_settings(env)
        assert settings.risk.profit_lock_sell_ratio == pytest.approx(0.60)

    def test_transaction_fee_bps_identity(self):
        env = _fresh_env(TRANSACTION_FEE_BPS=25)
        settings = load_settings(env)
        assert settings.execution.txn_fee_bps == 25


# ---------------------------------------------------------------------------
# Missing both old and new -> default value used
# ---------------------------------------------------------------------------


class TestMissingBothOldAndNew:
    def test_missing_stop_loss_uses_default(self):
        env = dict(ENV)
        env.pop("stop_loss_ratio", None)
        env.pop("STOP_LOSS_PCT", None)
        settings = load_settings(env)
        assert settings.risk.stop_loss_ratio == pytest.approx(_DEFAULTS["stop_loss_ratio"])

    def test_missing_take_profit_uses_default(self):
        env = dict(ENV)
        env.pop("take_profit_ratio", None)
        env.pop("TAKE_PROFIT_PCT", None)
        settings = load_settings(env)
        assert settings.risk.take_profit_ratio == pytest.approx(_DEFAULTS["take_profit_ratio"])

    def test_missing_trailing_uses_default(self):
        env = dict(ENV)
        env.pop("trailing_trigger_ratio", None)
        env.pop("TRAILING_ACTIVATION_PCT", None)
        env.pop("trailing_distance_ratio", None)
        env.pop("TRAILING_DISTANCE", None)
        settings = load_settings(env)
        assert settings.risk.trailing_trigger_ratio == pytest.approx(_DEFAULTS["trailing_trigger_ratio"])
        assert settings.risk.trailing_distance_ratio == pytest.approx(_DEFAULTS["trailing_distance_ratio"])

    def test_missing_near_max_uses_defaults(self):
        env = dict(ENV)
        env.pop("near_max_price_level", None)
        env.pop("NEAR_MAX_PRICE_EXIT", None)
        env.pop("near_max_warn_level", None)
        env.pop("NEAR_MAX_PRICE_WARN", None)
        settings = load_settings(env)
        assert settings.risk.near_max_price_level == pytest.approx(_DEFAULTS["near_max_price_level"])
        assert settings.risk.near_max_warn_level == pytest.approx(_DEFAULTS["near_max_warn_level"])


# ---------------------------------------------------------------------------
# Mix of old and new names -> correct behavior
# ---------------------------------------------------------------------------


class TestMixedOldAndNew:
    def test_old_stop_loss_new_take_profit(self):
        env = _fresh_env(
            STOP_LOSS_PCT=30,
            take_profit_ratio=0.90,
        )
        settings = load_settings(env)
        assert settings.risk.stop_loss_ratio == pytest.approx(0.30)  # /100
        assert settings.risk.take_profit_ratio == pytest.approx(0.90)  # direct

    def test_new_stop_loss_old_slippage(self):
        env = _fresh_env(
            stop_loss_ratio=0.15,
            SLIPPAGE_TOLERANCE=3.0,
        )
        settings = load_settings(env)
        assert settings.risk.stop_loss_ratio == pytest.approx(0.15)
        assert settings.execution.slippage_tolerance_ratio == pytest.approx(0.03)

    def test_multiple_old_names_migrated(self):
        """Several old-name keys at once all convert correctly."""
        env = _fresh_env(
            STOP_LOSS_PCT=25,
            TAKE_PROFIT_PCT=80,
            TRAILING_ACTIVATION_PCT=30,
            TRAILING_DISTANCE=0.08,
            NEAR_MAX_PRICE_EXIT=0.97,
            TRANSACTION_FEE_BPS=25,
        )
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            settings = load_settings(env)
        assert settings.risk.stop_loss_ratio == pytest.approx(0.25)
        assert settings.risk.take_profit_ratio == pytest.approx(0.80)
        assert settings.risk.trailing_trigger_ratio == pytest.approx(0.30)
        assert settings.risk.trailing_distance_ratio == pytest.approx(0.08)
        assert settings.risk.near_max_price_level == pytest.approx(0.97)
        assert settings.execution.txn_fee_bps == 25

    def test_partial_mix_only_some_overridden(self):
        """Override a few with old names, leave rest as defaults."""
        env = _fresh_env(STOP_LOSS_PCT=10)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            settings = load_settings(env)
        assert settings.risk.stop_loss_ratio == pytest.approx(0.10)
        # Other fields that don't have old-name keys in the base ENV
        # will fall through to their defaults. The base ENV may provide
        # old-name defaults for some keys (e.g., TAKE_PROFIT_PCT=60 -> 0.60).
        # Just verify the overridden one is correct.
        assert settings.risk.stop_loss_ratio == pytest.approx(0.10)


# ---------------------------------------------------------------------------
# All legacy mappings have a default
# ---------------------------------------------------------------------------


class TestLegacyMappingsCoverage:
    def test_all_legacy_mappings_have_defaults(self):
        """Every field in _LEGACY_MAPPINGS must have a default in _DEFAULTS."""
        for old_name, new_name, _ in _LEGACY_MAPPINGS:
            assert new_name in _DEFAULTS, f"Missing default for {new_name} (mapped from {old_name})"
