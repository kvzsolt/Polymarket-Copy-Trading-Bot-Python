"""Tests for TradingContext.__post_init__ dependency validation.

Verifies the fail-fast contract: production-injected contexts (those with
a config object) MUST supply all required runtime dependencies.  Test-only
contexts (no config) are allowed to omit them for convenience.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from pmcopy.application.trading_context import TradingContext


def _make_mock_config() -> MagicMock:
    """Create a minimal mock AppConfig that satisfies __post_init__ checks."""
    config = MagicMock()
    config.sizing.mode = "proportional"
    config.sizing.fixed_amount = 10.0
    config.sizing.trade_multiplier = 1.0
    config.sizing.tiered_trade_multipliers = None
    config.sizing.adaptive_basis = "trade_size"
    config.sizing.adaptive_min_percent = 10.0
    config.sizing.adaptive_max_percent = 90.0
    config.sizing.adaptive_threshold_usdc = 50.0
    return config


def _base_kwargs() -> dict:
    """Return the minimum required positional fields for TradingContext."""
    return {
        "user_id": "test-user",
        "wallet_address": "0xdead",
        "client": MagicMock(),
        "positions_repo": MagicMock(),
        "fills_repo": MagicMock(),
        "orders_repo": MagicMock(),
        "risk_blocks_repo": None,
        "pnl_tracker": None,
        "risk_engine": None,
        "accounting_service": None,
        "position_manager": None,
    }


class TestPostInitFailFast:
    """Production contexts must supply all runtime dependencies."""

    def test_production_context_missing_all_deps_raises(self) -> None:
        """When config is injected, omitting all runtime deps raises TypeError."""
        kwargs = _base_kwargs()
        kwargs["config"] = _make_mock_config()
        with pytest.raises(TypeError, match="missing required dependencies"):
            TradingContext(**kwargs)

    def test_production_context_missing_one_dep_raises(self) -> None:
        """Missing even one runtime dep with config present raises TypeError."""
        kwargs = _base_kwargs()
        kwargs["config"] = _make_mock_config()
        kwargs["risk_blocks_repo"] = MagicMock()
        kwargs["pnl_tracker"] = MagicMock()
        kwargs["risk_engine"] = MagicMock()
        kwargs["accounting_service"] = MagicMock()
        # position_manager intentionally omitted
        with pytest.raises(TypeError, match="position_manager"):
            TradingContext(**kwargs)

    def test_production_context_all_deps_succeeds(self) -> None:
        """When all runtime deps are supplied with config, construction succeeds."""
        kwargs = _base_kwargs()
        kwargs["config"] = _make_mock_config()
        kwargs["risk_blocks_repo"] = MagicMock()
        kwargs["pnl_tracker"] = MagicMock()
        kwargs["risk_engine"] = MagicMock()
        kwargs["accounting_service"] = MagicMock()
        kwargs["position_manager"] = MagicMock()
        ctx = TradingContext(**kwargs)
        assert ctx.user_id == "test-user"
        assert ctx.pnl_tracker is not None

    def test_test_context_no_config_omit_deps_ok(self) -> None:
        """Without config (test-only context), missing deps are allowed."""
        kwargs = _base_kwargs()
        # No config, no runtime deps — should NOT raise
        ctx = TradingContext(**kwargs)
        assert ctx.pnl_tracker is None
        assert ctx.risk_blocks_repo is None

    def test_error_message_lists_all_missing_deps(self) -> None:
        """The TypeError message enumerates every missing dependency."""
        kwargs = _base_kwargs()
        kwargs["config"] = _make_mock_config()
        kwargs["pnl_tracker"] = MagicMock()  # only this one supplied
        with pytest.raises(TypeError) as exc_info:
            TradingContext(**kwargs)
        msg = str(exc_info.value)
        for dep in ("risk_blocks_repo", "risk_engine", "accounting_service", "position_manager"):
            assert dep in msg, f"Expected '{dep}' in error message"
        assert "pnl_tracker" not in msg, "pnl_tracker was supplied, should not be listed"
