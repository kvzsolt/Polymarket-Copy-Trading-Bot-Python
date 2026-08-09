"""Contracts for the V5 config loader and packaged YAML defaults."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from services.copy_sizing_v5.config_loader import load_copy_sizing_v5_mapping
from services.copy_sizing_v5.runtime import build_copy_sizing_v5_runtime


def test_packaged_v5_yaml_has_requested_defaults() -> None:
    path = Path(__file__).resolve().parents[2] / "config" / "copy_sizing_v5.yaml"
    data = load_copy_sizing_v5_mapping(path)
    assert data["mode"] == "proportional"
    assert data["percentage"] == 4.0
    assert data["copyable_balance_percent"] == 75.0
    assert data["buy_min_amount"] == 5.0
    assert data["buy_max_amount"] == 8.0


def test_v5_yaml_overrides_legacy_branch_sizing_without_unknown_appconfig_fields(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "copy_sizing_v5.yaml"
    config_path.write_text(
        "sizing_v5:\n  copyable_balance_percent: 60\n  portfolio_weight_multiplier: 0.5\n",
        encoding="utf-8",
    )
    legacy = SimpleNamespace(
        sizing=SimpleNamespace(
            mode="proportional",
            percentage=4.0,
            fixed_amount=3.0,
            trade_multiplier=1.0,
            tiered_trade_multipliers="",
            adaptive_basis="trade_size",
            adaptive_min_percent=0.0,
            adaptive_max_percent=0.0,
            adaptive_threshold_usdc=500.0,
            buy_min_amount=5.0,
            buy_max_amount=8.0,
            position_threshold=0,
            max_position_pct=0.10,
            enable_trust_size_multipliers=False,
            trust_size_multiplier_min=0.5,
            trust_size_multiplier_max=1.25,
        )
    )
    runtime = build_copy_sizing_v5_runtime(legacy, config_path=str(config_path))
    assert runtime.policy.copyable_balance_percent == Decimal("60")
    assert runtime.policy.portfolio_weight_multiplier == Decimal("0.5")