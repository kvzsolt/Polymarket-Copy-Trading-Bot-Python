"""Tests binding packaged discovery configuration to the Falcon policy dataclass."""

from __future__ import annotations

from dataclasses import fields
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
import yaml
from pydantic import ValidationError

from config.app_config import DiscoverySchedulerConfig, FalconDiscoveryConfig, HysteresisConfig
from services.discovery.falcon_candidate_service import FalconDiscoveryPolicy
from services.discovery.policy_config import discovery_policy_from_config

DISCOVERY_YML = Path(__file__).resolve().parents[1] / "config" / "discovery.yml"

# Packaged values that deliberately diverge from the dataclass default. The
# shipped runtime evaluates more wallets concurrently than the library default.
DELIBERATE_OVERRIDES: dict[str, Any] = {"candidate_concurrency": 12}

# Packaged YAML values that additionally diverge from the pydantic default: one
# full leaderboard sweep is capped at 50 wallets because the `--auto-trader`
# startup burst re-checks the persisted roster before it ever sweeps, and the
# specialist bonus weight is 0.15 by intended operator policy (decided
# 2026-10-11); the FalconDiscoveryPolicy dataclass default stays 0.10.
PACKAGED_OVERRIDES: dict[str, Any] = {
    **DELIBERATE_OVERRIDES,
    "candidate_limit": 50,
    "enable_price_jump_risk": False,
    "specialist_bonus_weight": Decimal("0.15"),
}


def _packaged_falcon_section() -> dict[str, Any]:
    document = yaml.safe_load(DISCOVERY_YML.read_text(encoding="utf-8"))
    return dict(document["falcon"])


def test_falcon_config_accepts_new_policy_keys_and_lists() -> None:
    config = FalconDiscoveryConfig.model_validate(
        {
            "toxic_market_flags": ["whale_control", "squeeze"],
            "excluded_categories": ["Crypto", "Politics"],
            "activity_window_hours": 12,
            "min_activity_window_trades": 2,
            "drawdown_gate_enabled": True,
            "reject_perfect_timing_flag": False,
            "reject_suspicious_win_rate_flag": False,
            "max_similar_wallets": 4,
            "max_trades_per_day": "250",
            "min_avg_trade_size_usdc": "7.5",
            "max_excluded_category_trade_share": "0.4",
            "specialist_bonus_enabled": False,
            "specialist_min_dominant_share": "0.75",
            "specialist_min_win_rate": "0.6",
            "specialist_bonus_weight": "0.05",
            "enable_price_jump_risk": False,
        }
    )

    assert config.toxic_market_flags == ("whale_control", "squeeze")
    assert config.excluded_categories == ("Crypto", "Politics")
    assert config.specialist_min_dominant_share == Decimal("0.75")
    assert config.max_trades_per_day == Decimal("250")
    assert config.drawdown_gate_enabled is True


def test_falcon_config_defaults_match_policy_defaults() -> None:
    config = FalconDiscoveryConfig.model_validate({})
    policy = FalconDiscoveryPolicy()

    for item in fields(policy):
        if item.name == "bankroll":
            continue
        expected = DELIBERATE_OVERRIDES.get(item.name, getattr(policy, item.name))
        assert str(getattr(config, item.name)).casefold() == str(expected).casefold(), item.name


def test_falcon_config_rejects_removed_min_today_trades() -> None:
    with pytest.raises(ValidationError, match="min_today_trades"):
        FalconDiscoveryConfig.model_validate({"min_today_trades": 1})


def test_falcon_bankroll_config_defaults_are_loosened() -> None:
    bankroll = FalconDiscoveryConfig.model_validate({}).bankroll

    assert bankroll.min_copyable_trade_share == Decimal("0.35")
    assert bankroll.min_fit_score == Decimal("0.45")


def test_hysteresis_and_scheduler_defaults_allow_three_leaders() -> None:
    assert HysteresisConfig.model_validate({}).promote_score == 0.55
    assert DiscoverySchedulerConfig.model_validate({}).max_active_leaders == 3


@pytest.mark.parametrize(
    ("flags", "categories"),
    [
        (["whale_control", "squeeze"], ["Crypto", "Politics"]),
        ("whale_control, squeeze", "Crypto,Politics"),
    ],
)
def test_discovery_policy_from_config_coerces_string_tuples(
    flags: Any,
    categories: Any,
) -> None:
    policy = discovery_policy_from_config(
        {"toxic_market_flags": flags, "excluded_categories": categories}
    )

    assert policy.toxic_market_flags == ("whale_control", "squeeze")
    assert policy.excluded_categories == ("crypto", "politics")


def test_discovery_policy_from_config_keeps_integer_tuples() -> None:
    policy = discovery_policy_from_config({"wallet360_windows": ["1", "7", "15"]})

    assert policy.wallet360_windows == (1, 7, 15)


def test_packaged_discovery_yml_matches_policy_defaults() -> None:
    section = _packaged_falcon_section()
    config = FalconDiscoveryConfig.model_validate(section)
    policy = discovery_policy_from_config({"falcon": config})
    defaults = FalconDiscoveryPolicy()

    assert "min_today_trades" not in section
    for item in fields(defaults):
        if item.name == "bankroll":
            continue
        assert item.name in section, f"{item.name} missing from config/discovery.yml"
        expected = PACKAGED_OVERRIDES.get(item.name, getattr(defaults, item.name))
        assert getattr(policy, item.name) == expected, item.name
    assert policy.bankroll == defaults.bankroll


def test_packaged_discovery_yml_promotes_at_the_loosened_score() -> None:
    document = yaml.safe_load(DISCOVERY_YML.read_text(encoding="utf-8"))

    assert document["hysteresis"]["promote_score"] == 0.55
    assert document["scheduler"]["max_active_leaders"] == 3
    assert document["falcon"]["candidate_concurrency"] == 12
