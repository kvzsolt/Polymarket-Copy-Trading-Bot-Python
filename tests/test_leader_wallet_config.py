"""Focused tests for toggleable manual leader-wallet configuration."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from config.app_config import EnvValidationError
from orchestrators.runtime_bootstrap import configured_leaders
from tests.config_helpers import load_test_config


LEADER_A = "0x" + "a" * 40
LEADER_B = "0x" + "b" * 40


def test_disabled_manual_leader_is_retained_in_config_but_not_started() -> None:
    """An operator can disable one entry without deleting its wallet address."""
    config = load_test_config(
        copy_trading={
            "wallets": [
                {"wallet": LEADER_A, "enabled": True},
                {"wallet": LEADER_B, "enabled": False},
            ]
        }
    )

    assert [(entry.wallet, entry.enabled) for entry in config.copy_trading.wallets] == [
        (LEADER_A, True),
        (LEADER_B, False),
    ]
    assert config.copy_trading.enabled_wallets() == (LEADER_A,)
    assert configured_leaders(config) == [LEADER_A]


def test_legacy_string_wallet_entry_remains_enabled_by_default() -> None:
    """Existing environment and legacy YAML string lists remain readable."""
    config = load_test_config(copy_trading={"wallets": [LEADER_A]})

    assert config.copy_trading.wallets[0].wallet == LEADER_A
    assert config.copy_trading.wallets[0].enabled is True
    assert configured_leaders(config) == [LEADER_A]


def test_disabled_wallet_address_is_still_validated() -> None:
    """Disabling an entry must not let malformed public identity data load."""
    with pytest.raises(EnvValidationError, match="Invalid leader wallet"):
        load_test_config(copy_trading={"wallets": [{"wallet": "not-an-address", "enabled": False}]})


def test_live_roster_uses_explicit_enabled_entry_style() -> None:
    """The checked-in live roster exposes one obvious toggle per wallet."""
    root = Path(__file__).resolve().parents[1]
    payload = yaml.safe_load(
        (root / "config/environments/live/copy_trading.yml").read_text(encoding="utf-8")
    )

    assert payload["wallets"]
    assert all(set(entry) == {"wallet", "enabled"} for entry in payload["wallets"])
    assert all(entry["enabled"] is True for entry in payload["wallets"])


def test_per_leader_tuning_files_do_not_expose_a_second_enabled_switch() -> None:
    """Roster activation has one unambiguous owner in copy-trading config."""
    root = Path(__file__).resolve().parents[1]

    for path in sorted((root / "config/leaders").glob("*.yml")):
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        assert "enabled" not in payload, path
