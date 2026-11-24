"""Focused tests for toggleable manual leader-wallet configuration."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from pmcopy.config.app_config import EnvValidationError
from pmcopy.orchestrators.bootstrap.runtime_bootstrap import configured_leaders
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
    """Each checked-in trader file exposes one obvious toggle per wallet."""
    root = Path(__file__).resolve().parents[1]

    trader_files = sorted((root / "src" / "pmcopy" / "config/traders").glob("*.yml"))
    assert trader_files

    seen_wallets = set()
    for path in trader_files:
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
        assert set(payload) >= {"wallet", "enabled", "task"}, path
        assert isinstance(payload["enabled"], bool), path
        assert payload["wallet"] not in seen_wallets, path
        seen_wallets.add(payload["wallet"])

    # The environment file must not define a second roster.
    live_payload = yaml.safe_load(
        (root / "src" / "pmcopy" / "config/environments/live/copy_trading.yml").read_text(encoding="utf-8")
    )
    assert "wallets" not in live_payload


def test_roster_activation_has_one_unambiguous_owner() -> None:
    """Only config/traders files decide which wallets are followed."""
    root = Path(__file__).resolve().parents[1]

    assert not (root / "src" / "pmcopy" / "config" / "leaders").exists()
    live_payload = yaml.safe_load(
        (root / "src" / "pmcopy" / "config/environments/live/copy_trading.yml").read_text(encoding="utf-8")
    )
    assert "wallets" not in live_payload
    base_payload = yaml.safe_load((root / "src" / "pmcopy" / "config/copy_trading.yml").read_text(encoding="utf-8"))
    assert "wallets" not in base_payload
