"""Deterministic tests for configured-versus-discovered startup selection."""

from __future__ import annotations

import pytest

from config.app_config import EnvValidationError
from orchestrators.trader_startup_policy import TraderStartupPolicy
from tests.config_helpers import load_test_config


LEADER = "0x" + "d" * 40


def _config(*, token: str = "test-discovery-token", start_from_roster: bool = True):
    """Build a scheduler-disabled config to prove the CLI flag is authoritative."""
    return load_test_config(
        copy_trading={"wallets": [{"wallet": LEADER, "enabled": True}]},
        discovery={
            "api": {"token": token},
            "scheduler": {
                "enabled": False,
                "start_from_roster": start_from_roster,
            },
        },
    )


def test_auto_trader_policy_ignores_manual_wallets_and_forces_discovery() -> None:
    """The command-line opt-in selects only the automatic discovery path."""
    policy = TraderStartupPolicy(auto_trader=True)
    config = _config()

    policy.validate(config)

    assert policy.configured_leaders(config) == []
    assert policy.force_discovery is True
    assert policy.discovery_loop_enabled(config) is True
    assert policy.should_wait_for_discovery_roster(config) is True
    assert policy.startup_roster_event == "auto_trader_startup_burst_started"


def test_auto_trader_enables_falcon_for_only_the_effective_runtime_config() -> None:
    """The CLI override composes Falcon without mutating loaded configuration."""
    policy = TraderStartupPolicy(auto_trader=True)
    config = _config()

    effective = policy.effective_config(config)

    assert config.discovery.falcon.enabled is False
    assert effective.discovery.falcon.enabled is True


def test_discovery_uses_higher_bounded_candidate_concurrency() -> None:
    """The shipped discovery policy evaluates more wallets concurrently."""
    assert _config().discovery.falcon.candidate_concurrency == 12


def test_default_policy_keeps_enabled_manual_wallets() -> None:
    """Normal startup preserves the configured-roster behavior."""
    policy = TraderStartupPolicy()
    config = _config()

    assert policy.configured_leaders(config) == [LEADER]
    assert policy.force_discovery is False
    assert policy.discovery_loop_enabled(config) is False
    assert policy.startup_roster_event == "startup_waiting_for_discovery_roster"


def test_auto_trader_requires_discovery_credentials() -> None:
    """Forced discovery fails closed before runtime construction without auth."""
    policy = TraderStartupPolicy(auto_trader=True)

    with pytest.raises(EnvValidationError, match="discovery.api.token"):
        policy.validate(_config(token=""))


def test_auto_trader_requires_roster_startup() -> None:
    """Discovery cannot select a trading leader when roster reads are disabled."""
    policy = TraderStartupPolicy(auto_trader=True)

    with pytest.raises(EnvValidationError, match="start_from_roster"):
        policy.validate(_config(start_from_roster=False))
