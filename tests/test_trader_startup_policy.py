"""Deterministic tests for configured-versus-discovered startup selection."""

from __future__ import annotations

import pytest

from pmcopy.config.app_config import EnvValidationError
from pmcopy.orchestrators.trader_startup_policy import TraderStartupPolicy
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


def test_auto_trader_policy_uses_persistent_manager_without_forced_discovery() -> None:
    """Automatic mode selects the durable roster manager, not the legacy sweep."""
    policy = TraderStartupPolicy(auto_trader=True)
    config = _config()

    policy.validate(config)

    assert policy.configured_leaders(config) == []
    assert policy.force_discovery is False
    assert policy.discovery_loop_enabled(config) is False
    assert policy.should_wait_for_discovery_roster(config) is False
    assert policy.startup_roster_event == "auto_trader_roster_restored"


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


def test_restoring_auto_trader_roster_does_not_require_provider_credentials() -> None:
    """Provider availability is checked only when an evaluation is requested."""
    policy = TraderStartupPolicy(auto_trader=True)

    policy.validate(_config(token=""))


def test_auto_trader_requires_roster_startup() -> None:
    """Discovery cannot select a trading leader when roster reads are disabled."""
    policy = TraderStartupPolicy(auto_trader=True)

    with pytest.raises(EnvValidationError, match="start_from_roster"):
        policy.validate(_config(start_from_roster=False))
