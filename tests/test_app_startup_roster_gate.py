"""Startup must not wait for a roster it is configured never to read.

``LeaderRegistry.refresh`` only consults the discovery roster when
``discovery.scheduler.start_from_roster`` is true. With no manual
``leader.wallets`` and that flag false, no amount of waiting can ever produce
a startup wallet, so gating the wait on ``enabled and has_token`` alone parks
startup for ``startup_wait_timeout_seconds`` (3900s by default) before failing
with the same error it could have reported immediately.
"""

from __future__ import annotations

from types import SimpleNamespace

from config.app_config import DiscoveryConfig

from app import _should_wait_for_discovery_roster


def _config(**scheduler):
    payload = {"api": {"token": "secret"}, "scheduler": scheduler}
    return SimpleNamespace(discovery=DiscoveryConfig.model_validate(payload))


def test_waits_when_the_roster_can_supply_startup_wallets() -> None:
    config = _config(enabled=True, start_from_roster=True)

    assert _should_wait_for_discovery_roster(config) is True


def test_does_not_wait_when_roster_startup_is_disabled() -> None:
    config = _config(enabled=True, start_from_roster=False)

    assert _should_wait_for_discovery_roster(config) is False


def test_does_not_wait_when_discovery_is_disabled() -> None:
    config = _config(enabled=False, start_from_roster=True)

    assert _should_wait_for_discovery_roster(config) is False


def test_does_not_wait_without_a_discovery_token() -> None:
    config = SimpleNamespace(
        discovery=DiscoveryConfig.model_validate(
            {"scheduler": {"enabled": True, "start_from_roster": True}}
        )
    )

    assert _should_wait_for_discovery_roster(config) is False


def test_roster_gate_matches_the_registry_precondition() -> None:
    """The wait gate and the roster read must agree.

    ``LeaderRegistry.refresh`` reads ``leaders_repo.list_active`` only when
    ``start_from_roster`` is set; if these two ever diverge, startup waits for
    wallets that will never arrive.
    """
    for start_from_roster in (True, False):
        config = _config(enabled=True, start_from_roster=start_from_roster)

        assert (
            _should_wait_for_discovery_roster(config)
            is config.discovery.scheduler.start_from_roster
        )
