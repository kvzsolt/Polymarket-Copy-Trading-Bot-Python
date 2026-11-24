"""Contracts for the targeted (DB-first) discovery refresh.

``LeaderRegistry.refresh`` normally runs a full provider sweep. The
``--auto-trader`` startup burst re-checks the wallets already accepted by the
last persisted cycle instead, so the registry must forward that wallet list to
the duck-typed discovery service without changing the sweep call shape.
"""

from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from pmcopy.orchestrators.discovery.leader_registry import LeaderRegistry

WALLET_A = "0x" + "a" * 40
WALLET_B = "0x" + "b" * 40


def _discovery() -> SimpleNamespace:
    api = SimpleNamespace(has_token=True)
    scheduler = SimpleNamespace(enabled=True, max_active_leaders=3, start_from_roster=False)
    hysteresis = SimpleNamespace(
        promote_score=0.55,
        demote_score=0.50,
        min_tenure_cycles=3,
        min_dwell_seconds=10800,
    )
    return SimpleNamespace(api=api, scheduler=scheduler, hysteresis=hysteresis)


def _registry(service: MagicMock) -> LeaderRegistry:
    leaders_repo = MagicMock()
    leaders_repo.list_active = AsyncMock(return_value=[])
    leaders_repo.list_roster = AsyncMock(return_value=[])
    leaders_repo.save_roster = AsyncMock()
    return LeaderRegistry(
        discovery_service=service,
        leaders_repo=leaders_repo,
        config=SimpleNamespace(discovery=_discovery()),
        readiness_tracker=MagicMock(),
        logger_instance=logging.getLogger("test.LeaderRegistry"),
    )


def _service() -> MagicMock:
    service = MagicMock()
    service.run = AsyncMock(return_value=[])
    service.last_cycle_degraded = False
    service.config_hash = "hash-1"
    return service


@pytest.mark.asyncio
async def test_targeted_refresh_forwards_the_wallet_list_to_the_service() -> None:
    service = _service()
    registry = _registry(service)

    await registry.refresh([], force_discovery=True, target_wallets=[WALLET_A, WALLET_B])

    service.run.assert_awaited_once_with(wallets=[WALLET_A, WALLET_B])


@pytest.mark.asyncio
async def test_sweep_refresh_keeps_the_no_argument_service_call() -> None:
    service = _service()
    registry = _registry(service)

    await registry.refresh([], force_discovery=True)

    service.run.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_empty_target_wallets_fall_back_to_the_sweep_call_shape() -> None:
    service = _service()
    registry = _registry(service)

    await registry.refresh([], force_discovery=True, target_wallets=[])

    service.run.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_registry_delegates_persisted_wallets_to_the_discovery_service() -> None:
    service = _service()
    service.persisted_wallets = AsyncMock(return_value=[WALLET_A])
    registry = _registry(service)

    assert await registry.persisted_wallets() == [WALLET_A]
    service.persisted_wallets.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_registry_reports_no_persisted_wallets_for_a_legacy_service() -> None:
    """A duck-typed service without the new port is not an error."""
    service = _service()
    del service.persisted_wallets
    registry = _registry(service)

    assert await registry.persisted_wallets() == []
