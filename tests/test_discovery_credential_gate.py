"""Contracts for the single Falcon discovery credential gate."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from orchestrators.leader_registry import LeaderRegistry


def _discovery(*, api_has_token: bool, enabled: bool = True):
    """Build a minimal discovery configuration for registry tests."""
    api = SimpleNamespace(has_token=api_has_token)
    scheduler = SimpleNamespace(
        enabled=enabled,
        max_active_leaders=2,
        start_from_roster=False,
    )
    return SimpleNamespace(api=api, scheduler=scheduler)


def _registry(discovery: SimpleNamespace) -> LeaderRegistry:
    """Build a registry with deterministic fake collaborators."""
    config = SimpleNamespace(discovery=discovery)
    ds = MagicMock()
    ds.run = AsyncMock(return_value=[])
    ds.last_cycle_degraded = False
    ds.config_hash = "hash-1"
    leaders_repo = MagicMock()
    leaders_repo.list_active = AsyncMock(return_value=[])
    leaders_repo.upsert_active = AsyncMock()
    leaders_repo.deactivate = AsyncMock()
    return LeaderRegistry(
        discovery_service=ds,
        leaders_repo=leaders_repo,
        config=config,
        readiness_tracker=MagicMock(),
    )


@pytest.mark.asyncio
async def test_discovery_fires_when_falcon_api_token_is_set() -> None:
    """The one token used for auth must also activate the registry gate."""
    reg = _registry(_discovery(api_has_token=True))
    await reg.refresh(force_discovery=True)
    reg.discovery_service.run.assert_awaited()


@pytest.mark.asyncio
async def test_discovery_does_not_fire_when_neither_credential_set():
    """A forced cycle without the Falcon token must fail closed."""
    reg = _registry(_discovery(api_has_token=False))
    with pytest.raises(RuntimeError, match="discovery.api.token"):
        await reg.refresh(force_discovery=True)
    reg.discovery_service.run.assert_not_awaited()


@pytest.mark.asyncio
async def test_force_discovery_without_any_credential_raises():
    """Missing auth must remain an explicit operator-facing error."""
    reg = _registry(_discovery(api_has_token=False))
    with pytest.raises(RuntimeError, match="discovery.api.token"):
        await reg.refresh(force_discovery=True)
