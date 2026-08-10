"""Phase 3.13 — collapse the split-brain discovery credential gate.

The audit found ``discovery.api.token`` is only a boolean gate
(``leader_registry.refresh`` checks ``discovery.api.has_token``) while
``discovery.heisenberg.api_key`` is the only value actually sent as the Bearer
header (``bootstrap.py``). Setting only one of them silently does nothing
(zero candidates or never fires). The gate must check the credential that is
actually used: ``discovery.heisenberg.api_key``.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from orchestrators.leader_registry import LeaderRegistry


def _discovery(*, api_has_token: bool, heisenberg_has_key: bool, enabled: bool = True):
    api = SimpleNamespace(has_token=api_has_token)
    heisenberg = SimpleNamespace(
        enabled=enabled,
        api_key=SimpleNamespace(get_secret_value=lambda: "k" if heisenberg_has_key else ""),
    )
    scheduler = SimpleNamespace(
        enabled=enabled,
        max_active_leaders=2,
        start_from_roster=False,
    )
    return SimpleNamespace(api=api, heisenberg=heisenberg, scheduler=scheduler)


def _registry(discovery) -> LeaderRegistry:
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
async def test_discovery_fires_when_heisenberg_key_present_even_if_api_token_empty():
    """The gate must fire on the credential actually used (heisenberg.api_key),
    not the legacy api.token boolean. With only the heisenberg key set,
    discovery must run. Previously this never fired (the gate checked only
    api.token)."""
    reg = _registry(_discovery(api_has_token=False, heisenberg_has_key=True))
    await reg.refresh(force_discovery=True)
    reg.discovery_service.run.assert_awaited()


@pytest.mark.asyncio
async def test_discovery_fires_when_only_legacy_api_token_set():
    """The legacy api.token still triggers the gate (backward compatibility).
    The split-brain fix is that the heisenberg key ALSO triggers it now; the
    config collapse (both keys mirrored in secrets.env) ensures the Bearer is
    populated in production."""
    reg = _registry(_discovery(api_has_token=True, heisenberg_has_key=False))
    await reg.refresh(force_discovery=True)
    reg.discovery_service.run.assert_awaited()


@pytest.mark.asyncio
async def test_discovery_does_not_fire_when_neither_credential_set():
    reg = _registry(_discovery(api_has_token=False, heisenberg_has_key=False))
    with pytest.raises(RuntimeError, match="credential"):
        await reg.refresh(force_discovery=True)
    reg.discovery_service.run.assert_not_awaited()


@pytest.mark.asyncio
async def test_force_discovery_without_any_credential_raises():
    reg = _registry(_discovery(api_has_token=False, heisenberg_has_key=False))
    with pytest.raises(RuntimeError, match="credential"):
        await reg.refresh(force_discovery=True)
