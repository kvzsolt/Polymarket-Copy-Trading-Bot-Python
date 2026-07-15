from __future__ import annotations

import inspect
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import app
from orchestrators import runtime_discovery
from orchestrators.leader_registry import LeaderRegistryState


@pytest.mark.asyncio
async def test_startup_wait_is_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    now = 0.0

    class _Loop:
        @staticmethod
        def time() -> float:
            return now

    async def _sleep(seconds: float) -> None:
        nonlocal now
        now += seconds

    monkeypatch.setattr(runtime_discovery.asyncio, "get_running_loop", _Loop)
    monkeypatch.setattr(runtime_discovery.asyncio, "sleep", _sleep)
    registry = SimpleNamespace(refresh=AsyncMock())
    config = SimpleNamespace(
        discovery=SimpleNamespace(
            scheduler=SimpleNamespace(startup_wait_timeout_seconds=5)
        )
    )

    active = await runtime_discovery.wait_for_active_leaders(
        registry,
        [],
        1800,
        config=config,
        initial_state=LeaderRegistryState(),
    )

    assert active == []
    registry.refresh.assert_not_awaited()


def test_health_server_starts_before_discovery_promotion_wait() -> None:
    source = inspect.getsource(app._run_application)
    assert source.index("await start_health_server") < source.index(
        "active_leaders = await _wait_for_active_leaders"
    )


def test_discovery_loop_has_no_discarded_repository_argument() -> None:
    assert "leaders_repo" not in inspect.signature(
        runtime_discovery.discovery_refresh_loop
    ).parameters
    assert "leaders_repo" not in inspect.signature(app._discovery_refresh_loop).parameters
