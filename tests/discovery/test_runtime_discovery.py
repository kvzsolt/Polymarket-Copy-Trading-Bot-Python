from __future__ import annotations

import asyncio
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
        discovery=SimpleNamespace(scheduler=SimpleNamespace(startup_wait_timeout_seconds=5))
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
    assert (
        "leaders_repo" not in inspect.signature(runtime_discovery.discovery_refresh_loop).parameters
    )
    assert "leaders_repo" not in inspect.signature(app._discovery_refresh_loop).parameters


@pytest.mark.asyncio
async def test_discovery_restart_reuses_shared_monitor_state_and_resets_rescore(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = 0.0
    sleep_delays: list[float] = []

    class _Loop:
        @staticmethod
        def time() -> float:
            return now

    async def _sleep(seconds: float) -> None:
        nonlocal now
        sleep_delays.append(seconds)
        if len(sleep_delays) > 1:
            raise asyncio.CancelledError
        now += seconds

    monkeypatch.setattr(runtime_discovery.asyncio, "get_running_loop", _Loop)
    monkeypatch.setattr(runtime_discovery.asyncio, "sleep", _sleep)
    state = LeaderRegistryState(active_monitored=["0xDEF"])
    registry = SimpleNamespace(
        config=SimpleNamespace(
            discovery=SimpleNamespace(
                scheduler=SimpleNamespace(
                    rediscover_seconds=1,
                    rescore_seconds=1,
                )
            )
        ),
        refresh=AsyncMock(return_value=state),
        rescore=AsyncMock(return_value=state),
    )
    callback = AsyncMock(return_value=["0xDEF"])
    shared_monitored = {"0xABC"}

    with pytest.raises(asyncio.CancelledError):
        await runtime_discovery.discovery_refresh_loop(
            registry,
            [],
            on_new_leaders=callback,
            already_monitored=shared_monitored,
        )

    assert shared_monitored == {"0xabc", "0xdef"}
    registry.refresh.assert_awaited_once()
    registry.rescore.assert_not_awaited()
    callback.assert_awaited_once_with(["0xDEF"])
    assert sleep_delays == pytest.approx([1.0, 1.0])
