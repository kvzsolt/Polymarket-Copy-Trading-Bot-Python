from __future__ import annotations

import asyncio
import inspect
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import app
from orchestrators import runtime_discovery
from orchestrators.leader_registry import LeaderCandidate, LeaderRegistryState


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


@pytest.mark.asyncio
async def test_startup_wait_forces_each_auto_trader_discovery_cycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A CLI-forced auto run keeps discovering even when the scheduler is off."""
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
    leader = "0x" + "a" * 40
    candidate = LeaderCandidate(
        leader_id=leader,
        score=0.80,
        recent_trades=20,
        active=False,
    )
    registry = SimpleNamespace(
        refresh=AsyncMock(
            return_value=LeaderRegistryState(
                discovered=[candidate],
                active_monitored=[leader],
            )
        )
    )
    config = SimpleNamespace(
        discovery=SimpleNamespace(
            hysteresis=SimpleNamespace(min_tenure_cycles=2),
            scheduler=SimpleNamespace(startup_wait_timeout_seconds=5),
        )
    )

    active = await runtime_discovery.wait_for_active_leaders(
        registry,
        [],
        1,
        config=config,
        initial_state=LeaderRegistryState(discovered=[candidate]),
        force_discovery=True,
    )

    assert active == [leader]
    registry.refresh.assert_awaited_once_with([], force_discovery=True)


@pytest.mark.asyncio
async def test_auto_trader_completes_remaining_tenure_cycles_without_sleep(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Viable startup candidates qualify in a bounded back-to-back burst."""

    async def _unexpected_sleep(_seconds: float) -> None:
        raise AssertionError("auto-trader startup must not sleep")

    monkeypatch.setattr(runtime_discovery.asyncio, "sleep", _unexpected_sleep)
    leader = "0x" + "b" * 40
    candidate = LeaderCandidate(
        leader_id=leader,
        score=0.80,
        recent_trades=20,
        active=False,
    )
    registry = SimpleNamespace(
        refresh=AsyncMock(
            side_effect=[
                LeaderRegistryState(discovered=[candidate]),
                LeaderRegistryState(
                    discovered=[candidate],
                    active_monitored=[leader],
                ),
            ]
        )
    )
    config = SimpleNamespace(
        discovery=SimpleNamespace(
            hysteresis=SimpleNamespace(min_tenure_cycles=3),
            scheduler=SimpleNamespace(startup_wait_timeout_seconds=3900),
        )
    )

    active = await runtime_discovery.wait_for_active_leaders(
        registry,
        [],
        1800,
        config=config,
        initial_state=LeaderRegistryState(discovered=[candidate]),
        force_discovery=True,
    )

    assert active == [leader]
    assert registry.refresh.await_count == 2
    registry.refresh.assert_awaited_with([], force_discovery=True)


@pytest.mark.asyncio
async def test_auto_trader_returns_immediately_after_empty_initial_refresh(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A provider failure or zero-safe-wallet result must not start a wait loop."""

    async def _unexpected_sleep(_seconds: float) -> None:
        raise AssertionError("auto-trader startup must not sleep")

    monkeypatch.setattr(runtime_discovery.asyncio, "sleep", _unexpected_sleep)
    registry = SimpleNamespace(refresh=AsyncMock())
    config = SimpleNamespace(
        discovery=SimpleNamespace(
            hysteresis=SimpleNamespace(min_tenure_cycles=3),
            scheduler=SimpleNamespace(startup_wait_timeout_seconds=3900),
        )
    )

    active = await runtime_discovery.wait_for_active_leaders(
        registry,
        [],
        1800,
        config=config,
        initial_state=LeaderRegistryState(),
        force_discovery=True,
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


@pytest.mark.asyncio
async def test_background_loop_forces_auto_trader_rediscovery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The one-shot CLI override continues through background refreshes."""
    now = 0.0
    sleeps = 0

    class _Loop:
        @staticmethod
        def time() -> float:
            return now

    async def _sleep(seconds: float) -> None:
        nonlocal now, sleeps
        sleeps += 1
        if sleeps > 1:
            raise asyncio.CancelledError
        now += seconds

    monkeypatch.setattr(runtime_discovery.asyncio, "get_running_loop", _Loop)
    monkeypatch.setattr(runtime_discovery.asyncio, "sleep", _sleep)
    registry = SimpleNamespace(
        config=SimpleNamespace(
            discovery=SimpleNamespace(
                scheduler=SimpleNamespace(rediscover_seconds=1, rescore_seconds=2)
            )
        ),
        refresh=AsyncMock(return_value=LeaderRegistryState()),
        rescore=AsyncMock(return_value=LeaderRegistryState()),
    )

    with pytest.raises(asyncio.CancelledError):
        await runtime_discovery.discovery_refresh_loop(
            registry,
            [],
            force_discovery=True,
        )

    registry.refresh.assert_awaited_once_with([], force_discovery=True)


def _log_extra(info: MagicMock, event: str) -> dict:
    """Return the structured extras of one logged burst event.

    The bot logger tree sets ``propagate = False`` below the root, so ``caplog``
    never sees these records; the logging call itself is captured instead.
    """
    calls = [call for call in info.call_args_list if call.args and call.args[0] == event]
    assert calls, f"{event} was not logged"
    return dict(calls[0].kwargs.get("extra") or {})


def _burst_config(min_tenure_cycles: int) -> SimpleNamespace:
    return SimpleNamespace(
        discovery=SimpleNamespace(
            hysteresis=SimpleNamespace(min_tenure_cycles=min_tenure_cycles),
            scheduler=SimpleNamespace(startup_wait_timeout_seconds=3900),
        )
    )


@pytest.mark.asyncio
async def test_auto_trader_rechecks_persisted_wallets_before_sweeping(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-empty persisted roster is re-checked instead of swept."""

    async def _unexpected_sleep(_seconds: float) -> None:
        raise AssertionError("auto-trader startup must not sleep")

    monkeypatch.setattr(runtime_discovery.asyncio, "sleep", _unexpected_sleep)
    leader = "0x" + "c" * 40
    registry = SimpleNamespace(
        persisted_wallets=AsyncMock(return_value=[leader]),
        refresh=AsyncMock(
            return_value=LeaderRegistryState(
                discovered=[
                    LeaderCandidate(leader_id=leader, score=0.8, recent_trades=20, active=True)
                ],
                active_monitored=[leader],
            )
        ),
    )

    with patch.object(runtime_discovery.logger, "info") as info:
        active = await runtime_discovery.wait_for_active_leaders(
            registry,
            [],
            1800,
            config=_burst_config(3),
            initial_state=None,
            force_discovery=True,
        )

    assert active == [leader]
    registry.refresh.assert_awaited_once_with([], force_discovery=True, target_wallets=[leader])
    assert _log_extra(info, "auto_trader_targeted_recheck")["wallets"] == 1


@pytest.mark.asyncio
async def test_auto_trader_reuses_the_composition_roots_targeted_refresh(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An ``initial_state`` counts as the first targeted cycle; no re-fetch."""

    async def _unexpected_sleep(_seconds: float) -> None:
        raise AssertionError("auto-trader startup must not sleep")

    monkeypatch.setattr(runtime_discovery.asyncio, "sleep", _unexpected_sleep)
    leader = "0x" + "c" * 40
    registry = SimpleNamespace(
        persisted_wallets=AsyncMock(return_value=[leader]),
        refresh=AsyncMock(side_effect=AssertionError("initial_state must be reused")),
    )
    initial_state = LeaderRegistryState(
        discovered=[LeaderCandidate(leader_id=leader, score=0.8, recent_trades=20, active=True)],
        active_monitored=[leader],
    )

    active = await runtime_discovery.wait_for_active_leaders(
        registry,
        [],
        1800,
        config=_burst_config(3),
        initial_state=initial_state,
        force_discovery=True,
    )

    assert active == [leader]
    registry.refresh.assert_not_awaited()


@pytest.mark.asyncio
async def test_startup_refresh_targets_reads_the_persisted_roster_only_for_auto_trader() -> None:
    """The composition root targets its first refresh exactly like the burst."""
    leader = "0x" + "d" * 40
    registry = SimpleNamespace(persisted_wallets=AsyncMock(return_value=[leader]))

    assert await runtime_discovery.startup_refresh_targets(registry, force_discovery=True) == [
        leader
    ]
    assert await runtime_discovery.startup_refresh_targets(registry, force_discovery=False) is None
    registry.persisted_wallets.assert_awaited_once()


@pytest.mark.asyncio
async def test_startup_refresh_targets_degrades_to_a_sweep_when_the_read_fails() -> None:
    """A failing roster read must not block startup; the sweep still runs."""
    registry = SimpleNamespace(persisted_wallets=AsyncMock(side_effect=RuntimeError("db down")))

    assert await runtime_discovery.startup_refresh_targets(registry, force_discovery=True) is None


@pytest.mark.asyncio
async def test_auto_trader_sweeps_when_no_candidate_is_persisted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An empty database keeps the historical full-sweep startup burst."""

    async def _unexpected_sleep(_seconds: float) -> None:
        raise AssertionError("auto-trader startup must not sleep")

    monkeypatch.setattr(runtime_discovery.asyncio, "sleep", _unexpected_sleep)
    leader = "0x" + "d" * 40
    candidate = LeaderCandidate(leader_id=leader, score=0.8, recent_trades=20, active=False)
    registry = SimpleNamespace(
        persisted_wallets=AsyncMock(return_value=[]),
        refresh=AsyncMock(
            return_value=LeaderRegistryState(
                discovered=[candidate],
                active_monitored=[leader],
            )
        ),
    )

    with patch.object(runtime_discovery.logger, "info") as info:
        active = await runtime_discovery.wait_for_active_leaders(
            registry,
            [],
            1800,
            config=_burst_config(2),
            initial_state=None,
            force_discovery=True,
        )

    assert active == [leader]
    registry.refresh.assert_awaited_once_with([], force_discovery=True)
    fallback = _log_extra(info, "auto_trader_startup_sweep_fallback")
    assert fallback["reason"] == "no_persisted_candidates"


@pytest.mark.asyncio
async def test_auto_trader_falls_back_to_a_sweep_when_the_recheck_is_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An empty targeted re-check sweeps inside the same bounded burst."""

    async def _unexpected_sleep(_seconds: float) -> None:
        raise AssertionError("auto-trader startup must not sleep")

    monkeypatch.setattr(runtime_discovery.asyncio, "sleep", _unexpected_sleep)
    stale = "0x" + "e" * 40
    leader = "0x" + "f" * 40
    candidate = LeaderCandidate(leader_id=leader, score=0.8, recent_trades=20, active=False)
    registry = SimpleNamespace(
        persisted_wallets=AsyncMock(return_value=[stale]),
        refresh=AsyncMock(
            side_effect=[
                LeaderRegistryState(),
                LeaderRegistryState(discovered=[candidate]),
                LeaderRegistryState(discovered=[candidate], active_monitored=[leader]),
            ]
        ),
    )

    with patch.object(runtime_discovery.logger, "info") as info:
        active = await runtime_discovery.wait_for_active_leaders(
            registry,
            [],
            1800,
            config=_burst_config(3),
            initial_state=None,
            force_discovery=True,
        )

    assert active == [leader]
    assert registry.refresh.await_count == 3
    assert registry.refresh.await_args_list[0].kwargs == {
        "force_discovery": True,
        "target_wallets": [stale],
    }
    assert registry.refresh.await_args_list[1].kwargs == {"force_discovery": True}
    assert registry.refresh.await_args_list[2].kwargs == {"force_discovery": True}
    fallback = _log_extra(info, "auto_trader_startup_sweep_fallback")
    assert fallback["reason"] == "targeted_recheck_empty"


@pytest.mark.asyncio
async def test_auto_trader_recheck_fallback_still_respects_required_cycles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The targeted re-check and its fallback sweep both consume the bound."""

    async def _unexpected_sleep(_seconds: float) -> None:
        raise AssertionError("auto-trader startup must not sleep")

    monkeypatch.setattr(runtime_discovery.asyncio, "sleep", _unexpected_sleep)
    stale = "0x" + "e" * 40
    leader = "0x" + "f" * 40
    candidate = LeaderCandidate(leader_id=leader, score=0.8, recent_trades=20, active=False)
    registry = SimpleNamespace(
        persisted_wallets=AsyncMock(return_value=[stale]),
        refresh=AsyncMock(return_value=LeaderRegistryState(discovered=[candidate])),
    )

    active = await runtime_discovery.wait_for_active_leaders(
        registry,
        [],
        1800,
        config=_burst_config(3),
        initial_state=None,
        force_discovery=True,
    )

    assert active == []
    assert registry.refresh.await_count == 3


@pytest.mark.asyncio
async def test_auto_trader_keeps_rechecking_targets_across_tenure_cycles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Tenure cycles continue in the mode that produced the candidates."""

    async def _unexpected_sleep(_seconds: float) -> None:
        raise AssertionError("auto-trader startup must not sleep")

    monkeypatch.setattr(runtime_discovery.asyncio, "sleep", _unexpected_sleep)
    leader = "0x" + "c" * 40
    candidate = LeaderCandidate(leader_id=leader, score=0.8, recent_trades=20, active=False)
    registry = SimpleNamespace(
        persisted_wallets=AsyncMock(return_value=[leader]),
        refresh=AsyncMock(
            side_effect=[
                LeaderRegistryState(discovered=[candidate]),
                LeaderRegistryState(discovered=[candidate], active_monitored=[leader]),
            ]
        ),
    )

    active = await runtime_discovery.wait_for_active_leaders(
        registry,
        [],
        1800,
        config=_burst_config(3),
        initial_state=None,
        force_discovery=True,
    )

    assert active == [leader]
    assert registry.refresh.await_count == 2
    for call in registry.refresh.await_args_list:
        assert call.kwargs == {"force_discovery": True, "target_wallets": [leader]}
