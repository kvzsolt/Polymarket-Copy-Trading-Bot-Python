"""Liveness heartbeat: fixed-cadence proof that the supervisor is still alive."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import pytest

from orchestrators import liveness_heartbeat
from orchestrators.liveness_heartbeat import (
    MINIMUM_HEARTBEAT_INTERVAL_SECONDS,
    bot_heartbeat_loop,
    heartbeat_extras,
    render_heartbeat,
)


def _healthy_state() -> dict[str, Any]:
    """Return a fully green snapshot for one running follower."""

    return {
        "detection_connected": True,
        "active_leaders": 4,
        "monitored_leaders": 4,
        "ready": True,
        "wss_user_ok": True,
        "wss_market_ok": True,
        "reconciler_ok": True,
        "db_ok": True,
        "api_ok": True,
    }


def test_render_heartbeat_reports_a_healthy_runtime() -> None:
    """A live, connected runtime renders one unambiguous alive line."""

    line = render_heartbeat(_healthy_state())

    assert "bot_heartbeat" in line
    assert "alive" in line
    assert "detection=connected" in line
    assert "leaders=4/4" in line
    assert "ready=yes" in line
    assert "wss=user:ok,market:ok" in line
    assert "recon=ok" in line
    assert "db=ok" in line
    assert "api=ok" in line
    assert "unknown" not in line
    assert "FAIL" not in line


def test_render_heartbeat_never_invents_a_green_light() -> None:
    """Absent facts read as unknown and false flags read as failures."""

    line = render_heartbeat(
        {
            "detection_connected": False,
            "active_leaders": 0,
            "monitored_leaders": 4,
            "ready": False,
            "wss_user_ok": False,
            "wss_market_ok": None,
            "reconciler_ok": False,
            "db_ok": None,
            "api_ok": False,
        }
    )

    assert "detection=DISCONNECTED" in line
    assert "leaders=0/4" in line
    assert "ready=NO" in line
    assert "wss=user:FAIL,market:unknown" in line
    assert "recon=FAIL" in line
    assert "db=unknown" in line
    assert "api=FAIL" in line


def test_render_heartbeat_reports_a_failed_snapshot_as_alive() -> None:
    """A broken snapshot still proves the process is scheduling work."""

    line = render_heartbeat({"snapshot_failed": "RuntimeError"})

    assert "alive" in line
    assert "snapshot_failed=RuntimeError" in line


def test_render_heartbeat_reports_unknown_for_an_absent_snapshot() -> None:
    """An empty snapshot must not be rendered as a healthy one."""

    line = render_heartbeat({})

    assert "detection=unknown" in line
    assert "leaders=unknown/unknown" in line
    assert "ready=unknown" in line


@pytest.mark.asyncio
async def test_loop_logs_immediately_then_once_per_interval(caplog) -> None:
    """The loop emits on the first tick and then honors the configured cadence."""

    delays: list[float] = []

    async def _fake_sleep(delay: float) -> None:
        delays.append(delay)
        if len(delays) >= 3:
            raise asyncio.CancelledError

    liveness_heartbeat._sleep = _fake_sleep
    try:
        with caplog.at_level(logging.INFO, logger="test.heartbeat"):
            with pytest.raises(asyncio.CancelledError):
                await bot_heartbeat_loop(
                    _healthy_state,
                    logger=logging.getLogger("test.heartbeat"),
                    interval_seconds=30.0,
                )
    finally:
        liveness_heartbeat._sleep = asyncio.sleep

    assert delays == [30.0, 30.0, 30.0]
    heartbeats = [r for r in caplog.records if getattr(r, "type", None) == "bot_heartbeat"]
    # Three sleeps means three logged ticks (log first, then sleep).
    assert len(heartbeats) == 3
    assert heartbeats[0].intervalSeconds == 30.0
    assert heartbeats[0].activeLeaders == 4
    assert heartbeats[0].detectionConnected is True
    assert heartbeats[0].ready is True


@pytest.mark.asyncio
async def test_loop_survives_a_raising_state_provider(caplog) -> None:
    """A broken snapshot degrades the line instead of killing the heartbeat."""

    def _broken() -> dict[str, Any]:
        raise RuntimeError("snapshot unavailable")

    async def _fake_sleep(delay: float) -> None:
        raise asyncio.CancelledError

    liveness_heartbeat._sleep = _fake_sleep
    try:
        with caplog.at_level(logging.INFO, logger="test.heartbeat"):
            with pytest.raises(asyncio.CancelledError):
                await bot_heartbeat_loop(
                    _broken,
                    logger=logging.getLogger("test.heartbeat"),
                    interval_seconds=30.0,
                )
    finally:
        liveness_heartbeat._sleep = asyncio.sleep

    heartbeats = [r for r in caplog.records if getattr(r, "type", None) == "bot_heartbeat"]
    assert len(heartbeats) == 1
    assert "snapshot_failed=RuntimeError" in heartbeats[0].getMessage()
    assert heartbeats[0].snapshotFailed == "RuntimeError"
    assert not hasattr(heartbeats[0], "detectionConnected")


def test_heartbeat_extras_flatten_the_snapshot_and_omit_unknowns() -> None:
    """Structured fields stay scalar so the shared log formatter can carry them."""

    extras = heartbeat_extras(
        {
            "detection_connected": True,
            "active_leaders": 4,
            "monitored_leaders": 4,
            "ready": False,
            "reconciler_ok": None,
            "db_ok": None,
            "api_ok": True,
        },
        interval_seconds=30.0,
    )

    assert extras["type"] == "bot_heartbeat"
    assert extras["intervalSeconds"] == 30.0
    assert extras["detectionConnected"] is True
    assert extras["activeLeaders"] == 4
    assert extras["ready"] is False
    assert extras["apiOk"] is True
    # Unknown facts are omitted rather than rendered as null.
    assert "reconcilerOk" not in extras
    assert "dbOk" not in extras
    assert all(
        value is None or isinstance(value, (bool, int, float, str))
        for value in extras.values()
    )


@pytest.mark.asyncio
async def test_loop_floors_a_misconfigured_interval(caplog) -> None:
    """A zero/sub-second interval is floored so it cannot spin the event loop."""

    delays: list[float] = []

    async def _fake_sleep(delay: float) -> None:
        delays.append(delay)
        raise asyncio.CancelledError

    liveness_heartbeat._sleep = _fake_sleep
    try:
        with caplog.at_level(logging.INFO, logger="test.heartbeat"):
            with pytest.raises(asyncio.CancelledError):
                await bot_heartbeat_loop(
                    _healthy_state,
                    logger=logging.getLogger("test.heartbeat"),
                    interval_seconds=0.0,
                )
    finally:
        liveness_heartbeat._sleep = asyncio.sleep

    assert delays == [MINIMUM_HEARTBEAT_INTERVAL_SECONDS]


@pytest.mark.asyncio
async def test_loop_survives_a_broken_log_sink(caplog) -> None:
    """A logging failure must not stop the liveness signal."""

    delays: list[float] = []

    async def _fake_sleep(delay: float) -> None:
        delays.append(delay)
        if len(delays) >= 2:
            raise asyncio.CancelledError

    class _ExplodingLogger:
        def info(self, *args: Any, **kwargs: Any) -> None:
            raise ValueError("log sink closed")

    liveness_heartbeat._sleep = _fake_sleep
    try:
        with pytest.raises(asyncio.CancelledError):
            await bot_heartbeat_loop(
                _healthy_state,
                logger=_ExplodingLogger(),  # type: ignore[arg-type]
                interval_seconds=30.0,
            )
    finally:
        liveness_heartbeat._sleep = asyncio.sleep

    assert delays == [30.0, 30.0]
