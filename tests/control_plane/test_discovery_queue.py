"""Bounded asynchronous manual discovery command tests."""

from __future__ import annotations

import asyncio

import pytest

from services.control_plane.contracts import (
    ControlPlaneError,
    ControlPlaneReason,
    DiscoveryRunCommand,
)
from services.telemetry import DiscoveryEvent, DiscoveryOutcome


class SequenceIds:
    """Return deterministic opaque discovery run identities."""

    def __init__(self) -> None:
        self._next = 0

    def new(self) -> str:
        """Return the next deterministic run identity."""

        self._next += 1
        return f"run-{self._next}"


class ControlledRunner:
    """Block each discovery cycle until the test releases it."""

    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.completed = asyncio.Event()
        self.release = asyncio.Event()
        self.forces: list[bool] = []

    async def run(self, *, force: bool) -> int:
        """Record one cycle and return a fixed candidate count."""

        self.forces.append(force)
        self.started.set()
        await self.release.wait()
        self.completed.set()
        return 7


class RecordingTelemetry:
    """Capture typed discovery telemetry without I/O."""

    def __init__(self) -> None:
        self.events: list[DiscoveryEvent] = []

    def emit(self, *, producer: object, payload: DiscoveryEvent) -> object:
        """Record one immutable payload."""

        del producer
        self.events.append(payload)
        return object()


class FakeRegistry:
    """Capture one forced provider-backed registry refresh."""

    def __init__(self) -> None:
        self.calls: list[tuple[list[str], bool]] = []

    async def refresh(
        self,
        configured: list[str],
        *,
        force_discovery: bool,
    ) -> object:
        """Return a state containing three evaluated candidates."""

        self.calls.append((configured, force_discovery))
        return type("State", (), {"discovered": (object(), object(), object())})()


async def test_discovery_queue_is_non_blocking_and_rejects_concurrent_run() -> None:
    """Return immediately while one bounded background cycle owns the slot."""

    from orchestrators.control_plane_discovery import DiscoveryRunQueue

    runner = ControlledRunner()
    telemetry = RecordingTelemetry()
    queue = DiscoveryRunQueue(
        runner=runner,
        identifiers=SequenceIds(),
        telemetry=telemetry,
    )
    await queue.start()
    try:
        first = await queue.enqueue(DiscoveryRunCommand(actor="operator", force=True))
        await asyncio.wait_for(runner.started.wait(), timeout=0.5)
        busy = await queue.enqueue(DiscoveryRunCommand(actor="operator", force=False))

        assert first.accepted is True
        assert first.reason_code == "accepted"
        assert busy.accepted is False
        assert busy.run_id == first.run_id
        assert busy.reason_code == "busy"
        assert runner.forces == [True]

        runner.release.set()
        await asyncio.wait_for(runner.completed.wait(), timeout=0.5)
        await asyncio.sleep(0)
        second = await queue.enqueue(DiscoveryRunCommand(actor="operator", force=False))
        assert second.accepted is True
    finally:
        runner.release.set()
        await queue.close()

    assert [event.outcome for event in telemetry.events[:2]] == [
        DiscoveryOutcome.REQUEST,
        DiscoveryOutcome.COMPLETED,
    ]
    assert telemetry.events[1].candidate_count == 7


async def test_discovery_queue_fails_closed_before_start_and_after_close() -> None:
    """Lifecycle misuse returns the stable subsystem-unavailable reason."""

    from orchestrators.control_plane_discovery import DiscoveryRunQueue

    queue = DiscoveryRunQueue(
        runner=ControlledRunner(),
        identifiers=SequenceIds(),
        telemetry=RecordingTelemetry(),
    )
    command = DiscoveryRunCommand(actor="operator")

    with pytest.raises(ControlPlaneError) as before:
        await queue.enqueue(command)
    await queue.start()
    await queue.close()
    with pytest.raises(ControlPlaneError) as after:
        await queue.enqueue(command)

    assert before.value.reason is ControlPlaneReason.DISCOVERY_UNAVAILABLE
    assert after.value.reason is ControlPlaneReason.DISCOVERY_UNAVAILABLE


async def test_registry_runner_always_executes_manual_provider_evaluation() -> None:
    """Manual command forces discovery but never starts monitors or trades."""

    from orchestrators.control_plane_discovery import LeaderRegistryDiscoveryRunner

    registry = FakeRegistry()
    runner = LeaderRegistryDiscoveryRunner(
        registry=registry,
        configured_leaders=("0xleader",),
    )

    candidate_count = await runner.run(force=False)

    assert candidate_count == 3
    assert registry.calls == [(["0xleader"], True)]
