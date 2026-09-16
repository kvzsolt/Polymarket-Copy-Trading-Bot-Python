"""Run manual discovery cycles behind a bounded asynchronous command queue.

HTTP producers only enqueue immutable commands. The single consumer owns
provider I/O, so a slow Falcon cycle cannot block the privileged request loop
or create overlapping roster evaluations.
"""

from __future__ import annotations

import asyncio
import logging
import secrets
from collections.abc import Sequence
from typing import Protocol

from services.control_plane.contracts import (
    ControlPlaneError,
    ControlPlaneReason,
    DiscoveryRunCommand,
    DiscoveryRunReceipt,
)
from services.telemetry import (
    DiscoveryEvent,
    DiscoveryOutcome,
    TelemetryProducer,
)

logger = logging.getLogger(__name__)


class DiscoveryCycleRunner(Protocol):
    """Execute one provider-backed discovery evaluation cycle."""

    async def run(self, *, force: bool) -> int:
        """Run one cycle and return the persisted candidate count."""

        ...


class DiscoveryRegistryState(Protocol):
    """Expose evaluated candidates from one registry refresh."""

    @property
    def discovered(self) -> Sequence[object]:
        """Return evaluated candidates without allowing mutation."""

        ...


class LeaderRegistryPort(Protocol):
    """Run one discovery evaluation without activating trade monitors."""

    async def refresh(
        self,
        configured: list[str],
        *,
        force_discovery: bool,
    ) -> DiscoveryRegistryState:
        """Evaluate and persist the current candidate roster."""

        ...


class RunIdentifierSource(Protocol):
    """Generate opaque identifiers for accepted manual runs."""

    def new(self) -> str:
        """Return one safe non-secret identifier."""

        ...


class DiscoveryTelemetry(Protocol):
    """Emit typed non-blocking discovery lifecycle events."""

    def emit(
        self,
        *,
        producer: TelemetryProducer,
        payload: DiscoveryEvent,
    ) -> object:
        """Publish one bounded in-memory event."""

        ...


class SecureRunIdentifierSource:
    """Generate cryptographically opaque manual-run identifiers."""

    def new(self) -> str:
        """Return one prefixed 128-bit random identifier."""

        return f"discovery-{secrets.token_hex(16)}"


class LeaderRegistryDiscoveryRunner:
    """Adapt the existing registry refresh into a manual cycle command."""

    def __init__(
        self,
        *,
        registry: LeaderRegistryPort,
        configured_leaders: tuple[str, ...],
    ) -> None:
        """Store the registry and immutable configured-leader snapshot.

        Args:
            registry: Existing discovery and roster application authority.
            configured_leaders: Explicit leaders retained in the roster view.
        """

        self._registry = registry
        self._configured_leaders = configured_leaders

    async def run(self, *, force: bool) -> int:
        """Force one provider evaluation and return its candidate count.

        Args:
            force: Reserved command flag; manual runs are always provider-forced.

        Returns:
            Number of candidates represented by the persisted cycle.
        """

        del force
        state = await self._registry.refresh(
            list(self._configured_leaders),
            force_discovery=True,
        )
        return len(state.discovered)


class DiscoveryRunQueue:
    """Own one non-overlapping manual-discovery worker and queue slot."""

    def __init__(
        self,
        *,
        runner: DiscoveryCycleRunner,
        telemetry: DiscoveryTelemetry,
        identifiers: RunIdentifierSource | None = None,
    ) -> None:
        """Store injected I/O and identifier boundaries.

        Args:
            runner: Provider-backed cycle application boundary.
            telemetry: Existing non-blocking telemetry service.
            identifiers: Optional deterministic identifier source for tests.
        """

        self._runner = runner
        self._telemetry = telemetry
        self._identifiers = identifiers or SecureRunIdentifierSource()
        self._queue: asyncio.Queue[tuple[str, DiscoveryRunCommand]] = asyncio.Queue(maxsize=1)
        self._worker: asyncio.Task[None] | None = None
        self._pending_run_id: str | None = None
        self._started = False
        self._closed = False

    async def start(self) -> None:
        """Start the sole provider-I/O consumer exactly once."""

        if self._closed:
            raise RuntimeError("closed discovery queue cannot be restarted")
        if self._started:
            return
        self._worker = asyncio.create_task(
            self._run(),
            name="control-plane-discovery",
        )
        self._started = True

    async def enqueue(self, command: DiscoveryRunCommand) -> DiscoveryRunReceipt:
        """Admit one command immediately or return an explicit busy receipt.

        Args:
            command: Validated operator command.

        Returns:
            Accepted receipt or the identity of the already-active run.

        Raises:
            ControlPlaneError: If the queue lifecycle is unavailable.
        """

        if not self._started or self._closed:
            raise ControlPlaneError(ControlPlaneReason.DISCOVERY_UNAVAILABLE)
        if self._pending_run_id is not None:
            return DiscoveryRunReceipt(
                run_id=self._pending_run_id,
                accepted=False,
                reason_code="busy",
            )
        run_id = self._identifiers.new()
        if not isinstance(run_id, str) or not run_id.strip():
            raise ControlPlaneError(ControlPlaneReason.DISCOVERY_UNAVAILABLE)
        self._pending_run_id = run_id
        try:
            self._queue.put_nowait((run_id, command))
        except asyncio.QueueFull:
            self._pending_run_id = None
            raise ControlPlaneError(ControlPlaneReason.DISCOVERY_BUSY) from None
        return DiscoveryRunReceipt(
            run_id=run_id,
            accepted=True,
            reason_code="accepted",
        )

    async def close(self) -> None:
        """Cancel provider work and release the bounded worker idempotently."""

        if self._closed:
            return
        self._closed = True
        worker = self._worker
        self._worker = None
        self._started = False
        if worker is not None:
            worker.cancel()
            try:
                await worker
            except asyncio.CancelledError:
                pass
        self._pending_run_id = None

    async def _run(self) -> None:
        """Consume queued cycles serially while containing provider failures."""

        while True:
            _run_id, command = await self._queue.get()
            try:
                self._telemetry.emit(
                    producer=TelemetryProducer.DISCOVERY,
                    payload=DiscoveryEvent(outcome=DiscoveryOutcome.REQUEST),
                )
                candidate_count = await self._runner.run(force=command.force)
                if (
                    isinstance(candidate_count, bool)
                    or not isinstance(candidate_count, int)
                    or candidate_count < 0
                ):
                    raise RuntimeError("discovery runner returned an invalid count")
                self._telemetry.emit(
                    producer=TelemetryProducer.DISCOVERY,
                    payload=DiscoveryEvent(
                        outcome=DiscoveryOutcome.COMPLETED,
                        candidate_count=candidate_count,
                    ),
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "manual_discovery_failed",
                    extra={"error_type": type(exc).__name__},
                )
                self._telemetry.emit(
                    producer=TelemetryProducer.DISCOVERY,
                    payload=DiscoveryEvent(outcome=DiscoveryOutcome.FAILURE),
                )
            finally:
                self._pending_run_id = None
                self._queue.task_done()


__all__ = [
    "DiscoveryCycleRunner",
    "DiscoveryRegistryState",
    "DiscoveryRunQueue",
    "DiscoveryTelemetry",
    "LeaderRegistryDiscoveryRunner",
    "LeaderRegistryPort",
    "RunIdentifierSource",
    "SecureRunIdentifierSource",
]
