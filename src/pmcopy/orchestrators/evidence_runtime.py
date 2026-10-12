"""Compose local durable capture and an independent private evidence delivery task."""

from __future__ import annotations

import asyncio
from contextlib import suppress
from pathlib import Path
from typing import Protocol

from pmcopy.adapters.clock import SystemReceiptClock
from pmcopy.config.evidence import EvidenceConfig
from pmcopy.domain.evidence import EvidenceError, EvidenceEvent, EvidenceHealth
from pmcopy.domain.source_time import TransportReceipt
from pmcopy.integrations.pmrecon.codec import PmreconEvidenceCodec
from pmcopy.integrations.pmrecon.http_transport import PmreconHttpTransport
from pmcopy.integrations.pmrecon.observation_sink import PmreconObservationSink
from pmcopy.interfaces.evidence import EvidenceClock, EvidenceTransport
from pmcopy.services.evidence_publisher import EvidencePublisher
from pmcopy.storage.evidence_outbox import SqliteEvidenceOutbox


class _ApplicationEvidenceConfig(Protocol):
    """Read only the immutable evidence section of the owning AppConfig."""

    @property
    def evidence(self) -> EvidenceConfig:
        """Return the section whose secret was loaded by the approved config loader."""
        ...


class NativeEvidenceClock:
    """Use the same process observer identity as source transport capture."""

    def __init__(self) -> None:
        """Create a source-clock adapter without reconstructing timestamps."""
        self._source = SystemReceiptClock()
        self._clock_id = self._source.capture().boot_id

    @property
    def clock_id(self) -> str:
        """Return the observer identity shared with native source receipt clocks."""
        return self._clock_id

    def time_ns(self) -> int:
        """Capture native epoch nanoseconds for locally generated loss events."""
        value = self._source.capture().wall_timestamp_ns
        if value is None:
            raise EvidenceError("evidence_missing_native_clock")
        return value


class EvidenceRuntime:
    """Raw capture never awaits Hungary; loss of local durability is sticky and visible."""

    def __init__(self, outbox: SqliteEvidenceOutbox, publisher: EvidencePublisher) -> None:
        """Take ownership of local storage and a separate lifecycle-managed publisher."""
        self._outbox, self._publisher = outbox, publisher
        self._sink = PmreconObservationSink(outbox)
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._closed = False
        self._closed_health: EvidenceHealth | None = None
        self._worker_failed = False

    @classmethod
    def from_config(
        cls,
        config: _ApplicationEvidenceConfig,
        *,
        clock: EvidenceClock | None = None,
        transport: EvidenceTransport | None = None,
    ) -> EvidenceRuntime:
        """Build local persistence; construct but never contact the private HTTP receiver."""
        settings = config.evidence
        settings.require_runtime()
        outbox = SqliteEvidenceOutbox(
            Path(settings.outbox_path),
            clock=clock or NativeEvidenceClock(),
            max_events=settings.max_events,
            max_bytes=settings.max_bytes,
            max_history=settings.max_history,
        )
        try:
            network = transport or PmreconHttpTransport(
                settings.origin, settings.token.get_secret_value(), timeout=settings.timeout_seconds
            )
            publisher = EvidencePublisher(
                outbox,
                network,
                batch_size=settings.batch_size,
                retry_delay=settings.retry_delay_seconds,
            )
            return cls(outbox, publisher)
        except Exception:
            outbox.close()
            raise

    def record(
        self,
        provider: str,
        kind: str,
        payload: object,
        receipt: TransportReceipt,
        *,
        recovered: bool,
    ) -> None:
        """Synchronously commit original raw source evidence using the injected local sink."""
        if self._closed:
            raise EvidenceError("evidence_runtime_closed")
        self._sink.record(provider, kind, payload, receipt, recovered=recovered)

    def append_event(self, value: object) -> int:
        """Append BookTap or transactional follower evidence with unchanged wire identity."""
        if self._closed:
            raise EvidenceError("evidence_runtime_closed")
        try:
            event = (
                value if isinstance(value, EvidenceEvent) else PmreconEvidenceCodec.encode(value)
            )
        except ValueError:
            self._outbox.invalidate("invalid_payload")
            raise
        return self._sink.append(event)

    def health(self) -> EvidenceHealth:
        """Expose local evidence continuity separately from remote delivery availability."""
        return self._closed_health or self._outbox.health()

    def invalidate(self, reason: str) -> None:
        """Latch capture/queue continuity loss through the same reserved durable gap path."""
        if self._closed:
            raise EvidenceError("evidence_runtime_closed")
        self._outbox.invalidate(reason)

    @property
    def delivery_status(self) -> str:
        """Expose offline/failed delivery without claiming locally retained data was lost."""
        if self._closed:
            return "closed"
        return "failed" if self._worker_failed else self._publisher.delivery_status

    async def start(self) -> None:
        """Schedule delivery without waiting for a network handshake or first response."""
        if self._closed:
            raise EvidenceError("evidence_runtime_closed")
        if self._task is None:
            self._task = asyncio.create_task(
                self._publisher.run(self._stop), name="evidence-publisher"
            )
            self._task.add_done_callback(self._publisher_done)

    def _publisher_done(self, task: asyncio.Task[None]) -> None:
        """Make unexpected delivery worker termination visible without logging raw errors."""
        if not self._closed and not task.cancelled() and task.exception() is not None:
            self._worker_failed = True

    async def aclose(self) -> None:
        """Cancel a stalled sender and close local ownership without discarding its prefix."""
        if self._closed:
            return
        self._closed = True
        self._stop.set()
        if self._task is not None:
            self._task.cancel()
            with suppress(asyncio.CancelledError, Exception):
                await self._task
        try:
            await self._publisher.aclose()
        finally:
            self._closed_health = self._outbox.health()
            self._outbox.close()
