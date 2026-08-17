"""Class-based telemetry service with injected time, randomness, and I/O."""

from __future__ import annotations

import asyncio
import secrets
import threading
from collections import deque
from datetime import UTC, datetime
from typing import Protocol

from services.metrics import (
    GAUGE_TELEMETRY_ACTIVE_ENTITIES,
    GAUGE_TELEMETRY_CACHE_ENTRIES,
    GAUGE_TELEMETRY_FALCON_DISCOVERY_STATE,
    GAUGE_TELEMETRY_QUEUE_DEPTH,
    GAUGE_TELEMETRY_QUEUE_UTILIZATION,
    GAUGE_TELEMETRY_RECONCILIATION_AGE,
    GAUGE_TELEMETRY_WSS_AGE,
    METRIC_TELEMETRY_CANDIDATES_EVALUATED,
    METRIC_TELEMETRY_COPY_OUTCOME,
    METRIC_TELEMETRY_FALCON,
    METRIC_TELEMETRY_FILL,
    METRIC_TELEMETRY_LATENCY,
    METRIC_TELEMETRY_LEADER_QUEUE_OVERFLOW,
    METRIC_TELEMETRY_LEADER_SIGNAL,
    METRIC_TELEMETRY_ORDER_OUTCOME,
    METRIC_TELEMETRY_PROTECTIVE_EXIT,
    METRIC_TELEMETRY_RECONCILIATION,
    METRIC_TELEMETRY_REENTRY_LOCK,
    METRIC_TELEMETRY_WSS_RECONNECT,
    TELEMETRY_LATENCY_BUCKETS,
    TELEMETRY_METRIC_SCHEMAS,
    ActiveEntity,
    FalconDiscoveryState,
    LatencyPhase,
    MetricsRegistry,
    TelemetryCache,
    TelemetryQueue,
    TelemetryStream,
)
from services.telemetry.contracts import (
    AmbiguousEvent,
    DiscoveryEvent,
    DiscoveryOutcome,
    FailureEvent,
    FillEvent,
    LeaderEvent,
    LockEvent,
    ProtectiveExitEvent,
    ReconciliationEvent,
    RosterEvent,
    RosterOutcome,
    RuntimeEvent,
    SkipEvent,
    TaskEvent,
    TelemetryEvent,
    TelemetryPayload,
    TelemetryProducer,
    TelemetrySeverity,
    TradeEvent,
    WssEvent,
    event_type_for_payload,
)
from services.telemetry.hub import (
    HubPublishResult,
    HubPublishStatus,
    TelemetryHub,
    TelemetrySubscription,
)


class Clock(Protocol):
    """Provide wall-clock time without hard-coding system time in tests."""

    def now(self) -> datetime:
        """Return a timezone-aware wall-clock instant."""
        ...


class RandomSource(Protocol):
    """Generate opaque event identifiers through an injected boundary."""

    def token_hex(self, nbytes: int) -> str:
        """Return ``nbytes`` of randomness encoded as hexadecimal."""
        ...


class TelemetrySink(Protocol):
    """Consume telemetry asynchronously away from producer code paths."""

    async def write(self, event: TelemetryEvent) -> None:
        """Persist or forward one immutable event."""
        ...

    async def resync(self, events: tuple[TelemetryEvent, ...]) -> None:
        """Replace sink state after subscriber overflow."""
        ...

    async def close(self) -> None:
        """Release sink resources."""
        ...


class SystemClock:
    """Default UTC wall clock."""

    def now(self) -> datetime:
        """Return the current aware UTC instant."""
        return datetime.now(tz=UTC)


class SecureRandomSource:
    """Default cryptographically strong opaque-ID source."""

    def token_hex(self, nbytes: int) -> str:
        """Return secure random bytes encoded as hexadecimal."""
        return secrets.token_hex(nbytes)


class TelemetryService:
    """Stamp, count, retain, and fan out typed telemetry events.

    Producers call :meth:`emit`, a synchronous non-awaiting method that only
    performs bounded in-memory work. Optional I/O runs on a subscriber task.

    Args:
        hub: Bounded fan-out hub owned by this service lifecycle.
        clock: Injected source for event timestamps.
        rng: Injected source for opaque event identifiers.
        sink: Optional asynchronous I/O sink.
        metrics: Existing process-wide metrics truth.
        history_capacity: Maximum events retained for local query/resync.
    """

    def __init__(
        self,
        *,
        hub: TelemetryHub,
        clock: Clock | None = None,
        rng: RandomSource | None = None,
        sink: TelemetrySink | None = None,
        metrics: MetricsRegistry | None = None,
        history_capacity: int = 2048,
    ) -> None:
        if history_capacity <= 0:
            raise ValueError("history_capacity must be positive")
        self._hub = hub
        self._clock = clock if clock is not None else SystemClock()
        self._rng = rng if rng is not None else SecureRandomSource()
        self._sink = sink
        if metrics is not None and metrics is not hub.metrics_registry:
            raise ValueError("hub and service must use the same metrics registry")
        self._metrics = hub.metrics_registry
        self._metrics.register_schemas(TELEMETRY_METRIC_SCHEMAS)
        self._history: deque[TelemetryEvent] = deque(maxlen=history_capacity)
        self._history_lock = threading.Lock()
        self._sequences: dict[TelemetryProducer, int] = {}
        self._sink_subscription: TelemetrySubscription | None = None
        self._sink_task: asyncio.Task[None] | None = None
        self._started = False
        self._closed = False

    @property
    def started(self) -> bool:
        """Return whether service startup completed."""
        return self._started

    @property
    def closed(self) -> bool:
        """Return whether service shutdown completed."""
        return self._closed

    @property
    def metrics_registry(self) -> MetricsRegistry:
        """Return the single metrics registry used by this service."""
        return self._metrics

    async def start(self) -> None:
        """Start hub dispatch and the optional asynchronous sink."""
        if self._closed:
            raise RuntimeError("closed telemetry service cannot be restarted")
        if self._started:
            return
        await self._hub.start()
        if self._sink is not None:
            self._sink_subscription = self._hub.subscribe()
            self._sink_task = asyncio.create_task(
                self._run_sink(self._sink_subscription, self._sink),
                name="telemetry-service-sink",
            )
        self._started = True

    def emit(
        self,
        *,
        producer: TelemetryProducer,
        payload: TelemetryPayload,
        severity: TelemetrySeverity = TelemetrySeverity.INFO,
        correlation_id: str | None = None,
    ) -> HubPublishResult:
        """Stamp and enqueue one typed event without awaiting.

        Args:
            producer: Stable producer identity used for FIFO sequencing.
            payload: One immutable, category-specific safe payload.
            severity: Bounded operator severity.
            correlation_id: Optional opaque correlation token.

        Returns:
            The explicit accepted or dropped result from the hub.
        """
        sequence = self._sequences.get(producer, 0) + 1
        self._sequences[producer] = sequence
        event = TelemetryEvent(
            event_id=f"evt_{self._rng.token_hex(16)}",
            event_type=event_type_for_payload(payload),
            timestamp=self._clock.now(),
            severity=severity,
            correlation_id=correlation_id,
            payload=payload,
            producer=producer,
            sequence=sequence,
        )
        result = self._hub.publish_nowait(event)
        if result.status is HubPublishStatus.ACCEPTED:
            with self._history_lock:
                self._history.append(event)
            self._record_event_metrics(payload)
            self._metrics.set_gauge(
                GAUGE_TELEMETRY_CACHE_ENTRIES,
                float(len(self._history)),
                labels={"cache": TelemetryCache.EVENT_HISTORY.value},
            )
        return result

    def observe_latency(self, *, phase: LatencyPhase, seconds: float) -> None:
        """Observe one exact pipeline latency phase.

        Args:
            phase: Stable low-cardinality phase.
            seconds: Non-negative elapsed seconds.
        """
        if seconds < 0:
            raise ValueError("latency seconds must be non-negative")
        self._metrics.observe(
            METRIC_TELEMETRY_LATENCY,
            seconds,
            buckets=TELEMETRY_LATENCY_BUCKETS,
            labels={"phase": phase.value},
        )

    def set_active_count(self, *, entity: ActiveEntity, value: int) -> None:
        """Set one aggregate active-entity gauge without identifier labels."""
        if value < 0:
            raise ValueError("active count must be non-negative")
        self._metrics.set_gauge(
            GAUGE_TELEMETRY_ACTIVE_ENTITIES,
            float(value),
            labels={"entity": entity.value},
        )

    def set_queue_state(
        self,
        *,
        queue: TelemetryQueue,
        depth: int,
        capacity: int,
    ) -> None:
        """Set aggregate queue depth and utilization gauges."""
        if depth < 0:
            raise ValueError("queue depth must be non-negative")
        if capacity <= 0:
            raise ValueError("queue capacity must be positive")
        if depth > capacity:
            raise ValueError("queue depth cannot exceed capacity")
        labels = {"queue": queue.value}
        self._metrics.set_gauge(GAUGE_TELEMETRY_QUEUE_DEPTH, float(depth), labels=labels)
        self._metrics.set_gauge(
            GAUGE_TELEMETRY_QUEUE_UTILIZATION,
            depth / capacity,
            labels=labels,
        )

    def set_cache_entries(self, *, cache: TelemetryCache, value: int) -> None:
        """Set one aggregate bounded-cache entry gauge."""
        if value < 0:
            raise ValueError("cache entries must be non-negative")
        self._metrics.set_gauge(
            GAUGE_TELEMETRY_CACHE_ENTRIES,
            float(value),
            labels={"cache": cache.value},
        )

    def set_wss_age(self, *, stream: TelemetryStream, seconds: float) -> None:
        """Set the age of the latest message for one bounded WSS stream."""
        if seconds < 0:
            raise ValueError("WSS age must be non-negative")
        self._metrics.set_gauge(
            GAUGE_TELEMETRY_WSS_AGE,
            seconds,
            labels={"stream": stream.value},
        )

    def set_reconciliation_age(self, *, seconds: float) -> None:
        """Set the age of the latest successful reconciliation."""
        if seconds < 0:
            raise ValueError("reconciliation age must be non-negative")
        self._metrics.set_gauge(GAUGE_TELEMETRY_RECONCILIATION_AGE, seconds)

    def set_falcon_discovery_state(self, state: FalconDiscoveryState) -> None:
        """Set a one-hot bounded Falcon discovery state gauge."""
        for candidate in FalconDiscoveryState:
            self._metrics.set_gauge(
                GAUGE_TELEMETRY_FALCON_DISCOVERY_STATE,
                1.0 if candidate is state else 0.0,
                labels={"state": candidate.value},
            )

    def record_leader_queue_overflow(self, *, count: int = 1) -> None:
        """Record dropped leader work without a high-cardinality label."""
        if count <= 0:
            raise ValueError("overflow count must be positive")
        self._metrics.inc(METRIC_TELEMETRY_LEADER_QUEUE_OVERFLOW, value=float(count))

    def history_snapshot(self, *, limit: int | None = None) -> tuple[TelemetryEvent, ...]:
        """Return an immutable newest-tail snapshot of bounded history.

        Args:
            limit: Optional maximum number of newest events.

        Returns:
            Events in their original accepted order.
        """
        if limit is not None and limit < 0:
            raise ValueError("limit must be non-negative")
        with self._history_lock:
            events = tuple(self._history)
        if limit is None:
            return events
        if limit == 0:
            return ()
        return events[-limit:]

    async def close(self) -> None:
        """Cancel sink I/O and close every hub subscription and task."""
        if self._closed:
            return
        self._closed = True
        subscription = self._sink_subscription
        self._sink_subscription = None
        if subscription is not None:
            self._hub.unsubscribe(subscription)
        sink_task = self._sink_task
        self._sink_task = None
        if sink_task is not None:
            sink_task.cancel()
            try:
                await sink_task
            except asyncio.CancelledError:
                pass
        try:
            if self._sink is not None:
                await self._sink.close()
        finally:
            try:
                await self._hub.close()
            finally:
                self._started = False

    async def _run_sink(
        self,
        subscription: TelemetrySubscription,
        sink: TelemetrySink,
    ) -> None:
        while True:
            delivery = await subscription.get()
            if delivery.stream_closed:
                return
            try:
                if delivery.resync_required:
                    await sink.resync(self.history_snapshot())
                elif delivery.event is not None:
                    await sink.write(delivery.event)
            except asyncio.CancelledError:
                raise
            except Exception:
                operation = "resync" if delivery.resync_required else "write"
                self._metrics.inc(
                    "telemetry_sink_failure_total",
                    labels={"operation": operation},
                )

    def _record_event_metrics(self, payload: TelemetryPayload) -> None:
        if isinstance(payload, LeaderEvent):
            self._metrics.inc(
                METRIC_TELEMETRY_LEADER_SIGNAL,
                labels={"outcome": payload.outcome.value},
            )
        elif isinstance(payload, TaskEvent):
            self._metrics.inc(
                "telemetry_task_decision_total",
                labels={"outcome": payload.outcome.value},
            )
        elif isinstance(payload, TradeEvent):
            if payload.outcome.value == "candidate_evaluated":
                self._metrics.inc(METRIC_TELEMETRY_CANDIDATES_EVALUATED)
            else:
                self._metrics.inc(
                    METRIC_TELEMETRY_COPY_OUTCOME,
                    labels={"outcome": payload.outcome.value, "reason": "none"},
                )
            if payload.order_outcome is not None:
                self._metrics.inc(
                    METRIC_TELEMETRY_ORDER_OUTCOME,
                    labels={"outcome": payload.order_outcome.value},
                )
        elif isinstance(payload, FillEvent):
            self._metrics.inc(
                METRIC_TELEMETRY_FILL,
                labels={"outcome": payload.outcome.value},
            )
        elif isinstance(payload, SkipEvent):
            self._metrics.inc(
                METRIC_TELEMETRY_COPY_OUTCOME,
                labels={"outcome": "skipped", "reason": payload.reason.value},
            )
        elif isinstance(payload, FailureEvent):
            self._metrics.inc(
                METRIC_TELEMETRY_COPY_OUTCOME,
                labels={"outcome": "failed", "reason": "none"},
            )
            if payload.order_outcome is not None:
                self._metrics.inc(
                    METRIC_TELEMETRY_ORDER_OUTCOME,
                    labels={"outcome": payload.order_outcome.value},
                )
        elif isinstance(payload, AmbiguousEvent):
            self._metrics.inc(
                METRIC_TELEMETRY_COPY_OUTCOME,
                labels={"outcome": "ambiguous", "reason": "none"},
            )
            self._metrics.inc(
                METRIC_TELEMETRY_ORDER_OUTCOME,
                labels={"outcome": "ambiguous"},
            )
        elif isinstance(payload, ProtectiveExitEvent):
            self._metrics.inc(
                METRIC_TELEMETRY_PROTECTIVE_EXIT,
                labels={"outcome": payload.outcome.value},
            )
        elif isinstance(payload, LockEvent):
            self._metrics.inc(
                METRIC_TELEMETRY_REENTRY_LOCK,
                labels={"outcome": payload.outcome.value},
            )
        elif isinstance(payload, ReconciliationEvent):
            self._metrics.inc(
                METRIC_TELEMETRY_RECONCILIATION,
                labels={"outcome": payload.outcome.value},
            )
        elif isinstance(payload, DiscoveryEvent):
            if payload.outcome is DiscoveryOutcome.COMPLETED:
                self._metrics.inc(
                    METRIC_TELEMETRY_CANDIDATES_EVALUATED,
                    value=float(payload.candidate_count),
                )
            else:
                self._metrics.inc(
                    METRIC_TELEMETRY_FALCON,
                    labels={"outcome": payload.outcome.value},
                )
        elif isinstance(payload, RosterEvent):
            self.set_active_count(entity=ActiveEntity.LEADERS, value=payload.active_count)
            if payload.outcome is not RosterOutcome.UNCHANGED:
                outcome = "demoted" if payload.outcome is RosterOutcome.EMPTY else "promoted"
                self._metrics.inc(
                    METRIC_TELEMETRY_LEADER_SIGNAL,
                    labels={"outcome": outcome},
                )
        elif isinstance(payload, WssEvent) and payload.outcome.value == "reconnected":
            self._metrics.inc(
                METRIC_TELEMETRY_WSS_RECONNECT,
                labels={"stream": payload.stream},
            )
        elif isinstance(payload, RuntimeEvent):
            self._metrics.inc(
                "telemetry_runtime_state_total",
                labels={"outcome": payload.outcome.value},
            )
