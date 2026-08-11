"""Synchronous JSON-safe queries over bounded in-memory telemetry state."""

from __future__ import annotations

from typing import TypeAlias

from services.metrics import MetricsRegistry
from services.telemetry.contracts import (
    AmbiguousEvent,
    DiscoveryEvent,
    FailureEvent,
    FillEvent,
    LeaderEvent,
    LockEvent,
    ProtectiveExitEvent,
    ReconciliationEvent,
    RiskEvent,
    RosterEvent,
    RuntimeEvent,
    SkipEvent,
    TaskEvent,
    TelemetryEvent,
    TradeEvent,
    WssEvent,
)
from services.telemetry.service import Clock, SystemClock, TelemetryService


JsonScalar: TypeAlias = str | int | float | bool | None
JsonValue: TypeAlias = JsonScalar | list["JsonValue"] | dict[str, "JsonValue"]


class TelemetryQueryService:
    """Build bounded JSON-safe snapshots without database or network I/O.

    Args:
        service: In-memory telemetry event source.
        metrics: Existing process-wide metrics truth.
        clock: Injected clock for deterministic snapshot timestamps.
    """

    def __init__(
        self,
        *,
        service: TelemetryService,
        metrics: MetricsRegistry | None = None,
        clock: Clock | None = None,
    ) -> None:
        self._service = service
        if metrics is not None and metrics is not service.metrics_registry:
            raise ValueError("service and query must use the same metrics registry")
        self._metrics = service.metrics_registry
        self._clock = clock if clock is not None else SystemClock()

    @property
    def metrics_registry(self) -> MetricsRegistry:
        """Return the single metrics registry used by this query service."""
        return self._metrics

    def snapshot(self, *, limit: int = 100) -> dict[str, JsonValue]:
        """Return events, counters, gauges, and latency summaries.

        Args:
            limit: Maximum number of newest events to serialize.

        Returns:
            A structure accepted directly by :func:`json.dumps`.
        """
        if limit < 0:
            raise ValueError("limit must be non-negative")
        counters, gauges = self._metrics.snapshot()
        histograms: list[JsonValue] = []
        for histogram in self._metrics.histogram_snapshots():
            histograms.append(
                {
                    "name": histogram.name,
                    "labels": dict(histogram.labels),
                    "count": histogram.count,
                    "sum": histogram.total,
                    "buckets": [
                        {"le": boundary, "count": count} for boundary, count in histogram.buckets
                    ],
                    "p50": histogram.p50,
                    "p95": histogram.p95,
                    "p99": histogram.p99,
                }
            )
        return {
            "generated_at": self._clock.now().isoformat(),
            "events": [
                self._event_to_json(event) for event in self._service.history_snapshot(limit=limit)
            ],
            "metrics": {
                "counters": dict(sorted(counters.items())),
                "gauges": dict(sorted(gauges.items())),
                "histograms": histograms,
            },
        }

    @staticmethod
    def _event_to_json(event: TelemetryEvent) -> dict[str, JsonValue]:
        return {
            "event_id": event.event_id,
            "event_type": event.event_type.value,
            "timestamp": event.timestamp.isoformat(),
            "severity": event.severity.value,
            "correlation_id": event.correlation_id,
            "producer": event.producer.value,
            "sequence": event.sequence,
            "payload": TelemetryQueryService._payload_to_json(event),
        }

    @staticmethod
    def _payload_to_json(event: TelemetryEvent) -> dict[str, JsonValue]:
        payload = event.payload
        result: dict[str, JsonValue] = {"category": payload.category.value}
        if isinstance(payload, LeaderEvent):
            result.update(
                outcome=payload.outcome.value,
                leader_ref=payload.leader_ref,
                source=payload.source,
            )
        elif isinstance(payload, TaskEvent):
            result.update(
                outcome=payload.outcome.value,
                task_ref=payload.task_ref,
                leader_ref=payload.leader_ref,
            )
        elif isinstance(payload, TradeEvent):
            result.update(
                outcome=payload.outcome.value,
                task_ref=payload.task_ref,
                side=payload.side.value,
                order_ref=payload.order_ref,
                order_outcome=(
                    None if payload.order_outcome is None else payload.order_outcome.value
                ),
            )
        elif isinstance(payload, FillEvent):
            result.update(
                outcome=payload.outcome.value,
                order_ref=payload.order_ref,
                fill_ref=payload.fill_ref,
            )
        elif isinstance(payload, SkipEvent):
            result.update(reason=payload.reason.value, task_ref=payload.task_ref)
        elif isinstance(payload, FailureEvent):
            result.update(
                component=payload.component.value,
                code=payload.code,
                retryable=payload.retryable,
                order_outcome=(
                    None if payload.order_outcome is None else payload.order_outcome.value
                ),
            )
        elif isinstance(payload, AmbiguousEvent):
            result.update(
                operation=payload.operation.value,
                order_ref=payload.order_ref,
                reconciliation_required=payload.reconciliation_required,
            )
        elif isinstance(payload, ProtectiveExitEvent):
            result.update(outcome=payload.outcome.value, task_ref=payload.task_ref)
        elif isinstance(payload, LockEvent):
            result.update(outcome=payload.outcome.value, lock_ref=payload.lock_ref)
        elif isinstance(payload, ReconciliationEvent):
            result.update(
                outcome=payload.outcome.value,
                affected_count=payload.affected_count,
            )
        elif isinstance(payload, DiscoveryEvent):
            result.update(
                outcome=payload.outcome.value,
                candidate_count=payload.candidate_count,
            )
        elif isinstance(payload, RosterEvent):
            result.update(outcome=payload.outcome.value, active_count=payload.active_count)
        elif isinstance(payload, WssEvent):
            result.update(outcome=payload.outcome.value, stream=payload.stream)
        elif isinstance(payload, RuntimeEvent):
            result.update(outcome=payload.outcome.value)
        elif isinstance(payload, RiskEvent):
            result.update(outcome=payload.outcome.value, task_ref=payload.task_ref)
        return result
