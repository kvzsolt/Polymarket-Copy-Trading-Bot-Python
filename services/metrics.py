"""Thread-safe process metrics with bounded telemetry schemas and snapshots."""

from __future__ import annotations

import re
import threading
from collections import defaultdict, deque
from dataclasses import dataclass, field
from enum import StrEnum
from math import ceil, floor, inf, isfinite
from typing import Iterable

from domain.detection import DetectionMode


_METRIC_NAME_RE = re.compile(r"^[a-zA-Z_:][a-zA-Z0-9_:]*$")


def _escape_label(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _metric_key(name: str, labels: dict[str, str] | None = None) -> str:
    if not labels:
        return name
    parts = [f'{key}="{_escape_label(value)}"' for key, value in sorted(labels.items())]
    return f"{name}{{{','.join(parts)}}}"


class MetricKind(StrEnum):
    """Supported Prometheus metric kinds."""

    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"


class LatencyPhase(StrEnum):
    """Stable latency phases for the v1 copy-trading pipeline."""

    SOURCE_DETECTION_TO_NORMALIZED = "source_detection_to_normalized"
    # Compatibility name for early telemetry callers; the serialized value is
    # the exact v1 phase above and aliases do not add schema cardinality.
    DETECTION_TO_NORMALIZED = "source_detection_to_normalized"
    NORMALIZED_TO_WORKER_START = "normalized_to_worker_start"
    LEADER_QUEUE_WAIT = "leader_queue_wait"
    COPY_DECISION = "copy_decision"
    SIZING = "sizing"
    RISK = "risk"
    RESERVATION = "reservation"
    DATABASE_ACQUISITION = "database_acquisition"
    METADATA_CACHE_MISS = "metadata_cache_miss"
    MARKET_RECONNECT_RECOVERY = "market_reconnect_recovery"
    ORDER_CONSTRUCTION = "order_construction"
    VENUE_SUBMISSION = "venue_submission"
    SUBMISSION_TO_FIRST_ACK = "submission_to_first_ack"
    SUBMISSION_TO_FIRST_FILL_EVIDENCE = "submission_to_first_fill_evidence"
    SUBMISSION_TO_TERMINAL_RECONCILIATION = "submission_to_terminal_reconciliation"
    PROTECTIVE_EXIT_DECISION = "protective_exit_decision"
    FALCON_DISCOVERY_CYCLE = "falcon_discovery_cycle"


class ActiveEntity(StrEnum):
    """Low-cardinality active-entity gauge values."""

    LEADERS = "leaders"
    TASKS = "tasks"
    POSITIONS = "positions"
    ORDERS = "orders"
    INTENTS = "intents"
    RESERVATIONS = "reservations"
    LOCKS = "locks"


class TelemetryQueue(StrEnum):
    """Low-cardinality queue gauge values."""

    LEADER = "leader"
    WORKER = "worker"
    INGRESS = "ingress"
    SUBSCRIBER = "subscriber"


class TelemetryCache(StrEnum):
    """Low-cardinality cache gauge values."""

    MARKET_PRICE = "market_price"
    EVENT_HISTORY = "event_history"


class TelemetryStream(StrEnum):
    """Low-cardinality WebSocket stream values."""

    LEADER = "leader"
    MARKET = "market"
    USER = "user"


class FalconDiscoveryState(StrEnum):
    """One-hot Falcon discovery gauge states."""

    IDLE = "idle"
    RUNNING = "running"
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class MetricSchema:
    """Declare metric kind, bounded labels, and optional histogram buckets."""

    name: str
    kind: MetricKind
    allowed_label_values: tuple[tuple[str, tuple[str, ...]], ...] = ()
    buckets: tuple[float, ...] = ()
    non_negative: bool = False

    def __post_init__(self) -> None:
        if not _METRIC_NAME_RE.fullmatch(self.name):
            raise ValueError(f"invalid metric name: {self.name}")
        label_names = [name for name, _ in self.allowed_label_values]
        if len(label_names) != len(set(label_names)):
            raise ValueError(f"duplicate label schema for {self.name}")
        for label_name, allowed_values in self.allowed_label_values:
            if not _METRIC_NAME_RE.fullmatch(label_name):
                raise ValueError(f"invalid label name for {self.name}: {label_name}")
            if not allowed_values:
                raise ValueError(f"label {label_name} for {self.name} must be bounded")
            if len(allowed_values) != len(set(allowed_values)):
                raise ValueError(f"duplicate allowed values for {self.name}.{label_name}")
        if self.kind is MetricKind.HISTOGRAM:
            if not self.buckets:
                raise ValueError(f"histogram {self.name} requires finite buckets")
            if any(not isfinite(bucket) for bucket in self.buckets):
                raise ValueError(f"histogram {self.name} buckets must be finite")
            if tuple(sorted(set(self.buckets))) != self.buckets:
                raise ValueError(f"histogram {self.name} buckets must be strictly increasing")
        elif self.buckets:
            raise ValueError(f"non-histogram {self.name} cannot declare buckets")

    @property
    def label_names(self) -> tuple[str, ...]:
        """Return the stable label-name set in declaration order."""
        return tuple(name for name, _ in self.allowed_label_values)

    def validate_labels(self, labels: dict[str, str] | None) -> None:
        """Reject missing, extra, or unbounded metric label values.

        Args:
            labels: Labels supplied for one metric observation.

        Raises:
            ValueError: If names or values fall outside this schema.
        """
        supplied = labels or {}
        if set(supplied) != set(self.label_names):
            raise ValueError(f"metric {self.name} label names must be {sorted(self.label_names)}")
        for label_name, allowed_values in self.allowed_label_values:
            value = supplied[label_name]
            if value not in allowed_values:
                raise ValueError(f"metric {self.name}.{label_name} requires a bounded value")


@dataclass(slots=True)
class HistogramState:
    """Mutable cumulative histogram state guarded by the registry lock."""

    name: str
    boundaries: tuple[float, ...]
    counts: list[int]
    labels: dict[str, str]
    samples: deque[float]
    count: int = 0
    total: float = 0.0


@dataclass(frozen=True, slots=True)
class HistogramSnapshot:
    """Immutable histogram summary with bounded-window percentiles."""

    name: str
    labels: tuple[tuple[str, str], ...]
    count: int
    total: float
    buckets: tuple[tuple[str, int], ...]
    sample_count: int
    p50: float | None
    p95: float | None
    p99: float | None


def _quantile(samples: tuple[float, ...], quantile: float) -> float | None:
    if not samples:
        return None
    ordered = sorted(samples)
    position = (len(ordered) - 1) * quantile
    lower = floor(position)
    upper = ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] + ((ordered[upper] - ordered[lower]) * weight)


def _boundary_text(boundary: float) -> str:
    return "+Inf" if boundary == inf else str(boundary)


@dataclass
class MetricsRegistry:
    """Store counters, gauges, and bounded-window histogram observations."""

    schemas: Iterable[MetricSchema] = ()
    sample_capacity: int = 2048
    counters: dict[str, float] = field(default_factory=lambda: defaultdict(float))
    gauges: dict[str, float] = field(default_factory=lambda: defaultdict(float))
    histograms: dict[str, HistogramState] = field(default_factory=dict)
    _lock: threading.Lock = field(init=False, repr=False)
    _schemas: dict[str, MetricSchema] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if self.sample_capacity <= 0:
            raise ValueError("sample_capacity must be positive")
        self._lock = threading.Lock()
        self._schemas = {}
        self.register_schemas(self.schemas)

    def register_schemas(self, schemas: Iterable[MetricSchema]) -> None:
        """Register compatible schemas idempotently.

        Args:
            schemas: Stable low-cardinality declarations to enforce.

        Raises:
            ValueError: If a name is redefined incompatibly.
        """
        with self._lock:
            for schema in schemas:
                existing = self._schemas.get(schema.name)
                if existing is not None and existing != schema:
                    raise ValueError(f"incompatible metric schema for {schema.name}")
                self._schemas[schema.name] = schema

    def inc(
        self,
        name: str,
        value: float = 1.0,
        labels: dict[str, str] | None = None,
    ) -> None:
        """Increment one counter."""
        self._record_counter(name, value, labels)

    def inc_sync(
        self,
        name: str,
        value: float = 1.0,
        labels: dict[str, str] | None = None,
    ) -> None:
        """Increment one counter from an explicitly synchronous call site."""
        self._record_counter(name, value, labels)

    def _record_counter(
        self,
        name: str,
        value: float,
        labels: dict[str, str] | None,
    ) -> None:
        numeric = float(value)
        if not isfinite(numeric):
            raise ValueError("counter value must be finite")
        self._validate(name, MetricKind.COUNTER, labels)
        key = _metric_key(name, labels)
        with self._lock:
            self.counters[key] += numeric

    def set_gauge(
        self,
        name: str,
        value: float,
        labels: dict[str, str] | None = None,
    ) -> None:
        """Set one gauge to a finite numeric value."""
        numeric = float(value)
        if not isfinite(numeric):
            raise ValueError("gauge value must be finite")
        self._validate(name, MetricKind.GAUGE, labels)
        key = _metric_key(name, labels)
        with self._lock:
            self.gauges[key] = numeric

    def observe(
        self,
        name: str,
        value: float,
        *,
        buckets: tuple[float, ...],
        labels: dict[str, str] | None = None,
    ) -> None:
        """Record one finite histogram value and bounded percentile sample."""
        numeric = float(value)
        if not isfinite(numeric):
            raise ValueError("histogram value must be finite")
        schema = self._schemas.get(name)
        if numeric < 0 and schema is not None and schema.non_negative:
            raise ValueError(f"histogram {name} must be non-negative")
        finite_boundaries = tuple(sorted(set(float(bucket) for bucket in buckets)))
        if not finite_boundaries or any(not isfinite(bucket) for bucket in finite_boundaries):
            raise ValueError("histogram buckets must be finite")
        self._validate(
            name,
            MetricKind.HISTOGRAM,
            labels,
            buckets=finite_boundaries,
        )
        key = _metric_key(name, labels)
        boundaries = finite_boundaries + (inf,)
        with self._lock:
            state = self.histograms.get(key)
            if state is None:
                # Percentile windows are retained only for registered schemas.
                # Legacy/unknown histograms preserve their cumulative
                # Prometheus behavior without multiplying memory by an
                # unbounded high-cardinality series count.
                retained_sample_capacity = self.sample_capacity if schema is not None else 0
                state = HistogramState(
                    name=name,
                    boundaries=boundaries,
                    counts=[0 for _ in boundaries],
                    labels=dict(labels or {}),
                    samples=deque(maxlen=retained_sample_capacity),
                )
                self.histograms[key] = state
            elif state.boundaries != boundaries:
                raise ValueError(f"histogram bucket mismatch for {name}")
            for index, boundary in enumerate(boundaries):
                if numeric <= boundary:
                    state.counts[index] += 1
            state.count += 1
            state.total += numeric
            state.samples.append(numeric)

    def snapshot(self) -> tuple[dict[str, float], dict[str, float]]:
        """Return isolated counter and gauge dictionaries."""
        with self._lock:
            return dict(self.counters), dict(self.gauges)

    def histogram_snapshots(self) -> tuple[HistogramSnapshot, ...]:
        """Return deterministic count/sum/bucket and p50/p95/p99 snapshots."""
        with self._lock:
            states = [
                (
                    state.name,
                    tuple(sorted(state.labels.items())),
                    state.count,
                    state.total,
                    tuple(zip(state.boundaries, state.counts, strict=True)),
                    tuple(state.samples),
                )
                for _, state in sorted(self.histograms.items())
            ]
        return tuple(
            HistogramSnapshot(
                name=name,
                labels=labels,
                count=count,
                total=total,
                buckets=tuple(
                    (_boundary_text(boundary), bucket_count) for boundary, bucket_count in buckets
                ),
                sample_count=len(samples),
                p50=_quantile(samples, 0.50),
                p95=_quantile(samples, 0.95),
                p99=_quantile(samples, 0.99),
            )
            for name, labels, count, total, buckets, samples in states
        )

    def clear(self) -> None:
        """Clear observations while preserving registered schemas."""
        with self._lock:
            self.counters.clear()
            self.gauges.clear()
            self.histograms.clear()

    def render_prometheus(self) -> str:
        """Render all observations in Prometheus text exposition format."""
        counters, gauges = self.snapshot()
        with self._lock:
            histograms = [
                (
                    state.name,
                    state.boundaries,
                    tuple(state.counts),
                    dict(state.labels),
                    state.count,
                    state.total,
                )
                for _, state in sorted(self.histograms.items())
            ]
        lines: list[str] = []
        for name, value in sorted(counters.items()):
            metric_name = name.split("{", 1)[0]
            lines.append(f"# TYPE {metric_name} counter")
            lines.append(f"{name} {value}")
        for name, value in sorted(gauges.items()):
            metric_name = name.split("{", 1)[0]
            lines.append(f"# TYPE {metric_name} gauge")
            lines.append(f"{name} {value}")
        for name, boundaries, counts, labels, count, total in histograms:
            lines.append(f"# TYPE {name} histogram")
            for boundary, bucket_count in zip(boundaries, counts, strict=True):
                lines.append(
                    f"{_metric_key(name + '_bucket', {**labels, 'le': _boundary_text(boundary)})} "
                    f"{bucket_count}"
                )
            lines.append(f"{_metric_key(name + '_sum', labels)} {total}")
            lines.append(f"{_metric_key(name + '_count', labels)} {count}")
        return "\n".join(lines) + "\n"

    def _validate(
        self,
        name: str,
        kind: MetricKind,
        labels: dict[str, str] | None,
        *,
        buckets: tuple[float, ...] | None = None,
    ) -> None:
        schema = self._schemas.get(name)
        if schema is None:
            return
        if schema.kind is not kind:
            raise ValueError(f"metric {name} must be recorded as {schema.kind.value}")
        schema.validate_labels(labels)
        if kind is MetricKind.HISTOGRAM and buckets != schema.buckets:
            raise ValueError(f"histogram bucket mismatch for {name}")


# Existing stable operator metric names.
METRIC_ORDERS_SUBMITTED = "orders_submitted_total"
METRIC_ORDERS_REJECTED = "orders_rejected_total"
METRIC_ORDERS_SUBMISSION_AMBIGUOUS = "orders_submission_ambiguous_total"
METRIC_RECONCILIATION_DELTA_APPLIED = "reconciliation_delta_applied_total"
METRIC_RECONCILIATION_NEGATIVE_DELTA = "reconciliation_negative_delta_total"
METRIC_RECONCILIATION_AUTO_RELEASE_AMBIGUOUS = "reconciliation_auto_release_ambiguous_total"
METRIC_COORDINATOR_MISSING_SKIP = "coordinator_missing_skip_total"
METRIC_RISK_EXIT_TRIGGERED = "risk_exits_total"
METRIC_WSS_RECONNECT = "wss_reconnect_total"
METRIC_WSS_HEARTBEAT_STALL = "wss_heartbeat_stall_total"
METRIC_CACHE_STALE_EVICTIONS = "cache_stale_evictions_total"
METRIC_COPY_LATENCY = "copy_latency_seconds"
COPY_LATENCY_BUCKETS = (0.05, 0.10, 0.25, 0.50, 1.0, 2.0, 5.0, 10.0)
GAUGE_RESERVATION_ACTIVE = "reservation_active"
GAUGE_STALE_LOCKS_ACTIVE = "stale_locks_active"
GAUGE_PENDING_INTENTS = "pending_intents"


# Leader-detection provenance. ``source`` is bounded to ``DetectionMode`` values.
METRIC_DETECTION_SIGNALS = "detection_signals_total"
METRIC_DETECTION_SIGNALS_DEDUPED = "detection_signals_deduped_total"
GAUGE_DETECTION_MODE_SELECTED = "detection_mode_selected"
DETECTION_SOURCE_LABEL_VALUES = tuple(mode.value for mode in DetectionMode)

# On-chain (Alchemy) subscription health. ``stream`` is bounded to the two
# subscription names ``OnchainDetectionRuntime`` opens — ``orderfilled`` (mined
# ``OrderFilled`` logs) and ``pendingtx`` (``alchemy_pendingTransactions``).
GAUGE_ONCHAIN_WS_CONNECTED = "onchain_ws_connected"
METRIC_ONCHAIN_WS_RECONNECT = "onchain_ws_reconnect_total"
ONCHAIN_WS_STREAM_LABEL_VALUES = ("orderfilled", "pendingtx")

# Pending-copy confirmation (``detection.mode=on_chain_pending``): a copy taken
# off a provisional mempool sighting is tracked until its leader transaction
# mines, reverts, or misses the deadline.
METRIC_PENDING_COPY_CONFIRMED = "pending_copy_confirmed_total"
METRIC_PENDING_COPY_UNCONFIRMED = "pending_copy_unconfirmed_total"
METRIC_PENDING_COPY_TRACKER_DROPPED = "pending_copy_tracker_dropped_total"
GAUGE_PENDING_COPY_TRACKED = "pending_copy_tracked"
PENDING_COPY_UNCONFIRMED_REASONS = ("reverted", "not_mined", "rpc_error")
PENDING_COPY_DROP_REASONS = ("full",)

# V1 telemetry metric names. Every label in these schemas has an explicit,
# bounded value set; opaque leader/task/order identifiers never become labels.
METRIC_TELEMETRY_LEADER_SIGNAL = "telemetry_leader_signal_total"
METRIC_TELEMETRY_CANDIDATES_EVALUATED = "telemetry_candidates_evaluated_total"
METRIC_TELEMETRY_COPY_OUTCOME = "telemetry_copy_outcome_total"
METRIC_TELEMETRY_ORDER_OUTCOME = "telemetry_order_outcome_total"
METRIC_TELEMETRY_FILL = "telemetry_fill_total"
METRIC_TELEMETRY_PROTECTIVE_EXIT = "telemetry_protective_exit_total"
METRIC_TELEMETRY_REENTRY_LOCK = "telemetry_reentry_lock_total"
METRIC_TELEMETRY_FALCON = "telemetry_falcon_total"
METRIC_TELEMETRY_WSS_RECONNECT = "telemetry_wss_reconnect_total"
METRIC_TELEMETRY_LEADER_QUEUE_OVERFLOW = "telemetry_leader_queue_overflow_total"
METRIC_TELEMETRY_QUEUE_OVERFLOW = "telemetry_queue_overflow_total"
METRIC_TELEMETRY_RECONCILIATION = "telemetry_reconciliation_total"
METRIC_TELEMETRY_LATENCY = "telemetry_latency_seconds"
METRIC_TELEMETRY_EMIT_FAILURE = "telemetry_emit_failure_total"
GAUGE_TELEMETRY_ACTIVE_ENTITIES = "telemetry_active_entities"
GAUGE_TELEMETRY_QUEUE_DEPTH = "telemetry_queue_depth"
GAUGE_TELEMETRY_QUEUE_UTILIZATION = "telemetry_queue_utilization_ratio"
GAUGE_TELEMETRY_SUBSCRIPTIONS = "telemetry_subscriptions"
GAUGE_TELEMETRY_CACHE_ENTRIES = "telemetry_cache_entries"
GAUGE_TELEMETRY_WSS_AGE = "telemetry_wss_age_seconds"
GAUGE_TELEMETRY_RECONCILIATION_AGE = "telemetry_reconciliation_age_seconds"
GAUGE_TELEMETRY_FALCON_DISCOVERY_STATE = "telemetry_falcon_discovery_state"

TELEMETRY_LATENCY_BUCKETS = (
    0.001,
    0.0025,
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
    30.0,
    60.0,
    120.0,
    300.0,
)

# Bounded producer identities. Kept as a literal so ``services.metrics`` stays
# free of a ``services.telemetry`` import cycle; ``tests/telemetry`` pins the
# tuple to ``TelemetryProducer`` so the two can never drift.
TELEMETRY_PRODUCER_LABEL_VALUES = (
    "detection",
    "normalization",
    "worker",
    "copy_decision",
    "sizing",
    "risk",
    "execution",
    "reconciliation",
    "protective_exit",
    "discovery",
    "wss",
    "runtime",
    "control_plane",
)

# Copy-level outcomes. Mirrors ``TradeOutcome`` minus ``candidate_evaluated``,
# which is counted by its own metric; pinned to the enum by a telemetry test.
TELEMETRY_COPY_OUTCOME_VALUES = (
    "submitted",
    "executed",
    "partial",
    "skipped",
    "failed",
    "ambiguous",
)

_SKIP_REASONS = (
    "none",
    "duplicate",
    "risk_rejected",
    "task_inactive",
    "schedule_closed",
    "market_not_allowed",
    "reentry_locked",
    "budget_exhausted",
    "insufficient_balance",
    "below_minimum_size",
    "stale_market_data",
    "price_limit",
    "capacity",
    "finality_retracted",
    "unsupported",
    "paper_only",
    "other",
)

TELEMETRY_METRIC_SCHEMAS: tuple[MetricSchema, ...] = (
    MetricSchema(
        METRIC_DETECTION_SIGNALS,
        MetricKind.COUNTER,
        (("source", DETECTION_SOURCE_LABEL_VALUES),),
    ),
    MetricSchema(
        METRIC_DETECTION_SIGNALS_DEDUPED,
        MetricKind.COUNTER,
        (("source", DETECTION_SOURCE_LABEL_VALUES),),
    ),
    MetricSchema(
        GAUGE_DETECTION_MODE_SELECTED,
        MetricKind.GAUGE,
        (("mode", DETECTION_SOURCE_LABEL_VALUES),),
    ),
    MetricSchema(
        GAUGE_ONCHAIN_WS_CONNECTED,
        MetricKind.GAUGE,
        (("stream", ONCHAIN_WS_STREAM_LABEL_VALUES),),
    ),
    MetricSchema(
        METRIC_ONCHAIN_WS_RECONNECT,
        MetricKind.COUNTER,
        (("stream", ONCHAIN_WS_STREAM_LABEL_VALUES),),
    ),
    MetricSchema(METRIC_PENDING_COPY_CONFIRMED, MetricKind.COUNTER),
    MetricSchema(
        METRIC_PENDING_COPY_UNCONFIRMED,
        MetricKind.COUNTER,
        (("reason", PENDING_COPY_UNCONFIRMED_REASONS),),
    ),
    MetricSchema(
        METRIC_PENDING_COPY_TRACKER_DROPPED,
        MetricKind.COUNTER,
        (("reason", PENDING_COPY_DROP_REASONS),),
    ),
    MetricSchema(GAUGE_PENDING_COPY_TRACKED, MetricKind.GAUGE),
    MetricSchema(
        METRIC_TELEMETRY_LEADER_SIGNAL,
        MetricKind.COUNTER,
        (("outcome", ("received", "dedup", "pending", "promoted", "demoted", "retracted")),),
    ),
    MetricSchema(METRIC_TELEMETRY_CANDIDATES_EVALUATED, MetricKind.COUNTER),
    MetricSchema(
        METRIC_TELEMETRY_COPY_OUTCOME,
        MetricKind.COUNTER,
        (
            ("outcome", TELEMETRY_COPY_OUTCOME_VALUES),
            ("reason", _SKIP_REASONS),
        ),
    ),
    MetricSchema(
        METRIC_TELEMETRY_ORDER_OUTCOME,
        MetricKind.COUNTER,
        (("outcome", ("submitted", "canceled", "rejected", "not_fillable", "ambiguous")),),
    ),
    MetricSchema(
        METRIC_TELEMETRY_FILL,
        MetricKind.COUNTER,
        (("outcome", ("observed", "duplicate")),),
    ),
    MetricSchema(
        METRIC_TELEMETRY_PROTECTIVE_EXIT,
        MetricKind.COUNTER,
        (("outcome", ("triggered", "filled")),),
    ),
    MetricSchema(
        METRIC_TELEMETRY_REENTRY_LOCK,
        MetricKind.COUNTER,
        (("outcome", ("created", "blocked", "reset")),),
    ),
    MetricSchema(
        METRIC_TELEMETRY_FALCON,
        MetricKind.COUNTER,
        (("outcome", ("request", "failure", "schema")),),
    ),
    MetricSchema(
        METRIC_TELEMETRY_WSS_RECONNECT,
        MetricKind.COUNTER,
        (("stream", ("leader", "market", "user")),),
    ),
    MetricSchema(METRIC_TELEMETRY_LEADER_QUEUE_OVERFLOW, MetricKind.COUNTER),
    MetricSchema(
        METRIC_TELEMETRY_QUEUE_OVERFLOW,
        MetricKind.COUNTER,
        (("queue", ("ingress", "subscriber")),),
    ),
    MetricSchema(
        METRIC_TELEMETRY_RECONCILIATION,
        MetricKind.COUNTER,
        (("outcome", ("run", "drift")),),
    ),
    MetricSchema(
        METRIC_TELEMETRY_LATENCY,
        MetricKind.HISTOGRAM,
        (("phase", tuple(phase.value for phase in LatencyPhase)),),
        TELEMETRY_LATENCY_BUCKETS,
        non_negative=True,
    ),
    MetricSchema(
        GAUGE_TELEMETRY_ACTIVE_ENTITIES,
        MetricKind.GAUGE,
        (
            (
                "entity",
                ("leaders", "tasks", "positions", "orders", "intents", "reservations", "locks"),
            ),
        ),
    ),
    MetricSchema(
        GAUGE_TELEMETRY_QUEUE_DEPTH,
        MetricKind.GAUGE,
        (("queue", ("leader", "worker", "ingress", "subscriber")),),
    ),
    MetricSchema(
        GAUGE_TELEMETRY_QUEUE_UTILIZATION,
        MetricKind.GAUGE,
        (("queue", ("leader", "worker", "ingress", "subscriber")),),
    ),
    MetricSchema(GAUGE_TELEMETRY_SUBSCRIPTIONS, MetricKind.GAUGE),
    MetricSchema(
        GAUGE_TELEMETRY_CACHE_ENTRIES,
        MetricKind.GAUGE,
        (("cache", ("market_price", "event_history")),),
    ),
    MetricSchema(
        GAUGE_TELEMETRY_WSS_AGE,
        MetricKind.GAUGE,
        (("stream", ("leader", "market", "user")),),
    ),
    MetricSchema(GAUGE_TELEMETRY_RECONCILIATION_AGE, MetricKind.GAUGE),
    MetricSchema(
        GAUGE_TELEMETRY_FALCON_DISCOVERY_STATE,
        MetricKind.GAUGE,
        (("state", ("idle", "running", "healthy", "degraded", "failed")),),
    ),
    MetricSchema(
        "telemetry_sink_failure_total",
        MetricKind.COUNTER,
        (("operation", ("write", "resync")),),
    ),
    MetricSchema(
        METRIC_TELEMETRY_EMIT_FAILURE,
        MetricKind.COUNTER,
        (("producer", TELEMETRY_PRODUCER_LABEL_VALUES),),
    ),
    MetricSchema("telemetry_out_of_order_total", MetricKind.COUNTER),
    MetricSchema(
        "telemetry_task_decision_total",
        MetricKind.COUNTER,
        (("outcome", ("enqueued", "admitted", "started", "completed", "canceled", "failed")),),
    ),
    MetricSchema(
        "telemetry_runtime_state_total",
        MetricKind.COUNTER,
        (("outcome", ("started", "ready", "degraded", "stopping", "stopped")),),
    ),
)


metrics = MetricsRegistry(schemas=TELEMETRY_METRIC_SCHEMAS)
