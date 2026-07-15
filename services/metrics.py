from __future__ import annotations

import threading
from collections import defaultdict
from dataclasses import dataclass, field
from math import inf



def _metric_key(name: str, labels: dict[str, str] | None = None) -> str:
    if not labels:
        return name
    parts = [f'{k}="{v}"' for k, v in sorted(labels.items())]
    return f"{name}{{{','.join(parts)}}}"


@dataclass
class HistogramState:
    name: str
    boundaries: tuple[float, ...]
    counts: list[int]
    labels: dict[str, str]
    count: int = 0
    total: float = 0.0


@dataclass
class MetricsRegistry:
    counters: dict[str, float] = field(default_factory=lambda: defaultdict(float))
    gauges: dict[str, float] = field(default_factory=lambda: defaultdict(float))
    histograms: dict[str, HistogramState] = field(default_factory=dict)
    _lock: threading.Lock = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._lock = threading.Lock()

    def inc(self, name: str, value: float = 1.0, labels: dict[str, str] | None = None) -> None:
        key = _metric_key(name, labels)
        with self._lock:
            self.counters[key] += value

    def inc_sync(self, name: str, value: float = 1.0, labels: dict[str, str] | None = None) -> None:
        """Counter bump for sync code paths (best-effort precision).

        Use only from synchronous call sites where blocking for the
        lock is acceptable. Counter values are approximate under
        contention; that's acceptable for ops dashboards.
        """
        key = _metric_key(name, labels)
        with self._lock:
            self.counters[key] += value

    def set_gauge(self, name: str, value: float, labels: dict[str, str] | None = None) -> None:
        key = _metric_key(name, labels)
        with self._lock:
            self.gauges[key] = value

    def observe(
        self,
        name: str,
        value: float,
        *,
        buckets: tuple[float, ...],
        labels: dict[str, str] | None = None,
    ) -> None:
        key = _metric_key(name, labels)
        boundaries = tuple(sorted(set(float(bucket) for bucket in buckets))) + (inf,)
        with self._lock:
            state = self.histograms.get(key)
            if state is None:
                state = HistogramState(
                    name=name,
                    boundaries=boundaries,
                    counts=[0 for _ in boundaries],
                    labels=dict(labels or {}),
                )
                self.histograms[key] = state
            elif state.boundaries != boundaries:
                raise ValueError(f"histogram bucket mismatch for {name}")
            for index, boundary in enumerate(boundaries):
                if value <= boundary:
                    state.counts[index] += 1
            state.count += 1
            state.total += float(value)

    def snapshot(self) -> tuple[dict[str, float], dict[str, float]]:
        with self._lock:
            return dict(self.counters), dict(self.gauges)

    def clear(self) -> None:
        with self._lock:
            self.counters.clear()
            self.gauges.clear()
            self.histograms.clear()

    def render_prometheus(self) -> str:
        counters, gauges = self.snapshot()
        with self._lock:
            histograms = [
                HistogramState(
                    name=state.name,
                    boundaries=state.boundaries,
                    counts=list(state.counts),
                    labels=dict(state.labels),
                    count=state.count,
                    total=state.total,
                )
                for state in self.histograms.values()
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
        for state in histograms:
            name = state.name
            labels = state.labels
            lines.append(f"# TYPE {name} histogram")
            for boundary, count in zip(state.boundaries, state.counts):
                le = "+Inf" if boundary == inf else str(boundary)
                lines.append(
                    f"{_metric_key(name + '_bucket', {**labels, 'le': le})} {count}"
                )
            lines.append(
                f"{_metric_key(name + '_sum', labels)} {state.total}"
            )
            lines.append(
                f"{_metric_key(name + '_count', labels)} {state.count}"
            )
        return "\n".join(lines) + "\n"


metrics = MetricsRegistry()


# ── Operator-facing metric names (issue #76) ─────────────────────────────────
# Use these constants when emitting so names stay stable across the codebase.
METRIC_ORDERS_SUBMITTED = "orders_submitted_total"
METRIC_ORDERS_REJECTED = "orders_rejected_total"
METRIC_ORDERS_SUBMISSION_AMBIGUOUS = "orders_submission_ambiguous_total"
METRIC_RECONCILIATION_DELTA_APPLIED = "reconciliation_delta_applied_total"
METRIC_RECONCILIATION_NEGATIVE_DELTA = "reconciliation_negative_delta_total"
METRIC_RECONCILIATION_AUTO_RELEASE_AMBIGUOUS = "reconciliation_auto_release_ambiguous_total"
METRIC_COORDINATOR_MISSING_SKIP = "coordinator_missing_skip_total"
METRIC_RISK_EXIT_TRIGGERED = "risk_exits_total"  # legacy site already emits this name
METRIC_WSS_RECONNECT = "wss_reconnect_total"
METRIC_WSS_HEARTBEAT_STALL = "wss_heartbeat_stall_total"
METRIC_CACHE_STALE_EVICTIONS = "cache_stale_evictions_total"
METRIC_COPY_LATENCY = "copy_latency_seconds"
COPY_LATENCY_BUCKETS = (0.05, 0.10, 0.25, 0.50, 1.0, 2.0, 5.0, 10.0)
# Gauges
GAUGE_RESERVATION_ACTIVE = "reservation_active"
GAUGE_STALE_LOCKS_ACTIVE = "stale_locks_active"
GAUGE_PENDING_INTENTS = "pending_intents"
