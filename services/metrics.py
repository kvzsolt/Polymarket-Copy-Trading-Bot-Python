from __future__ import annotations

import threading
from collections import defaultdict
from dataclasses import dataclass, field



def _metric_key(name: str, labels: dict[str, str] | None = None) -> str:
    if not labels:
        return name
    parts = [f'{k}="{v}"' for k, v in sorted(labels.items())]
    return f"{name}{{{','.join(parts)}}}"


@dataclass
class MetricsRegistry:
    counters: dict[str, float] = field(default_factory=lambda: defaultdict(float))
    gauges: dict[str, float] = field(default_factory=lambda: defaultdict(float))
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

    def snapshot(self) -> tuple[dict[str, float], dict[str, float]]:
        with self._lock:
            return dict(self.counters), dict(self.gauges)

    def clear(self) -> None:
        with self._lock:
            self.counters.clear()
            self.gauges.clear()

    def render_prometheus(self) -> str:
        counters, gauges = self.snapshot()
        lines: list[str] = []
        for name, value in sorted(counters.items()):
            metric_name = name.split("{", 1)[0]
            lines.append(f"# TYPE {metric_name} counter")
            lines.append(f"{name} {value}")
        for name, value in sorted(gauges.items()):
            metric_name = name.split("{", 1)[0]
            lines.append(f"# TYPE {metric_name} gauge")
            lines.append(f"{name} {value}")
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
METRIC_RISK_EXIT_TRIGGERED = "risk_exits_total"  # legacy site already emits this name
METRIC_WSS_RECONNECT = "wss_reconnect_total"
METRIC_WSS_HEARTBEAT_STALL = "wss_heartbeat_stall_total"
METRIC_CACHE_STALE_EVICTIONS = "cache_stale_evictions_total"
# Gauges
GAUGE_RESERVATION_ACTIVE = "reservation_active"
GAUGE_STALE_LOCKS_ACTIVE = "stale_locks_active"
GAUGE_PENDING_INTENTS = "pending_intents"