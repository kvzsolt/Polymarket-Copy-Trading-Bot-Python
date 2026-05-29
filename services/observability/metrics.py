"""Simple in-process metrics counters.

Thread-safe counters for execution lifecycle events so the health endpoint
and operator tooling can surface rate/volume data without external deps.
"""

from __future__ import annotations

import threading


class Metrics:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: dict[str, int] = {}
        self._gauges: dict[str, float] = {}

    def inc(self, name: str, delta: int = 1) -> None:
        with self._lock:
            self._counters[name] = self._counters.get(name, 0) + delta

    def get(self, name: str) -> int:
        with self._lock:
            return self._counters.get(name, 0)

    def set_gauge(self, name: str, value: float) -> None:
        with self._lock:
            self._gauges[name] = value

    def get_gauge(self, name: str) -> float:
        with self._lock:
            return self._gauges.get(name, 0.0)

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "counters": dict(self._counters),
                "gauges": dict(self._gauges),
            }

    def reset(self) -> None:
        with self._lock:
            self._counters.clear()
            self._gauges.clear()


# Pre-defined counter names
METRIC_INTENTS_ACCEPTED = "intents_accepted_total"
METRIC_INTENTS_SUPPRESSED = "intents_suppressed_total"
METRIC_ORDERS_SUBMITTED = "orders_submitted_total"
METRIC_ORDERS_FILLED = "orders_filled_total"
METRIC_ORDERS_CANCELLED = "orders_cancelled_total"
METRIC_ORDERS_REJECTED = "orders_rejected_total"
METRIC_RECONCILIATIONS = "reconciliations_total"
METRIC_ERRORS = "errors_total"
METRIC_GEOBLOCK_CHECKS = "geoblock_checks_total"
METRIC_WALLET_VALIDATIONS = "wallet_validations_total"


# Process-level singleton — simple, No DI framework needed.
_registry = Metrics()


def get_metrics() -> Metrics:
    return _registry
