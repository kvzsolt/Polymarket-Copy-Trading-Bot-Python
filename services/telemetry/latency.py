"""Bounded session-local latency measurements at actual application boundaries."""

import logging
from collections import OrderedDict
from types import TracebackType

from domain.source_time import TransportReceipt
from interfaces.source_time import ReceiptClock
from services.metrics import (
    LatencyPhase,
    METRIC_TELEMETRY_LATENCY,
    MetricsRegistry,
    TELEMETRY_LATENCY_BUCKETS,
)

_LOGGER = logging.getLogger(__name__)


class PipelineLatency:
    """Measure elapsed monotonic time without creating durable execution ownership.

    Args:
        clock: Receipt clock sharing the transport boot identity.
        metrics: Existing bounded process metrics registry.
        max_orders: Maximum acknowledged orders awaiting first fill evidence.

    Missing or evicted traces remain missing evidence. Restart never creates
    measurements from persisted wall time or another boot's monotonic clock.
    """

    def __init__(
        self, *, clock: ReceiptClock, metrics: MetricsRegistry, max_orders: int = 4096
    ) -> None:
        if isinstance(max_orders, bool) or not isinstance(max_orders, int) or max_orders <= 0:
            raise ValueError("max_orders must be a positive integer")
        self._clock = clock
        self._metrics = metrics
        self._max_orders = max_orders
        self._orders: OrderedDict[str, TransportReceipt] = OrderedDict()

    @property
    def pending_orders(self) -> int:
        """Return the bounded count of process-local first-fill traces."""
        return len(self._orders)

    def capture(self) -> TransportReceipt | None:
        """Sample this boundary; a clock failure yields missing evidence."""
        try:
            return self._clock.capture()
        except Exception as exc:
            self._failed(exc)
            return None

    def span(self, phase: LatencyPhase) -> "LatencyMeasurement":
        """Start one idempotent measurement that also finishes on failure."""
        return LatencyMeasurement(self, phase, self.capture())

    def observe(self, phase: LatencyPhase, started: TransportReceipt | None) -> None:
        """Record seconds only for a known start in this monotonic session."""
        try:
            ended = self._clock.capture()
            elapsed = ended.elapsed_since(started) if started is not None else None
            if elapsed is None:
                self._gap("clock_unavailable")
                return
            self._metrics.observe(
                METRIC_TELEMETRY_LATENCY,
                elapsed / 1_000_000_000,
                buckets=TELEMETRY_LATENCY_BUCKETS,
                labels={"phase": phase.value},
            )
        except Exception as exc:
            self._failed(exc)

    def acknowledged(self, order_id: str, started: TransportReceipt | None) -> None:
        """Measure an actual acknowledgement and retain its first-fill start."""
        if order_id in self._orders:
            return
        self.observe(LatencyPhase.SUBMISSION_TO_FIRST_ACK, started)
        if started is None:
            return
        self._orders[order_id] = started
        if len(self._orders) > self._max_orders:
            self._orders.popitem(last=False)
            self._gap("order_capacity")

    def first_fill(self, order_id: str) -> None:
        """Measure first fill evidence once when this boot observed submission."""
        started = self._orders.pop(order_id, None)
        if started is not None:
            self.observe(LatencyPhase.SUBMISSION_TO_FIRST_FILL_EVIDENCE, started)

    def _gap(self, reason: str) -> None:
        try:
            self._metrics.inc("pipeline_latency_gap_total", labels={"reason": reason})
        except Exception as exc:
            self._failed(exc)

    @staticmethod
    def _failed(error: Exception) -> None:
        _LOGGER.warning("pipeline_latency_unavailable | error_type=%s", type(error).__name__)


class LatencyMeasurement:
    """Own one phase duration, ending explicitly or when its context exits."""

    def __init__(
        self, owner: PipelineLatency, phase: LatencyPhase, started: TransportReceipt | None
    ) -> None:
        self._owner = owner
        self._phase = phase
        self._started = started
        self._finished = False

    def __enter__(self) -> "LatencyMeasurement":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.finish()

    def finish(self) -> None:
        """Observe this phase once; subsequent cleanup cannot double count it."""
        if not self._finished:
            self._finished = True
            self._owner.observe(self._phase, self._started)
