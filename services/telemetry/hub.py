"""Bounded, nonblocking in-process fan-out for telemetry events."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from enum import StrEnum
from uuid import uuid4

from services.metrics import (
    TELEMETRY_METRIC_SCHEMAS,
    MetricsRegistry,
    metrics as default_metrics,
)
from services.telemetry.contracts import TelemetryEvent, TelemetryProducer


_OVERFLOW_METRIC = "telemetry_queue_overflow_total"
_OUT_OF_ORDER_METRIC = "telemetry_out_of_order_total"
_QUEUE_DEPTH_GAUGE = "telemetry_queue_depth"
_QUEUE_UTILIZATION_GAUGE = "telemetry_queue_utilization_ratio"
_SUBSCRIPTIONS_GAUGE = "telemetry_subscriptions"


class HubPublishStatus(StrEnum):
    """Result of a nonblocking hub publication."""

    ACCEPTED = "accepted"
    DROPPED_OVERFLOW = "dropped_overflow"
    DROPPED_CLOSED = "dropped_closed"
    DROPPED_OUT_OF_ORDER = "dropped_out_of_order"


@dataclass(frozen=True, slots=True)
class HubPublishResult:
    """Return the event together with its bounded publication status."""

    status: HubPublishStatus
    event: TelemetryEvent

    @property
    def accepted(self) -> bool:
        """Return whether the event entered the bounded ingress queue."""
        return self.status is HubPublishStatus.ACCEPTED


@dataclass(frozen=True, slots=True)
class TelemetryDelivery:
    """Deliver an event or instruct a slow subscriber to resynchronize."""

    event: TelemetryEvent | None = None
    resync_required: bool = False
    dropped_count: int = 0
    stream_closed: bool = False


class TelemetrySubscription:
    """One bounded subscriber queue owned by a :class:`TelemetryHub`."""

    def __init__(self, *, subscription_id: int, capacity: int) -> None:
        if capacity <= 0:
            raise ValueError("subscriber capacity must be positive")
        self._subscription_id = subscription_id
        self._capacity = capacity
        self._queue: asyncio.Queue[TelemetryDelivery] = asyncio.Queue(maxsize=capacity)
        self._awaiting_resync = False
        self._dropped_while_waiting = 0
        self._closed = False

    @property
    def subscription_id(self) -> int:
        """Return the internal stable subscription identifier."""
        return self._subscription_id

    @property
    def capacity(self) -> int:
        """Return the maximum pending delivery count."""
        return self._capacity

    @property
    def depth(self) -> int:
        """Return the current bounded queue depth."""
        return self._queue.qsize()

    @property
    def closed(self) -> bool:
        """Return whether shutdown has closed the subscription."""
        return self._closed

    async def get(self) -> TelemetryDelivery:
        """Wait for one event, resync notice, or shutdown notice.

        Returns:
            The next bounded delivery in FIFO order.
        """
        delivery = await self._queue.get()
        self._queue.task_done()
        if delivery.resync_required:
            self._awaiting_resync = False
            self._dropped_while_waiting = 0
        return delivery

    def get_nowait(self) -> TelemetryDelivery:
        """Return one queued delivery without blocking.

        Returns:
            The next bounded delivery in FIFO order.

        Raises:
            asyncio.QueueEmpty: If no delivery is ready.
        """
        delivery = self._queue.get_nowait()
        self._queue.task_done()
        if delivery.resync_required:
            self._awaiting_resync = False
            self._dropped_while_waiting = 0
        return delivery

    def _deliver_nowait(self, event: TelemetryEvent) -> int:
        if self._closed:
            return 1
        if self._awaiting_resync:
            self._dropped_while_waiting += 1
            try:
                self._queue.get_nowait()
                self._queue.task_done()
            except asyncio.QueueEmpty:
                # All subscription operations run on the owning event loop;
                # this is defensive for an external close race.
                pass
            self._queue.put_nowait(
                TelemetryDelivery(
                    resync_required=True,
                    dropped_count=self._dropped_while_waiting,
                )
            )
            return 1
        try:
            self._queue.put_nowait(TelemetryDelivery(event=event))
            return 0
        except asyncio.QueueFull:
            dropped = 1
            while True:
                try:
                    self._queue.get_nowait()
                    self._queue.task_done()
                    dropped += 1
                except asyncio.QueueEmpty:
                    break
            self._awaiting_resync = True
            self._dropped_while_waiting = dropped
            self._queue.put_nowait(
                TelemetryDelivery(
                    resync_required=True,
                    dropped_count=dropped,
                )
            )
            return dropped

    def _close(self) -> None:
        if self._closed:
            return
        self._closed = True
        while True:
            try:
                self._queue.get_nowait()
                self._queue.task_done()
            except asyncio.QueueEmpty:
                break
        self._queue.put_nowait(TelemetryDelivery(stream_closed=True))


class TelemetryHub:
    """Fan events out through bounded queues without awaiting producers."""

    def __init__(
        self,
        *,
        ingress_capacity: int = 1024,
        default_subscriber_capacity: int = 256,
        max_subscribers: int = 32,
        metrics: MetricsRegistry | None = None,
        boot_id: str | None = None,
    ) -> None:
        if ingress_capacity <= 0:
            raise ValueError("ingress_capacity must be positive")
        if default_subscriber_capacity <= 0:
            raise ValueError("default_subscriber_capacity must be positive")
        if max_subscribers <= 0:
            raise ValueError("max_subscribers must be positive")
        identity = uuid4().hex if boot_id is None else boot_id
        if (
            not isinstance(identity, str)
            or not 1 <= len(identity) <= 128
            or any(
                not (character.isascii() and (character.isalnum() or character in "_-"))
                for character in identity
            )
        ):
            raise ValueError("Telemetry boot identity must be a bounded safe token")
        self._boot_id = identity
        self._ingress_capacity = ingress_capacity
        self._default_subscriber_capacity = default_subscriber_capacity
        self._max_subscribers = max_subscribers
        self._metrics = metrics if metrics is not None else default_metrics
        self._metrics.register_schemas(TELEMETRY_METRIC_SCHEMAS)
        self._ingress: asyncio.Queue[TelemetryEvent] = asyncio.Queue(maxsize=ingress_capacity)
        self._subscriptions: dict[int, TelemetrySubscription] = {}
        self._next_subscription_id = 1
        self._last_sequence: dict[TelemetryProducer, int] = {}
        self._dispatcher: asyncio.Task[None] | None = None
        self._closed = False

    @property
    def boot_id(self) -> str:
        """Return the session identity; producer sequences are scoped to this hub."""
        return self._boot_id

    @property
    def closed(self) -> bool:
        """Return whether the hub has completed shutdown."""
        return self._closed

    @property
    def running(self) -> bool:
        """Return whether the dispatcher task is active."""
        return self._dispatcher is not None and not self._dispatcher.done()

    @property
    def ingress_depth(self) -> int:
        """Return current ingress depth."""
        return self._ingress.qsize()

    @property
    def subscription_count(self) -> int:
        """Return the number of open subscribers."""
        return len(self._subscriptions)

    @property
    def metrics_registry(self) -> MetricsRegistry:
        """Return the single metrics registry used by this hub."""
        return self._metrics

    async def start(self) -> None:
        """Start the single FIFO fan-out dispatcher.

        Raises:
            RuntimeError: If the hub was already closed.
        """
        if self._closed:
            raise RuntimeError("closed telemetry hub cannot be restarted")
        if self.running:
            return
        self._dispatcher = asyncio.create_task(
            self._dispatch(),
            name="telemetry-hub-dispatch",
        )
        self._update_gauges()

    def subscribe(self, *, capacity: int | None = None) -> TelemetrySubscription:
        """Create one bounded subscriber queue.

        Args:
            capacity: Optional subscriber-specific capacity.

        Returns:
            A subscription that receives accepted events in FIFO order.

        Raises:
            RuntimeError: If the hub is closed.
        """
        if self._closed:
            raise RuntimeError("cannot subscribe to a closed telemetry hub")
        if len(self._subscriptions) >= self._max_subscribers:
            raise RuntimeError("telemetry hub subscriber capacity reached")
        bounded_capacity = self._default_subscriber_capacity if capacity is None else capacity
        subscription = TelemetrySubscription(
            subscription_id=self._next_subscription_id,
            capacity=bounded_capacity,
        )
        self._next_subscription_id += 1
        self._subscriptions[subscription.subscription_id] = subscription
        self._update_gauges()
        return subscription

    def unsubscribe(self, subscription: TelemetrySubscription) -> None:
        """Close and remove one subscriber without blocking."""
        current = self._subscriptions.pop(subscription.subscription_id, None)
        if current is not None:
            current._close()
        self._update_gauges()

    def publish_nowait(self, event: TelemetryEvent) -> HubPublishResult:
        """Attempt to enqueue an event without blocking or awaiting.

        The accepted path deliberately performs no gauge writes: the queue
        gauges are refreshed by the dispatcher for every drained event, so
        touching them here only added a metrics-lock round trip to the
        detection hot path. Overflow keeps its refresh because a dropped event
        is never drained and would otherwise leave the gauges stale.

        Args:
            event: Immutable event to enqueue.

        Returns:
            A status that makes every drop explicit to the producer.
        """
        if self._closed or not self.running:
            return HubPublishResult(HubPublishStatus.DROPPED_CLOSED, event)
        last_sequence = self._last_sequence.get(event.producer, 0)
        if event.sequence <= last_sequence:
            self._metrics.inc(_OUT_OF_ORDER_METRIC)
            return HubPublishResult(HubPublishStatus.DROPPED_OUT_OF_ORDER, event)
        try:
            self._ingress.put_nowait(event)
        except asyncio.QueueFull:
            self._metrics.inc(_OVERFLOW_METRIC, labels={"queue": "ingress"})
            self._update_gauges()
            return HubPublishResult(HubPublishStatus.DROPPED_OVERFLOW, event)
        self._last_sequence[event.producer] = event.sequence
        return HubPublishResult(HubPublishStatus.ACCEPTED, event)

    async def close(self) -> None:
        """Cancel dispatch and close all queues deterministically."""
        if self._closed:
            return
        self._closed = True
        dispatcher = self._dispatcher
        self._dispatcher = None
        if dispatcher is not None:
            dispatcher.cancel()
            try:
                await dispatcher
            except asyncio.CancelledError:
                pass
        while True:
            try:
                self._ingress.get_nowait()
                self._ingress.task_done()
            except asyncio.QueueEmpty:
                break
        subscriptions = tuple(self._subscriptions.values())
        self._subscriptions.clear()
        for subscription in subscriptions:
            subscription._close()
        self._update_gauges()

    async def _dispatch(self) -> None:
        while True:
            event = await self._ingress.get()
            try:
                for subscription in tuple(self._subscriptions.values()):
                    dropped = subscription._deliver_nowait(event)
                    if dropped:
                        self._metrics.inc(
                            _OVERFLOW_METRIC,
                            value=float(dropped),
                            labels={"queue": "subscriber"},
                        )
            finally:
                self._ingress.task_done()
                self._update_gauges()

    def _update_gauges(self) -> None:
        ingress_depth = self._ingress.qsize()
        subscriber_depth = sum(subscription.depth for subscription in self._subscriptions.values())
        subscriber_capacity = sum(
            subscription.capacity for subscription in self._subscriptions.values()
        )
        self._metrics.set_gauge(
            _QUEUE_DEPTH_GAUGE,
            float(ingress_depth),
            labels={"queue": "ingress"},
        )
        self._metrics.set_gauge(
            _QUEUE_DEPTH_GAUGE,
            float(subscriber_depth),
            labels={"queue": "subscriber"},
        )
        self._metrics.set_gauge(
            _QUEUE_UTILIZATION_GAUGE,
            ingress_depth / self._ingress_capacity,
            labels={"queue": "ingress"},
        )
        self._metrics.set_gauge(
            _QUEUE_UTILIZATION_GAUGE,
            0.0 if subscriber_capacity == 0 else subscriber_depth / subscriber_capacity,
            labels={"queue": "subscriber"},
        )
        self._metrics.set_gauge(_SUBSCRIPTIONS_GAUGE, float(len(self._subscriptions)))
