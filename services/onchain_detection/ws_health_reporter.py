"""Operator-visible health for the on-chain (Alchemy) WebSocket subscriptions.

``clients.onchain_ws.OnchainWebSocket`` reconnects silently. In
``detection.mode=on_chain_pending`` and in ``on_chain_confirmed`` with
``confirmed_source=logs_only`` a dropped subscription means every copy
falls back to REST polling — correct, but seconds slower and invisible: no
metric, no readiness signal, only an INFO log line. This module turns the three
connection-lifecycle transitions into:

* ``onchain_ws_connected{stream}`` — 1 while subscribed, 0 while down;
* ``onchain_ws_reconnect_total{stream}`` — reconnect attempts;
* one **non-required** readiness check (``onchain_ws_ok``).

The check is deliberately not required for strict readiness: REST polling keeps
copying in every detection mode, so a WSS drop is *degraded*, not *unready*,
and marking it required would flap the container health target on a routine
Alchemy reconnect.
"""

from __future__ import annotations

from services.healthServer import ReadinessTracker
from services.metrics import (
    GAUGE_ONCHAIN_WS_CONNECTED,
    METRIC_ONCHAIN_WS_RECONNECT,
    MetricsRegistry,
)

#: Bounded reason code reported while no subscription is connected.
DISCONNECTED_REASON = "ws_disconnected"


class OnchainWsHealthReporter:
    """Project on-chain WebSocket connection state onto metrics + readiness.

    One reporter serves every subscription of one runtime: the gauge and the
    reconnect counter are per ``stream``, while the single readiness check is
    ready whenever *any* subscription is connected (the runtime opens only the
    subscriptions its mode needs, so requiring all of them would report the
    unused one as down forever). Connections are **counted per stream name**:
    ``logs_only`` confirmed mode opens two sockets that both report as
    ``orderfilled``, and one of them dropping must not mark the stream down
    while its sibling is still subscribed.
    """

    def __init__(
        self,
        metrics: MetricsRegistry,
        readiness: ReadinessTracker,
        *,
        check_key: str = "onchain_ws_ok",
    ) -> None:
        """Register the readiness check and bind the metrics registry.

        Args:
            metrics: Registry that owns the bounded on-chain WS schemas.
            readiness: Tracker the check is registered on.
            check_key: Stable readiness key; one reporter owns one key.

        Raises:
            ValueError: If ``check_key`` is already registered on ``readiness``
                (two reporters may not share one key).
        """

        self._metrics = metrics
        self._readiness = readiness
        self._check_key = check_key
        self._connection_counts: dict[str, int] = {}
        self._readiness.register_check(check_key, required=False, ready=False)

    @property
    def check_key(self) -> str:
        """Return the readiness key this reporter owns."""

        return self._check_key

    @property
    def connected_streams(self) -> frozenset[str]:
        """Return the stream names with at least one subscribed socket."""

        return frozenset(self._connection_counts)

    def on_connected(self, stream: str) -> None:
        """Record that one socket of ``stream`` is subscribed.

        Args:
            stream: Subscription name (bounded by the metric schema).
        """

        self._metrics.set_gauge(GAUGE_ONCHAIN_WS_CONNECTED, 1.0, labels={"stream": stream})
        self._connection_counts[stream] = self._connection_counts.get(stream, 0) + 1
        self._readiness.set_check(self._check_key, ready=True)

    def on_disconnected(self, stream: str) -> None:
        """Record that one socket of ``stream`` lost its connection.

        Args:
            stream: Subscription name (bounded by the metric schema).
        """

        remaining = max(0, self._connection_counts.get(stream, 0) - 1)
        if remaining:
            self._connection_counts[stream] = remaining
            return
        self._connection_counts.pop(stream, None)
        self._metrics.set_gauge(GAUGE_ONCHAIN_WS_CONNECTED, 0.0, labels={"stream": stream})
        if not self._connection_counts:
            self._readiness.set_check(
                self._check_key,
                ready=False,
                reason=DISCONNECTED_REASON,
            )

    def on_reconnect_scheduled(self, stream: str) -> None:
        """Count one scheduled reconnect for ``stream``.

        Args:
            stream: Subscription name (bounded by the metric schema).
        """

        self._metrics.inc(METRIC_ONCHAIN_WS_RECONNECT, labels={"stream": stream})


__all__ = ["DISCONNECTED_REASON", "OnchainWsHealthReporter"]
