"""Tests for the on-chain WebSocket health surface.

``clients.onchain_ws.OnchainWebSocket`` reconnects silently: a dropped Alchemy
subscription degrades ``detection.mode=on_chain_pending`` to REST polling with
nothing visible to the operator. ``OnchainWsHealthReporter`` turns the three
connection-lifecycle transitions into a bounded gauge, a reconnect counter, and
one non-required readiness check.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

import pytest

from pmcopy.clients.onchain_ws import OnchainWebSocket, OnchainWsStateListener
from pmcopy.services.healthServer import ReadinessTracker
from pmcopy.services.metrics import (
    GAUGE_ONCHAIN_WS_CONNECTED,
    METRIC_ONCHAIN_WS_RECONNECT,
    ONCHAIN_WS_STREAM_LABEL_VALUES,
    MetricKind,
    MetricsRegistry,
    TELEMETRY_METRIC_SCHEMAS,
)
from pmcopy.services.onchain_detection.ws_health_reporter import OnchainWsHealthReporter

_WS_URL = "wss://polygon-mainnet.g.alchemy.com/v2/SuperSecretAlchemyKey123"


def _reporter(
    *,
    metrics: MetricsRegistry | None = None,
    readiness: ReadinessTracker | None = None,
) -> tuple[OnchainWsHealthReporter, MetricsRegistry, ReadinessTracker]:
    metrics = metrics or MetricsRegistry(schemas=TELEMETRY_METRIC_SCHEMAS)
    readiness = readiness or ReadinessTracker()
    return OnchainWsHealthReporter(metrics, readiness), metrics, readiness


def _check(readiness: ReadinessTracker, key: str = "onchain_ws_ok") -> dict[str, Any]:
    return readiness.snapshot()["checks"][key]


# ---------------------------------------------------------------------------
# Reporter
# ---------------------------------------------------------------------------


def test_reporter_registers_a_non_required_readiness_check() -> None:
    """A WSS drop must be visible without failing strict readiness.

    REST polling keeps copying in every detection mode, so a disconnected
    subscription is degraded — not unready. ``required=True`` would flap the
    Docker HEALTHCHECK target on a routine Alchemy reconnect.
    """
    _reporter_obj, _metrics, readiness = _reporter()

    check = _check(readiness)
    assert check["required"] is False
    assert check["ready"] is False


def test_unready_before_first_connect_reads_not_connected_not_ws_down() -> None:
    """Zero sockets (logs_only idling on an empty roster, or plain startup)
    must not read as a WSS outage: the bounded reason is ``not_connected``,
    distinct from ``ws_disconnected``, and the first connection clears it."""
    from pmcopy.services.onchain_detection.ws_health_reporter import NOT_CONNECTED_REASON

    reporter, _metrics, readiness = _reporter()

    check = _check(readiness)
    assert check["ready"] is False
    assert check["reason"] == NOT_CONNECTED_REASON
    assert check["reason"] != "ws_disconnected"

    reporter.on_connected("orderfilled")
    assert _check(readiness)["reason"] is None


def test_reporter_satisfies_the_listener_protocol() -> None:
    reporter, _metrics, _readiness = _reporter()

    assert isinstance(reporter, OnchainWsStateListener)


def test_connected_sets_the_gauge_and_marks_the_check_ready() -> None:
    reporter, metrics, readiness = _reporter()

    reporter.on_connected("pendingtx")

    assert metrics.gauges[f'{GAUGE_ONCHAIN_WS_CONNECTED}{{stream="pendingtx"}}'] == 1.0
    check = _check(readiness)
    assert check["ready"] is True
    assert check["reason"] is None


def test_disconnected_clears_the_gauge_and_reports_the_reason() -> None:
    reporter, metrics, readiness = _reporter()
    reporter.on_connected("pendingtx")

    reporter.on_disconnected("pendingtx")

    assert metrics.gauges[f'{GAUGE_ONCHAIN_WS_CONNECTED}{{stream="pendingtx"}}'] == 0.0
    check = _check(readiness)
    assert check["ready"] is False
    assert check["reason"] == "ws_disconnected"


def test_reconnect_scheduled_counts_per_stream() -> None:
    reporter, metrics, _readiness = _reporter()

    reporter.on_reconnect_scheduled("pendingtx")
    reporter.on_reconnect_scheduled("pendingtx")
    reporter.on_reconnect_scheduled("orderfilled")

    assert metrics.counters[f'{METRIC_ONCHAIN_WS_RECONNECT}{{stream="pendingtx"}}'] == 2
    assert metrics.counters[f'{METRIC_ONCHAIN_WS_RECONNECT}{{stream="orderfilled"}}'] == 1


def test_a_second_stream_disconnect_does_not_unset_a_healthy_one() -> None:
    """Two subscriptions share one check; readiness follows the connected set."""
    reporter, _metrics, readiness = _reporter()
    reporter.on_connected("pendingtx")
    reporter.on_connected("orderfilled")

    reporter.on_disconnected("orderfilled")

    assert _check(readiness)["ready"] is True

    reporter.on_disconnected("pendingtx")

    assert _check(readiness)["ready"] is False


def test_two_sockets_sharing_one_stream_name_keep_the_check_ready() -> None:
    """``logs_only`` opens two ``orderfilled`` sockets (maker/taker filters).

    One socket's disconnect must not mark the shared stream down while its
    sibling is still subscribed; the gauge and readiness follow the per-name
    connection count, not just the last event.
    """
    reporter, metrics, readiness = _reporter()
    reporter.on_connected("orderfilled")
    reporter.on_connected("orderfilled")

    reporter.on_disconnected("orderfilled")

    assert metrics.gauges[f'{GAUGE_ONCHAIN_WS_CONNECTED}{{stream="orderfilled"}}'] == 1.0
    assert _check(readiness)["ready"] is True
    assert reporter.connected_streams == frozenset({"orderfilled"})

    reporter.on_disconnected("orderfilled")

    assert metrics.gauges[f'{GAUGE_ONCHAIN_WS_CONNECTED}{{stream="orderfilled"}}'] == 0.0
    assert _check(readiness)["ready"] is False
    assert reporter.connected_streams == frozenset()


def test_unknown_stream_names_are_rejected_by_the_schema() -> None:
    reporter, _metrics, _readiness = _reporter()

    with pytest.raises(ValueError):
        reporter.on_connected("mempool")


# ---------------------------------------------------------------------------
# Metric schemas
# ---------------------------------------------------------------------------


def test_onchain_ws_metric_schemas_are_bounded() -> None:
    schemas = {schema.name: schema for schema in TELEMETRY_METRIC_SCHEMAS}

    connected = schemas[GAUGE_ONCHAIN_WS_CONNECTED]
    assert connected.kind is MetricKind.GAUGE
    assert connected.allowed_label_values == (("stream", ONCHAIN_WS_STREAM_LABEL_VALUES),)

    reconnects = schemas[METRIC_ONCHAIN_WS_RECONNECT]
    assert reconnects.kind is MetricKind.COUNTER
    assert reconnects.allowed_label_values == (("stream", ONCHAIN_WS_STREAM_LABEL_VALUES),)

    registry = MetricsRegistry(schemas=TELEMETRY_METRIC_SCHEMAS)
    with pytest.raises(ValueError):
        registry.inc(METRIC_ONCHAIN_WS_RECONNECT, labels={"stream": "mempool"})


async def test_stream_label_values_match_the_runtime_subscription_names() -> None:
    """The bounded label set must cover every stream the runtime opens.

    ``OnchainDetectionRuntime`` names its two subscriptions inline; a rename
    there would make every health metric raise on an unbounded label instead
    of reporting the drop.
    """
    from pmcopy.orchestrators.onchain_detection_runtime import OnchainDetectionRuntime

    opened: list[str] = []

    class _FakeSocket:
        def __init__(self, **kwargs: Any) -> None:
            opened.append(str(kwargs["name"]))

        def start(self) -> None:
            return None

        async def stop(self) -> None:
            return None

    async def _sink(_signal) -> None:  # pragma: no cover - never dispatched
        raise AssertionError("no signal expected")

    runtime = OnchainDetectionRuntime(
        watched_wallets={"0x" + "11" * 20},
        ws_url=_WS_URL,
        signal_sink=_sink,
        enable_pending=True,
        enable_mined=True,
        websocket_factory=_FakeSocket,
    )
    await runtime.start()
    await runtime.stop()

    assert set(opened) == set(ONCHAIN_WS_STREAM_LABEL_VALUES)


# ---------------------------------------------------------------------------
# OnchainWebSocket -> listener
# ---------------------------------------------------------------------------


class _RecordingListener:
    """Records lifecycle callbacks; optionally raises to prove they are guarded."""

    def __init__(self, *, raises: bool = False) -> None:
        self.events: list[tuple[str, str]] = []
        self._raises = raises

    def on_connected(self, stream: str) -> None:
        self.events.append(("connected", stream))
        if self._raises:
            raise RuntimeError("listener exploded")

    def on_disconnected(self, stream: str) -> None:
        self.events.append(("disconnected", stream))
        if self._raises:
            raise RuntimeError("listener exploded")

    def on_reconnect_scheduled(self, stream: str) -> None:
        self.events.append(("reconnect", stream))
        if self._raises:
            raise RuntimeError("listener exploded")


class _FakeSocket:
    """One connect cycle: subscribe ack, then an immediate stream end."""

    def __init__(self, sub_id: str = "sub-1") -> None:
        self._ack = json.dumps({"id": 1, "jsonrpc": "2.0", "result": sub_id})
        self._recv_calls = 0

    async def send(self, _payload: str) -> None:
        return None

    async def recv(self) -> str:
        self._recv_calls += 1
        if self._recv_calls == 1:
            return self._ack
        await asyncio.sleep(0)
        raise asyncio.TimeoutError

    def __aiter__(self):
        return self

    async def __anext__(self):
        raise StopAsyncIteration

    async def close(self) -> None:
        return None


async def _drive_one_cycle(
    monkeypatch: pytest.MonkeyPatch,
    listener: Any,
) -> OnchainWebSocket:
    """Run exactly one connect -> disconnect -> reconnect-scheduled cycle."""
    from pmcopy.clients import onchain_ws as onchain_ws_module

    class _Connect:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            self._socket = _FakeSocket()

        async def __aenter__(self):
            return self._socket

        async def __aexit__(self, *_exc: Any) -> bool:
            return False

    monkeypatch.setattr(onchain_ws_module.websockets, "connect", _Connect)
    monkeypatch.setattr(onchain_ws_module.random, "uniform", lambda _a, _b: 0.0)

    client = OnchainWebSocket(
        ws_url=_WS_URL,
        subscribe_params=["alchemy_pendingTransactions", {}],
        name="pendingtx",
        on_message=_noop_message,
        reconnect_min_delay=0.0,
        reconnect_max_delay=0.0,
        state_listener=listener,
    )

    # Stop the loop the moment the reconnect is scheduled so the test never
    # opens a second cycle.
    original = listener.on_reconnect_scheduled

    def _stop_after_reconnect(stream: str) -> None:
        client._running = False
        original(stream)

    monkeypatch.setattr(listener, "on_reconnect_scheduled", _stop_after_reconnect)

    client._running = True
    await client._run()
    return client


async def _noop_message(_msg: Any) -> None:
    return None


async def test_websocket_reports_connect_disconnect_and_reconnect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    listener = _RecordingListener()

    await _drive_one_cycle(monkeypatch, listener)

    assert listener.events == [
        ("connected", "pendingtx"),
        ("disconnected", "pendingtx"),
        ("reconnect", "pendingtx"),
    ]


async def test_listener_errors_never_break_the_connection_loop(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    listener = _RecordingListener(raises=True)

    with caplog.at_level(logging.DEBUG, logger="OnchainWS"):
        client = await _drive_one_cycle(monkeypatch, listener)

    assert [event for event, _stream in listener.events] == [
        "connected",
        "disconnected",
        "reconnect",
    ]
    assert client.running is False
    for record in caplog.records:
        assert "SuperSecretAlchemyKey123" not in record.getMessage()


async def test_websocket_without_a_listener_is_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The listener is optional; the loop behaves exactly as before without one."""
    from pmcopy.clients import onchain_ws as onchain_ws_module

    holder: dict[str, OnchainWebSocket] = {}
    connects: list[int] = []

    class _Connect:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            connects.append(1)
            # Stop after the second connect so the reconnect branch is
            # exercised exactly once and the loop terminates.
            if len(connects) >= 2:
                holder["client"]._running = False
            self._socket = _FakeSocket()

        async def __aenter__(self):
            return self._socket

        async def __aexit__(self, *_exc: Any) -> bool:
            return False

    monkeypatch.setattr(onchain_ws_module.websockets, "connect", _Connect)
    monkeypatch.setattr(onchain_ws_module.random, "uniform", lambda _a, _b: 0.0)

    client = OnchainWebSocket(
        ws_url=_WS_URL,
        subscribe_params=["alchemy_pendingTransactions", {}],
        name="pendingtx",
        on_message=_noop_message,
        reconnect_min_delay=0.0,
        reconnect_max_delay=0.0,
    )
    holder["client"] = client

    client._running = True
    await client._run()

    assert connects == [1, 1]
    assert client.connected is False
