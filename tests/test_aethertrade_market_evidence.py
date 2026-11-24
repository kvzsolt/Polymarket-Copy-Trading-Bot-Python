"""Original CLOB frames are durable before decoding, with explicit continuity loss."""

from types import SimpleNamespace
from unittest.mock import Mock

from pmcopy.domain.source_time import TransportReceipt


class Clock:
    """Stable source receipt, including its original submillisecond sample."""

    def capture(self):
        return TransportReceipt(1800000000123, 10, "observer", 1800000000123456789)


class Recorder:
    """Local durable append port with an explicit loss latch."""

    def __init__(self):
        self.events = []
        self.fail = False

    def append_event(self, event):
        if self.fail:
            raise ValueError("storage failed")
        self.events.append(event)
        return len(self.events)

    def invalidate(self, reason):
        self.events.append({"invalid": reason})


class TestMarketEvidence:
    """Tests exercise actual input callbacks, not a separate synthetic book feed."""

    def test_original_text_and_ns_survive_and_disconnect_advances_stream(self):
        from pmcopy.services.market_data.market_evidence import MarketEvidenceTap

        recorder = Recorder()
        tap = MarketEvidenceTap(recorder, Clock(), stream_id="account-market")
        raw = '{ "event_type": "book", "size": "0.000000001" }'
        tap.observe(raw)
        first = recorder.events[-1]
        assert first["frame"] == raw and first["sequence"] == 1
        assert first["received_ns"] == "1800000000123456789"
        assert first["clock_id"] == "observer"
        tap.disconnected()
        assert recorder.events[-1]["kind"] == "gap"
        tap.observe(raw)
        assert recorder.events[-1]["sequence"] == 1
        assert recorder.events[-1]["stream_id"] != first["stream_id"]

    def test_heartbeat_does_not_create_book_and_bad_utf8_latches_loss(self):
        from pmcopy.services.market_data.market_evidence import MarketEvidenceTap

        recorder = Recorder()
        tap = MarketEvidenceTap(recorder, Clock(), stream_id="m")
        for raw in ("PING", "PONG", "ping", "pong", b"PING"):
            tap.observe(raw)
        assert recorder.events == []
        tap.observe(b"\xff")
        assert recorder.events == [{"invalid": "invalid_market_frame"}]

    def test_local_failure_does_not_stop_risk_market_updates(self):
        from pmcopy.services.market_data.market_evidence import MarketEvidenceTap

        recorder = Recorder()
        recorder.fail = True
        tap = MarketEvidenceTap(recorder, Clock(), stream_id="m")
        assert tap.observe('{"book":1}') is False
        assert recorder.events[-1]["invalid"] == "market_evidence_unavailable"

    async def test_socket_capture_precedes_normalizer(self):
        from pmcopy.clients.ws.market_ws import MarketWebSocket

        order = []
        evidence = SimpleNamespace(observe=lambda raw: order.append(("raw", raw)))
        cache = SimpleNamespace()
        client = MarketWebSocket(cache, evidence=evidence)
        client._running = True
        client._handle_message = lambda raw: order.append(("decoded", raw))

        class Socket:
            def __aiter__(self):
                return self.messages()

            async def messages(self):
                yield '{"event_type":"book"}'

        await client._message_loop(Socket())
        assert [step[0] for step in order] == ["raw", "decoded"]

    async def test_market_runtime_passes_tap_and_invalidates_on_disconnect(self, monkeypatch):
        from pmcopy.cache.market_state_store import MarketStateStore
        from pmcopy.orchestrators.streaming.market_data_runtime import MarketDataRuntime
        from pmcopy.services.observability.health_server import ReadinessTracker
        from tests.test_market_data_runtime import _FakeMarketWebSocket

        seen = []
        def socket(**kwargs):
            seen.append(kwargs)
            return _FakeMarketWebSocket(**kwargs)

        monkeypatch.setattr("pmcopy.orchestrators.streaming.market_data_runtime.MarketWebSocket", socket)
        tap = SimpleNamespace(disconnected=Mock())
        runtime = MarketDataRuntime(market_store=MarketStateStore(), gateway=SimpleNamespace(),
                                    readiness=ReadinessTracker(), config=SimpleNamespace(), evidence=tap,
                                    monotonic=lambda: 0)
        await runtime.start([])
        assert seen[0]["evidence"] is tap
        await runtime._on_wss_disconnected()
        tap.disconnected.assert_called_once()
        await runtime.stop()

    async def test_application_composes_existing_stream_with_same_writer(self, monkeypatch):
        import pmcopy.app as app
        from unittest.mock import AsyncMock
        from pmcopy.services.observability.health_server import ReadinessTracker

        seen = []
        def runtime(**kwargs):
            seen.append(kwargs)
            return SimpleNamespace(start=AsyncMock())

        monkeypatch.setattr(app, "MarketDataRuntime", runtime)
        monkeypatch.setattr("pmcopy.adapters.time.clock.SystemReceiptClock", Clock)
        writer = Recorder()
        await app._start_market_data_runtimes(
            runtime_users=[SimpleNamespace(user_id="a")],
            user_states={"a": SimpleNamespace(market_store=object(), clob_gateway=object())},
            live_positions_by_user={}, contexts_by_leader={}, readiness=ReadinessTracker(),
            config=SimpleNamespace(), evidence_runtime=writer,
        )
        seen[0]["evidence"].observe('{"event_type":"book"}')
        assert writer.events[0]["kind"] == "market_frame"
