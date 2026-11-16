"""Source receipt clocks are sampled before frame decoding and enrichment."""

from unittest.mock import AsyncMock

import pytest

from pmcopy.clients.onchain_ws import OnchainWebSocket


@pytest.mark.asyncio
async def test_socket_samples_injected_receipt_before_decoding_and_overwrites_provider_value(
    monkeypatch,
):
    import pmcopy.clients.onchain_ws as module
    from pmcopy.domain.source_time import TransportReceipt

    order = []
    receipt = TransportReceipt(wall_timestamp_ms=100000, monotonic_ns=123456789, boot_id="boot-a")

    class Clock:
        def capture(self):
            order.append("capture")
            return receipt

    original_loads = module.orjson.loads

    def decode(raw):
        order.append("decode")
        return original_loads(raw)

    monkeypatch.setattr(module.orjson, "loads", decode)
    sink = AsyncMock()
    socket = OnchainWebSocket(
        ws_url="wss://unused.invalid",
        subscribe_params=[],
        name="test",
        on_message=sink,
        receipt_clock=Clock(),
    )
    await socket._handle_raw(
        '{"method":"eth_subscription","params":{"result":{"hash":"tx","_source_receipt":"untrusted"}}}'
    )
    assert order == ["capture", "decode"]
    assert sink.await_args.args[0]["_source_receipt"] == receipt


def test_transport_receipt_rejects_cross_session_elapsed_comparison():
    from pmcopy.domain.source_time import TransportReceipt

    older = TransportReceipt(wall_timestamp_ms=100000, monotonic_ns=100, boot_id="boot-a")
    later = TransportReceipt(wall_timestamp_ms=101000, monotonic_ns=200, boot_id="boot-b")
    assert later.elapsed_since(older) is None
    assert older.elapsed_since(older) == 0


@pytest.mark.asyncio
async def test_mined_receipt_survives_enrichment_and_canonical_normalization():
    from pmcopy.domain.source_time import TransportReceipt
    from pmcopy.domain.detection import DetectionMode
    from pmcopy.orchestrators.onchain_detection_runtime import OnchainDetectionRuntime
    from pmcopy.orchestrators.v1_event_adapters import LeaderEventNormalizer
    from tests.test_onchain_detection_runtime import _WATCHED, _build_log
    from tests.test_confirmed_signal_execution_adapter import FakeClock

    receipt = TransportReceipt(1786615000000, 123456789, "boot-a")
    runtime = OnchainDetectionRuntime(
        watched_wallets={_WATCHED},
        ws_url="wss://unused.invalid",
        signal_sink=AsyncMock(),
        condition_id_resolver=AsyncMock(return_value="condition"),
    )
    await runtime._on_mined_log(
        {
            **_build_log(maker_amount_filled=500000, taker_amount_filled=1000000),
            "_source_receipt": receipt,
        }
    )
    signal = runtime._queue.get_nowait()
    event = LeaderEventNormalizer(clock=FakeClock()).normalize_raw(
        _WATCHED,
        signal.as_raw_signal_dict(),
        mode=DetectionMode.ON_CHAIN_LOGS_ONLY,
    )
    assert event.trade.detected_at == receipt.received_at
    assert event.trade.source_receipt == receipt
    assert event.trade.venue_timestamp is None


def test_pending_entry_without_receipt_or_source_time_remains_unknown():
    from dataclasses import replace
    from pmcopy.domain.detection import DetectionMode, DetectionFinality
    from pmcopy.services.copy_tasks.source_freshness import SourceFreshnessPolicy
    from tests.v1.test_copy_task_event_routing import source_event, _NOW

    event = replace(
        source_event().trade,
        venue_timestamp=None,
        mode=DetectionMode.ON_CHAIN_PENDING,
        finality=DetectionFinality.PROVISIONAL,
    )
    assert SourceFreshnessPolicy().rejection(event, _NOW) == "signal_age_unknown"


def test_pending_qualification_uses_receipt_time_and_retains_clock_provenance():
    from pmcopy.domain.source_time import TransportReceipt
    from tests.test_pending_transaction_factory import (
        PendingNotificationFactory,
        _factory,
        _EXCHANGE,
        _LEADER,
    )

    fixtures = PendingNotificationFactory()
    receipt = TransportReceipt(1786615000000, 123456789, "boot-a")
    pending = _factory().from_notification(
        {
            **fixtures.notification(target=_EXCHANGE, calldata=fixtures.inner_calldata()),
            "_source_receipt": receipt,
        },
        watched_wallets=frozenset({_LEADER}),
    )
    assert pending.observed_at == receipt.received_at
    assert pending.source_receipt == receipt


@pytest.mark.asyncio
async def test_multiplexed_source_socket_preserves_receipt_before_queue_dispatch():
    import asyncio
    from pmcopy.adapters.polygon.alchemy_json_rpc import AlchemyJsonRpcSession, AlchemySubscriptionOptions
    from pmcopy.domain.source_time import TransportReceipt
    from tests.test_alchemy_json_rpc_session import (
        FakeClock,
        FakeJsonRpcSocket,
        FakeEventSink,
        _EXCHANGE,
        _SAFE,
    )

    receipt = TransportReceipt(1786615000000, 123456789, "boot-a")

    class Clock:
        def capture(self):
            return receipt

    socket, sink = FakeJsonRpcSocket(), FakeEventSink()
    session = AlchemyJsonRpcSession(
        socket=socket,
        options=AlchemySubscriptionOptions(
            chain_id=137,
            pending_targets=(_EXCHANGE,),
            log_addresses=(_EXCHANGE,),
            watched_wallets=(_SAFE,),
        ),
        sink=sink,
        clock=FakeClock(),
        receipt_clock=Clock(),
    )
    start = asyncio.create_task(session.start())
    try:
        for index, subscription in enumerate(("pending-sub", "logs-maker-sub", "logs-taker-sub"), start=1):
            await socket.wait_for_send_count(index)
            await socket.feed({"jsonrpc": "2.0", "id": index, "result": subscription})
        await start
        await socket.feed(
            {
                "method": "eth_subscription",
                "params": {
                    "subscription": "pending-sub",
                    "result": {"hash": "tx", "_source_receipt": "untrusted"},
                },
            }
        )
        await sink.pending_ready.wait()
        assert sink.pending[0]["_source_receipt"] == receipt
    finally:
        if not start.done():
            start.cancel()
        await asyncio.gather(start, return_exceptions=True)
        await session.stop()


def test_transport_receipt_survives_the_actual_restart_codec():
    from dataclasses import replace
    from pmcopy.adapters.persistence.source_transaction_state_store import SourceTransactionSnapshotCodec
    from pmcopy.domain.source_time import TransportReceipt
    from tests.test_postgres_source_transaction_state_store import _snapshot

    snapshot = _snapshot()
    record = snapshot.records[0]
    receipt = TransportReceipt(1786615000000, 123456789, "boot-before-restart")
    pending = replace(record.reservation.transaction, source_receipt=receipt)
    reservation = replace(record.reservation, transaction=pending)
    updated = replace(
        snapshot,
        records=(
            replace(
                record,
                reservation=reservation,
                confirmed=replace(record.confirmed, reservation=reservation),
            ),
        ),
    )
    codec = SourceTransactionSnapshotCodec()
    restored = codec.decode(codec.encode(updated))
    assert restored == updated
    assert restored.records[0].reservation.transaction.source_receipt == receipt


def test_out_of_range_receipt_time_is_rejected_at_construction():
    from pmcopy.domain.source_time import TransportReceipt

    with pytest.raises(ValueError, match="range"):
        TransportReceipt(253402300800000, 100, "boot-a")
