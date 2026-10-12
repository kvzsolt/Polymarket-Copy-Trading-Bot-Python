"""Detection uses block time even when enrichment and receipt confirmation happen later."""

from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from pmcopy.application.confirmed_signal_execution_adapter import ConfirmedSignalExecutionAdapter
from pmcopy.orchestrators.onchain_detection_runtime import OnchainDetectionRuntime
from pmcopy.orchestrators.v1_event_adapters import LeaderEventNormalizer
from pmcopy.domain.detection import DetectionMode
from tests.test_onchain_detection_runtime import _WATCHED, _build_log
from tests.test_confirmed_signal_execution_adapter import FakeClock, FakeSignalSink, _claim


@pytest.mark.asyncio
@pytest.mark.parametrize("source_ms", [None, 1786615000000])
async def test_logs_use_block_source_time_instead_of_post_enrichment_clock(source_ms):
    reader = AsyncMock()
    reader.timestamp_ms.return_value = source_ms
    runtime = OnchainDetectionRuntime(
        watched_wallets={_WATCHED},
        ws_url="wss://unused.invalid",
        signal_sink=AsyncMock(),
        condition_id_resolver=AsyncMock(return_value="condition"),
        **({"block_timestamp_reader": reader} if source_ms is not None else {}),
    )
    block_hash = "0x" + "5" * 64
    log = {
        **_build_log(maker_amount_filled=500000, taker_amount_filled=1000000),
        "blockHash": block_hash,
    }
    await runtime._on_mined_log(log)
    signal = runtime._queue.get_nowait()
    assert signal.event_ts == source_ms
    if source_ms is not None:
        reader.timestamp_ms.assert_awaited_once_with(block_hash)
    event = LeaderEventNormalizer(clock=FakeClock()).normalize_raw(
        _WATCHED,
        signal.as_raw_signal_dict(),
        mode=DetectionMode.ON_CHAIN_CONFIRMED,
    )
    expected = (
        None if source_ms is None else datetime.fromtimestamp(source_ms // 1000, timezone.utc)
    )
    assert event.trade.venue_timestamp == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("source_ms", [None, 1786615000000])
async def test_receipt_claim_uses_immutable_block_time_instead_of_release_time(source_ms):
    reader = AsyncMock()
    reader.timestamp_ms.return_value = source_ms
    sink = FakeSignalSink()
    adapter = ConfirmedSignalExecutionAdapter(
        sink,
        FakeClock(),
        **({"block_timestamp_reader": reader} if source_ms is not None else {}),
    )
    await adapter.execute_confirmed_source(_claim())
    assert sink.signals[0].event_ts == source_ms
    if source_ms is not None:
        reader.timestamp_ms.assert_awaited_once_with(_claim().fill.block_hash)


def test_explicit_millisecond_epoch_does_not_depend_on_its_magnitude():
    from dataclasses import replace
    from tests.test_onchain_detection_runtime import _make_mined_signal

    signal = replace(_make_mined_signal(), event_ts=100000, price=Decimal(".5"))
    event = LeaderEventNormalizer(clock=FakeClock()).normalize_raw(
        _WATCHED,
        signal.as_raw_signal_dict(),
        mode=DetectionMode.ON_CHAIN_CONFIRMED,
    )
    assert event.trade.venue_timestamp == datetime.fromtimestamp(100, timezone.utc)


@pytest.mark.parametrize("timestamp", [None, 2001])
def test_pending_unknown_or_future_receipt_cannot_pass_staleness(timestamp):
    from dataclasses import replace
    from pmcopy.services.onchain_detection.pending_tx_detector import is_stale_pending
    from tests.test_onchain_detection_runtime import _make_pending_signal

    signal = replace(_make_pending_signal(event_ts=1000), event_ts=timestamp)
    assert is_stale_pending(signal, now_ms=2000, max_age_ms=0)
