"""The composition root supplies the existing detectors with real block-time reads."""

from unittest.mock import AsyncMock

import pytest

import app
from domain.detection import DetectionMode
from orchestrators.detection_source_factory import DetectionSourceRequest
from tests.config_helpers import load_test_config
from tests.test_confirmed_signal_execution_adapter import _claim
from tests.test_onchain_detection_runtime import _WATCHED, _build_log


@pytest.mark.asyncio
@pytest.mark.parametrize("variant", ["logs_only", "mempool_reserved"])
async def test_selected_detector_receives_hash_verified_public_timestamp_reader(
    monkeypatch, variant
):
    header_client = AsyncMock()
    block_hash = _claim().fill.block_hash
    header_client.post_json.return_value = {
        "id": 1,
        "jsonrpc": "2.0",
        "result": {
            "hash": block_hash,
            "timestamp": "0x64",
        },
    }
    monkeypatch.setattr("utils.fetchData._get_http_client", lambda: header_client)
    monkeypatch.setattr(
        app, "_build_onchain_condition_id_resolver", lambda: AsyncMock(return_value="condition")
    )
    sink = AsyncMock()
    factory = app._build_detection_source_factory(
        load_test_config(), state_store_provider=AsyncMock(return_value=None)
    )
    runtime = await factory.build(
        DetectionSourceRequest(
            mode=DetectionMode.ON_CHAIN_CONFIRMED,
            watched_wallets=frozenset({_WATCHED}),
            ws_url="wss://unused.invalid",
            chain_id=137,
            signal_sink=sink,
            confirmed_source=variant,
        )
    )
    if variant == "logs_only":
        await runtime._on_mined_log({**_build_log(), "blockHash": block_hash})
        signal = runtime._queue.get_nowait()
    else:
        await runtime._pipeline._gate._execution.execute_confirmed_source(_claim())
        signal = sink.await_args.args[0]
    assert signal.event_ts == 100000
    header_client.post_json.assert_awaited_once()
