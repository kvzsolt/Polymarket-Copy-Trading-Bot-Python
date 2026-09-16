"""Block source timestamps are bounded, hash-verified public RPC evidence."""

from unittest.mock import AsyncMock

import pytest

from adapters.polygon.block_timestamp import RpcBlockTimestampReader

_HASH = "0x" + "a" * 64


@pytest.mark.asyncio
async def test_block_time_is_exact_milliseconds_cached_by_immutable_hash():
    client = AsyncMock()
    client.post_json.return_value = {
        "jsonrpc": "2.0",
        "id": 1,
        "result": {
            "hash": _HASH,
            "timestamp": "0x64",
        },
    }
    reader = RpcBlockTimestampReader(client, rpc_url="https://rpc.invalid/SYNTHETIC_KEY")
    assert await reader.timestamp_ms(_HASH) == 100000
    assert await reader.timestamp_ms(_HASH.upper()) == 100000
    client.post_json.assert_awaited_once_with(
        "https://rpc.invalid/SYNTHETIC_KEY",
        json_body={
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_getBlockByHash",
            "params": [_HASH, False],
        },
        silent=True,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "result",
    [
        None,
        {},
        {"hash": "wrong", "timestamp": "0x64"},
        {"hash": _HASH, "timestamp": True},
        {"hash": _HASH, "timestamp": "NaN"},
        {"hash": _HASH, "timestamp": "-1"},
    ],
)
async def test_missing_or_malformed_header_never_becomes_current_time(result):
    client = AsyncMock()
    client.post_json.return_value = {"jsonrpc": "2.0", "id": 1, "result": result}
    reader = RpcBlockTimestampReader(client, rpc_url="https://rpc.invalid/SYNTHETIC_KEY")
    assert await reader.timestamp_ms(_HASH) is None


@pytest.mark.asyncio
async def test_rpc_failure_is_unknown_without_secret_log_or_exception(caplog):
    client = AsyncMock()
    client.post_json.side_effect = RuntimeError("SYNTHETIC_PROVIDER_KEY")
    reader = RpcBlockTimestampReader(client, rpc_url="https://rpc.invalid/SYNTHETIC_PROVIDER_KEY")
    assert await reader.timestamp_ms(_HASH) is None
    assert "SYNTHETIC_PROVIDER_KEY" not in caplog.text
