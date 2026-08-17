"""Capture the outbound JSON-RPC subscribe payload sent by
``OnchainWebSocket._send_subscribe`` and assert the JSON-RPC ``params``
field is a positional array (Greptile P1 #1 regression guard).

Both ``log_subscription_params`` (mined ``OrderFilled``) and
``pending_subscription_params`` (``alchemy_pendingTransactions``) MUST
return JSON-RPC ``params`` as a list (``["logs", filter]`` /
``["alchemy_pendingTransactions", options]``), not as a dict, because
``eth_subscribe`` is a positional JSON-RPC method. Sending the helper
dict directly results in a malformed request that Alchemy silently
ignores — the subscription never starts in production.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest

from clients.onchain_ws import OnchainWebSocket
from services.onchain_detection.order_filled_detector import (
    ORDER_FILLED_TOPIC0,
    log_subscription_params,
)
from services.onchain_detection.pending_tx_detector import (
    pending_subscription_params,
)


class _FakeWebSocket:
    """Minimal fake websocket that records ``send()`` payloads and
    hands back a single pre-configured ``recv()`` ack."""

    def __init__(self, sub_id: str = "sub-id-1") -> None:
        self.sent_payloads: list[str] = []
        self._ack = json.dumps({"id": 1, "jsonrpc": "2.0", "result": sub_id})
        self._recv_calls = 0

    async def send(self, payload: str) -> None:
        self.sent_payloads.append(payload)

    async def recv(self) -> str:
        self._recv_calls += 1
        if self._recv_calls == 1:
            return self._ack
        await asyncio.sleep(0)
        raise asyncio.TimeoutError  # subsequent reads should not happen

    async def close(self) -> None:
        pass


async def _empty_callback(_msg: Any) -> None:
    """No-op message callback (we only inspect the subscribe payload)."""
    return None


def _build_client(subscribe_params: list[Any]) -> OnchainWebSocket:
    return OnchainWebSocket(
        ws_url="wss://example.alchemy.com/v2/dummy",
        subscribe_params=subscribe_params,
        name="test-sub",
        on_message=_empty_callback,
    )


@pytest.mark.asyncio
async def test_subscribe_logs_params_is_json_rpc_positional_array() -> None:
    fake = _FakeWebSocket()
    client = _build_client(log_subscription_params())

    await client._send_subscribe(fake)

    assert fake.sent_payloads, "expected subscribe JSON-RPC frame to be sent"
    request = json.loads(fake.sent_payloads[0])
    assert request["jsonrpc"] == "2.0"
    assert request["method"] == "eth_subscribe"
    assert isinstance(request["params"], list), (
        "params must be a JSON-RPC positional array, not a dict"
    )
    assert request["params"][0] == "logs"
    filter_obj = request["params"][1]
    assert isinstance(filter_obj["address"], list)
    assert filter_obj["topics"] == ["0x" + ORDER_FILLED_TOPIC0.hex()]


@pytest.mark.asyncio
async def test_subscribe_pending_params_is_json_rpc_positional_array() -> None:
    fake = _FakeWebSocket()
    client = _build_client(pending_subscription_params())

    await client._send_subscribe(fake)

    assert fake.sent_payloads
    request = json.loads(fake.sent_payloads[0])
    assert request["method"] == "eth_subscribe"
    assert isinstance(request["params"], list), (
        "params must be a JSON-RPC positional array, not a dict"
    )
    assert request["params"][0] == "alchemy_pendingTransactions"
    options = request["params"][1]
    assert isinstance(options["toAddress"], list)
    assert "hashesOnly" in options
    assert options["hashesOnly"] is False


@pytest.mark.asyncio
async def test_subscribe_pending_hashes_only_flag_propagates_to_payload() -> None:
    fake = _FakeWebSocket()
    client = _build_client(pending_subscription_params(hashes_only=True))

    await client._send_subscribe(fake)

    request = json.loads(fake.sent_payloads[0])
    options = request["params"][1]
    assert options["hashesOnly"] is True


@pytest.mark.asyncio
async def test_sent_payload_is_valid_json_string() -> None:
    """The wire frame is a single JSON string (sanity guard against the
    builder accidentally returning a non-serializable object)."""
    fake = _FakeWebSocket()
    client = _build_client(log_subscription_params())

    await client._send_subscribe(fake)

    raw = fake.sent_payloads[0]
    assert isinstance(raw, str), "ws.send must receive a string frame"
    # Must round-trip through json.loads without raising.
    json.loads(raw)
