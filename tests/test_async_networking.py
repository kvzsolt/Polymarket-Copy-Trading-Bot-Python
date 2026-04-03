from __future__ import annotations

import asyncio
import pytest

from network.http_client import AsyncHttpClient, HttpRetryPolicy
from network.web3_adapter import AsyncWeb3Adapter


class _FakeResponse:
    def __init__(self, payload: dict):
        self.status = 200
        self._payload = payload

    async def json(self, content_type=None):  # noqa: ARG002
        await asyncio.sleep(0.25)
        return self._payload

    def raise_for_status(self):
        return None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):  # noqa: ANN001
        return False


class _FakeSession:
    def request(self, method, url, timeout=None, **kwargs):  # noqa: ANN001, ARG002
        return _FakeResponse({"ok": True, "method": method, "url": url})


@pytest.mark.asyncio
async def test_slow_http_does_not_block_event_loop():
    ticks = 0
    client = AsyncHttpClient(
        _FakeSession(),
        retry_policy=HttpRetryPolicy(attempts=1, timeout_seconds=2.0),
    )

    async def ticker():
        nonlocal ticks
        for _ in range(8):
            await asyncio.sleep(0.05)
            ticks += 1

    async def slow_call():
        payload = await client.get_json("https://example.test/slow")
        assert payload["ok"] is True

    await asyncio.gather(ticker(), slow_call())
    assert ticks >= 4


@pytest.mark.asyncio
async def test_slow_web3_call_does_not_block_event_loop():
    class FakeEth:
        def get_balance(self, _address: str) -> int:
            return 123

    class FakeWeb3:
        eth = FakeEth()

    async def fake_to_thread(func, *args, **kwargs):
        await asyncio.sleep(0.25)
        return func(*args, **kwargs)

    # Keep test in-process (no real threads) while still validating adapter uses to_thread.
    import network.web3_adapter as web3_adapter_module
    original_to_thread = web3_adapter_module.asyncio.to_thread
    web3_adapter_module.asyncio.to_thread = fake_to_thread

    adapter = AsyncWeb3Adapter(FakeWeb3())
    ticks = 0

    async def ticker():
        nonlocal ticks
        for _ in range(8):
            await asyncio.sleep(0.05)
            ticks += 1

    async def slow_balance():
        balance = await adapter.get_native_balance("0xabc")
        assert balance == 123

    try:
        await asyncio.gather(ticker(), slow_balance())
    finally:
        web3_adapter_module.asyncio.to_thread = original_to_thread

    assert ticks >= 4
