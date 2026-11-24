"""RPC body receipt is sampled before JSON interpretation under shared admission."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import httpx
import pytest

from pmcopy.clients.network.provider_transport import ProviderEndpoints, ProviderHttpTransport
from pmcopy.domain.source_time import TransportReceipt


class TestObservedRpc:
    """Native body timing cannot be reconstructed by a pending evaluator later."""

    async def test_production_runtime_injects_native_receipt_into_shared_gateway(self, tmp_path):
        from pmcopy.orchestrators.bootstrap.provider_runtime import ProviderRuntime
        from tests.test_aethertrade_provider_runtime import Clock, TestProviderRuntime

        receipt = TransportReceipt(1800000000123, 7, "finland", 1800000000123456789)
        runtime = ProviderRuntime.from_config(
            TestProviderRuntime().config(tmp_path),
            clock=Clock(),
            receipt_clock=SimpleNamespace(capture=lambda: receipt),
            http_transport=httpx.MockTransport(
                lambda request: httpx.Response(
                    200, json={"jsonrpc": "2.0", "id": 1, "result": None}
                )
            ),
        )
        try:
            result = await runtime.gateway.call_observed(
                "alchemy", "eth_getTransactionByHash", ["0x" + "11" * 32], category="pending"
            )
            assert result.payload is None and result.receipt is receipt
            assert runtime.budget.snapshot()["alchemy"]["used"] > 0
        finally:
            runtime.close()

    async def test_receipt_is_captured_before_json_decode_and_close(self, monkeypatch):
        import pmcopy.clients.network.provider_transport as module

        sequence = []
        receipt = TransportReceipt(1800000000123, 7, "finland", 1800000000123456789)

        class Clock:
            def capture(self):
                sequence.append("capture")
                return receipt

        class Body(httpx.AsyncByteStream):
            async def __aiter__(self):
                sequence.append("body")
                yield b'{"jsonrpc":"2.0","id":1,"result":{"hash":"observed"}}'

            async def aclose(self):
                sequence.append("close")

        original = module.json.loads

        def decode(value):
            sequence.append("decode")
            return original(value)

        monkeypatch.setattr(module.json, "loads", decode)
        transport = ProviderHttpTransport(
            ProviderEndpoints(alchemy="https://polygon-mainnet.g.alchemy.com/v2/synthetic"),
            http_transport=httpx.MockTransport(lambda request: httpx.Response(200, stream=Body())),
            receipt_clock=Clock(),
        )
        result = await transport.request_observed("alchemy", "eth_getTransactionByHash", ["hash"])
        assert result.payload == {"hash": "observed"} and result.receipt is receipt
        assert sequence.index("body") < sequence.index("capture") < sequence.index("decode")

    async def test_unconfigured_clock_fails_before_network(self):
        io = Mock(side_effect=AssertionError("unexpected provider call"))
        transport = ProviderHttpTransport(
            ProviderEndpoints(alchemy="https://polygon-mainnet.g.alchemy.com/v2/synthetic"),
            http_transport=httpx.MockTransport(io),
        )
        with pytest.raises(ValueError, match="clock"):
            await transport.request_observed("alchemy", "eth_getTransactionByHash", ["hash"])
        io.assert_not_called()

    async def test_observed_gateway_admits_pending_before_single_read(self):
        from pmcopy.services.providers.observed_provider_gateway import ObservedProviderGateway

        sequence = []
        admitted = Mock(side_effect=lambda *a, **k: sequence.append("admitted"))
        result = object()

        async def observed(*args):
            sequence.append("read")
            return result

        gateway = ObservedProviderGateway(
            SimpleNamespace(admit_rpc=admitted), SimpleNamespace(request_observed=observed)
        )
        assert (
            await gateway.call_observed(
                "alchemy", "eth_getTransactionByHash", ["hash"], category="pending"
            )
            is result
        )
        admitted.assert_called_once_with("alchemy", "eth_getTransactionByHash", category="pending")
        assert sequence == ["admitted", "read"]

    async def test_admission_failure_cannot_reach_observed_transport(self):
        from pmcopy.services.providers.observed_provider_gateway import ObservedProviderGateway

        transport = SimpleNamespace(request_observed=AsyncMock())
        gateway = ObservedProviderGateway(
            SimpleNamespace(admit_rpc=Mock(side_effect=ValueError("budget"))), transport
        )
        with pytest.raises(ValueError, match="budget"):
            await gateway.call_observed(
                "alchemy", "eth_getTransactionByHash", [], category="pending"
            )
        transport.request_observed.assert_not_awaited()
