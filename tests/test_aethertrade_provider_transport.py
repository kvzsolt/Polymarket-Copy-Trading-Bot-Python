"""Fixed provider transports reject redirect, unsafe methods and malformed evidence."""

import json

import httpx
import pytest

from pmcopy.clients.network.provider_transport import ProviderEndpoints, ProviderHttpTransport
from pmcopy.domain.provider_budget import ProviderConfigurationError
from pmcopy.services.provider_gateway import ProviderUnavailable


class TestProviderEndpoints:
    """Credentials stay inside validated Polygon provider endpoints."""

    @pytest.mark.parametrize(
        "url",
        [
            "http://polygon-mainnet.g.alchemy.com/v2/synthetic-secret",
            "https://user:password@polygon-mainnet.g.alchemy.com/v2/synthetic-secret",
            "https://polygon-mainnet.g.alchemy.com.evil.test/v2/synthetic-secret",
            "https://polygon-mainnet.g.alchemy.com/v2/synthetic-secret?x=1",
            "https://polygon-mainnet.infura.io/v3/synthetic-secret",
        ],
    )
    def test_unsafe_alchemy_url_rejected_without_disclosing_value(self, url):
        with pytest.raises(ProviderConfigurationError) as error:
            ProviderEndpoints(alchemy=url)
        assert "synthetic-secret" not in str(error.value)

    def test_endpoint_repr_and_alias_validation(self):
        endpoints = ProviderEndpoints(
            alchemy="https://polygon-mainnet.g.alchemy.com/v2/synthetic-secret"
        )
        assert "synthetic-secret" not in repr(endpoints)
        assert endpoints.alias_for(endpoints.http_url("alchemy")) == "alchemy"
        assert endpoints.websocket_url("alchemy").startswith("wss://")
        with pytest.raises(ProviderConfigurationError):
            endpoints.http_url("third-provider")


class TestProviderHttpTransport:
    """One JSON-RPC request is bounded and validated without implicit retry."""

    @pytest.mark.asyncio
    async def test_valid_result_and_identity(self):
        calls = []

        def respond(request):
            body = json.loads(request.content)
            calls.append(body)
            return httpx.Response(200, json={"jsonrpc": "2.0", "id": body["id"], "result": None})

        transport = ProviderHttpTransport(
            ProviderEndpoints(alchemy="https://polygon-mainnet.g.alchemy.com/v2/test-key"),
            http_transport=httpx.MockTransport(respond),
        )
        assert await transport.request("alchemy", "eth_getTransactionReceipt", ["0x1"]) is None
        assert len(calls) == 1
        assert calls[0]["params"] == ["0x1"]

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "response",
        [
            httpx.Response(302, headers={"location": "https://evil.test/"}),
            httpx.Response(401, text="synthetic-secret"),
            httpx.Response(429, text="synthetic-secret"),
            httpx.Response(200, json={"jsonrpc": "2.0", "id": 999, "result": []}),
            httpx.Response(
                200, json={"jsonrpc": "2.0", "id": 1, "error": {"message": "synthetic-secret"}}
            ),
            httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "result": 0, "error": {}}),
            httpx.Response(200, content=b"not-json"),
            httpx.Response(200, content=b"x" * 4097),
        ],
    )
    async def test_invalid_response_fails_redacted_once(self, response):
        calls = []

        def respond(request):
            calls.append(request)
            return response

        transport = ProviderHttpTransport(
            ProviderEndpoints(alchemy="https://polygon-mainnet.g.alchemy.com/v2/test-key"),
            http_transport=httpx.MockTransport(respond),
            max_response_bytes=4096,
        )
        with pytest.raises(ProviderUnavailable) as error:
            await transport.request("alchemy", "eth_getLogs", [])
        assert "synthetic-secret" not in str(error.value)
        assert error.value.__cause__ is None
        assert len(calls) == 1

    @pytest.mark.asyncio
    async def test_write_method_rejected_before_transport(self):
        calls = []

        def respond(request):
            calls.append(request)
            raise AssertionError("must not dispatch")

        transport = ProviderHttpTransport(
            ProviderEndpoints(alchemy="https://polygon-mainnet.g.alchemy.com/v2/test-key"),
            http_transport=httpx.MockTransport(respond),
        )
        with pytest.raises(ProviderConfigurationError):
            await transport.request("alchemy", "eth_sendRawTransaction", ["0x"])
        assert not calls

    @pytest.mark.asyncio
    async def test_http_library_logging_never_exposes_endpoint_credential(self, caplog):
        import logging

        caplog.set_level(logging.DEBUG)
        transport = ProviderHttpTransport(
            ProviderEndpoints(alchemy="https://polygon-mainnet.g.alchemy.com/v2/test-key"),
            http_transport=httpx.MockTransport(
                lambda request: httpx.Response(
                    200, json={"jsonrpc": "2.0", "id": 1, "result": "0x89"}
                )
            ),
        )
        assert await transport.request("alchemy", "eth_chainId", []) == "0x89"
        assert "test-key" not in caplog.text


class TestAlchemyOnlyWebSocketEndpoint:
    """ProviderEndpoints derives Alchemy WSS URLs and rejects Infura."""

    def test_alchemy_socket_path_and_infura_rejection(self):
        endpoints = ProviderEndpoints(alchemy="https://polygon-mainnet.g.alchemy.com/v2/test-key")
        expected = "wss://polygon-mainnet.g.alchemy.com/v2/test-key"
        assert endpoints.websocket_url("alchemy") == expected
        assert endpoints.alias_for(expected) == "alchemy"
        with pytest.raises(ProviderConfigurationError):
            endpoints.websocket_url("infura")
