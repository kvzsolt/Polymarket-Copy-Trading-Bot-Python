"""Metered Web3 compatibility adapter for existing synchronous read consumers."""

from __future__ import annotations

import json
from typing import Any, cast

import httpx
from web3.providers.base import JSONBaseProvider
from web3.types import RPCResponse

from clients.network.provider_transport import ProviderEndpoints
from interfaces.provider_budget import ProviderAdmission
from services.provider_gateway import ProviderUnavailable


class MeteredWeb3Provider(JSONBaseProvider):
    """Preserve SDK response formatting while admitting every read-only attempt.

    This provider never falls back or submits transactions. A caller can choose
    the other configured provider explicitly, which requires another admission.
    """

    def __init__(
        self,
        admission: ProviderAdmission,
        endpoints: ProviderEndpoints,
        alias: str,
        *,
        http_transport: httpx.BaseTransport | None = None,
        max_response_bytes: int = 2_000_000,
    ) -> None:
        """Inject the same account ledger used by asynchronous and socket reads."""
        super().__init__()
        self._admission = admission
        self._endpoint = endpoints.http_url(alias)
        self._alias = alias
        self._transport = http_transport
        if type(max_response_bytes) is not int or not 1024 <= max_response_bytes <= 8_000_000:
            raise ValueError("invalid provider response bound")
        self._maximum = max_response_bytes

    def make_request(self, method: str, params: Any) -> RPCResponse:
        """Charge once, read once and validate the JSON-RPC envelope without URL logs."""
        self._admission.admit_rpc(self._alias, method)
        try:
            request = httpx.Request(
                "POST",
                self._endpoint,
                json={
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": method,
                    "params": params,
                },
                extensions={"timeout": {"connect": 3.0, "read": 8.0, "write": 8.0, "pool": 3.0}},
            )
            transport = self._transport or httpx.HTTPTransport(retries=0, trust_env=False)
            with transport:
                response = transport.handle_request(request)
                try:
                    if response.status_code != 200:
                        raise ProviderUnavailable("provider HTTP read unavailable")
                    content = bytearray()
                    for chunk in response.iter_bytes():
                        if len(content) + len(chunk) > self._maximum:
                            raise ProviderUnavailable("provider response exceeds bound")
                        content.extend(chunk)
                finally:
                    response.close()
            raw = json.loads(content)
            if (
                not isinstance(raw, dict)
                or raw.get("jsonrpc") != "2.0"
                or type(raw.get("id")) is not int
                or raw["id"] != 1
                or "result" not in raw
                or "error" in raw
            ):
                raise ProviderUnavailable("invalid provider response")
            return cast(RPCResponse, raw)
        except Exception:
            raise ProviderUnavailable("provider read unavailable") from None
