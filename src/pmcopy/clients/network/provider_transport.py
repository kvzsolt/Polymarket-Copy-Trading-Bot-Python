"""Credential-owning single-attempt transport for the two approved Polygon providers."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from urllib.parse import urlsplit

import httpx

from pmcopy.domain.provider_budget import ProviderConfigurationError
from pmcopy.domain.observed_rpc import ObservedRpcResult
from pmcopy.domain.source_time import TransportReceipt
from pmcopy.interfaces.source_time import ReceiptClock
from pmcopy.services.provider_gateway import ProviderUnavailable


@dataclass(frozen=True, slots=True, repr=False)
class ProviderEndpoints:
    """Allow only configured HTTPS Polygon endpoints; never render credential paths."""

    alchemy: str = field(default="", repr=False)
    infura: str = field(default="", repr=False)

    def __post_init__(self) -> None:
        """Reject authority confusion, userinfo, query credentials and extra providers."""
        for alias, endpoint in (("alchemy", self.alchemy), ("infura", self.infura)):
            if not endpoint:
                continue
            host, prefix = (
                ("polygon-mainnet.g.alchemy.com", "/v2/")
                if alias == "alchemy"
                else ("polygon-mainnet.infura.io", "/v3/")
            )
            try:
                parsed = urlsplit(endpoint)
                valid = (
                    parsed.scheme == "https"
                    and parsed.hostname == host
                    and parsed.port in (None, 443)
                    and parsed.username is None
                    and parsed.password is None
                    and not parsed.query
                    and not parsed.fragment
                    and parsed.path.startswith(prefix)
                    and bool(parsed.path[len(prefix) :])
                    and all(
                        c.isascii() and (c.isalnum() or c in "-_")
                        for c in parsed.path[len(prefix) :]
                    )
                    and not any(c.isspace() for c in endpoint)
                )
            except (TypeError, ValueError):
                valid = False
            if not valid:
                raise ProviderConfigurationError(
                    "invalid configured Polygon provider endpoint"
                ) from None
        if not self.alchemy and not self.infura:
            raise ProviderConfigurationError("at least one approved provider endpoint is required")

    def http_url(self, alias: str) -> str:
        """Resolve an enabled alias internally without accepting arbitrary destinations."""
        value = self.alchemy if alias == "alchemy" else self.infura if alias == "infura" else ""
        if not value:
            raise ProviderConfigurationError("provider is not configured")
        return value

    def websocket_url(self, alias: str) -> str:
        """Return the same approved credential endpoint using TLS WebSocket transport."""
        url = "wss://" + self.http_url(alias).removeprefix("https://")
        return url.replace("infura.io/v3/", "infura.io/ws/v3/") if alias == "infura" else url

    def alias_for(self, endpoint: str) -> str:
        """Map an exact configured endpoint to its account-wide budget alias."""
        for alias in ("alchemy", "infura"):
            configured = self.alchemy if alias == "alchemy" else self.infura
            if configured and endpoint in (configured, self.websocket_url(alias)):
                return alias
        raise ProviderConfigurationError(
            "endpoint does not match the shared provider configuration"
        )


class ProviderHttpTransport:
    """Bound responses without full-URL client logs, redirects, proxies or retries.

    Provider credentials occupy the URL path. The low-level transport avoids
    AsyncClient's informational full-URL log and cannot follow redirects.
    """

    _READ_METHODS = frozenset(
        {
            "eth_chainId",
            "eth_blockNumber",
            "eth_call",
            "eth_getCode",
            "eth_getBalance",
            "eth_getBlockByNumber",
            "eth_getBlockByHash",
            "eth_getTransactionByHash",
            "eth_getTransactionReceipt",
            "eth_getTransactionCount",
            "eth_getLogs",
            "eth_gasPrice",
            "eth_estimateGas",
            "eth_getStorageAt",
        }
    )

    def __init__(
        self,
        endpoints: ProviderEndpoints,
        *,
        http_transport: httpx.AsyncBaseTransport | None = None,
        max_response_bytes: int = 2_000_000,
        receipt_clock: ReceiptClock | None = None,
    ) -> None:
        """Inject immutable endpoints and optionally an in-memory HTTP transport."""
        if type(max_response_bytes) is not int or not 1024 <= max_response_bytes <= 8_000_000:
            raise ProviderConfigurationError("invalid provider response bound")
        self._endpoints = endpoints
        self._transport = http_transport
        self._maximum = max_response_bytes
        self._receipt_clock = receipt_clock

    async def request(self, provider: str, method: str, params: list[object]) -> object:
        """Return one identity-checked RPC result; errors never include provider payloads."""
        payload, _ = await self._request(provider, method, params, observe=False)
        return payload

    async def request_observed(self, provider: str, method: str, params: list[object]) -> ObservedRpcResult:
        """Retain the complete body receipt before JSON decoding or attribution."""
        if self._receipt_clock is None:
            raise ProviderConfigurationError("an observed RPC receipt clock is required")
        payload, receipt = await self._request(provider, method, params, observe=True)
        if receipt is None or receipt.wall_timestamp_ns is None:
            raise ProviderUnavailable("native provider receipt unavailable")
        return ObservedRpcResult(payload, receipt)

    async def _request(
        self,
        provider: str,
        method: str,
        params: list[object],
        *,
        observe: bool,
    ) -> tuple[object, TransportReceipt | None]:
        if method not in self._READ_METHODS:
            raise ProviderConfigurationError("provider transport is read-only")
        endpoint = self._endpoints.http_url(provider)
        try:
            transport = self._transport or httpx.AsyncHTTPTransport(retries=0, trust_env=False)
            request = httpx.Request(
                "POST",
                endpoint,
                json={
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": method,
                    "params": params,
                },
                extensions={"timeout": {"connect": 3.0, "read": 8.0, "write": 8.0, "pool": 3.0}},
            )
            async with transport:
                response = await transport.handle_async_request(request)
                try:
                    if response.status_code != 200:
                        raise ProviderUnavailable("provider HTTP read unavailable")
                    content = bytearray()
                    async for chunk in response.aiter_bytes():
                        if len(content) + len(chunk) > self._maximum:
                            raise ProviderUnavailable("provider response exceeds bound")
                        content.extend(chunk)
                    receipt = self._receipt_clock.capture() if observe and self._receipt_clock is not None else None
                finally:
                    await response.aclose()
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
            return raw["result"], receipt
        except Exception:
            raise ProviderUnavailable("provider read unavailable") from None
