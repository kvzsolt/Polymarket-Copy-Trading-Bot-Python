"""Compose one Finland-local admission ledger for all approved RPC consumers."""

from __future__ import annotations

from datetime import UTC, datetime
from dataclasses import dataclass
from pathlib import Path
import time
from typing import Any
from urllib.parse import urlsplit

import httpx
from web3 import Web3

from pmcopy.clients.network.provider_transport import ProviderEndpoints, ProviderHttpTransport
from pmcopy.clients.network.provider_web3 import MeteredWeb3Provider
from pmcopy.domain.provider_budget import ProviderConfigurationError
from pmcopy.interfaces.provider_budget import ProviderClock
from pmcopy.adapters.clock import SystemReceiptClock
from pmcopy.interfaces.source_time import ReceiptClock
from pmcopy.services.observed_provider_gateway import ObservedProviderGateway
from pmcopy.services.provider_balances import ProviderBalanceReader
from pmcopy.storage.provider_budget import SqliteProviderBudget


class SystemProviderClock:
    """Production clock; deterministic tests inject their own clock."""

    def now(self) -> datetime:
        """Return UTC billing time."""
        return datetime.now(UTC)

    def monotonic_ns(self) -> int:
        """Return monotonic duration units."""
        return time.monotonic_ns()


@dataclass(frozen=True)
class ProviderRuntime:
    """Own the ledger and transport adapters independently of the Hungary network."""

    budget: SqliteProviderBudget
    endpoints: ProviderEndpoints
    gateway: ObservedProviderGateway
    primary: str
    balances: ProviderBalanceReader

    @classmethod
    def from_config(
        cls,
        config: Any,
        *,
        clock: ProviderClock | None = None,
        receipt_clock: ReceiptClock | None = None,
        http_transport: httpx.AsyncBaseTransport | None = None,
    ) -> ProviderRuntime:
        """Validate the configured account before any startup RPC or detector starts."""
        rpc_url = str(getattr(config.polymarket, "rpc_url", "") or "").strip()
        if rpc_url in {"", "https://polygon-rpc.com"}:
            onchain = getattr(getattr(config, "detection", None), "onchain", None)
            wss_url = str(getattr(onchain, "alchemy_wss_url", "") or "").strip()
            if wss_url.startswith("wss://"):
                rpc_url = "https://" + wss_url.removeprefix("wss://")
        fallback_url = str(getattr(config.polymarket, "fallback_rpc_url", "") or "").strip()
        urls = tuple(dict.fromkeys(url for url in (rpc_url, fallback_url) if url))
        if len(urls) != 1:
            raise ProviderConfigurationError("exactly one Alchemy Polygon endpoint is supported")
        only_url = urls[0]
        if urlsplit(only_url).hostname != "polygon-mainnet.g.alchemy.com":
            raise ProviderConfigurationError("only the Alchemy Polygon provider is supported")
        fixed = ProviderEndpoints(alchemy=only_url)
        primary = "alchemy"
        settings = config.provider_budget
        budget = SqliteProviderBudget(
            Path(settings.ledger_path), settings.limits, clock or SystemProviderClock()
        )
        gateway = ObservedProviderGateway(
            budget,
            ProviderHttpTransport(
                fixed,
                http_transport=http_transport,
                receipt_clock=receipt_clock or SystemReceiptClock(),
            ),
        )
        contracts = tuple(
            getattr(config.polymarket, name, "")
            for name in (
                "collateral_contract_address",
                "pusd_contract_address",
                "usdc_contract_address",
            )
        )
        return cls(
            budget, fixed, gateway, primary, ProviderBalanceReader(gateway, primary, contracts)
        )

    def web3(self, alias: str | None = None) -> Web3:
        """Build a metered read-only SDK adapter sharing the process ledger."""
        return Web3(MeteredWeb3Provider(self.budget, self.endpoints, alias or self.primary))

    async def post_json(self, url: str, *, json_body: dict[str, object], silent: bool) -> object:
        """Adapt existing single-read ports without bypassing admission or retry accounting."""
        method, params = json_body.get("method"), json_body.get("params")
        if not isinstance(method, str) or not isinstance(params, list):
            raise ProviderConfigurationError("invalid RPC read envelope")
        result = await self.gateway.call(self.endpoints.alias_for(url), method, params)
        return {"jsonrpc": "2.0", "id": json_body.get("id"), "result": result}

    def close(self) -> None:
        """Close local resources without resetting provider history."""
        self.budget.close()

    async def aclose(self) -> None:
        """Release resources through the application's asynchronous lifecycle."""
        self.close()
