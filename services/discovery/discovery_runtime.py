"""Composition seam for the new discovery engine.

No existing Falcon discovery implementation is removed.  Without a configured
Heisenberg API key, or when the primary service is disabled, the existing
provider is returned untouched through the failover wrapper.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Mapping

from integrations.heisenberg.client import HeisenbergClient
from services.discovery.ccxt_capacity_probe import CcxtCapacityProbe
from services.discovery.composite_candidate_service import CompositeCandidateService
from services.discovery.heisenberg_candidate_service import (
    HeisenbergCandidateService,
    HeisenbergDiscoveryPolicy,
)
from services.discovery.policy_config import discovery_policy_from_config


def _attr(source: Any, name: str, default: Any = None) -> Any:
    if source is None:
        return default
    if isinstance(source, Mapping):
        return source.get(name, default)
    return getattr(source, name, default)


def _secret(value: Any) -> str:
    getter = getattr(value, "get_secret_value", None)
    if callable(getter):
        value = getter()
    return str(value or "").strip()


def _enabled(value: Any, default: bool = True) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


@dataclass(slots=True)
class DiscoveryRuntimeBundle:
    service: Any
    heisenberg_client: HeisenbergClient | None = None

    async def close(self) -> None:
        if self.heisenberg_client is not None:
            await self.heisenberg_client.close()


def build_discovery_runtime(
    *,
    config: Any,
    fallback: Any,
    ccxt_market_data: Any | None = None,
    policy: HeisenbergDiscoveryPolicy | None = None,
    http_client: Any | None = None,
    environ: Mapping[str, str] | None = None,
) -> DiscoveryRuntimeBundle:
    env = os.environ if environ is None else environ
    discovery = _attr(config, "discovery")
    heisenberg_cfg = _attr(discovery, "heisenberg")
    token = _secret(
        _attr(heisenberg_cfg, "api_key")
        or _attr(discovery, "heisenberg_api_key")
        or env.get("PMCOPY_HEISENBERG_API_KEY")
        or env.get("HEISENBERG_API_KEY")
    )
    enabled = _enabled(
        _attr(heisenberg_cfg, "enabled", _attr(discovery, "heisenberg_enabled")),
        default=True,
    )
    if not enabled or not token:
        return DiscoveryRuntimeBundle(service=fallback)

    client = HeisenbergClient(api_key=token, http_client=http_client)
    capacity_probe = CcxtCapacityProbe(ccxt_market_data) if ccxt_market_data is not None else None
    resolved_policy = policy or discovery_policy_from_config(config)
    primary = HeisenbergCandidateService(
        client,
        policy=resolved_policy,
        capacity_probe=capacity_probe,
    )
    return DiscoveryRuntimeBundle(
        service=CompositeCandidateService(primary=primary, fallback=fallback),
        heisenberg_client=client,
    )


__all__ = ["DiscoveryRuntimeBundle", "build_discovery_runtime"]
