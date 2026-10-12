"""Contracts for the Falcon-only analytics provider boundary."""

from __future__ import annotations

import importlib.util
from typing import Any

import pytest

from pmcopy.config.app_config import DiscoveryConfig
from pmcopy.integrations.falcon.client import FalconClient
from pmcopy.integrations.falcon.models import LeaderboardEntry
from pmcopy.services.discovery.operator_cli import DiscoveryCliError, resolve_api_key


class _Response:
    """Return one valid Falcon result envelope."""

    status_code = 200

    def json(self) -> dict[str, Any]:
        """Return a deterministic current Falcon response."""
        return {
            "timestamp": "2026-08-12T00:00:00Z",
            "params": {},
            "pagination": {"limit": 1, "offset": 0, "has_more": False},
            "data": {"results": [{"wallet": "0xabc"}]},
            "request_id": "request-1",
        }


class _HttpClient:
    """Capture the private Falcon request without performing network I/O."""

    def __init__(self) -> None:
        self.url = ""
        self.headers: dict[str, str] = {}
        self.body: dict[str, Any] = {}

    async def post(
        self,
        url: str,
        *,
        headers: dict[str, str],
        json: dict[str, Any],
    ) -> _Response:
        """Capture one request and return a valid response."""
        self.url = url
        self.headers = headers
        self.body = json
        return _Response()


@pytest.mark.asyncio
async def test_falcon_client_preserves_injected_private_endpoint() -> None:
    """The adapter must never substitute a public URL for a paid endpoint."""
    http = _HttpClient()
    private_endpoint = "https://paid-falcon.invalid/private/query"
    client = FalconClient(
        api_key="tkn1",
        base_url=private_endpoint,
        http_client=http,
    )

    page = await client.request(584, limit=1)

    assert http.url == private_endpoint
    assert http.headers["Authorization"] == "Bearer tkn1"
    assert http.body["agent_id"] == 584
    assert page.request_id == "request-1"


def test_discovery_config_has_one_falcon_provider_policy() -> None:
    """Configuration must expose Falcon and omit the retired provider name."""
    config = DiscoveryConfig.model_validate({})

    assert config.falcon.enabled is False
    assert config.api.has_token is False
    assert not hasattr(config.falcon, "api_key")
    assert not hasattr(config, "heisenberg")


def test_retired_provider_package_is_absent() -> None:
    """The obsolete provider package must not remain as an active alias."""
    assert importlib.util.find_spec("pmcopy.integrations.heisenberg") is None


def test_operator_cli_uses_the_canonical_falcon_token_name() -> None:
    """The CLI and runtime must read one deployment credential authority."""
    assert resolve_api_key({"PMCOPY__DISCOVERY__API__TOKEN": "tkn1"}) == "tkn1"
    retired_env_name = "PMCOPY_" + "FALCON_" + "API_KEY"
    with pytest.raises(DiscoveryCliError):
        resolve_api_key({retired_env_name: "retired-alias"})


def test_leaderboard_parser_accepts_paid_h_score_field() -> None:
    """The paid Falcon endpoint's ``h_score`` must not become zero."""
    entry = LeaderboardEntry.from_mapping({"wallet": "0xabc", "h_score": "87.4"})

    assert str(entry.h_score) == "87.4"
