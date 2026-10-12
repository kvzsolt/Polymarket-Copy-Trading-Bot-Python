"""Regressions for premium Falcon routing and fail-closed auto startup."""

from __future__ import annotations

import io
import logging
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
import yaml

from pmcopy.config.app_config import AppConfig, DiscoveryConfig, FalconApiConfig
from pmcopy.integrations.falcon.client import FalconClient
from pmcopy.integrations.falcon.errors import (
    FalconAuthenticationError,
    FalconBadRequestError,
    FalconConfigurationError,
    FalconError,
    FalconPermissionError,
    FalconProtocolError,
    FalconUnavailableError,
)
from pmcopy.orchestrators.leader_registry import LeaderRegistry
from pmcopy.orchestrators.runtime_discovery import AutoTraderStartupBurst
from pmcopy.services.discovery import operator_cli
from pmcopy.services.discovery.discovery_models import DiscoveryResult
from tests.test_falcon_client import FakeHttpClient, FakeResponse, page

PREMIUM_ENDPOINT = "https://retriever.falconapi.net/api/v2/semantic/retrieve/parameterized"
OVERRIDE_ENDPOINT = "https://falcon-operator.invalid/query"
WALLET = "0x" + "a" * 40
# Synthetic bearer tokens assembled from parts at import time so no
# credential-shaped literal appears in source; the tests still assert the
# exact configured value flows to the transport unchanged.
API_TOKEN = "-".join(("synthetic", "premium", "token"))
CLI_TOKEN = "-".join(("synthetic", "token"))
PERMANENT_ERRORS = (
    FalconAuthenticationError,
    FalconPermissionError,
    FalconConfigurationError,
    FalconBadRequestError,
    FalconProtocolError,
)


@pytest.mark.asyncio
async def test_default_client_uses_premium_endpoint_and_one_bearer_header() -> None:
    """A raw configured token reaches the premium host in the documented form."""
    http = FakeHttpClient([FakeResponse(200, page([], offset=0, has_more=False))])
    client = FalconClient(api_key=API_TOKEN, http_client=http)

    result = await client.request(584, limit=10)

    assert http.requests[0]["url"] == PREMIUM_ENDPOINT
    assert http.requests[0]["headers"]["Authorization"] == f"Bearer {API_TOKEN}"
    assert result.results == ()


def test_both_shipped_configuration_defaults_use_premium_endpoint() -> None:
    """The model and tracked service configuration must select the same host."""
    config_path = Path(__file__).resolve().parents[1] / "src" / "pmcopy" / "config" / "discovery.yml"
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))

    assert FalconApiConfig().base_url == PREMIUM_ENDPOINT
    assert config["api"]["base_url"] == PREMIUM_ENDPOINT
    assert FalconApiConfig(base_url=OVERRIDE_ENDPOINT).base_url == OVERRIDE_ENDPOINT


@pytest.mark.asyncio
async def test_ranking_cli_composition_preserves_explicit_endpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The standalone CLI must pass the selected endpoint to its provider."""
    factory = MagicMock(return_value=SimpleNamespace(close=AsyncMock()))
    monkeypatch.setattr(operator_cli, "FalconClient", factory)

    resources = await operator_cli.build_cli_resources(
        api_key=CLI_TOKEN,
        ccxt_depth="off",
        timeout_seconds=3,
        base_url=OVERRIDE_ENDPOINT,
    )
    await resources.close()

    assert factory.call_args.kwargs["base_url"] == OVERRIDE_ENDPOINT


@pytest.mark.asyncio
@pytest.mark.parametrize("endpoint", [None, OVERRIDE_ENDPOINT])
async def test_ranking_cli_forwards_endpoint_environment_override(
    monkeypatch: pytest.MonkeyPatch, endpoint: str | None
) -> None:
    """One canonical environment override controls premium CLI routing."""
    resources = SimpleNamespace(
        service=SimpleNamespace(
            discover=AsyncMock(
                return_value=DiscoveryResult(ranked=(), rejected=(), source="falcon")
            )
        ),
        close=AsyncMock(),
    )
    builder = AsyncMock(return_value=resources)
    monkeypatch.setattr(operator_cli, "build_cli_resources", builder)
    environ = {"PMCOPY__DISCOVERY__API__TOKEN": "synthetic-token"}
    if endpoint is not None:
        environ["PMCOPY__DISCOVERY__API__BASE_URL"] = endpoint

    status = await operator_cli.run_cli(
        ["--funds", "100", "--ccxt-depth", "off"],
        environ=environ,
        stdout=io.StringIO(),
        stderr=io.StringIO(),
    )

    assert status == 3
    assert builder.await_args.kwargs["base_url"] == (endpoint or PREMIUM_ENDPOINT)
    resources.close.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("status_code", [400, 404])
async def test_provider_request_rejection_never_exposes_error_body(status_code: int) -> None:
    """Provider validation errors are terminal and safe to show to an operator."""
    secret = "synthetic-echoed-credential"
    http = FakeHttpClient([FakeResponse(status_code, {"error": f"invalid key {secret}"})])
    client = FalconClient(api_key=secret, http_client=http)

    with pytest.raises(FalconBadRequestError) as captured:
        await client.request(584, limit=10)

    assert str(captured.value) == f"Falcon request rejected ({status_code})"
    assert len(http.requests) == 1


class StartupFixture:
    """Compose the real startup coordinator around deterministic boundaries."""

    def __init__(self, error: FalconError) -> None:
        self.service = SimpleNamespace(
            run=AsyncMock(side_effect=error),
            persisted_wallets=AsyncMock(return_value=[WALLET]),
            config_hash="current-policy",
        )
        self.repository = SimpleNamespace(
            list_active=AsyncMock(return_value=[WALLET]),
            list_roster=AsyncMock(return_value=[]),
            save_roster=AsyncMock(),
        )
        self.registry = LeaderRegistry(
            discovery_service=self.service,
            leaders_repo=self.repository,
            config=AppConfig(
                discovery=DiscoveryConfig.model_validate({"api": {"token": "synthetic-token"}})
            ),
            readiness_tracker=MagicMock(),
            logger_instance=logging.getLogger("test.PremiumStartup"),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("error_type", PERMANENT_ERRORS)
async def test_forced_auto_startup_stops_after_permanent_provider_failure(
    error_type: type[FalconError], caplog: pytest.LogCaptureFixture
) -> None:
    """Neither the burst nor a persisted roster may mask a permanent failure."""
    secret = "synthetic-untrusted-provider-detail"
    fixture = StartupFixture(error_type(secret))
    burst = AutoTraderStartupBurst(fixture.registry, (), 3)

    with caplog.at_level(logging.WARNING, logger="test.PremiumStartup"):
        with pytest.raises(error_type) as captured:
            await burst.run()

    assert secret not in str(captured.value)
    assert captured.value.__suppress_context__
    assert all(secret not in str(record.__dict__) for record in caplog.records)
    fixture.service.run.assert_awaited_once_with(wallets=[WALLET])
    fixture.repository.list_active.assert_not_awaited()
    fixture.repository.save_roster.assert_not_awaited()


@pytest.mark.asyncio
async def test_transient_failure_preserves_existing_roster_policy() -> None:
    """The permanent-error fix does not alter existing outage handling."""
    fixture = StartupFixture(FalconUnavailableError("provider temporarily unavailable"))

    state = await fixture.registry.refresh(force_discovery=True)

    assert state.active_monitored == [WALLET]
    fixture.repository.list_active.assert_awaited_once()
    fixture.repository.save_roster.assert_not_awaited()
