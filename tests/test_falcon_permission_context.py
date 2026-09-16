"""Keep denied Falcon agent identity actionable without exposing provider details."""

from __future__ import annotations

import logging
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
import yaml

from config.app_config import DiscoveryConfig
from integrations.falcon.client import FalconClient
from integrations.falcon.errors import FalconPermissionError
from services.discovery.falcon_candidate_service import (
    FalconCandidateService,
    FalconDiscoveryPolicy,
)
from services.discovery.policy_config import discovery_policy_from_config
from tests.test_falcon_candidate_service import GOOD, NOW, SYBIL, FakeFalcon
from tests.test_falcon_client import FakeHttpClient, FakeResponse
from tests.test_falcon_premium_startup import StartupFixture

SECRET = "synthetic-provider-credential"


@pytest.fixture
def operator_policy() -> FalconDiscoveryPolicy:
    """Load only the shipped discovery policy without operator secrets."""
    path = Path(__file__).resolve().parents[1] / "config" / "discovery.yml"
    config = DiscoveryConfig.model_validate(yaml.safe_load(path.read_text(encoding="utf-8")))
    return discovery_policy_from_config(config)


@pytest.mark.asyncio
async def test_operator_policy_skips_price_jumps_but_rejects_sybil_wallets(
    operator_policy: FalconDiscoveryPolicy,
) -> None:
    """The explicit plan configuration omits agent 596 without bypassing other gates."""
    assert operator_policy.enable_price_jump_risk is False
    fake = FakeFalcon()
    price_jumps = AsyncMock(side_effect=FalconPermissionError("agent not in plan"))
    fake.price_jumps = price_jumps

    result = await FalconCandidateService(fake, policy=operator_policy, now=lambda: NOW).discover(
        Decimal("500"),
        wallets=[{"wallet": GOOD, "h_score": "82"}, {"wallet": SYBIL, "h_score": "95"}],
    )

    price_jumps.assert_not_awaited()
    assert "sybil_risk" in next(row.reasons for row in result.rejected if row.wallet == SYBIL)


def denied_response() -> FakeResponse:
    """Reproduce the premium plan denial with hostile response diagnostics."""
    return FakeResponse(
        403,
        {
            "error": {
                "code": "AGENT_NOT_ALLOWED",
                "message": f"agent not in user's plan: {SECRET}",
                "details": {"agent_id": 999, "user_id": "synthetic-user"},
            }
        },
        headers={"X-Request-Id": SECRET},
    )


@pytest.mark.asyncio
async def test_permission_error_identifies_requested_agent_without_retry_or_response_details() -> (
    None
):
    """The request agent is trusted; response metadata and messages are not."""
    http = FakeHttpClient([denied_response()])
    client = FalconClient(api_key=SECRET, http_client=http, max_attempts=5)

    with pytest.raises(FalconPermissionError) as captured:
        await client.price_jumps(token_id="123", limit=10)

    assert getattr(captured.value, "agent_id", None) == 596
    assert "agent_id=596" in str(captured.value)
    assert SECRET not in str(captured.value)
    assert "synthetic-user" not in str(captured.value)
    assert len(http.requests) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("endpoint", "agent_id"),
    [("price_jumps", 596), ("leaderboard", 584), ("pnl_leaderboard", 579)],
)
async def test_startup_preserves_denied_agent_through_candidate_and_registry_redaction(
    endpoint: str,
    agent_id: int,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The operator can identify the denied agent without accepting a stale roster."""
    http = FakeHttpClient([denied_response() for _ in range(6)])
    client = FalconClient(api_key=SECRET, http_client=http)
    fake = FakeFalcon()
    monkeypatch.setattr(fake, endpoint, getattr(client, endpoint), raising=False)
    candidates = FalconCandidateService(fake, now=lambda: NOW)
    fixture = StartupFixture(FalconPermissionError("unused"))

    async def discover() -> None:
        """Run the actual candidate pipeline with deterministic provider evidence."""
        wallets = None if endpoint == "leaderboard" else [{"wallet": GOOD, "h_score": "82"}]
        await candidates.discover(Decimal("500"), wallets=wallets)

    fixture.service.run = AsyncMock(side_effect=discover)

    with caplog.at_level(logging.WARNING, logger="test.PremiumStartup"):
        with pytest.raises(FalconPermissionError) as captured:
            await fixture.registry.refresh(force_discovery=True)

    assert getattr(captured.value, "agent_id", None) == agent_id
    assert f"agent_id={agent_id}" in str(captured.value)
    assert "plan" in str(captured.value).lower()
    assert SECRET not in str(captured.value)
    assert captured.value.__suppress_context__
    failures = [record for record in caplog.records if record.message == "discovery_refresh_failed"]
    assert len(failures) == 1
    assert getattr(failures[0], "falcon_agent_id", None) == agent_id
    assert all(SECRET not in str(record.__dict__) for record in caplog.records)
    fixture.repository.list_active.assert_not_awaited()
    fixture.repository.save_roster.assert_not_awaited()
