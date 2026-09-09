"""PnL endpoint evidence uses integer outcome counts instead of ambiguous rate units."""

from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from integrations.falcon.errors import FalconAuthenticationError, FalconProtocolError
from services.discovery.falcon_candidate_service import FalconCandidateService
from tests.test_falcon_candidate_service import FakeFalcon, NOW


@pytest.mark.parametrize("raw", ["0", "0.5", "1", "1.01", "2", "50", "100"])
def test_unversioned_pnl_win_rate_without_counts_is_missing_evidence(raw):
    assert FalconCandidateService._pnl_summary([{"trades": 10, "win_rate": raw}])[2] is None


def test_outcome_counts_preserve_genuine_zero_win_rate():
    assert FalconCandidateService._pnl_summary([{"wins": 0, "losses": 10, "win_rate": "1"}])[2] == 0


@pytest.mark.parametrize("raw", [True, "NaN", "Infinity", "0.5", "-1"])
def test_malformed_outcome_count_is_a_provider_protocol_error(raw):
    with pytest.raises(FalconProtocolError):
        FalconCandidateService._pnl_summary([{"wins": raw, "losses": 10}])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "endpoint", ["wallet_360", "pnl", "trades", "wallet_stats_all_time", "market_360", "markets"]
)
async def test_auxiliary_authentication_failure_never_becomes_a_ranked_candidate(endpoint):
    client = FakeFalcon()
    setattr(client, endpoint, AsyncMock(side_effect=FalconAuthenticationError("synthetic-token")))
    with pytest.raises(FalconAuthenticationError) as error:
        await FalconCandidateService(client, now=lambda: NOW).discover(Decimal(350))
    assert "synthetic-token" not in str(error.value)


@pytest.mark.asyncio
@pytest.mark.parametrize("endpoint", ["pnl_leaderboard", "price_jumps"])
async def test_optional_provider_requests_do_not_hide_authentication_errors(endpoint):
    client = FakeFalcon()
    setattr(client, endpoint, AsyncMock(side_effect=FalconAuthenticationError("synthetic-token")))
    with pytest.raises(FalconAuthenticationError) as error:
        await FalconCandidateService(client, now=lambda: NOW).discover(Decimal(350))
    assert "synthetic-token" not in str(error.value)


@pytest.mark.asyncio
async def test_invalid_wallet_schema_cannot_be_downgraded_to_one_missing_window():
    client = FakeFalcon()
    read = client.wallet_360

    async def malformed(wallet, window):
        rows = await read(wallet, window)
        if window == 1:
            rows[0]["win_rate"] = True
        return rows

    client.wallet_360 = malformed
    with pytest.raises(FalconProtocolError):
        await FalconCandidateService(client, now=lambda: NOW).discover(Decimal(350))
