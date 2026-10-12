from unittest.mock import AsyncMock, Mock, patch
from datetime import datetime

import aiohttp
import pytest

from pmcopy.services.compliance import GEOBLOCK_URL, check_geoblock


@pytest.mark.asyncio
async def test_geoblock_blocked_fails_closed():
    with patch("pmcopy.services.compliance.fetch_data", new=AsyncMock(return_value={"blocked": True})):
        decision = await check_geoblock()
    assert decision.allowed is False
    assert decision.reason == "region_blocked_by_polymarket"
    assert decision.source == "polymarket"
    assert isinstance(decision.checked_at, datetime)


@pytest.mark.asyncio
async def test_geoblock_allowed_passes():
    with patch("pmcopy.services.compliance.fetch_data", new=AsyncMock(return_value={"blocked": False})):
        decision = await check_geoblock()
    assert decision.allowed is True
    assert decision.reason == "ok"
    assert decision.source == "polymarket"
    assert isinstance(decision.expires_at, datetime)


@pytest.mark.asyncio
async def test_geoblock_http_error_fails_closed():
    request_info = Mock()
    request_info.real_url = GEOBLOCK_URL
    exc = aiohttp.ClientResponseError(
        request_info=request_info,
        history=(),
        status=503,
        message="Service Unavailable",
    )
    with patch("pmcopy.services.compliance.fetch_data", new=AsyncMock(side_effect=exc)):
        decision = await check_geoblock()
    assert decision.allowed is False
    assert decision.reason == "geoblock_failed_closed_http_503"
    assert "error" in decision.raw


@pytest.mark.asyncio
async def test_geoblock_timeout_fails_closed():
    with patch(
        "pmcopy.services.compliance.fetch_data", new=AsyncMock(side_effect=RuntimeError("network down"))
    ):
        decision = await check_geoblock()
    assert decision.allowed is False
    assert decision.reason == "geoblock_failed_closed"
    assert "error" in decision.raw


@pytest.mark.asyncio
async def test_geoblock_invalid_payload_fails_closed():
    with patch("pmcopy.services.compliance.fetch_data", new=AsyncMock(return_value=["not", "a", "dict"])):
        decision = await check_geoblock()
    assert decision.allowed is False
    assert decision.reason == "geoblock_failed_closed_invalid_payload"
    assert decision.raw.get("payload_type") == "list"


@pytest.mark.asyncio
async def test_geoblock_missing_blocked_field_fails_closed():
    with patch("pmcopy.services.compliance.fetch_data", new=AsyncMock(return_value={"other": "value"})):
        decision = await check_geoblock()
    assert decision.allowed is False
    assert decision.reason == "geoblock_failed_closed_missing_field"
    assert "payload_keys" in decision.raw


@pytest.mark.asyncio
async def test_live_mode_refuses_when_compliance_unknown():
    with patch("pmcopy.services.compliance.fetch_data", new=AsyncMock(return_value={"blocked": None})):
        decision = await check_geoblock()
    assert decision.allowed is False
    assert decision.reason == "geoblock_failed_closed_missing_field"
