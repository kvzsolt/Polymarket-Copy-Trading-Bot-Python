from unittest.mock import AsyncMock, Mock, patch

import aiohttp
import pytest

from services.compliance import GEOBLOCK_URL, check_geoblock


@pytest.mark.asyncio
async def test_check_geoblock_blocks_region():
    with patch("services.compliance.fetch_data", new=AsyncMock(return_value={"blocked": True})):
        decision = await check_geoblock()
    assert decision.allowed is False
    assert decision.reason == "region_blocked_by_polymarket"


@pytest.mark.asyncio
async def test_check_geoblock_allows_when_not_blocked():
    with patch("services.compliance.fetch_data", new=AsyncMock(return_value={"blocked": False})):
        decision = await check_geoblock()
    assert decision.allowed is True
    assert decision.reason == "ok"


@pytest.mark.asyncio
async def test_check_geoblock_fails_closed_on_http_status_error():
    request_info = Mock()
    request_info.real_url = GEOBLOCK_URL
    exc = aiohttp.ClientResponseError(
        request_info=request_info,
        history=(),
        status=503,
        message="Service Unavailable",
    )
    with patch("services.compliance.fetch_data", new=AsyncMock(side_effect=exc)):
        decision = await check_geoblock()
    assert decision.allowed is False
    assert decision.reason == "geoblock_failed_closed_http_503"
    assert "error" in decision.metadata


@pytest.mark.asyncio
async def test_check_geoblock_fails_closed_on_transport_error():
    with patch("services.compliance.fetch_data", new=AsyncMock(side_effect=RuntimeError("network down"))):
        decision = await check_geoblock()
    assert decision.allowed is False
    assert decision.reason == "geoblock_failed_closed"
    assert "error" in decision.metadata


@pytest.mark.asyncio
async def test_check_geoblock_fails_closed_on_invalid_payload_shape():
    with patch("services.compliance.fetch_data", new=AsyncMock(return_value=["not", "a", "dict"])):
        decision = await check_geoblock()
    assert decision.allowed is False
    assert decision.reason == "geoblock_failed_closed_invalid_payload"
    assert decision.metadata.get("payload_type") == "list"


@pytest.mark.asyncio
async def test_check_geoblock_fails_closed_on_missing_blocked_field():
    with patch("services.compliance.fetch_data", new=AsyncMock(return_value={"other": "value"})):
        decision = await check_geoblock()
    assert decision.allowed is False
    assert decision.reason == "geoblock_failed_closed_missing_field"
    assert "payload_keys" in decision.metadata
