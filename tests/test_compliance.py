from unittest.mock import AsyncMock, Mock, patch

import aiohttp
import pytest

from services.compliance import GEOBLOCK_URL, check_geoblock


@pytest.mark.asyncio
async def test_check_geoblock_blocks_region():
    with patch("services.compliance.fetch_data", new=AsyncMock(return_value={"blocked": True})):
        allowed, reason = await check_geoblock()
    assert allowed is False
    assert reason == "region_blocked_by_polymarket"


@pytest.mark.asyncio
async def test_check_geoblock_allows_when_not_blocked():
    with patch("services.compliance.fetch_data", new=AsyncMock(return_value={"blocked": False})):
        allowed, reason = await check_geoblock()
    assert allowed is True
    assert reason == "ok"


@pytest.mark.asyncio
async def test_check_geoblock_fails_open_on_http_status_error():
    request_info = Mock()
    request_info.real_url = GEOBLOCK_URL
    exc = aiohttp.ClientResponseError(
        request_info=request_info,
        history=(),
        status=503,
        message="Service Unavailable",
    )
    with patch("services.compliance.fetch_data", new=AsyncMock(side_effect=exc)):
        allowed, reason = await check_geoblock()
    assert allowed is True
    assert reason == "geoblock_check_http_503"


@pytest.mark.asyncio
async def test_check_geoblock_fails_open_on_transport_error():
    with patch("services.compliance.fetch_data", new=AsyncMock(side_effect=RuntimeError("network down"))):
        allowed, reason = await check_geoblock()
    assert allowed is True
    assert reason == "geoblock_check_failed_open"


@pytest.mark.asyncio
async def test_check_geoblock_fails_open_on_invalid_payload_shape():
    with patch("services.compliance.fetch_data", new=AsyncMock(return_value=["not", "a", "dict"])):
        allowed, reason = await check_geoblock()
    assert allowed is True
    assert reason == "geoblock_check_failed_open"
