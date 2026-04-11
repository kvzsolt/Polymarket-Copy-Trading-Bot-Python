from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from aiohttp.test_utils import make_mocked_request

from services.healthServer import health_handler, start_health_server, update_readiness


@pytest.mark.asyncio
async def test_health_includes_readiness_fields():
    update_readiness(
        db_ok=True,
        api_ok=True,
        last_trade_poll_ts=123,
        last_risk_check_ts=124,
        last_successful_order_ts=125,
        monitored_leader_count=3,
        runtime_user_count=2,
    )
    response = await health_handler(make_mocked_request("GET", "/health"))
    payload = response.text
    assert '"ok": true' in payload
    assert '"runtime_user_count": 2' in payload
    assert '"monitored_leader_count": 3' in payload


@pytest.mark.asyncio
async def test_metrics_server_binds_to_localhost_by_default():
    with patch.dict("os.environ", {}, clear=True):
        with patch("services.healthServer.web.TCPSite") as site_cls:
            site_instance = AsyncMock()
            site_cls.return_value = site_instance

            runner = await start_health_server(port=9102)

            _, kwargs = site_cls.call_args
            assert kwargs["host"] == "127.0.0.1"
            assert kwargs["port"] == 9102

            await runner.cleanup()


@pytest.mark.asyncio
async def test_metrics_server_uses_validated_bind_host_from_env():
    with patch.dict("os.environ", {"METRICS_BIND_HOST": "127.0.0.1"}, clear=True):
        with patch("services.healthServer.web.TCPSite") as site_cls:
            site_instance = AsyncMock()
            site_cls.return_value = site_instance

            runner = await start_health_server(port=9102)

            _, kwargs = site_cls.call_args
            assert kwargs["host"] == "127.0.0.1"
            assert kwargs["port"] == 9102

            await runner.cleanup()
