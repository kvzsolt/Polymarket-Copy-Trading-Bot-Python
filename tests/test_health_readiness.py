from __future__ import annotations

import pytest
from aiohttp.test_utils import make_mocked_request

from services.healthServer import health_handler, update_readiness


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
