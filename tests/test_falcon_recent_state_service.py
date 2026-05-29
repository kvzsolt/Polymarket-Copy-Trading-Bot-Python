from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pytest

from services.discovery.falcon_recent_state_service import (
    FALCON_PROFIT_AND_LOSS_AGENT_ID,
    FalconRecentStateService,
)


@pytest.mark.asyncio
async def test_falcon_recent_state_sums_3d_and_7d_pnl_windows():
    class _Client:
        def __init__(self):
            self.calls = []

        async def query(self, *, agent_id, params, limit=100, offset=0, format_type="raw"):
            self.calls.append(
                {
                    "agent_id": agent_id,
                    "params": params,
                    "limit": limit,
                    "offset": offset,
                    "format_type": format_type,
                }
            )
            if len(self.calls) == 1:
                return SimpleNamespace(results=[{"pnl": "2.50"}, {"pnl": "-1.00"}])
            return SimpleNamespace(results=[{"pnl": "5"}, {"pnl": None}])

    client = _Client()
    state = await FalconRecentStateService(client).fetch_recent_state(
        "0x1111111111111111111111111111111111111111"
    )

    assert state.pnl_7d == Decimal("1.50")
    assert state.pnl_3d == Decimal("5")
    assert [c["agent_id"] for c in client.calls] == [
        FALCON_PROFIT_AND_LOSS_AGENT_ID,
        FALCON_PROFIT_AND_LOSS_AGENT_ID,
    ]
    assert client.calls[0]["params"]["granularity"] == "1d"
    assert client.calls[0]["params"]["condition_id"] == "ALL"
    assert client.calls[0]["params"]["wallet"] == "0x1111111111111111111111111111111111111111"
