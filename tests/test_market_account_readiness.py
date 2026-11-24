"""One healthy account cannot overwrite another market runtime's failure."""

import asyncio
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import pmcopy.app as app
from pmcopy.services.observability.health_server import ReadinessTracker
from tests.test_book_update_atomicity import seeded_store


@pytest.mark.asyncio
async def test_real_market_runtime_wiring_aggregates_account_readiness(monkeypatch) -> None:
    stopped = asyncio.Event()

    async def connect(tokens):
        await stopped.wait()

    monkeypatch.setattr(
        "pmcopy.orchestrators.streaming.market_data_runtime.MarketWebSocket",
        lambda **kwargs: SimpleNamespace(connect=connect, stop=AsyncMock()),
    )
    users = [SimpleNamespace(user_id=account) for account in ("a", "b")]
    states = {
        user.user_id: SimpleNamespace(market_store=seeded_store(), clob_gateway=AsyncMock())
        for user in users
    }
    for state in states.values():
        state.market_store.set_tick(
            "token", tick_size=Decimal(".005"), min_order_size=Decimal(5), neg_risk=False
        )
    readiness = ReadinessTracker()
    runtimes = await app._start_market_data_runtimes(
        runtime_users=users,
        user_states=states,
        live_positions_by_user={user.user_id: [{"asset": "token"}] for user in users},
        contexts_by_leader={},
        readiness=readiness,
        config=SimpleNamespace(),
    )
    try:
        await runtimes["a"]._on_wss_connected()
        await runtimes["a"]._on_market_state_change("token", "book")
        assert readiness.snapshot()["wss_market_ok"] is False
        assert readiness.snapshot()["market_data_ready"] is False
        await runtimes["b"]._on_wss_connected()
        await runtimes["b"]._on_market_state_change("token", "book")
        assert readiness.snapshot()["wss_market_ok"] is True
        assert readiness.snapshot()["market_data_ready"] is True
        assert readiness.snapshot()["subscribed_token_count"] == 2
        await runtimes["b"]._on_wss_disconnected()
        await runtimes["a"]._on_market_state_change("token", "book")
        assert readiness.snapshot()["wss_market_ok"] is False
        assert readiness.snapshot()["market_data_ready"] is False
    finally:
        stopped.set()
        for runtime in runtimes.values():
            await runtime.stop()
