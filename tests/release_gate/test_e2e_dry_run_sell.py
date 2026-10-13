"""Release-gate: end-to-end dry-run copy SELL (Issue #75 item 16).

Same harness as the BUY-side gate but exercises the SELL path so the
gateway request emitted by ``ExecutionService`` carries ``side="SELL"``.
"""

from __future__ import annotations

import contextlib
import time
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from pmcopy.cache.market_state_store import BookSnapshot, QuoteSnapshot, TickSnapshot
from pmcopy.integrations.polymarket.models import (
    MarketableOrderRequest,
    VenueSubmitResult,
)
from pmcopy.services.executionService import ExecutionRequest, ExecutionService
from pmcopy.services.market_data_service import MarketDataBundle, MarketDataSource
from pmcopy.storage.postgres.orders_repo import OrdersRepo
from tests.conftest import make_clob_gateway


@contextlib.asynccontextmanager
async def _pg_pool():
    try:
        from tests.integration_postgres import PostgresContainer
        import asyncpg
        from pmcopy.storage.postgres.migrations import apply_migrations as pg_apply
    except Exception as exc:
        pytest.skip(f"Docker / postgres image unavailable: {exc!r}")
    ctr = PostgresContainer("postgres:16-alpine")
    try:
        ctr.start()
    except Exception as exc:
        pytest.skip(f"Docker / postgres image unavailable: {exc!r}")
    try:
        dsn = ctr.get_connection_url(driver="asyncpg").replace(
            "postgresql+asyncpg://", "postgresql://"
        )
        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=5)
        async with pool.acquire() as conn:
            await pg_apply(conn)
        try:
            yield pool
        finally:
            await pool.close()
    finally:
        ctr.stop()


class _FeeSvc:
    async def get_fee_bps(self, _token_id: str) -> int:
        return 0


def _make_bundle(token_id: str = "0xtok") -> MarketDataBundle:
    now = time.monotonic()
    return MarketDataBundle(
        token_id=token_id,
        quote=QuoteSnapshot(
            token_id=token_id,
            best_bid=Decimal("0.49"),
            best_ask=Decimal("0.51"),
            last_trade_price=None,
            source="wss",
            monotonic_ts=now,
        ),
        book=BookSnapshot(
            token_id=token_id,
            bids={Decimal("0.49"): Decimal("100")},
            asks={Decimal("0.51"): Decimal("100")},
            best_bid=Decimal("0.49"),
            best_ask=Decimal("0.51"),
            source="wss",
            monotonic_ts=now,
        ),
        tick=TickSnapshot(
            token_id=token_id,
            tick_size=Decimal("0.01"),
            min_order_size=Decimal("1"),
            neg_risk=False,
            source="wss",
            monotonic_ts=now,
        ),
        source=MarketDataSource.WSS,
    )


def _mock_mds(token_id: str = "0xtok"):
    mds = AsyncMock()
    mds.require_fresh_data = AsyncMock(return_value=_make_bundle(token_id))
    return mds


@pytest.fixture(autouse=True)
def _reset_circuit():
    import pmcopy.services.executionService as _es

    _es._CLOB_CIRCUIT.reset()
    yield
    _es._CLOB_CIRCUIT.reset()


@pytest.mark.asyncio
async def test_dry_run_sell_routes_through_gateway(
    exec_config,
    stub_clob_client,
) -> None:
    async with _pg_pool() as pool:
        repo = OrdersRepo(pool)
        gateway = make_clob_gateway(
            submit_result=VenueSubmitResult(
                success=True, order_id="ord-sell-e2e", status="submitted"
            )
        )
        svc = ExecutionService(
            stub_clob_client,
            _FeeSvc(),
            repo,
            config=exec_config,
            clob_gateway=gateway,
            market_data_service=_mock_mds(),
        )
        request = ExecutionRequest(
            user_id="u1",
            leader_id="leader-1",
            token_id="0xtok",
            side="SELL",
            amount=Decimal("5"),
            max_slippage_bps=100,
            reason="copy_trade",
            signal_id="sig-e2e-sell",
        )
        result = await svc.execute(request)

        assert result.accepted is True
        gateway.submit_marketable_order.assert_awaited_once()
        submitted = gateway.submit_marketable_order.await_args.args[0]
        assert isinstance(submitted, MarketableOrderRequest)
        assert submitted.token_id == "0xtok"
        assert submitted.side == "SELL"
