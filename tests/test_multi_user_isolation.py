"""Per-user repo isolation under the trade-pipeline path (PG-A6 port).

Both users share a single PG schema but every read/write is scoped by
user_id. A SELL from one user must not touch another user's open
positions. This pins that scoping invariant via the canonical
``orchestrators.trade_pipeline.execute_trades`` entry point.
"""

from __future__ import annotations

import contextlib
import time
from decimal import Decimal

import pytest

from pmcopy.config.app_config import load_config
from pmcopy.application.trading_context import TradingContext
from pmcopy.domain.execution import ExecutionMetadata
from pmcopy.orchestrators.trade_pipeline import execute_trades
from pmcopy.services.executionService import ExecutionResult
from pmcopy.services.positionManager import PositionManager
from pmcopy.storage.postgres.fills_repo import FillsRepo
from pmcopy.storage.postgres.orders_repo import OrdersRepo
from pmcopy.storage.postgres.positions_repo import PositionsRepo
from pmcopy.storage.postgres.risk_blocks_repo import RiskBlocksRepo

FAKE_TOKEN_ID = "71321045679252212594626385532706912750332728571942532223535764789812338637396"


@contextlib.asynccontextmanager
async def _pg_pool():
    try:
        from testcontainers.postgres import PostgresContainer
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


class _FakeExecutionService:
    async def execute(self, req):
        return ExecutionResult(
            accepted=True,
            order_id=f"ord-{req.user_id}-{req.side}",
            status="filled",
            requested_amount=Decimal(str(req.amount)),
            filled_amount=req.amount / Decimal("0.5") if req.side == "BUY" else req.amount,
            avg_price=Decimal("0.5"),
            fees_paid=Decimal("0"),
            error=None,
        )


class _FakePnlTracker:
    def __init__(self):
        self.buys: list[tuple[str, float]] = []
        self.sells: list[tuple[str, float]] = []

    def record_buy(self, *, condition_id: str, size_tokens: float, **_kwargs):
        self.buys.append((condition_id, size_tokens))

    def record_sell(self, *, condition_id: str, size_tokens: float, **_kwargs):
        self.sells.append((condition_id, size_tokens))


def _build_context(*, user_id: str, pool) -> TradingContext:
    import pmcopy.config.app_config as app_config_mod
    import time
    from decimal import Decimal as _D
    from unittest.mock import AsyncMock, patch
    from pmcopy.cache.market_state_store import BookSnapshot, QuoteSnapshot, TickSnapshot
    from pmcopy.services.market_data_service import MarketDataBundle, MarketDataSource

    with patch.object(app_config_mod, "_runtime_python_supported", return_value=True):
        cfg = load_config(
            {
                "database": {"dsn": "postgresql://test:test@localhost/test"},
                "wallet": {
                    "funder_address": "0x" + "b" * 40,
                    "polygon_rpc_url": "https://polygon-rpc.example.com",
                },
                "network": {"rpc_url": "https://polygon-rpc.example.com"},
                "leader": {
                    "wallets": ["0x" + "c" * 40],
                    "copy_decision_min_score": 0.01,
                    "copy_max_signal_age_seconds": 999999,
                    "copy_max_price_drift_pct": 100,
                    "copy_max_edge_decay_pct": 100,
                },
            }
        )

    now = time.monotonic()
    bundle = MarketDataBundle(
        token_id=FAKE_TOKEN_ID,
        quote=QuoteSnapshot(
            token_id=FAKE_TOKEN_ID,
            best_bid=_D("0.49"),
            best_ask=_D("0.51"),
            last_trade_price=None,
            source="wss",
            monotonic_ts=now,
        ),
        book=BookSnapshot(
            token_id=FAKE_TOKEN_ID,
            bids={_D("0.49"): _D("100")},
            asks={_D("0.51"): _D("100")},
            best_bid=_D("0.49"),
            best_ask=_D("0.51"),
            source="wss",
            monotonic_ts=now,
        ),
        tick=TickSnapshot(
            token_id=FAKE_TOKEN_ID,
            tick_size=_D("0.01"),
            min_order_size=_D("0.001"),
            neg_risk=False,
            source="wss",
            monotonic_ts=now,
        ),
        source=MarketDataSource.WSS,
    )
    mds = AsyncMock()
    mds.require_fresh_data = AsyncMock(return_value=bundle)
    mds.require_execution_metadata = AsyncMock(
        return_value=ExecutionMetadata(
            token_id=FAKE_TOKEN_ID,
            tick_size=Decimal("0.01"),
            min_order_size=Decimal("0.001"),
            neg_risk=False,
        )
    )

    ctx = TradingContext(
        user_id=user_id,
        wallet_address=("0x" + ("a" if user_id.endswith("a") else "b") * 40),
        client=object(),
        positions_repo=PositionsRepo(pool),
        fills_repo=FillsRepo(pool),
        orders_repo=OrdersRepo(pool),
        risk_blocks_repo=RiskBlocksRepo(pool),
        pnl_tracker=_FakePnlTracker(),
        risk_engine=object(),
        accounting_service=object(),
        position_manager=PositionManager(),
        execution_service=_FakeExecutionService(),
        config=cfg,
        allocation_pct=1.0,
        buy_min_amount=1.0,
        buy_max_amount=1000.0,
        max_market_exposure_usdc=1000.0,
        market_data_service=mds,
        extra={"slippage_tolerance_bps": 50},
    )
    ctx.market_data_runtime = AsyncMock()

    fake_exec = ctx.execution_service

    class _PassThrough:
        async def submit_copy_trade(self, intent):
            from pmcopy.services.execution.coordinator import ExecutionCoordinatorResult
            from pmcopy.services.execution.types import ExecutionRequest

            req = ExecutionRequest(
                user_id=intent.user_id,
                leader_id=intent.leader_wallet or "",
                token_id=intent.token_id,
                side=intent.side,
                amount=intent.notional_usdc if intent.side == "BUY" else intent.size,
                max_slippage_bps=intent.max_slippage_bps,
                reason=intent.reason or intent.source,
                signal_id=intent.signal_id,
                condition_id=intent.condition_id,
                recovery_metadata=dict(intent.recovery_metadata or {}),
            )
            result = await fake_exec.execute(req)
            return ExecutionCoordinatorResult(
                accepted=getattr(result, "accepted", False),
                status=getattr(result, "status", ""),
                order_id=getattr(result, "order_id", None),
                lifecycle_status=getattr(result, "status", ""),
                raw_result=result,
            )

    ctx.execution_coordinator = _PassThrough()
    return ctx


@pytest.mark.asyncio
async def test_multi_user_positions_and_pnl_are_isolated(monkeypatch):
    # ``_build_context`` passes an explicit override dict to ``load_config``,
    # so this test never reads process env for its config.
    import pmcopy.orchestrators.trade_pipeline as trade_pipeline

    async def _fake_balance(_wallet: str):
        return 1000.0

    async def _fake_live_position_size(_context, *, token_id: str):
        return Decimal("20")

    monkeypatch.setattr(trade_pipeline, "get_my_balance", _fake_balance)
    monkeypatch.setattr(trade_pipeline, "_fetch_live_position_size", _fake_live_position_size)

    async with _pg_pool() as pool:
        leader_id = "0xleader"
        buy_signal = [
            {
                "transactionHash": "0xtx-buy",
                "timestamp": int(time.time()),
                "type": "TRADE",
                "side": "BUY",
                "asset": FAKE_TOKEN_ID,
                "conditionId": "cond-1",
                "size": 10,
                "price": 0.5,
                "usdcSize": 10,
            }
        ]

        ctx_a = _build_context(user_id="user-a", pool=pool)
        ctx_b = _build_context(user_id="user-b", pool=pool)

        await execute_trades(ctx_a, leader_id, buy_signal, now_ts=time.time())
        await execute_trades(ctx_b, leader_id, buy_signal, now_ts=time.time())

        pos_a = await ctx_a.positions_repo.get_position("user-a", "cond-1")
        pos_b = await ctx_b.positions_repo.get_position("user-b", "cond-1")
        assert pos_a is not None
        assert pos_b is not None
        assert pos_a.size_tokens > 0
        assert pos_b.size_tokens > 0

        sell_signal = [
            {
                "transactionHash": "0xtx-sell-a",
                "timestamp": int(time.time()),
                "type": "TRADE",
                "side": "SELL",
                "asset": FAKE_TOKEN_ID,
                "conditionId": "cond-1",
                "size": 0,
                "price": 0.5,
                "_leader_sell_fraction": 1.0,
            }
        ]
        await execute_trades(ctx_a, leader_id, sell_signal, now_ts=time.time())

        pos_a_after = await ctx_a.positions_repo.get_position("user-a", "cond-1")
        pos_b_after = await ctx_b.positions_repo.get_position("user-b", "cond-1")
        assert pos_a_after is None
        assert pos_b_after is not None
        assert pos_b_after.size_tokens > 0
