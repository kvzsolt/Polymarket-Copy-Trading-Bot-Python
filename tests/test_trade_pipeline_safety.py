"""Safety tests for the trade pipeline — duplicate prevention, failure handling, position updates."""

from __future__ import annotations

import contextlib
import time
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import config.app_config as app_config_mod
from tests.config_helpers import load_test_config
from domain.trading_context import TradingContext
from orchestrators.runtime_bootstrap import startup_reconcile_unconfirmed
from orchestrators.trade_pipeline import (
    build_trade_key,
    execute_trades,
    reconcile_submitted_unconfirmed,
    seed_position_pct_cap_snapshot,
)
from storage.postgres.risk_blocks_repo import RiskBlocksRepo
from services.copyDecisionService import CopyDecision
from services.executionService import ExecutionResult
from services.pnlTracker import PnLTracker
from services.positionManager import PositionManager
from services.risk.risk_engine import RiskEngine
from services.accountingService import AccountingService
from storage.postgres.fills_repo import FillRecord, FillsRepo


@pytest.fixture(autouse=True)
def _lower_decision_gates_for_tests(monkeypatch):
    """Lower copy-decision thresholds so test signals pass the gate."""
    monkeypatch.setenv("COPY_DECISION_MIN_SCORE", "0.01")
    monkeypatch.setenv("COPY_MAX_SIGNAL_AGE_SECONDS", "999999")
    monkeypatch.setenv("COPY_MAX_PRICE_DRIFT_RATIO", "1.0")
    monkeypatch.setenv("COPY_MAX_EDGE_DECAY_RATIO", "1.0")
    monkeypatch.setenv("COPY_MIN_SHARES", "0.001")
    monkeypatch.setenv("COPY_MIN_NOTIONAL_USDC", "0.01")
    monkeypatch.setenv("BUY_MIN_AMOUNT", "0.01")
    monkeypatch.setenv("MAX_POSITION_PCT", "0")
    monkeypatch.setenv("ENABLE_BALANCE_TIERED_POLICY", "false")


@pytest.fixture(autouse=True)
def _mock_balance_for_tests(monkeypatch):
    """Mock get_my_balance so tests never hit real RPC."""

    async def _fake_balance(_addr):
        return 10000.0

    monkeypatch.setattr(
        "orchestrators.trade_pipeline.get_my_balance",
        _fake_balance,
    )


def _config(**overrides):
    env = {
        "PRIVATE_KEY": "0x" + "a" * 64,
        "FUNDER_ADDRESS": "0x" + "b" * 40,
        "USER_ADDRESS": "0x" + "c" * 40,
        "LEADER_WALLETS": "0x" + "d" * 40,
        "RPC_URL": "https://polygon-rpc.example.com",
        "CHAIN_ID": "137",
        "BUY_MIN_AMOUNT": "0.01",
        "BUY_MAX_AMOUNT": "1000",
        "MAX_MARKET_EXPOSURE_USDC": "300",
        "SLIPPAGE_TOLERANCE": "2.0",
        "COPY_DECISION_MIN_SCORE": "0.01",
        "COPY_MAX_SIGNAL_AGE_SECONDS": "999999",
        "COPY_MAX_PRICE_DRIFT_PCT": "999",
        "COPY_MAX_EDGE_DECAY_PCT": "999",
        "COPY_MIN_SHARES": "0.001",
        "COPY_MIN_NOTIONAL_USDC": "0.01",
        "ENABLE_BALANCE_TIERED_POLICY": "false",
        # These safety tests rely on the legacy WSS-mode prefetch path with a
        # ``_FakeMDS`` exposing only ``require_fresh_data``. Pin WSS primary
        # mode so the trade pipeline keeps using that path.
        "MARKET_DATA_PRIMARY_SOURCE": "wss",
    }
    env.update(overrides)
    with patch.object(app_config_mod, "_runtime_python_supported", return_value=True):
        return load_test_config(env)


from storage.postgres.orders_repo import OrderSubmissionRecord, OrdersRepo  # noqa: E402
from storage.postgres.positions_repo import PositionsRepo  # noqa: E402

FAKE_TOKEN_ID = (
    "71321045679252212594626385532706912750332728571942532223535764789812338637396"
)


@contextlib.asynccontextmanager
async def _pg_pool():
    try:
        from testcontainers.postgres import PostgresContainer
        import asyncpg
        from storage.postgres.migrations import apply_migrations as pg_apply
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


def _make_bundle(
    *,
    min_order_size: Decimal = Decimal("0.001"),
    tick_size: Decimal = Decimal("0.01"),
    token_id: str = FAKE_TOKEN_ID,
):
    """Build an executable WSS-fresh MarketDataBundle for trade-pipeline tests."""
    from cache.market_state_store import BookSnapshot, QuoteSnapshot, TickSnapshot
    from services.market_data_service import MarketDataBundle, MarketDataSource

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
            tick_size=tick_size,
            min_order_size=min_order_size,
            neg_risk=False,
            source="wss",
            monotonic_ts=now,
        ),
        source=MarketDataSource.WSS,
    )


class _FakeMDS:
    """Minimal MarketDataService stand-in for trade_pipeline tests."""

    def __init__(self, bundle):
        self._bundle = bundle
        self.calls: list[str] = []

    async def require_fresh_data(self, token_id):  # noqa: D401  test helper
        self.calls.append(token_id)
        return self._bundle

    async def get_quote_or_none(self, token_id):
        return self._bundle.quote

    async def get_book_or_none(self, token_id):
        return self._bundle.book

    async def get_tick_or_none(self, token_id):
        return self._bundle.tick


class _PassThroughCoordinator:
    """Minimal coordinator stand-in used by trade_pipeline safety tests.

    PR6.1 makes ExecutionCoordinator mandatory in live mode. These tests do
    not exercise reservation semantics; they mock ExecutionService directly.
    A pass-through coordinator keeps the live fail-closed guard satisfied
    while delegating to the underlying mock.
    """

    def __init__(self, execution_service):
        self._exec = execution_service

    async def submit_copy_trade(self, intent):
        from services.execution.coordinator import ExecutionCoordinatorResult
        from services.execution.types import ExecutionRequest

        req = ExecutionRequest(
            user_id=intent.user_id,
            leader_id=intent.leader_wallet or "",
            token_id=intent.token_id,
            side=intent.side,
            amount=intent.size,
            max_slippage_bps=intent.max_slippage_bps,
            reason=intent.reason or intent.source,
            signal_id=intent.signal_id,
            condition_id=intent.condition_id,
            recovery_metadata=dict(intent.recovery_metadata or {}),
        )
        result = await self._exec.execute(req)
        return ExecutionCoordinatorResult(
            accepted=getattr(result, "accepted", False),
            status=getattr(result, "status", ""),
            order_id=getattr(result, "order_id", None),
            lifecycle_status=getattr(result, "status", ""),
            raw_result=result,
        )


def _make_context(
    pool,
    tmp_path,
    *,
    execution_service=None,
    config=None,
    market_data_service=None,
    market_data_runtime=None,
):
    exec_svc = execution_service or AsyncMock()
    mds = market_data_service or _FakeMDS(_make_bundle())
    runtime = market_data_runtime or AsyncMock()
    ctx = TradingContext(
        user_id="user1",
        wallet_address="0x" + "a" * 40,
        client=MagicMock(),
        positions_repo=PositionsRepo(pool),
        fills_repo=FillsRepo(pool),
        orders_repo=OrdersRepo(pool),
        risk_blocks_repo=RiskBlocksRepo(pool),
        pnl_tracker=PnLTracker(log_path=str(tmp_path / "test.pnl")),
        risk_engine=RiskEngine(),
        accounting_service=AccountingService(),
        position_manager=PositionManager(),
        execution_service=exec_svc,
        config=config or _config(),
        allocation_pct=1.0,
        buy_min_amount=0.01,
        buy_max_amount=1000.0,
        market_data_service=mds,
        extra={"slippage_tolerance_bps": 200},
    )
    ctx.market_data_runtime = runtime
    ctx.execution_coordinator = _PassThroughCoordinator(exec_svc)
    return ctx


def _signal(
    *,
    side="BUY",
    tx_hash="0xabc",
    asset=FAKE_TOKEN_ID,
    condition_id="cond1",
    price=0.50,
    size=10.0,
    leader_sell_fraction=1.0,
):
    sig = {
        "side": side,
        "transactionHash": tx_hash,
        "asset": asset,
        "conditionId": condition_id,
        "price": price,
        "size": size,
        "usdcSize": size * price,
        "timestamp": int(time.time()),
        "title": "Test Market",
        "eventSlug": "test-slug",
    }
    # SELL signals carry the trade-monitor annotation by default (leader sold
    # 100%), matching what these tests always meant. Pass
    # ``leader_sell_fraction=None`` to exercise the missing-annotation path
    # governed by ``execution.sell_fraction_missing_policy``.
    if side == "SELL" and leader_sell_fraction is not None:
        sig["_leader_sell_fraction"] = leader_sell_fraction
    return sig


def _exec_result(
    *, accepted=True, filled=Decimal("10"), status="filled", order_id="ord1", error=None
):
    return ExecutionResult(
        accepted=accepted,
        order_id=order_id,
        status=status,
        requested_amount=Decimal("5"),
        filled_amount=filled,
        avg_price=Decimal("0.50") if filled > 0 else None,
        fees_paid=Decimal("0.01"),
        error=error,
    )


class _RecordingDecisionService:
    def __init__(self):
        self.buy_calls: list[dict[str, float]] = []

    def decide_buy(
        self,
        *,
        trade,
        leader_score,
        live_price,
        base_notional_usdc,
        max_notional_usdc,
        market_min_size,
        tick_size=0.0,
    ):
        self.buy_calls.append(
            {
                "base_notional_usdc": float(base_notional_usdc),
                "max_notional_usdc": float(max_notional_usdc),
                "live_price": float(live_price),
                "market_min_size": float(market_min_size),
            }
        )
        target_notional = float(max_notional_usdc)
        target_size = target_notional / max(float(live_price), 1e-9)
        return CopyDecision(
            allow=True,
            skip_code="ok",
            reason="decision-approved BUY",
            side="BUY",
            decision_score=0.80,
            leader_score=float(leader_score.score),
            trade_score=0.80,
            live_price=float(live_price),
            reference_price=float(trade.get("price") or 0),
            target_notional_usdc=target_notional,
            target_size_tokens=target_size,
            price_drift_pct=0.0,
            edge_decay_pct=0.0,
            details={},
        )


@pytest.mark.asyncio
async def test_market_risk_quarantine_skips_buy_copy_trades(tmp_path):
    """A risk block must block BUY re-entry on the quarantined position."""
    async with _pg_pool() as pool:
        exec_svc = AsyncMock()
        ctx = _make_context(pool, tmp_path, execution_service=exec_svc)

        await ctx.risk_blocks_repo.mark_risk_block(
            user_id="user1",
            condition_id="cond1",
            token_id=FAKE_TOKEN_ID,
            reason="stop_loss",
            source="test",
            leader_anchor="leader1",
        )

        signal = _signal(side="BUY", tx_hash="0xquarantine-buy")
        results = await execute_trades(ctx, "leader1", [signal], time.time())

        assert len(results) == 1
        assert results[0].status == "skipped"
        assert results[0].reason == "market_risk_quarantined"
        exec_svc.execute.assert_not_called()


@pytest.mark.asyncio
async def test_sell_not_blocked_when_risk_block_active(tmp_path):
    """A risk block must NOT block SELL paths — exits must always be allowed."""
    async with _pg_pool() as pool:
        exec_svc = AsyncMock()
        exec_svc.execute.return_value = _exec_result()
        ctx = _make_context(pool, tmp_path, execution_service=exec_svc)

        await ctx.risk_blocks_repo.mark_risk_block(
            user_id="user1",
            condition_id="cond1",
            token_id=FAKE_TOKEN_ID,
            reason="stop_loss",
            source="test",
            leader_anchor="leader1",
        )

        # Seed a position so the SELL has something to act on
        from storage.postgres.positions_repo import PositionRecord

        await ctx.positions_repo.upsert_position(
            PositionRecord(
                user_id="user1",
                condition_id="cond1",
                token_id=FAKE_TOKEN_ID,
                leader_id="leader1",
                size_tokens=Decimal("10"),
                avg_entry_price=Decimal("0.50"),
            )
        )

        signal = _signal(side="SELL", tx_hash="0xquarantine-sell")
        with patch(
            "orchestrators.trade_pipeline._fetch_live_position_size",
            new_callable=AsyncMock,
            return_value=Decimal("10"),
        ):
            results = await execute_trades(ctx, "leader1", [signal], time.time())

        assert len(results) == 1
        exec_svc.execute.assert_awaited_once()
        assert results[0].status == "executed"
        # SELL must proceed — must NOT be skipped with market_risk_quarantined
        assert results[0].reason != "market_risk_quarantined"


@pytest.mark.asyncio
async def test_fresh_leader_rebuy_auto_clears_risk_block_in_live_pipeline(tmp_path):
    async with _pg_pool() as pool:
        exec_svc = AsyncMock()
        exec_svc.execute.return_value = _exec_result()
        ctx = _make_context(pool, tmp_path, execution_service=exec_svc)

        await ctx.risk_blocks_repo.mark_risk_block(
            user_id="user1",
            condition_id="cond1",
            token_id=FAKE_TOKEN_ID,
            reason="stop_loss",
            source="test",
            leader_anchor="leader1",
            metadata={"leader_notional_at_trigger": 100.0},
        )

        # With leader_notional_usdc=None, the fresh_leader_buy_after_net_flat
        # rule is intentionally disabled because the caller does not have the
        # leader's total position. Small routine BUY signals should NOT
        # auto-clear risk blocks (P1 PR #63). The rule will be re-enabled when
        # leader position fetch is available on the BUY path.
        signal = _signal(side="BUY", tx_hash="0xfresh-buy", size=10.0, price=0.5)
        results = await execute_trades(ctx, "leader1", [signal], time.time())

        assert len(results) == 1
        assert results[0].status == "skipped"
        assert results[0].reason == "market_risk_quarantined"


@pytest.mark.asyncio
async def test_buy_fail_closed_when_entry_policy_service_missing(tmp_path):
    async with _pg_pool() as pool:
        exec_svc = AsyncMock()
        ctx = _make_context(pool, tmp_path, execution_service=exec_svc)
        ctx.entry_permission_service = None

        signal = _signal(side="BUY", tx_hash="0xpolicy-missing")
        results = await execute_trades(ctx, "leader1", [signal], time.time())

        assert len(results) == 1
        assert results[0].status == "skipped"
        assert results[0].reason == "entry_policy_unavailable"
        exec_svc.execute.assert_not_called()


@pytest.mark.asyncio
async def test_duplicate_trade_is_skipped(tmp_path):
    async with _pg_pool() as pool:
        exec_svc = AsyncMock()
        ctx = _make_context(pool, tmp_path, execution_service=exec_svc)

        signal = _signal()
        trade_key = build_trade_key("user1", "leader1", signal)
        await ctx.orders_repo.mark_processed_trade(
            trade_key=trade_key,
            user_id="user1",
            leader_id="leader1",
            tx_hash="0xabc",
            side="BUY",
            asset=FAKE_TOKEN_ID,
            condition_id="cond1",
            status="executed",
        )

        results = await execute_trades(ctx, "leader1", [signal], time.time())
        assert len(results) == 1
        assert results[0].status == "skipped"
        assert results[0].reason == "already_processed"
        exec_svc.execute.assert_not_called()


@pytest.mark.asyncio
async def test_transient_failure_not_marked_processed(tmp_path):
    async with _pg_pool() as pool:
        exec_svc = AsyncMock()
        exec_svc.execute.return_value = _exec_result(
            accepted=False,
            filled=Decimal("0"),
            status="failed",
            order_id=None,
            error="connection timeout",
        )
        ctx = _make_context(pool, tmp_path, execution_service=exec_svc)

        signal = _signal()
        results = await execute_trades(ctx, "leader1", [signal], time.time())
        assert len(results) == 1
        assert results[0].status == "failed"

        # The trade should NOT be in processed_trades so it can be retried
        trade_key = build_trade_key("user1", "leader1", signal)
        assert await ctx.orders_repo.has_processed_trade(trade_key) is False


@pytest.mark.asyncio
async def test_transient_failure_marked_processed_after_retry_budget(tmp_path):
    async with _pg_pool() as pool:
        exec_svc = AsyncMock()
        exec_svc.execute.return_value = _exec_result(
            accepted=False,
            filled=Decimal("0"),
            status="failed",
            order_id=None,
            error="connection timeout",
        )
        ctx = _make_context(
            pool,
            tmp_path,
            execution_service=exec_svc,
            config=_config(NON_TERMINAL_FAILURE_MAX_ATTEMPTS="2"),
        )

        signal = _signal(tx_hash="0xretry")
        trade_key = build_trade_key("user1", "leader1", signal)

        first = await execute_trades(ctx, "leader1", [signal], time.time())
        assert first[0].status == "failed"
        assert "retry budget exhausted" not in str(first[0].reason or "")
        assert await ctx.orders_repo.has_processed_trade(trade_key) is False

        second = await execute_trades(ctx, "leader1", [signal], time.time())
        assert second[0].status == "failed"
        assert "retry budget exhausted" in str(second[0].reason or "")
        assert await ctx.orders_repo.has_processed_trade(trade_key) is True

        third = await execute_trades(ctx, "leader1", [signal], time.time())
        assert third[0].status == "skipped"
        assert third[0].reason == "already_processed"
        assert exec_svc.execute.await_count == 2


@pytest.mark.asyncio
async def test_hard_rejection_is_marked_processed(tmp_path):
    async with _pg_pool() as pool:
        exec_svc = AsyncMock()
        exec_svc.execute.return_value = _exec_result(
            accepted=False,
            filled=Decimal("0"),
            status="rejected",
            order_id=None,
            error="amount below min_order_size",
        )
        ctx = _make_context(pool, tmp_path, execution_service=exec_svc)

        signal = _signal()
        results = await execute_trades(ctx, "leader1", [signal], time.time())
        assert len(results) == 1
        assert results[0].status == "failed"

        trade_key = build_trade_key("user1", "leader1", signal)
        assert await ctx.orders_repo.has_processed_trade(trade_key) is True


@pytest.mark.asyncio
async def test_submitted_unconfirmed_is_marked_processed(tmp_path):
    async with _pg_pool() as pool:
        exec_svc = AsyncMock()
        exec_svc.execute.return_value = _exec_result(
            accepted=True,
            filled=Decimal("0"),
            status="submitted_unconfirmed",
            order_id="ord1",
        )
        ctx = _make_context(pool, tmp_path, execution_service=exec_svc)

        signal = _signal()
        results = await execute_trades(ctx, "leader1", [signal], time.time())
        assert len(results) == 1
        assert results[0].status == "submitted_unconfirmed"

        trade_key = build_trade_key("user1", "leader1", signal)
        assert await ctx.orders_repo.has_processed_trade(trade_key) is True


@pytest.mark.asyncio
async def test_buy_terminal_no_fill_unlocks_retry(tmp_path):
    async with _pg_pool() as pool:
        # Provide a clob_gateway mock so ReconciliationService.confirm_fill
        # can poll the venue (issue #71 migration).
        gateway = AsyncMock()
        gateway.get_order.return_value = {"status": "cancelled", "size_matched": "0"}

        ctx = _make_context(pool, tmp_path)
        ctx.extra["clob_gateway"] = gateway
        signal = _signal(tx_hash="0xbuyretry")
        trade_key = build_trade_key("user1", "leader1", signal)
        await ctx.orders_repo.mark_processed_trade(
            trade_key=trade_key,
            user_id="user1",
            leader_id="leader1",
            tx_hash=signal["transactionHash"],
            side="BUY",
            asset=signal["asset"],
            condition_id=signal["conditionId"],
            status="submitted_unconfirmed",
            order_id="ord-buy-retry",
        )

        reconciled = await reconcile_submitted_unconfirmed(ctx)

        assert reconciled == 1
        assert await ctx.orders_repo.has_processed_trade(trade_key) is False


@pytest.mark.asyncio
async def test_stale_buy_unconfirmed_is_still_reconciled(tmp_path):
    async with _pg_pool() as pool:
        gateway = AsyncMock()
        gateway.get_order.return_value = {"status": "expired", "size_matched": "0"}

        ctx = _make_context(pool, tmp_path)
        ctx.extra["clob_gateway"] = gateway
        signal = _signal(tx_hash="0xstalebuy")
        trade_key = build_trade_key("user1", "leader1", signal)
        await ctx.orders_repo.mark_processed_trade(
            trade_key=trade_key,
            user_id="user1",
            leader_id="leader1",
            tx_hash=signal["transactionHash"],
            side="BUY",
            asset=signal["asset"],
            condition_id=signal["conditionId"],
            status="submitted_unconfirmed",
            order_id="ord-stale-buy",
        )
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE processed_trades SET processed_at = $1 WHERE trade_key = $2",
                int(time.time() - 3600),
                trade_key,
            )

        reconciled = await reconcile_submitted_unconfirmed(ctx, max_age_seconds=1)

        assert reconciled == 1
        assert await ctx.orders_repo.has_processed_trade(trade_key) is False


@pytest.mark.asyncio
async def test_partial_progress_reconcile_applies_only_incremental_buy_delta(tmp_path):
    async with _pg_pool() as pool:
        gateway = AsyncMock()
        gateway.get_order.return_value = {
            "status": "partially_filled",
            "size_matched": "5",
            "avg_price": "0.50",
        }

        ctx = _make_context(pool, tmp_path)
        ctx.extra["clob_gateway"] = gateway
        signal = _signal(tx_hash="0xpartial-buy")
        trade_key = build_trade_key("user1", "leader1", signal)

        # Simulate a previous restart checkpoint where 2 tokens were already
        # durably applied locally and recorded in the submission response.
        await ctx.fills_repo.append_fill(
            FillRecord(
                user_id="user1",
                leader_id="leader1",
                order_id="ord-partial-buy",
                condition_id=signal["conditionId"],
                token_id=signal["asset"],
                side="BUY",
                size_tokens=Decimal("2"),
                price=Decimal("0.50"),
                fee=Decimal("0"),
                fill_ts=1_700_000_000,
            )
        )
        await ctx.positions_repo.rebuild_user_positions_from_fills(
            "user1", ctx.fills_repo
        )
        await ctx.orders_repo.mark_processed_trade(
            trade_key=trade_key,
            user_id="user1",
            leader_id="leader1",
            tx_hash=signal["transactionHash"],
            side="BUY",
            asset=signal["asset"],
            condition_id=signal["conditionId"],
            status="submitted_unconfirmed",
            order_id="ord-partial-buy",
        )
        await ctx.orders_repo.save_submission(
            OrderSubmissionRecord(
                idempotency_key="idem-partial-buy",
                user_id="user1",
                leader_id="leader1",
                order_id="ord-partial-buy",
                status="submitted_unconfirmed",
                request_payload={"side": "BUY"},
                response_payload={
                    "status": "submitted_unconfirmed",
                    "orderID": "ord-partial-buy",
                    "size_matched": "2",
                },
            )
        )

        reconciled = await reconcile_submitted_unconfirmed(ctx)

        assert reconciled == 1
        pos = await ctx.positions_repo.get_position(
            "user1", signal["conditionId"], token_id=signal["asset"]
        )
        assert pos is not None
        assert pos.size_tokens == Decimal("5")
        processed = await ctx.orders_repo.get_processed_trade(trade_key)
        assert processed is not None
        assert processed["status"] == "submitted_unconfirmed"
        submission = await ctx.orders_repo.get_by_order_id("ord-partial-buy")
        assert submission is not None
        assert submission["status"] == "submitted_unconfirmed"
        assert submission["response"].get("size_matched") == "5"


@pytest.mark.asyncio
async def test_final_reconcile_with_no_new_delta_marks_buy_fully_reconciled(tmp_path):
    async with _pg_pool() as pool:
        gateway = AsyncMock()
        gateway.get_order.return_value = {
            "status": "filled",
            "size_matched": "5",
            "avg_price": "0.50",
        }

        ctx = _make_context(pool, tmp_path)
        ctx.extra["clob_gateway"] = gateway
        signal = _signal(tx_hash="0xfinal-buy")
        trade_key = build_trade_key("user1", "leader1", signal)

        await ctx.fills_repo.append_fill(
            FillRecord(
                user_id="user1",
                leader_id="leader1",
                order_id="ord-final-buy",
                condition_id=signal["conditionId"],
                token_id=signal["asset"],
                side="BUY",
                size_tokens=Decimal("5"),
                price=Decimal("0.50"),
                fee=Decimal("0"),
                fill_ts=1_700_000_000,
            )
        )
        await ctx.positions_repo.rebuild_user_positions_from_fills(
            "user1", ctx.fills_repo
        )
        await ctx.orders_repo.mark_processed_trade(
            trade_key=trade_key,
            user_id="user1",
            leader_id="leader1",
            tx_hash=signal["transactionHash"],
            side="BUY",
            asset=signal["asset"],
            condition_id=signal["conditionId"],
            status="submitted_unconfirmed",
            order_id="ord-final-buy",
        )
        await ctx.orders_repo.save_submission(
            OrderSubmissionRecord(
                idempotency_key="idem-final-buy",
                user_id="user1",
                leader_id="leader1",
                order_id="ord-final-buy",
                status="submitted_unconfirmed",
                request_payload={"side": "BUY"},
                response_payload={
                    "status": "submitted_unconfirmed",
                    "orderID": "ord-final-buy",
                    "size_matched": "5",
                },
            )
        )

        reconciled = await reconcile_submitted_unconfirmed(ctx)

        assert reconciled == 1
        pos = await ctx.positions_repo.get_position(
            "user1", signal["conditionId"], token_id=signal["asset"]
        )
        assert pos is not None
        assert pos.size_tokens == Decimal("5")
        processed = await ctx.orders_repo.get_processed_trade(trade_key)
        assert processed is not None
        assert processed["status"] == "filled_reconciled"
        submission = await ctx.orders_repo.get_by_order_id("ord-final-buy")
        assert submission is not None
        assert submission["status"] == "filled_reconciled"


@pytest.mark.asyncio
async def test_startup_reconciled_risk_sell_blocks_reentry_on_same_scope(tmp_path):
    async with _pg_pool() as pool:

        class _ReconcilingExecSvc:
            def __init__(self):
                self.execute = AsyncMock(return_value=_exec_result())

        gateway = AsyncMock()
        gateway.get_order.return_value = {
            "status": "filled",
            "size_matched": "10",
            "avg_price": "0.30",
        }

        exec_svc = _ReconcilingExecSvc()
        ctx = _make_context(pool, tmp_path, execution_service=exec_svc)
        ctx.extra["clob_gateway"] = gateway

        sell_signal = _signal(
            side="SELL",
            tx_hash="0xrestart-sell",
            price=0.97,
            size=10.0,
        )

        await ctx.fills_repo.append_fill(
            FillRecord(
                user_id="user1",
                leader_id="leader1",
                order_id="buy-seed-1",
                condition_id=sell_signal["conditionId"],
                token_id=sell_signal["asset"],
                side="BUY",
                size_tokens=Decimal("10"),
                price=Decimal("0.60"),
                fee=Decimal("0"),
                fill_ts=1_700_000_000,
            )
        )
        await ctx.positions_repo.rebuild_user_positions_from_fills(
            "user1", ctx.fills_repo
        )
        await ctx.orders_repo.save_submission(
            OrderSubmissionRecord(
                idempotency_key="idem-restart-sell",
                user_id="user1",
                leader_id="leader1",
                order_id="venue-risk-1",
                status="submitted_unconfirmed",
                request_payload={
                    "user_id": "user1",
                    "leader_id": "leader1",
                    "token_id": sell_signal["asset"],
                    "condition_id": sell_signal["conditionId"],
                    "side": "SELL",
                    "amount": "10",
                    "reason": "near_max_full_exit",
                    "signal_id": "risk:near_max_full_exit:cond1:1700000001",
                    "recovery_metadata": {
                        "leader_notional_at_trigger": 9.7,
                        "copied_position_notional_at_trigger": 9.7,
                        "trigger_price": 0.97,
                        "trigger_size_tokens": 10.0,
                        "trigger_fraction": 1.0,
                    },
                },
                response_payload={
                    "status": "submitted_unconfirmed",
                    "orderID": "venue-risk-1",
                },
            )
        )

        reconciled = await startup_reconcile_unconfirmed([ctx])
        assert reconciled == {"user1": 1}
        assert (
            await ctx.positions_repo.get_position("user1", sell_signal["conditionId"])
            is None
        )
        assert (
            await ctx.risk_blocks_repo.get_active_risk_block(
                user_id="user1",
                condition_id=sell_signal["conditionId"],
                token_id=sell_signal["asset"],
            )
            is not None
        )

        buy_results = await execute_trades(
            ctx, "leader1", [_signal(tx_hash="0xrestart-buy")], time.time()
        )

        assert len(buy_results) == 1
        assert buy_results[0].status == "skipped"
        assert buy_results[0].reason == "market_risk_quarantined"
        exec_svc.execute.assert_not_called()


@pytest.mark.asyncio
async def test_max_position_pct_caps_cash_only_even_when_balance_policy_disabled(
    tmp_path,
):
    async with _pg_pool() as pool:
        exec_svc = AsyncMock()
        exec_svc.execute.return_value = _exec_result(
            accepted=True,
            filled=Decimal("10"),
            status="filled",
            order_id="ord-balance-off",
        )
        ctx = _make_context(
            pool,
            tmp_path,
            execution_service=exec_svc,
            config=_config(
                ENABLE_BALANCE_TIERED_POLICY="false", MAX_POSITION_PCT="0.20"
            ),
        )
        await ctx.positions_repo.upsert_position(
            SimpleNamespace(
                user_id="user1",
                condition_id="cond1",
                token_id=FAKE_TOKEN_ID,
                leader_id="leader1",
                size_tokens=Decimal("20"),
                avg_entry_price=Decimal("1.0"),
            )
        )

        decision_service = _RecordingDecisionService()
        ctx.copy_decision_service = decision_service

        with patch(
            "orchestrators.trade_pipeline.get_my_balance",
            new_callable=AsyncMock,
            return_value=80.0,
        ):
            results = await execute_trades(
                ctx, "leader1", [_signal(tx_hash="0xbal-off")], time.time()
            )

        assert results[0].status == "executed"
        assert decision_service.buy_calls[0]["max_notional_usdc"] == pytest.approx(16.0)
        req = exec_svc.execute.await_args.args[0]
        assert req.side == "BUY"
        assert req.amount == Decimal("16")


@pytest.mark.asyncio
async def test_balance_tiered_policy_blocks_buy_without_tradable_cash(tmp_path):
    async with _pg_pool() as pool:
        exec_svc = AsyncMock()
        ctx = _make_context(
            pool,
            tmp_path,
            execution_service=exec_svc,
            config=_config(
                ENABLE_BALANCE_TIERED_POLICY="true",
                DAY_START_EQUITY_USDC="4",
                BALANCE_POLICY_RESERVE_PCT="0.10",
                BALANCE_POLICY_RESERVE_FLOOR="5",
            ),
        )

        with (
            patch(
                "orchestrators.trade_pipeline.get_my_balance",
                new_callable=AsyncMock,
                return_value=4.0,
            ),
            patch(
                "orchestrators.trade_pipeline.get_day_start_equity",
                return_value=Decimal("4"),
            ),
        ):
            results = await execute_trades(
                ctx, "leader1", [_signal(tx_hash="0xbal-block")], time.time()
            )

        assert results[0].status == "skipped"
        assert results[0].reason == "no_tradable_cash_after_reserve"
        exec_svc.execute.assert_not_called()


@pytest.mark.asyncio
async def test_max_position_pct_caps_additional_buy_on_same_market(tmp_path):
    async with _pg_pool() as pool:
        exec_svc = AsyncMock()
        exec_svc.execute.return_value = _exec_result(
            accepted=True, filled=Decimal("10"), status="filled", order_id="ord-pos-cap"
        )
        ctx = _make_context(
            pool,
            tmp_path,
            execution_service=exec_svc,
            config=_config(
                ENABLE_BALANCE_TIERED_POLICY="false", MAX_POSITION_PCT="0.20"
            ),
        )
        await ctx.positions_repo.upsert_position(
            SimpleNamespace(
                user_id="user1",
                condition_id="cond1",
                token_id=FAKE_TOKEN_ID,
                leader_id="leader1",
                size_tokens=Decimal("10"),
                avg_entry_price=Decimal("1.0"),
            )
        )

        decision_service = _RecordingDecisionService()
        ctx.copy_decision_service = decision_service

        with patch(
            "orchestrators.trade_pipeline.get_my_balance",
            new_callable=AsyncMock,
            return_value=90.0,
        ):
            results = await execute_trades(
                ctx, "leader1", [_signal(tx_hash="0xpos-cap")], time.time()
            )

        assert results[0].status == "executed"
        assert decision_service.buy_calls
        # MAX_POSITION_PCT is cash-only: $90 free cash * 0.20 = $18.
        # Existing market exposure is handled by separate exposure policies.
        assert decision_service.buy_calls[0]["max_notional_usdc"] == pytest.approx(18.0)

        req = exec_svc.execute.await_args.args[0]
        assert req.side == "BUY"
        assert req.amount == Decimal("18")


@pytest.mark.asyncio
async def test_max_position_pct_uses_startup_snapshot_before_submit(tmp_path):
    async with _pg_pool() as pool:
        exec_svc = AsyncMock()
        exec_svc.execute.return_value = _exec_result(
            accepted=True,
            filled=Decimal("10"),
            status="filled",
            order_id="ord-fresh-cash",
        )
        ctx = _make_context(
            pool,
            tmp_path,
            execution_service=exec_svc,
            config=_config(
                ENABLE_BALANCE_TIERED_POLICY="false", MAX_POSITION_PCT="0.20"
            ),
        )
        decision_service = _RecordingDecisionService()
        ctx.copy_decision_service = decision_service

        balance_mock = AsyncMock(side_effect=[100.0, 50.0])
        with patch("orchestrators.trade_pipeline.get_my_balance", balance_mock):
            results = await execute_trades(
                ctx, "leader1", [_signal(tx_hash="0xfresh-cash")], time.time()
            )

        assert results[0].status == "executed"
        assert balance_mock.await_count == 1
        assert [call.args[0] for call in balance_mock.await_args_list] == [
            ctx.config.wallet.funder_address,
        ]
        assert decision_service.buy_calls[0]["max_notional_usdc"] == pytest.approx(20.0)
        req = exec_svc.execute.await_args.args[0]
        assert req.side == "BUY"
        assert req.amount == Decimal("20")


@pytest.mark.asyncio
async def test_max_position_pct_refreshes_snapshot_after_24_hours(tmp_path):
    async with _pg_pool() as pool:
        exec_svc = AsyncMock()
        exec_svc.execute.return_value = _exec_result(
            accepted=True,
            filled=Decimal("10"),
            status="filled",
            order_id="ord-refreshed-cash",
        )
        ctx = _make_context(
            pool,
            tmp_path,
            execution_service=exec_svc,
            config=_config(
                ENABLE_BALANCE_TIERED_POLICY="false", MAX_POSITION_PCT="0.20"
            ),
        )
        decision_service = _RecordingDecisionService()
        ctx.copy_decision_service = decision_service
        seed_position_pct_cap_snapshot(
            ctx,
            Decimal("100"),
            timestamp=time.time() - (24 * 60 * 60) - 1,
        )

        balance_mock = AsyncMock(return_value=50.0)
        with patch("orchestrators.trade_pipeline.get_my_balance", balance_mock):
            results = await execute_trades(
                ctx, "leader1", [_signal(tx_hash="0xstale-cash")], time.time()
            )

        assert results[0].status == "executed"
        assert balance_mock.await_count == 1
        assert decision_service.buy_calls[0]["max_notional_usdc"] == pytest.approx(10.0)
        req = exec_svc.execute.await_args.args[0]
        assert req.side == "BUY"
        assert req.amount == Decimal("10")


@pytest.mark.asyncio
async def test_balance_tiered_policy_caps_buy_before_execution(tmp_path):
    async with _pg_pool() as pool:
        exec_svc = AsyncMock()
        exec_svc.execute.return_value = _exec_result(
            accepted=True,
            filled=Decimal("10"),
            status="filled",
            order_id="ord-balance-cap",
        )
        ctx = _make_context(
            pool,
            tmp_path,
            execution_service=exec_svc,
            config=_config(
                ENABLE_BALANCE_TIERED_POLICY="true",
                DAY_START_EQUITY_USDC="25",
                BALANCE_POLICY_RESERVE_PCT="0.10",
                BALANCE_POLICY_RESERVE_FLOOR="5",
                BALANCE_POLICY_HARD_CAP_PCT="0.18",
                BALANCE_POLICY_HARD_CAP_CEILING="4.5",
            ),
        )

        decision_service = _RecordingDecisionService()
        ctx.copy_decision_service = decision_service

        with (
            patch(
                "orchestrators.trade_pipeline.get_my_balance",
                new_callable=AsyncMock,
                return_value=25.0,
            ),
            patch(
                "orchestrators.trade_pipeline.get_day_start_equity",
                return_value=Decimal("25"),
            ),
        ):
            results = await execute_trades(
                ctx, "leader1", [_signal(tx_hash="0xbal-cap")], time.time()
            )

        assert results[0].status == "executed"
        assert decision_service.buy_calls
        assert decision_service.buy_calls[0]["max_notional_usdc"] == pytest.approx(4.5)

        req = exec_svc.execute.await_args.args[0]
        assert req.side == "BUY"
        assert req.amount == Decimal("4.5")


@pytest.mark.asyncio
async def test_sell_with_no_position_skipped(tmp_path):
    async with _pg_pool() as pool:
        exec_svc = AsyncMock()
        ctx = _make_context(pool, tmp_path, execution_service=exec_svc)

        signal = _signal(side="SELL")
        results = await execute_trades(ctx, "leader1", [signal], time.time())
        assert len(results) == 1
        assert results[0].status == "skipped"
        assert results[0].reason == "never_held"

        trade_key = build_trade_key("user1", "leader1", signal)
        assert await ctx.orders_repo.has_processed_trade(trade_key) is True
        exec_svc.execute.assert_not_called()


@pytest.mark.asyncio
async def test_buy_can_raise_from_small_target_to_exchange_min_when_within_buy_max(
    tmp_path,
):
    async with _pg_pool() as pool:
        exec_svc = AsyncMock()
        exec_svc.execute.return_value = _exec_result(
            accepted=True,
            filled=Decimal("5"),
            status="filled",
            order_id="ord-buy-min-bump",
        )
        ctx = _make_context(
            pool,
            tmp_path,
            execution_service=exec_svc,
            market_data_service=_FakeMDS(_make_bundle(min_order_size=Decimal("5"))),
        )
        ctx.buy_min_amount = 1.0
        ctx.buy_max_amount = 8.0

        signal = _signal(price=0.56, size=20.0, tx_hash="0xmin-bump")
        results = await execute_trades(ctx, "leader1", [signal], time.time())

        assert len(results) == 1
        assert results[0].status == "executed"

        req = exec_svc.execute.await_args.args[0]
        assert req.side == "BUY"
        # Exchange minimum is 5 shares, so the notional should be bumped above the
        # tiny $1 strategy target but stay below the configured $8 max.
        assert float(req.amount) > 2.80
        assert float(req.amount) < 8.0


@pytest.mark.asyncio
async def test_buy_fill_updates_position_repo(tmp_path):
    async with _pg_pool() as pool:
        exec_svc = AsyncMock()
        exec_svc.execute.return_value = _exec_result(
            accepted=True, filled=Decimal("10"), status="filled", order_id="ord1"
        )
        ctx = _make_context(pool, tmp_path, execution_service=exec_svc)

        signal = _signal(price=0.50, size=20.0)
        results = await execute_trades(ctx, "leader1", [signal], time.time())
        assert len(results) == 1
        assert results[0].status == "executed"

        pos = await ctx.positions_repo.get_position("user1", "cond1")
        assert pos is not None
        assert pos.size_tokens == Decimal("10")
        assert pos.avg_entry_price == Decimal("0.50")


@pytest.mark.asyncio
async def test_sell_reduces_position(tmp_path):
    async with _pg_pool() as pool:
        exec_svc = AsyncMock()
        ctx = _make_context(pool, tmp_path, execution_service=exec_svc)

        # Set up existing position
        from storage.postgres.positions_repo import PositionRecord

        await ctx.positions_repo.upsert_position(
            PositionRecord(
                user_id="user1",
                condition_id="cond1",
                token_id=FAKE_TOKEN_ID,
                leader_id="leader1",
                size_tokens=Decimal("20"),
                avg_entry_price=Decimal("0.40"),
            )
        )

        exec_svc.execute.return_value = _exec_result(
            accepted=True, filled=Decimal("10"), status="filled", order_id="ord2"
        )

        signal = _signal(side="SELL", price=0.60, size=10.0)
        signal["_leader_sell_fraction"] = (
            0.5  # sell 50% of our 20-token position = 10 tokens
        )
        with patch(
            "orchestrators.trade_pipeline._fetch_live_position_size",
            new_callable=AsyncMock,
            return_value=Decimal("20"),
        ):
            results = await execute_trades(ctx, "leader1", [signal], time.time())
        assert len(results) == 1
        assert results[0].status == "executed"

        pos = await ctx.positions_repo.get_position("user1", "cond1")
        assert pos is not None
        assert pos.size_tokens == Decimal("10")


@pytest.mark.asyncio
async def test_sell_uses_leader_fraction_without_sell_decision_gate(tmp_path):
    async with _pg_pool() as pool:
        exec_svc = AsyncMock()
        exec_svc.execute.return_value = _exec_result(
            accepted=True,
            filled=Decimal("10"),
            status="filled",
            order_id="ord-sell-fraction",
        )
        ctx = _make_context(pool, tmp_path, execution_service=exec_svc)

        from storage.postgres.positions_repo import PositionRecord

        await ctx.positions_repo.upsert_position(
            PositionRecord(
                user_id="user1",
                condition_id="cond1",
                token_id=FAKE_TOKEN_ID,
                leader_id="leader1",
                size_tokens=Decimal("20"),
                avg_entry_price=Decimal("0.40"),
            )
        )

        class _SellAwareDecisionService:
            def decide_buy(self, **_kwargs):
                raise AssertionError("BUY path should not run in this SELL test")

            def decide_sell(self, **_kwargs):
                raise AssertionError("leader SELL path must not call decision gate")

        ctx.copy_decision_service = _SellAwareDecisionService()

        signal = _signal(side="SELL", price=0.50, size=100.0)
        signal["_leader_sell_fraction"] = 0.5
        signal["_leader_position_size_after"] = 100.0
        signal["_leader_position_size_before"] = 200.0

        with patch(
            "orchestrators.trade_pipeline._fetch_live_position_size",
            new_callable=AsyncMock,
            return_value=Decimal("20"),
        ):
            results = await execute_trades(ctx, "leader1", [signal], time.time())
        assert len(results) == 1
        assert results[0].status == "executed"

        req = exec_svc.execute.await_args.args[0]
        assert req.side == "SELL"
        assert req.amount == Decimal("10")


@pytest.mark.asyncio
async def test_sell_bumps_tiny_request_to_executable_minimum(tmp_path):
    async with _pg_pool() as pool:
        exec_svc = AsyncMock()
        exec_svc.execute.return_value = _exec_result(
            accepted=True, filled=Decimal("5"), status="filled", order_id="ord-sell-min"
        )
        ctx = _make_context(
            pool,
            tmp_path,
            execution_service=exec_svc,
            market_data_service=_FakeMDS(_make_bundle(min_order_size=Decimal("5.0"))),
        )

        from storage.postgres.positions_repo import PositionRecord

        await ctx.positions_repo.upsert_position(
            PositionRecord(
                user_id="user1",
                condition_id="cond1",
                token_id=FAKE_TOKEN_ID,
                leader_id="leader1",
                size_tokens=Decimal("10"),
                avg_entry_price=Decimal("0.40"),
            )
        )

        signal = _signal(side="SELL", price=0.50, size=1.0)
        signal["_leader_sell_fraction"] = 0.1

        with patch(
            "orchestrators.trade_pipeline._fetch_live_position_size",
            new_callable=AsyncMock,
            return_value=Decimal("10"),
        ):
            results = await execute_trades(ctx, "leader1", [signal], time.time())

        assert len(results) == 1
        assert results[0].status == "executed"

        req = exec_svc.execute.await_args.args[0]
        assert req.side == "SELL"
        assert req.amount == Decimal("5")


@pytest.mark.asyncio
async def test_sell_below_minimum_sells_largest_remaining_amount(tmp_path):
    async with _pg_pool() as pool:
        exec_svc = AsyncMock()
        exec_svc.execute.return_value = _exec_result(
            accepted=True,
            filled=Decimal("3"),
            status="filled",
            order_id="ord-sell-largest",
        )
        ctx = _make_context(
            pool,
            tmp_path,
            execution_service=exec_svc,
            market_data_service=_FakeMDS(_make_bundle(min_order_size=Decimal("5.0"))),
        )

        from storage.postgres.positions_repo import PositionRecord

        await ctx.positions_repo.upsert_position(
            PositionRecord(
                user_id="user1",
                condition_id="cond1",
                token_id=FAKE_TOKEN_ID,
                leader_id="leader1",
                size_tokens=Decimal("3"),
                avg_entry_price=Decimal("0.40"),
            )
        )

        signal = _signal(side="SELL", price=0.50, size=0.3)
        signal["_leader_sell_fraction"] = 0.1

        with patch(
            "orchestrators.trade_pipeline._fetch_live_position_size",
            new_callable=AsyncMock,
            return_value=Decimal("3"),
        ):
            results = await execute_trades(ctx, "leader1", [signal], time.time())

        assert len(results) == 1
        assert results[0].status == "executed"

        req = exec_svc.execute.await_args.args[0]
        assert req.side == "SELL"
        assert req.amount == Decimal("3")


@pytest.mark.asyncio
async def test_sell_uses_notional_when_size_missing(tmp_path):
    """SELL without _leader_sell_fraction defaults to full close (100% of held position)."""
    async with _pg_pool() as pool:
        exec_svc = AsyncMock()
        exec_svc.execute.return_value = _exec_result(
            accepted=True,
            filled=Decimal("20"),
            status="filled",
            order_id="ord-sell-full-close",
        )
        ctx = _make_context(pool, tmp_path, execution_service=exec_svc)

        from storage.postgres.positions_repo import PositionRecord

        await ctx.positions_repo.upsert_position(
            PositionRecord(
                user_id="user1",
                condition_id="cond1",
                token_id=FAKE_TOKEN_ID,
                leader_id="leader1",
                size_tokens=Decimal("20"),
                avg_entry_price=Decimal("0.40"),
            )
        )

        # No _leader_sell_fraction — pipeline defaults to selling 100% of the position
        signal = _signal(side="SELL", price=0.50, size=0.0)
        signal["usdcSize"] = 2.0
        with patch(
            "orchestrators.trade_pipeline._fetch_live_position_size",
            new_callable=AsyncMock,
            return_value=Decimal("20"),
        ):
            results = await execute_trades(ctx, "leader1", [signal], time.time())
        assert len(results) == 1
        assert results[0].status == "executed"

        req = exec_svc.execute.await_args.args[0]
        assert req.side == "SELL"
        assert req.amount == Decimal("20")  # full position closed


@pytest.mark.asyncio
async def test_sell_clamps_to_live_holdings_before_submit(tmp_path):
    async with _pg_pool() as pool:
        exec_svc = AsyncMock()
        exec_svc.execute.return_value = _exec_result(
            accepted=True,
            filled=Decimal("27.36"),
            status="filled",
            order_id="ord-sell-clamped",
        )
        ctx = _make_context(pool, tmp_path, execution_service=exec_svc)

        from storage.postgres.positions_repo import PositionRecord

        await ctx.positions_repo.upsert_position(
            PositionRecord(
                user_id="user1",
                condition_id="cond1",
                token_id=FAKE_TOKEN_ID,
                leader_id="leader1",
                size_tokens=Decimal("27.4623"),
                avg_entry_price=Decimal("0.40"),
            )
        )

        signal = _signal(side="SELL", price=0.3773, size=27.4623)
        signal["_leader_sell_fraction"] = 1.0

        with patch(
            "orchestrators.trade_pipeline.fetch_data",
            new_callable=AsyncMock,
            return_value=[
                {"asset": FAKE_TOKEN_ID, "size": "27.367906", "currentValue": 10.33}
            ],
        ):
            results = await execute_trades(ctx, "leader1", [signal], time.time())

        assert len(results) == 1
        assert results[0].status == "executed"

        req = exec_svc.execute.await_args.args[0]
        assert req.side == "SELL"
        assert req.amount == Decimal("27.36")


@pytest.mark.asyncio
async def test_sell_insufficient_balance_syncs_local_position_from_live_holdings(
    tmp_path,
):
    async with _pg_pool() as pool:
        exec_svc = AsyncMock()
        exec_svc.execute.return_value = _exec_result(
            accepted=False,
            filled=Decimal("0"),
            status="rejected",
            order_id=None,
            error="insufficient_balance | {'error': 'not enough balance / allowance: the balance is not enough -> balance: 4518, order amount: 35870000'}",
        )
        ctx = _make_context(pool, tmp_path, execution_service=exec_svc)

        from storage.postgres.positions_repo import PositionRecord

        await ctx.positions_repo.upsert_position(
            PositionRecord(
                user_id="user1",
                condition_id="cond1",
                token_id=FAKE_TOKEN_ID,
                leader_id="leader1",
                size_tokens=Decimal("35.8759"),
                avg_entry_price=Decimal("0.40"),
            )
        )

        signal = _signal(side="SELL", price=0.999, size=35.0)
        signal["_leader_sell_fraction"] = 1.0

        with patch(
            "orchestrators.trade_pipeline.fetch_data",
            new_callable=AsyncMock,
            return_value=[
                {"asset": FAKE_TOKEN_ID, "size": 4.518, "currentValue": 4.51}
            ],
        ):
            results = await execute_trades(ctx, "leader1", [signal], time.time())

        assert len(results) == 1
        assert results[0].status == "failed"
        # Post-fix the drift reason is emitted by reconcile_position_drift, not
        # the legacy _sync_sell_position_from_live_holdings helper. The new
        # contract: ``drift_reconciled action=shrunk live_size=<live>``.
        reason = str(results[0].reason)
        assert "drift_reconciled" in reason
        assert "action=shrunk" in reason
        assert "live_size=4.518" in reason

        pos = await ctx.positions_repo.get_position(
            "user1", "cond1", token_id=FAKE_TOKEN_ID
        )
        assert pos is not None
        assert pos.size_tokens == Decimal("4.518")


@pytest.mark.asyncio
async def test_sell_insufficient_balance_deletes_local_position_when_live_holding_is_zero(
    tmp_path,
):
    async with _pg_pool() as pool:
        exec_svc = AsyncMock()
        exec_svc.execute.return_value = _exec_result(
            accepted=False,
            filled=Decimal("0"),
            status="rejected",
            order_id=None,
            error="insufficient_balance | {'error': 'not enough balance / allowance: the balance is not enough -> balance: 0, order amount: 5000000'}",
        )
        ctx = _make_context(pool, tmp_path, execution_service=exec_svc)

        from storage.postgres.positions_repo import PositionRecord

        await ctx.positions_repo.upsert_position(
            PositionRecord(
                user_id="user1",
                condition_id="cond1",
                token_id=FAKE_TOKEN_ID,
                leader_id="leader1",
                size_tokens=Decimal("5"),
                avg_entry_price=Decimal("0.40"),
            )
        )

        signal = _signal(side="SELL", price=0.981, size=5.0)
        signal["_leader_sell_fraction"] = 1.0

        with patch(
            "orchestrators.trade_pipeline.fetch_data",
            new_callable=AsyncMock,
            return_value=[],
        ):
            results = await execute_trades(ctx, "leader1", [signal], time.time())

        assert len(results) == 1
        assert results[0].status == "skipped"
        assert results[0].reason == "SELL size is zero"
        assert (
            await ctx.positions_repo.get_position(
                "user1", "cond1", token_id=FAKE_TOKEN_ID
            )
            is None
        )


@pytest.mark.asyncio
async def test_sell_realized_pnl_falls_back_to_updated_position_or_signal_price(
    tmp_path,
):
    async with _pg_pool() as pool:
        exec_svc = AsyncMock()
        exec_svc.execute.return_value = _exec_result(
            accepted=True,
            filled=Decimal("10"),
            status="filled",
            order_id="ord-sell-fallback",
        )
        ctx = _make_context(pool, tmp_path, execution_service=exec_svc)

        pnl_tracker = MagicMock()
        ctx.pnl_tracker = pnl_tracker
        ctx.positions_repo.get_position = AsyncMock(
            return_value=SimpleNamespace(
                size_tokens=Decimal("20"), avg_entry_price=None
            )
        )
        # PR #108 fix: atomic append_fill + apply replaces the two-call
        # sequence in `execute_trades`; the mock returns ``(fill_id,
        # updated_position)``.
        ctx.fills_repo.append_fill_and_apply = AsyncMock(
            return_value=(1, SimpleNamespace(avg_entry_price=Decimal("0.45")))
        )

        signal = _signal(side="SELL", price=0.60, size=10.0)
        signal["_leader_sell_fraction"] = (
            0.5  # sell 50% of 20-token position = 10 tokens
        )
        with patch(
            "orchestrators.trade_pipeline._fetch_live_position_size",
            new_callable=AsyncMock,
            return_value=Decimal("20"),
        ):
            results = await execute_trades(ctx, "leader1", [signal], time.time())

        assert len(results) == 1
        assert results[0].status == "executed"

        record_sell_kwargs = pnl_tracker.record_sell.call_args.kwargs
        assert record_sell_kwargs["entry_price"] == pytest.approx(0.45)
        assert record_sell_kwargs["realized_pnl"] == pytest.approx(0.5)


@pytest.mark.asyncio
async def test_sell_realized_pnl_falls_back_to_signal_price_when_position_prices_missing(
    tmp_path,
):
    async with _pg_pool() as pool:
        exec_svc = AsyncMock()
        exec_svc.execute.return_value = ExecutionResult(
            accepted=True,
            order_id="ord-sell-signal-fallback",
            status="filled",
            requested_amount=Decimal("10"),
            filled_amount=Decimal("10"),
            avg_price=Decimal("0.50"),
            fees_paid=Decimal("0.01"),
            error=None,
        )
        ctx = _make_context(pool, tmp_path, execution_service=exec_svc)

        pnl_tracker = MagicMock()
        ctx.pnl_tracker = pnl_tracker
        ctx.positions_repo.get_position = AsyncMock(
            return_value=SimpleNamespace(
                size_tokens=Decimal("20"), avg_entry_price=None
            )
        )
        # PR #108 fix: atomic append_fill + apply mock returns (fill_id,
        # updated_position) — None updated_position triggers the signal-
        # price fallback path under test.
        ctx.fills_repo.append_fill_and_apply = AsyncMock(return_value=(1, None))

        signal = _signal(side="SELL", price=0.60, size=10.0)
        signal["_leader_sell_fraction"] = (
            0.5  # sell 50% of 20-token position = 10 tokens
        )
        with patch(
            "orchestrators.trade_pipeline._fetch_live_position_size",
            new_callable=AsyncMock,
            return_value=Decimal("20"),
        ):
            results = await execute_trades(ctx, "leader1", [signal], time.time())

        assert len(results) == 1
        assert results[0].status == "executed"

        record_sell_kwargs = pnl_tracker.record_sell.call_args.kwargs
        assert record_sell_kwargs["entry_price"] == pytest.approx(0.60)
        assert record_sell_kwargs["realized_pnl"] == pytest.approx(-1.0)


# ---------------------------------------------------------------------------
# Regression: token_id field-mapping priority (tokenID > token_id > asset)
# ---------------------------------------------------------------------------

from orchestrators.trade_pipeline import normalize_signals  # noqa: E402

HEX_ASSET = "0x951403abcdef1234567890abcdef1234567890ab8665"


def _raw_trade(**overrides):
    """Minimal raw trade payload accepted by normalize_signals."""
    base = {
        "side": "BUY",
        "conditionId": "cond1",
        "transactionHash": "0xdeadbeef",
        "price": 0.55,
        "size": 5.0,
        "usdcSize": 2.75,
        "timestamp": int(time.time()),
    }
    base.update(overrides)
    return base


class TestTokenIdFieldPriority:
    """tokenID must win over asset when both are present."""

    def test_tokenID_preferred_over_hex_asset(self):
        """Live-ish payload: asset=hex, tokenID=numeric → tokenID must win."""
        raw = _raw_trade(asset=HEX_ASSET, tokenID=FAKE_TOKEN_ID)
        signals = normalize_signals([raw], leader_id="L1")
        assert len(signals) == 1
        assert signals[0][0].token_id == FAKE_TOKEN_ID

    def test_token_id_preferred_over_hex_asset(self):
        """Underscore variant: asset=hex, token_id=numeric → token_id must win."""
        raw = _raw_trade(asset=HEX_ASSET, token_id=FAKE_TOKEN_ID)
        signals = normalize_signals([raw], leader_id="L1")
        assert len(signals) == 1
        assert signals[0][0].token_id == FAKE_TOKEN_ID

    def test_asset_fallback_when_no_tokenID(self):
        """If tokenID/token_id absent, asset (valid numeric) is used as fallback."""
        raw = _raw_trade(asset=FAKE_TOKEN_ID)
        signals = normalize_signals([raw], leader_id="L1")
        assert len(signals) == 1
        assert signals[0][0].token_id == FAKE_TOKEN_ID

    def test_hex_asset_only_is_rejected(self):
        """Hex-only asset fails is_valid_clob_token_id and signal is skipped."""
        raw = _raw_trade(asset=HEX_ASSET)
        signals = normalize_signals([raw], leader_id="L1")
        assert len(signals) == 0

    def test_tokenID_only(self):
        """Payload with only tokenID (no asset) works fine."""
        raw = _raw_trade(tokenID=FAKE_TOKEN_ID)
        signals = normalize_signals([raw], leader_id="L1")
        assert len(signals) == 1
        assert signals[0][0].token_id == FAKE_TOKEN_ID

    def test_onchain_snake_case_identifiers_are_accepted(self):
        """OnchainTradeSignal uses condition_id/token_id/tx_hash aliases."""
        raw = _raw_trade(
            conditionId=None,
            transactionHash=None,
            condition_id="cond-onchain",
            token_id=FAKE_TOKEN_ID,
            tx_hash="0xonchain",
        )

        signals = normalize_signals([raw], leader_id="L1")

        assert len(signals) == 1
        assert signals[0][0].condition_id == "cond-onchain"
        assert signals[0][0].token_id == FAKE_TOKEN_ID
        assert signals[0][0].tx_hash == "0xonchain"

    def test_trade_metadata_primary_fields(self):
        raw = _raw_trade(
            tokenID=FAKE_TOKEN_ID,
            outcome="Blackhawks",
            submarket="Moneyline",
        )
        signals = normalize_signals([raw], leader_id="L1")
        assert len(signals) == 1
        signal = signals[0][0]
        assert signal.outcome == "Blackhawks"
        assert signal.submarket == "Moneyline"

    def test_trade_metadata_fallback_fields(self):
        raw = _raw_trade(
            tokenID=FAKE_TOKEN_ID,
            selectionName="Hurricanes",
            marketSubtype="Spread",
        )
        signals = normalize_signals([raw], leader_id="L1")
        assert len(signals) == 1
        signal = signals[0][0]
        assert signal.outcome == "Hurricanes"
        assert signal.submarket == "Spread"

    def test_build_trade_key_prefers_tokenID(self):
        """build_trade_key must also prefer tokenID over hex asset."""
        trade = {
            "transactionHash": "0xabc",
            "side": "BUY",
            "asset": HEX_ASSET,
            "tokenID": FAKE_TOKEN_ID,
        }
        key = build_trade_key("u1", "l1", trade)
        assert FAKE_TOKEN_ID in key
        assert HEX_ASSET not in key

    def test_build_trade_key_reads_onchain_snake_case_tx_hash(self):
        """On-chain raw signals carry tx_hash, not transactionHash/txHash."""
        trade = {"tx_hash": "0xabc", "side": "BUY", "token_id": FAKE_TOKEN_ID}
        key = build_trade_key("u1", "l1", trade)
        assert "0xabc" in key


@pytest.mark.asyncio
async def test_full_flow_persisted_token_id(tmp_path):
    """End-to-end: raw payload → normalize → execute → positions_repo stores correct token_id."""
    async with _pg_pool() as pool:
        exec_svc = AsyncMock()
        exec_svc.execute = AsyncMock(return_value=_exec_result(filled=Decimal("5")))
        ctx = _make_context(pool, tmp_path, execution_service=exec_svc)

        # Simulate a live payload where asset is a hex string but tokenID is correct
        raw = _raw_trade(
            asset=HEX_ASSET,
            tokenID=FAKE_TOKEN_ID,
            size=100.0,
            price=0.50,
            usdcSize=50.0,
        )
        results = await execute_trades(ctx, "leader1", [raw], time.time())

        assert len(results) == 1
        assert results[0].status == "executed"

        # Verify the persisted position uses the correct (numeric) token_id
        pos = await ctx.positions_repo.get_position("user1", "cond1")
        assert pos is not None
        assert pos.token_id == FAKE_TOKEN_ID
        assert pos.token_id != HEX_ASSET


@pytest.mark.asyncio
async def test_immediate_fill_buys_record_daily_spend_once_each(tmp_path):
    """N1 regression — two immediately-filled BUYs must each consume the daily
    ledger exactly once. The old double-``record_buy`` bug counted each fill
    twice, so the daily cap read 2x actual spend on the dominant fast-fill
    path. Each fill is 10 tokens * 0.50 = 5.0 USDC → two BUYs must total 10,
    never 20."""
    async with _pg_pool() as pool:
        exec_svc = AsyncMock()
        exec_svc.execute.side_effect = [
            _exec_result(
                accepted=True, filled=Decimal("10"), status="filled", order_id="ord-a"
            ),
            _exec_result(
                accepted=True, filled=Decimal("10"), status="filled", order_id="ord-b"
            ),
        ]
        ctx = _make_context(pool, tmp_path)
        ctx.execution_coordinator = _PassThroughCoordinator(exec_svc)
        ctx.execution_service = exec_svc

        assert ctx.balance_regime is not None

        await execute_trades(ctx, "leader1", [_signal(tx_hash="0xfill-a")], time.time())
        await execute_trades(ctx, "leader1", [_signal(tx_hash="0xfill-b")], time.time())

        # 2 * (10 tokens * 0.50) = 10.0 ; the double-count regression would be 20.0
        assert ctx.balance_regime.daily_spent_usdc == Decimal("10.00")
