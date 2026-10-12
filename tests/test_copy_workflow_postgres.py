"""Exercise the composed copy runtime with PostgreSQL and a fake venue port."""

import asyncio
from datetime import UTC, datetime
from dataclasses import replace
from decimal import Decimal
from unittest.mock import AsyncMock

import json
import logging
import pytest
from testcontainers.postgres import PostgresContainer

from config.app_config import load_config
from config.env import WalletConfig
from domain.execution import ExecutionMetadata
from integrations.polymarket.models import VenueSubmitResult
from integrations.polymarket.models import OrderBook, OrderBookLevel
from orchestrators.runtime_bootstrap import (
    build_context_maps,
    build_trading_context,
    build_user_state,
)
from orchestrators.trade_pipeline import get_reconciliation_service
from orchestrators.v1_runtime_bootstrap import start_v1_copy_task_runtime
from services.copy_tasks.domain import (
    CopyTask,
    CopyTaskStatus,
    TaskSizingPolicy,
    TaskSpendPolicy,
)
from services.copy_tasks.protective_exits import ProtectiveRiskPolicy
from services.copy_tasks.event_routing import CopyTaskEventProcessor
from services.execution.pending_order_reconciliation import PendingOrderReconciliationQueryService
from services.multiUserConfig import RuntimeUser, UserFollowConfig
from services.metrics import LatencyPhase
from services.telemetry.latency import PipelineLatency
from tests.telemetry.test_pipeline_latency import ReceiptClock, histogram
from services.telemetry import TelemetryEmitters, TelemetryHub, TelemetryService
from storage.postgres.migrations import apply_migrations
from storage.postgres.connection import init_pool, shutdown_pool
from storage.postgres.fills_repo import FillRecord
from tests.conftest import make_clob_gateway

LEADER = "0x" + "1" * 40
FOLLOWER = "0x" + "2" * 40
NOW = datetime(2026, 9, 8, 12, tzinfo=UTC)


class FixedClock:
    """Return a deterministic timestamp for telemetry and audit records."""

    def now(self) -> datetime:
        """Return the fixed UTC time."""

        return NOW


class FixedBalance:
    """Supply exact collateral without an account or RPC request."""

    async def read(self, wallet: str) -> Decimal:
        """Return the configured follower's available collateral."""
        assert wallet == FOLLOWER
        return Decimal("100")


class EventIdentifiers:
    """Generate deterministic telemetry identifiers without random test inputs."""

    def __init__(self) -> None:
        self._sequence = 0

    def token_hex(self, nbytes: int) -> str:
        """Return a unique identifier with the requested hexadecimal width."""
        self._sequence += 1
        return f"{self._sequence:0{nbytes * 2}x}"


@pytest.mark.asyncio
async def test_activated_task_executes_accounts_partial_fills_and_recovers(monkeypatch, tmp_path):
    """Bind real services, submit once, account fills, and restart the runtime."""
    monkeypatch.chdir(tmp_path)
    balance = FixedBalance()
    monkeypatch.setattr("utils.getMyBalance.get_my_balance", balance.read)
    gateway = make_clob_gateway()
    gateway.get_fee_rate_bps = AsyncMock(return_value=Decimal("0"))
    gateway.get_price = AsyncMock(return_value={"price": "0.5"})
    gateway.get_execution_metadata = AsyncMock(
        return_value=ExecutionMetadata(
            token_id="123456",
            tick_size=Decimal("0.005"),
            min_order_size=Decimal("1"),
            neg_risk=False,
        )
    )
    decisions = []

    process_batch = CopyTaskEventProcessor.process_batch

    async def observe_process(processor, tasks, event):
        decisions.extend(await process_batch(processor, tasks, event))

    monkeypatch.setattr(CopyTaskEventProcessor, "process_batch", observe_process)
    with PostgresContainer("postgres:16-alpine") as container:
        dsn = container.get_connection_url().replace("postgresql+psycopg2://", "postgresql://")
        pool = await init_pool(dsn, min_size=1, max_size=4)
        handle = None
        hub = TelemetryHub()
        telemetry = TelemetryService(hub=hub, clock=FixedClock(), rng=EventIdentifiers())
        receipt_clock = ReceiptClock()
        latency = PipelineLatency(clock=receipt_clock, metrics=telemetry.metrics_registry)
        emitters = TelemetryEmitters.build(telemetry, latency=latency)
        # Stand in for the removed remote stream: one direct bounded hub
        # subscription observes the same ordered telemetry the trading path
        # publishes, with no transport or credential surface in between.
        subscription = hub.subscribe()
        try:
            await telemetry.start()
            async with pool.acquire() as connection:
                await apply_migrations(connection)
            config = load_config(
                {
                    "database": {"dsn": dsn},
                    "wallet": {
                        "funder_address": FOLLOWER,
                        "polygon_rpc_url": "https://rpc.invalid",
                    },
                    "polymarket": {"rpc_url": "https://rpc.invalid", "enforce_geoblock": False},
                    "execution": {"trading_mode": "paper", "reservations_enabled": True},
                    "copy_trading": {"copy_tasks": {"enabled": True}},
                    "risk": {"enable_risk_mgmt": True},
                }
            )
            user = RuntimeUser("account-a", WalletConfig(funder_address=FOLLOWER))
            follow = UserFollowConfig("account-a", LEADER, True, 100, 10, 100)
            state = build_user_state(
                user,
                None,
                pool,
                config,
                clob_gateway=gateway,
                telemetry=emitters,
            )
            execution_results = []
            execute = state.execution_service.execute

            async def capture_execution(request):
                result = await execute(request)
                execution_results.append(result)
                return result

            monkeypatch.setattr(state.execution_service, "execute", capture_execution)
            context = await build_trading_context(state, follow, config)
            handle = await start_v1_copy_task_runtime(
                pool=pool,
                config=config,
                copy_tasks_config=config.copy_trading.copy_tasks,
                contexts=(context,),
                balance_reader=balance,
                clock=lambda: NOW,
                decision_observer=emitters.trade,
                latency=latency,
            )
            assert handle is not None
            discovered_context = replace(
                context,
                task_position_manager=None,
                extra={**context.extra, "leader_wallet": "0x" + "3" * 40},
            )
            handle.add_contexts((discovered_context,))
            assert discovered_context.task_position_manager is context.task_position_manager
            commands = handle.bundle.tasks
            task = CopyTask(
                task_id="task-a",
                name="Task A",
                leader_wallet=LEADER,
                follower_account="account-a",
                sizing=TaskSizingPolicy(mode="fixed", fixed_amount=Decimal("5")),
                spend=TaskSpendPolicy(max_total=Decimal("10")),
                protective_risk=ProtectiveRiskPolicy(take_profit_price=Decimal("0.7")),
            )
            created = await commands.create(task)
            active = await commands.resume(task.task_id, expected_version=created.version)
            assert active.status == CopyTaskStatus.ACTIVE
            source = {
                "conditionId": "condition-a",
                "asset": "123456",
                "side": "BUY",
                "size": "20",
                "price": "0.5",
                "usdcSize": "10",
                "outcome": "YES",
                "transactionHash": "0xsource1",
                "timestamp": int(NOW.timestamp()),
                "_source_receipt": receipt_clock.capture(),
            }
            await handle.submit_raw_batch(LEADER, (source,))
            await handle.submit_raw_batch(
                LEADER, ({**source, "transactionHash": "0xunknown-time", "timestamp": None},)
            )
            await handle.close()
            assert decisions and decisions[0].allowed, decisions
            assert gateway.submit_marketable_order.await_count == 1, execution_results
            for phase in (
                LatencyPhase.SOURCE_DETECTION_TO_NORMALIZED,
                LatencyPhase.NORMALIZED_TO_WORKER_START,
                LatencyPhase.COPY_DECISION,
                LatencyPhase.RESERVATION,
                LatencyPhase.DATABASE_ACQUISITION,
                LatencyPhase.VENUE_SUBMISSION,
                LatencyPhase.SUBMISSION_TO_FIRST_ACK,
            ):
                assert histogram(telemetry.metrics_registry, phase).count >= 1
            async def next_event():
                delivery = await asyncio.wait_for(subscription.get(), timeout=5)
                assert delivery.event is not None, delivery
                return delivery.event

            event = await next_event()
            assert event.event_type.value == "trade_submitted"
            decision_event = await next_event()
            assert str(decision_event.payload.decision.source_price) == "0.5"
            skipped = await next_event()
            assert skipped.event_type.value == "trade_skip"
            assert skipped.payload.decision.reason_code == "signal_age_unknown"
            assert gateway.submit_marketable_order.await_count == 1
            submitted = gateway.submit_marketable_order.await_args.args[0]
            assert submitted.size == Decimal("5")
            pending = PendingOrderReconciliationQueryService(state.orders_repo)
            records = await pending.list_records(user_id="account-a", limit=20)
            assert len(records) == 1
            side, record = records[0]
            reconciler = get_reconciliation_service(context)
            for status, cumulative, average in (
                ("partially_filled", "4", "0.45"),
                ("filled", "10", "0.48"),
            ):
                confirm = AsyncMock(
                    return_value={
                        "status": status,
                        "size_matched": cumulative,
                        "avg_price": average,
                    }
                )
                await reconciler.reconcile_trade_record(record, side, confirm_fill=confirm)
                partial = await state.positions_repo.get_position(
                    "account-a",
                    "condition-a",
                    token_id="123456",
                )
                assert partial.size_tokens == Decimal(cumulative)
                assert partial.avg_entry_price == Decimal(average)
            position = await state.positions_repo.get_position(
                "account-a", "condition-a", token_id="123456"
            )
            assert position.size_tokens == Decimal("10")
            assert (
                histogram(
                    telemetry.metrics_registry, LatencyPhase.SUBMISSION_TO_FIRST_FILL_EVIDENCE
                ).count
                == 1
            )
            lots = await handle.bundle.risk_lots.list_open("account-a")
            assert sum((lot.lot.remaining_shares for lot in lots), Decimal(0)) == Decimal("10")
            restarted = build_user_state(
                user,
                None,
                pool,
                config,
                clob_gateway=gateway,
                telemetry=emitters,
            )
            _, risk_contexts = await build_context_maps(
                [user], {user.user_id: restarted}, [], config
            )
            assert set(risk_contexts) == {user.user_id}
            restored_context = risk_contexts[user.user_id]
            handle = await start_v1_copy_task_runtime(
                pool=pool,
                config=config,
                copy_tasks_config=config.copy_trading.copy_tasks,
                contexts=(restored_context,),
                balance_reader=balance,
                clock=lambda: NOW,
                decision_observer=emitters.trade,
            )
            assert handle.restored_lot_count == 2
            await handle.submit_raw_batch(LEADER, (source,))
            await handle.close()
            assert gateway.submit_marketable_order.await_count == 1
            other = CopyTask(
                task_id="task-b", name="Task B", leader_wallet=LEADER, follower_account="account-a"
            )
            await handle.bundle.tasks.create(other)
            await handle.bundle.tasks.pause(other.task_id, expected_version=1)
            async with pool.acquire() as connection:
                await connection.execute(
                    "INSERT INTO orders (idempotency_key, user_id, order_id, request_json) "
                    "VALUES ('other-order', 'account-a', 'other-order', $1::jsonb)",
                    json.dumps({"recovery_metadata": {"task_id": "task-b"}}),
                )
            await state.fills_repo.append_fill_and_apply(
                FillRecord(
                    "account-a",
                    LEADER,
                    "other-order",
                    "condition-a",
                    "123456",
                    "BUY",
                    Decimal("10"),
                    Decimal("0.5"),
                    fill_ts=int(NOW.timestamp()),
                ),
                state.positions_repo,
            )
            handle = await start_v1_copy_task_runtime(
                pool=pool,
                config=config,
                copy_tasks_config=config.copy_trading.copy_tasks,
                contexts=(restored_context,),
                balance_reader=balance,
                clock=lambda: NOW,
                decision_observer=emitters.trade,
            )
            gateway.submit_marketable_order.return_value = VenueSubmitResult(
                success=True,
                order_id="sell-order",
                status="submitted",
            )
            sell_source = dict(
                source, side="SELL", transactionHash="0xsource2", _leader_sell_fraction="0.5"
            )
            await handle.submit_raw_batch(LEADER, (sell_source,))
            await handle.close()
            assert gateway.submit_marketable_order.await_count == 2
            assert gateway.submit_marketable_order.await_args.args[0].size == Decimal("5")
            sell_records = await PendingOrderReconciliationQueryService(
                restarted.orders_repo
            ).list_records(
                user_id="account-a",
                limit=20,
            )
            sell_record = next(record for side, record in sell_records if side == "SELL")
            sell_reconciler = get_reconciliation_service(restored_context)
            confirmed = AsyncMock(
                return_value={
                    "status": "filled",
                    "size_matched": "5",
                    "avg_price": "0.55",
                }
            )
            for _ in range(2):
                assert await sell_reconciler.reconcile_trade_record(
                    sell_record,
                    "SELL",
                    confirm_fill=confirmed,
                    logger=logging.getLogger(__name__),
                )
            lots = await handle.bundle.risk_lots.list_open("account-a")
            assert sum(
                (lot.lot.remaining_shares for lot in lots if lot.lot.task_id == "task-a"),
                Decimal(0),
            ) == Decimal("5")
            assert sum(
                (lot.lot.remaining_shares for lot in lots if lot.lot.task_id == "task-b"),
                Decimal(0),
            ) == Decimal("10")
            await handle.bundle.tasks.pause("task-a", expected_version=2)
            gateway.get_orderbook = AsyncMock(
                return_value=OrderBook(
                    token_id="123456",
                    tick_size=Decimal("0.005"),
                    min_order_size=Decimal("1"),
                    neg_risk=False,
                    best_bid=Decimal("0.79"),
                    best_ask=Decimal("0.8"),
                    bids=[OrderBookLevel(Decimal("0.79"), Decimal("1000"))],
                    asks=[OrderBookLevel(Decimal("0.8"), Decimal("1000"))],
                )
            )
            gateway.submit_marketable_order.return_value = VenueSubmitResult(
                success=True,
                order_id="protective-order",
                status="submitted",
            )
            assert restored_context.task_position_manager is not None
            await restored_context.task_position_manager.run_once()
            assert gateway.submit_marketable_order.await_count == 3
            assert gateway.submit_marketable_order.await_args.args[0].size == Decimal("5")
            await restored_context.task_position_manager.run_once()
            assert gateway.submit_marketable_order.await_count == 3
            protective_records = await pending.list_records(user_id="account-a", limit=20)
            protective_record = next(
                record
                for _, record in protective_records
                if record["order_id"] == "protective-order"
            )
            assert await sell_reconciler.reconcile_trade_record(
                protective_record,
                "SELL",
                confirm_fill=AsyncMock(
                    return_value={
                        "status": "filled",
                        "size_matched": "5",
                        "avg_price": "0.79",
                    }
                ),
                logger=logging.getLogger(__name__),
            )
            async with pool.acquire() as connection:
                assert (
                    await connection.fetchval(
                        "SELECT COUNT(*) FROM market_reentry_locks WHERE follower_account = 'account-a' "
                        "AND task_id = 'task-a' AND market_id = 'condition-a'"
                    )
                    == 1
                )
        finally:
            if handle is not None:
                await handle.close()
            hub.unsubscribe(subscription)
            assert hub.subscription_count == 0
            await telemetry.close()
            await shutdown_pool()
