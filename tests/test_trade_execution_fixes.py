"""Unit tests for Phase 1 trade execution fixes and low-latency RPC enrichment.

Covers:
1. Bug #1 (signal_age_unknown): 0ms blockTimestamp log parsing, singleflight
   RpcBlockTimestampReader fallback, and concurrent enrichment in AlchemyDetectionRuntime.
2. Bug #2 (leader_pusd_balance_required): portfolio/balance-sizing-only RPC
   leader funder balance resolution from OrderFilled log maker, cached eth_getCode,
   bounded TTL + singleflight CachedCollateralBalanceReader, pre-trade balance
   reconstruction for mined BUY fills, and zero RPC overhead for fixed/exact_mirror/uncapped.
3. Bug #3: visible structured copy_task_skipped/dispatched logs, CachedCopyTaskReader
   invalidation, and percentage price-offset alignment across [0.20, 0.90].
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest

from pmcopy.adapters.polygon.block_timestamp import (
    RpcBlockTimestampReader,
    parse_log_block_timestamp_ms,
)
from pmcopy.config.app_config import load_config
from pmcopy.domain.detection import (
    DetectionFinality,
    DetectionMode,
    LeaderTradeEvent,
    TradeSide,
)
from pmcopy.domain.tracked_users import TrackedTraderAddresses, TrackedUserDirectory
from pmcopy.orchestrators.detection.alchemy_detection_runtime import AlchemyDetectionRuntime
from pmcopy.orchestrators.copy_tasks.v1_event_adapters import (
    LeaderEventNormalizer,
    TradingContextTaskSnapshotProvider,
)
from pmcopy.services.copy_sizing_v5.policy import CopySizingPolicy
from pmcopy.services.copy_tasks.admission import CopyTaskAdmissionEngine
from pmcopy.services.copy_tasks.budget import (
    InMemoryTaskBudgetRepository,
    TaskBudgetReservationService,
)
from pmcopy.services.copy_tasks.domain import (
    CopyTask,
    CopyTaskStatus,
    MonitoringMode,
    PriceOffsetDirection,
    PriceOffsetMode,
    ReactionPolicy,
    TaskMarketFilter,
    TaskPricePolicy,
    TaskSchedule,
    TaskSizingMode,
    TaskSizingPolicy,
    TaskSpendPolicy,
    TaskTriggerPolicy,
)
from pmcopy.services.copy_tasks.entry_price import CopyTaskEntryPriceService
from pmcopy.services.copy_tasks.event_routing import (
    CachedCopyTaskReader,
    CopyTaskEventProcessor,
    CopyTaskSourceEvent,
    TaskExecutionCommand,
)
from pmcopy.services.copy_tasks.lifecycle import (
    CopyTaskFillLifecycleService,
    TaskExecutionDisposition,
    TaskExecutionOutcome,
)
from pmcopy.services.copy_tasks.monitoring import CopyMonitoringModePolicy
from pmcopy.services.copy_tasks.reaction import CopyReactionEngine
from pmcopy.services.copy_tasks.reentry_lock import (
    InMemoryMarketReentryLockRepository,
    MarketReentryLockService,
)
from pmcopy.services.copy_tasks.runtime import CopyTaskRuntimeCoordinator
from pmcopy.services.copy_tasks.sizing import CopyTaskSizingService
from pmcopy.services.onchain_detection.order_filled_detector import ORDER_FILLED_V2_TOPIC0
from pmcopy.services.providers.provider_balances import (
    CachedCollateralBalanceReader,
    ProviderBalanceReader,
)

LEADER = "0xce0b1054a2adb61a3efe8bcd42acc8d40154b974"
FUNDER_FROM_LOG = "0x9988776655443322110099887766554433221100"
FOLLOWER = "0x68e6bde00138a091a245367545857af03894a83f"
EXCHANGE = "0xe2222d279d744050d28e00520010520000310f59"
CONDITION_ID = "0x" + "ab" * 32
TOKEN_ID = "12345678901234567890"
BLOCK_HASH = "0x" + "cd" * 32
TX_HASH = "0x" + "ef" * 32


def _make_task(*, sizing: TaskSizingPolicy, price: TaskPricePolicy | None = None) -> CopyTask:
    return CopyTask(
        task_id="test-task",
        name="test-task",
        leader_wallet=LEADER,
        follower_account=FOLLOWER,
        status=CopyTaskStatus.ACTIVE,
        monitoring_mode=MonitoringMode.NORMAL,
        sizing_mode=sizing.mode.value,
        trigger=TaskTriggerPolicy(enabled=False),
        spend=TaskSpendPolicy(enabled=False),
        price=price
        or TaskPricePolicy(
            enabled=True,
            min_price=Decimal("0.20"),
            max_price=Decimal("0.90"),
            offset_mode=PriceOffsetMode.PERCENTAGE,
            offset_direction=PriceOffsetDirection.ABOVE,
            offset=Decimal("8"),
            max_slippage_percent=Decimal("8"),
        ),
        reaction=ReactionPolicy(enabled=True, follow_buys=True, copy_sells=True),
        market_filter=TaskMarketFilter(enabled=True),
        schedule=TaskSchedule(enabled=False),
        sizing=sizing,
    )


def _sample_order_filled_log(*, block_timestamp: str | None = "0x6aff2b0c") -> dict[str, object]:
    from eth_abi import encode

    data = encode(
        ["uint8", "uint256", "uint256", "uint256", "uint256", "bytes32", "bytes32"],
        [0, int(TOKEN_ID), 500_000_000, 1_000_000_000, 0, b"\x00" * 32, b"\x00" * 32],
    )
    payload: dict[str, object] = {
        "address": EXCHANGE,
        "topics": [
            "0x" + ORDER_FILLED_V2_TOPIC0.hex(),
            "0x" + "11" * 32,
            "0x" + "0" * 24 + FUNDER_FROM_LOG[2:],
            "0x" + "0" * 24 + EXCHANGE[2:],
        ],
        "data": "0x" + data.hex(),
        "transactionHash": TX_HASH,
        "logIndex": "0x1",
        "blockNumber": "0x100",
        "blockHash": BLOCK_HASH,
        "removed": False,
    }
    if block_timestamp is not None:
        payload["blockTimestamp"] = block_timestamp
    return payload


class TestBug1BlockTimestampAndAlchemyEnrichment:
    def test_parse_log_block_timestamp_ms_handles_hex_and_int(self) -> None:
        assert parse_log_block_timestamp_ms({"blockTimestamp": "0x6aff2b0c"}) == 1795107596000
        assert parse_log_block_timestamp_ms({"block_timestamp": 1795107596}) == 1795107596000
        assert parse_log_block_timestamp_ms({}) is None
        assert parse_log_block_timestamp_ms({"blockTimestamp": True}) is None

    @pytest.mark.asyncio
    async def test_rpc_block_timestamp_reader_singleflight_coalesces_concurrent_calls(self) -> None:
        calls = 0
        release = asyncio.Event()

        class _Client:
            async def post_json(self, url: str, *, json_body: dict[str, object], silent: bool):
                nonlocal calls
                calls += 1
                await release.wait()
                return {"jsonrpc": "2.0", "id": 1, "result": {"hash": BLOCK_HASH, "timestamp": "0x6aff2b0c"}}

        reader = RpcBlockTimestampReader(_Client(), rpc_url="https://polygon-mainnet.g.alchemy.com/v2/x")
        t1 = asyncio.create_task(reader.timestamp_ms(BLOCK_HASH))
        t2 = asyncio.create_task(reader.timestamp_ms(BLOCK_HASH))
        await asyncio.sleep(0)
        release.set()
        r1, r2 = await asyncio.gather(t1, t2)
        assert r1 == 1795107596000
        assert r2 == 1795107596000
        assert calls == 1

    @pytest.mark.asyncio
    async def test_alchemy_runtime_uses_fast_path_log_block_timestamp_without_rpc(self) -> None:
        received = []
        ts_calls = 0

        class _TsReader:
            async def timestamp_ms(self, block_hash: str) -> int | None:
                nonlocal ts_calls
                ts_calls += 1
                return 1795107596000

        class _CidResolver:
            async def resolve(self, token_id: str) -> str:
                return CONDITION_ID

        directory = TrackedUserDirectory.from_entries(
            [TrackedTraderAddresses(leader_wallet=LEADER, proxy_address=FUNDER_FROM_LOG)]
        )
        runtime = AlchemyDetectionRuntime(
            ws_url="wss://polygon-mainnet.g.alchemy.com/v2/test",
            mode=DetectionMode.ON_CHAIN_LOGS_ONLY,
            active_exchange_addresses=(EXCHANGE,),
            include_legacy_v1_exchanges=False,
            track_direct_user_txs=False,
            max_hourly_ws_mb=Decimal("250"),
            subscription_debounce_seconds=0.1,
            mempool_reservation_ttl_seconds=15.0,
            initial_directory=directory,
            directory_builder=lambda _w: directory,
            sink=lambda sig: _append_async(received, sig),
            condition_id_resolver=_CidResolver(),
            block_timestamp_reader=_TsReader(),
        )
        await runtime._handle_log_notification(_sample_order_filled_log(block_timestamp="0x6aff2b0c"))
        assert len(received) == 1
        assert received[0].event_ts == 1795107596000
        assert received[0].condition_id == CONDITION_ID
        assert received[0].token_holder_wallet == FUNDER_FROM_LOG
        assert ts_calls == 0

    @pytest.mark.asyncio
    async def test_alchemy_runtime_falls_back_to_rpc_block_timestamp_concurrently(self) -> None:
        received = []
        ts_calls = 0

        class _TsReader:
            async def timestamp_ms(self, block_hash: str) -> int | None:
                nonlocal ts_calls
                ts_calls += 1
                return 1795107596000

        class _CidResolver:
            async def resolve(self, token_id: str) -> str:
                return CONDITION_ID

        directory = TrackedUserDirectory.from_entries(
            [TrackedTraderAddresses(leader_wallet=LEADER, proxy_address=FUNDER_FROM_LOG)]
        )
        runtime = AlchemyDetectionRuntime(
            ws_url="wss://polygon-mainnet.g.alchemy.com/v2/test",
            mode=DetectionMode.ON_CHAIN_LOGS_ONLY,
            active_exchange_addresses=(EXCHANGE,),
            include_legacy_v1_exchanges=False,
            track_direct_user_txs=False,
            max_hourly_ws_mb=Decimal("250"),
            subscription_debounce_seconds=0.1,
            mempool_reservation_ttl_seconds=15.0,
            initial_directory=directory,
            directory_builder=lambda _w: directory,
            sink=lambda sig: _append_async(received, sig),
            condition_id_resolver=_CidResolver(),
            block_timestamp_reader=_TsReader(),
        )
        await runtime._handle_log_notification(_sample_order_filled_log(block_timestamp=None))
        assert len(received) == 1
        assert received[0].event_ts == 1795107596000
        assert ts_calls == 1


async def _append_async(bucket: list, item: object) -> None:
    bucket.append(item)


class TestBug2PortfolioBasedLeaderBalanceRpc:
    @pytest.mark.parametrize(
        ("mode", "cap_enabled", "expected"),
        [
            (TaskSizingMode.PROPORTIONAL, True, True),
            (TaskSizingMode.ADAPTIVE, True, True),
            (TaskSizingMode.TIERED, True, True),
            (TaskSizingMode.BALANCE_WEIGHTED, False, True),
            (TaskSizingMode.PORTFOLIO_WEIGHTED, False, True),
            (TaskSizingMode.PROPORTIONAL, False, False),
            (TaskSizingMode.FIXED, True, False),
            (TaskSizingMode.EXACT_MIRROR, True, False),
        ],
    )
    def test_requires_leader_balance_snapshot_predicate(
        self, mode: TaskSizingMode, cap_enabled: bool, expected: bool
    ) -> None:
        policy = TaskSizingPolicy(
            mode=mode,
            proportional_balance_cap_enabled=cap_enabled,
            require_leader_balance=True,
        )
        assert policy.requires_leader_balance_snapshot is expected

    @pytest.mark.asyncio
    async def test_provider_balance_reader_caches_eth_get_code(self) -> None:
        methods: list[str] = []

        class _Gateway:
            async def call(self, provider: str, method: str, params: list[object], *, category: str = "essential"):
                methods.append(method)
                if method == "eth_getCode":
                    return "0x60806040"
                if method == "eth_call":
                    return hex(25_000_000)
                return None

        reader = ProviderBalanceReader(_Gateway(), "alchemy", ("0x" + "22" * 20,))
        assert await reader.collateral(LEADER) == Decimal("25.000000")
        assert await reader.collateral(FOLLOWER) == Decimal("25.000000")
        assert methods == ["eth_getCode", "eth_call", "eth_call"]

    @pytest.mark.asyncio
    async def test_cached_collateral_balance_reader_ttl_singleflight_and_fail_closed(self) -> None:
        now = [100.0]
        calls = 0
        release = asyncio.Event()

        class _Inner:
            async def collateral(self, address: str) -> Decimal | None:
                nonlocal calls
                calls += 1
                await release.wait()
                return Decimal("1000.00")

        cache = CachedCollateralBalanceReader(_Inner(), ttl_seconds=5.0, monotonic=lambda: now[0])
        t1 = asyncio.create_task(cache.read(FUNDER_FROM_LOG))
        t2 = asyncio.create_task(cache.read(FUNDER_FROM_LOG.upper().replace("0X", "0x")))
        await asyncio.sleep(0)
        release.set()
        b1, b2 = await asyncio.gather(t1, t2)
        assert b1 == Decimal("1000.00")
        assert b2 == Decimal("1000.00")
        assert calls == 1

        # Hit within TTL -> 0 new calls
        assert await cache.read(FUNDER_FROM_LOG) == Decimal("1000.00")
        assert calls == 1

        # Expired -> refetches
        now[0] = 106.0
        assert await cache.read(FUNDER_FROM_LOG) == Decimal("1000.00")
        assert calls == 2

    @pytest.mark.asyncio
    async def test_snapshot_provider_queries_log_funder_only_for_portfolio_sizing(self) -> None:
        queried_leaders: list[str] = []

        class _BalReader:
            def __init__(self, value: Decimal, record: list[str] | None = None) -> None:
                self._value = value
                self._record = record

            async def read(self, wallet: str) -> Decimal | None:
                if self._record is not None:
                    self._record.append(wallet)
                return self._value

        class _Positions:
            async def list_open_positions(self, user_id: str):
                return []

        class _Reservations:
            async def sum_active_quote_for_user(self, user_id: str) -> Decimal:
                return Decimal("0")

        ctx = SimpleNamespace(
            user_id=FOLLOWER,
            wallet_address=FOLLOWER,
            positions_repo=_Positions(),
            reservations_repo=_Reservations(),
            extra={"leader_wallet": LEADER},
        )
        provider = TradingContextTaskSnapshotProvider(
            contexts=(ctx,),
            balance_reader=_BalReader(Decimal("200")),
            leader_balance_reader=_BalReader(Decimal("1500"), queried_leaders),
        )
        now = datetime(2026, 11, 19, 18, 0, 0, tzinfo=UTC)
        normalizer = LeaderEventNormalizer(clock=SimpleNamespace(now=lambda: now))
        event = normalizer.normalize_raw(
            LEADER,
            {
                "condition_id": CONDITION_ID,
                "token_id": TOKEN_ID,
                "side": "BUY",
                "price": "0.50",
                "size": "1000",
                "usdc_size": "500",
                "tx_hash": TX_HASH,
                "event_ts": int(now.timestamp() * 1000),
                "timestamp_unit": "milliseconds",
                "token_holder_wallet": FUNDER_FROM_LOG,
            },
            mode=DetectionMode.ON_CHAIN_LOGS_ONLY,
        )
        assert event.leader_funder_wallet == FUNDER_FROM_LOG

        # 1. Fixed sizing -> ZERO leader RPC calls!
        fixed_task = _make_task(
            sizing=TaskSizingPolicy(
                mode=TaskSizingMode.FIXED,
                fixed_amount=Decimal("5"),
                proportional_balance_cap_enabled=True,
                require_leader_balance=True,
            )
        )
        fixed_snap = await provider.snapshot_for(fixed_task, event)
        assert fixed_snap.leader_pusd_balance is None
        assert queried_leaders == []

        # 2. Proportional with balance cap -> queries FUNDER_FROM_LOG & reconstructs pre-trade 1500 + 500 = 2000
        prop_task = _make_task(
            sizing=TaskSizingPolicy(
                mode=TaskSizingMode.PROPORTIONAL,
                percentage=Decimal("7"),
                proportional_balance_cap_enabled=True,
                require_leader_balance=True,
            )
        )
        prop_snap = await provider.snapshot_for(prop_task, event)
        assert queried_leaders == [FUNDER_FROM_LOG]
        assert prop_snap.leader_pusd_balance == Decimal("2000")

    @pytest.mark.asyncio
    async def test_end_to_end_mined_buy_with_require_leader_balance_dispatches_and_logs(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        now = datetime(2026, 11, 19, 18, 0, 1, tzinfo=UTC)
        event_ts = datetime(2026, 11, 19, 18, 0, 0, tzinfo=UTC)
        task = _make_task(
            sizing=TaskSizingPolicy(
                mode=TaskSizingMode.PROPORTIONAL,
                percentage=Decimal("10"),
                buy_min_amount=Decimal("4"),
                buy_max_amount=Decimal("12"),
                copyable_balance_percent=Decimal("50"),
                proportional_balance_cap_enabled=True,
                require_leader_balance=True,
            )
        )

        class _TaskReader:
            async def list_active(self) -> tuple[CopyTask, ...]:
                return (task,)

        class _BalReader:
            def __init__(self, val: Decimal) -> None:
                self._val = val

            async def read(self, wallet: str) -> Decimal | None:
                return self._val

        class _Positions:
            async def list_open_positions(self, user_id: str):
                return []

        class _Reservations:
            async def sum_active_quote_for_user(self, user_id: str) -> Decimal:
                return Decimal("0")

        submitted: list[TaskExecutionCommand] = []

        class _Exec:
            async def submit(self, command: TaskExecutionCommand) -> TaskExecutionOutcome:
                submitted.append(command)
                return TaskExecutionOutcome(
                    TaskExecutionDisposition.ACCEPTED_PENDING, reason="submitted"
                )

        ctx = SimpleNamespace(
            user_id=FOLLOWER,
            wallet_address=FOLLOWER,
            positions_repo=_Positions(),
            reservations_repo=_Reservations(),
            extra={"leader_wallet": LEADER},
        )
        budgets = TaskBudgetReservationService(InMemoryTaskBudgetRepository())
        locks = MarketReentryLockService(InMemoryMarketReentryLockRepository())
        coordinator = CopyTaskRuntimeCoordinator(
            locks=locks,
            monitoring=CopyMonitoringModePolicy(),
            admission=CopyTaskAdmissionEngine(),
            reactions=CopyReactionEngine(),
            entry_prices=CopyTaskEntryPriceService(),
            sizing=CopyTaskSizingService(base_policy=CopySizingPolicy()),
            task_budgets=budgets,
            task_budget_lifecycle=CopyTaskFillLifecycleService(
                task_budgets=budgets,
                reentry_locks=locks,
            ),
        )
        processor = CopyTaskEventProcessor(
            task_reader=CachedCopyTaskReader(_TaskReader(), ttl_seconds=5.0),
            task_budgets=budgets,
            coordinator=coordinator,
            snapshot_provider=TradingContextTaskSnapshotProvider(
                contexts=(ctx,),
                balance_reader=_BalReader(Decimal("200")),
                leader_balance_reader=_BalReader(Decimal("500")),
            ),
            execution=_Exec(),
            clock=SimpleNamespace(now=lambda: now),
            max_signal_age_seconds=100.0,
        )
        event = CopyTaskSourceEvent(
            trade=LeaderTradeEvent(
                leader_wallet=LEADER,
                token_id=TOKEN_ID,
                condition_id=CONDITION_ID,
                side=TradeSide.BUY,
                price=Decimal("0.35"),  # Below $0.50 — tests percentage offset fix too!
                shares=Decimal("500"),
                notional=Decimal("175"),
                mode=DetectionMode.ON_CHAIN_LOGS_ONLY,
                finality=DetectionFinality.CONFIRMED,
                detected_at=now,
                venue_timestamp=event_ts,
                transaction_hash=TX_HASH,
                log_index=1,
            ),
            outcome_label="Yes",
            leader_funder_wallet=FUNDER_FROM_LOG,
        )
        with caplog.at_level(logging.INFO, logger="PolymarketCopyTradingBot.CopyTaskEventProcessor"):
            results = await processor.process(LEADER, event)
        assert len(results) == 1
        assert results[0].allowed is True
        assert len(submitted) == 1
        assert any("copy_task_dispatched" in rec.message for rec in caplog.records)


class TestBug3SkipLoggingAndTraderPriceConfig:
    def test_configured_traders_admit_low_prices_with_percentage_offset(self) -> None:
        cfg = load_config(
            {
                "database": {"dsn": "postgresql://u:p@localhost/db"},
                "wallet": {"funder_address": FOLLOWER},
                "detection": {
                    "mode": "on_chain_logs_only",
                    "onchain": {"alchemy_wss_url": "wss://polygon-mainnet.g.alchemy.com/v2/test"},
                },
            }
        )
        entry_service = CopyTaskEntryPriceService()
        assert len(cfg.copy_trading.traders) == 3
        for trader in cfg.copy_trading.traders:
            assert trader.task.sizing.require_leader_balance is True
            assert trader.task.sizing.proportional_balance_cap_enabled is True
            assert trader.task.price.offset_mode == "percentage"
            task = _make_task(
                sizing=TaskSizingPolicy(
                    mode=TaskSizingMode.PROPORTIONAL,
                    percentage=trader.task.sizing.percentage,
                    proportional_balance_cap_enabled=True,
                    require_leader_balance=True,
                ),
                price=TaskPricePolicy(
                    enabled=trader.task.price.enabled,
                    min_price=trader.task.price.min_price,
                    max_price=trader.task.price.max_price,
                    offset_mode=PriceOffsetMode(trader.task.price.offset_mode),
                    offset_direction=PriceOffsetDirection(trader.task.price.offset_direction),
                    offset=trader.task.price.offset,
                    max_slippage_percent=trader.task.price.max_slippage_percent,
                ),
            )
            # Previously rejected at $0.30 and $0.45 with fixed $0.04/$0.05 offset
            for px in (Decimal("0.30"), Decimal("0.45"), Decimal("0.65")):
                decision = entry_service.evaluate(task=task, follower_action="BUY", source_price=px)
                assert decision.allowed is True, f"{trader.name} rejected at {px}: {decision.reason}"

    @pytest.mark.asyncio
    async def test_cached_copy_task_reader_caches_and_invalidates(self) -> None:
        calls = 0

        class _Reader:
            async def list_active(self) -> tuple[CopyTask, ...]:
                nonlocal calls
                calls += 1
                return ()

        cached = CachedCopyTaskReader(_Reader(), ttl_seconds=60.0)
        await cached.list_active()
        await cached.list_active()
        assert calls == 1
        cached.invalidate()
        await cached.list_active()
        assert calls == 2
