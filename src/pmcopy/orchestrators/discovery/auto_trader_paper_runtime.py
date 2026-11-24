"""Bounded off-path paper observations for durable automatic-trader candidates."""

import asyncio
from collections.abc import Callable, Mapping, Sequence
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal

import asyncpg

from pmcopy.application.copy_trade_service import CopyTradeIdentityBuilder
from pmcopy.application.trading_context import TradingContext
from pmcopy.config.app_config import AppConfig
from pmcopy.domain.auto_trader import AutoTraderScope, TraderStatus
from pmcopy.domain.paper_evaluation import PaperLedger
from pmcopy.integrations.polymarket.ccxt_prediction_gateway import CcxtPredictionGateway
from pmcopy.integrations.polymarket.paper_evaluation_execution import PaperEvaluationExecution
from pmcopy.integrations.polymarket.paper_execution import PaperExecutionGateway
from pmcopy.integrations.polymarket.paper_market_reader import CcxtPaperMarketReader
from pmcopy.orchestrators.discovery.auto_trader_runtime import AutoTraderRuntime
from pmcopy.orchestrators.bootstrap.runtime_bootstrap import UserRuntimeState
from pmcopy.orchestrators.pipeline.trade_pipeline import normalize_signals
from pmcopy.services.copy_tasks.source_freshness import SourceFreshnessPolicy
from pmcopy.services.discovery.auto_trader_performance import LiveCopyPerformanceReader
from pmcopy.storage.postgres.auto_trader_performance import PostgresAutoTraderPerformance
from pmcopy.services.discovery.paper_evaluation import PaperCopyEvaluator, PaperCopyOrder
from pmcopy.services.risk.risk_engine import RiskEngine, policy_from_config
from pmcopy.storage.postgres.paper_evaluation_repository import PostgresPaperLedgerRepository
from pmcopy.utils.logger import get_logger

logger = get_logger("AutoTraderPaperRuntime")


class AutoTraderPaperRuntime:
    """Keep paper I/O off live signal dispatch and invalidate evidence on dropped work."""

    def __init__(
        self,
        *,
        lifecycle: AutoTraderRuntime,
        evaluators: Mapping[AutoTraderScope, PaperCopyEvaluator],
        contexts: Mapping[str, Sequence[TradingContext]],
        seed_funds: Mapping[str, Decimal],
        clock: Callable[[], datetime],
        capacity: int = 256,
        live_results: LiveCopyPerformanceReader | None = None,
    ) -> None:
        """Inject source routing, isolated funds, and bounded worker capacity."""
        self._lifecycle = lifecycle
        self._evaluators = dict(evaluators)
        self._contexts = contexts
        self._seed_funds = dict(seed_funds)
        self._clock = clock
        self._live_results = live_results
        self._queue: asyncio.Queue[tuple[str, dict[str, object]]] = asyncio.Queue(
            maxsize=max(1, capacity)
        )
        self._gap = False
        self._identity = CopyTradeIdentityBuilder()

    @classmethod
    def create(
        cls,
        *,
        lifecycle: AutoTraderRuntime,
        pool: asyncpg.Pool,
        states: Mapping[str, UserRuntimeState],
        config: AppConfig,
        contexts: Mapping[str, Sequence[TradingContext]],
        seed_funds: Mapping[str, Decimal],
        clock: Callable[[], datetime],
    ) -> "AutoTraderPaperRuntime":
        """Compose public CCXT reads and structurally paper-only writes per account."""
        evaluators = {}
        for scope in lifecycle.services:
            state = states[scope.follower_account]
            gateway = state.clob_gateway
            market = (
                gateway.market_data_gateway
                if isinstance(gateway, PaperExecutionGateway)
                else gateway
            )
            if not isinstance(market, CcxtPredictionGateway):
                raise ValueError(
                    "automatic paper evaluation requires the canonical CCXT market gateway"
                )
            evaluators[scope] = PaperCopyEvaluator(
                repository=PostgresPaperLedgerRepository(pool),
                market=CcxtPaperMarketReader(market),
                execution=PaperEvaluationExecution(market),
                risk=RiskEngine(),
                policy=policy_from_config(config),
                clock=clock,
                config=config,
            )
        return cls(
            lifecycle=lifecycle,
            evaluators=evaluators,
            contexts=contexts,
            seed_funds=seed_funds,
            clock=clock,
            live_results=PostgresAutoTraderPerformance(pool),
        )

    @property
    def pending_count(self) -> int:
        """Expose bounded queue depth for tests and operator diagnostics."""
        return self._queue.qsize()

    def offer(self, wallet: str, rows: Sequence[Mapping[str, object]]) -> None:
        """Enqueue without awaiting or growing an unbounded signal buffer."""
        for row in rows:
            try:
                self._queue.put_nowait((wallet.lower(), dict(row)))
            except asyncio.QueueFull:
                self._gap = True
                logger.warning("auto_trader_paper_queue_full")
                break

    async def prepare(self) -> None:
        """Retain inventory while excluding the unobserved offline interval.

        Event receipts remain durable. A flat portfolio can begin a new measured
        window; open portfolios must finish their exits before qualification.
        """
        now = self._clock()
        for scope, evaluator in self._evaluators.items():
            service = self._lifecycle.services[scope]
            roster = await service.repository.load(scope)
            for row in roster.traders:
                ledger = await evaluator.repository.load(scope, row.wallet)
                if ledger is None:
                    continue
                ledger = replace(ledger, evidence_complete=False)
                ledger = self._restart_flat_window(ledger, now)
                event = f"restart:{ledger.revision}:{now.isoformat()}"
                if not await evaluator.repository.save(
                    scope, row.wallet, ledger, event=event, now=now
                ):
                    raise RuntimeError("paper recovery changed concurrently")
                await service.record_performance(
                    scope, row.wallet, ledger.performance(now, event), source="paper"
                )

    @staticmethod
    def _restart_flat_window(ledger: PaperLedger, now: datetime) -> PaperLedger:
        if not ledger.evidence_complete and not ledger.positions:
            return replace(PaperLedger.start(ledger.initial_cash, now), revision=ledger.revision)
        return ledger

    async def run(self) -> None:
        """Process one paper signal at a time with bounded memory and isolated failures."""
        while True:
            wallet, row = await self._queue.get()
            try:
                await self.process(wallet, row)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._gap = True
                logger.warning("auto_trader_paper_failed", extra={"error_type": type(exc).__name__})
            finally:
                self._queue.task_done()

    async def process(self, wallet: str, raw: Mapping[str, object]) -> None:
        """Copy eligible paper signals using the same canonical identity and sizing."""
        now = self._clock()
        for signal, row in normalize_signals([dict(raw)], wallet):
            for context in self._contexts.get(wallet, ()):
                config = context.config
                if config is None:
                    continue
                if signal.side == "BUY" and SourceFreshnessPolicy(
                    config.copy_trading.copy_max_signal_age_seconds
                ).entry_rejection(
                    datetime.fromtimestamp(signal.event_ts, UTC) if signal.event_ts else None, now
                ):
                    continue
                scope = AutoTraderScope(context.user_id, config.execution.trading_mode)
                evaluator = self._evaluators.get(scope)
                if evaluator is None:
                    continue
                cohort = await self._lifecycle.paper_wallets(context.user_id)
                ledger = await evaluator.repository.load(scope, wallet)
                if wallet not in cohort and (
                    signal.side != "SELL" or ledger is None or not ledger.positions
                ):
                    continue
                allocation = self._seed_funds.get(context.user_id, Decimal(0))
                if ledger is None and allocation <= 0:
                    continue
                fraction = Decimal(str(row.get("_leader_sell_fraction", "0")))
                if signal.side == "SELL" and fraction <= 0:
                    if ledger is not None:
                        await self._invalidate(scope, wallet, evaluator, now)
                    continue
                roster = await self._lifecycle.services[scope].repository.load(scope)
                candidate = next((item for item in roster.traders if item.wallet == wallet), None)
                if candidate is None:
                    continue
                order = PaperCopyOrder(
                    scope,
                    wallet,
                    signal.token_id,
                    signal.condition_id,
                    signal.side,
                    context.copy_notional(signal.usdc_size).final_notional_usdc,
                    signal.price,
                    fraction,
                    self._identity.build(context.user_id, wallet, row),
                    allocation,
                    Decimal(str(config.entry_price_drift_pct_for_leader(wallet))),
                    candidate.score,
                    signal.event_ts,
                    signal.usdc_size,
                    transaction_event=self._identity.build(
                        context.user_id,
                        wallet,
                        {
                            key: value
                            for key, value in row.items()
                            if key not in {"log_index", "logIndex", "order_hash", "orderHash"}
                        },
                    ),
                    observation_source="on_chain" if "_source_receipt" in row else "polling",
                )
                updated = await evaluator.evaluate(order)
                if self._gap:
                    await self._invalidate(scope, wallet, evaluator, now)
                    updated = replace(updated, evidence_complete=False)
                await self._lifecycle.services[scope].record_performance(
                    scope,
                    wallet,
                    updated.performance(now, f"paper:{updated.revision}:{order.event}"),
                    source="paper",
                )

    async def evaluate_results(self) -> None:
        """Refresh saved paper valuations independently of Falcon discovery."""
        now = self._clock()
        gap = self._gap
        self._gap = False
        try:
            await self._evaluate_results(now, gap=gap)
        except BaseException:
            self._gap = self._gap or gap
            raise

    async def _evaluate_results(self, now: datetime, *, gap: bool) -> None:
        for scope, evaluator in self._evaluators.items():
            service = self._lifecycle.services[scope]
            roster = await service.repository.load(scope)
            for row in roster.traders:
                if (
                    scope.trading_mode == "live"
                    and row.status is TraderStatus.ACTIVE
                    and self._live_results is not None
                ):
                    try:
                        live = await self._live_results.read(scope, row.wallet, now)
                    except Exception as exc:
                        logger.warning(
                            "auto_trader_live_results_unavailable",
                            extra={"error_type": type(exc).__name__},
                        )
                        live = (
                            replace(
                                row.live,
                                complete=False,
                                observed_at=now,
                                evaluation_id=f"live-unavailable:{now.isoformat()}",
                            )
                            if row.live is not None
                            else None
                        )
                    if live is not None:
                        await service.record_performance(scope, row.wallet, live, source="live")
                ledger = await evaluator.repository.load(scope, row.wallet)
                if ledger is None:
                    continue
                if gap:
                    ledger = replace(ledger, evidence_complete=False)
                else:
                    ledger = self._restart_flat_window(ledger, now)
                try:
                    ledger = await evaluator.mark(ledger)
                except Exception as exc:
                    ledger = replace(ledger, valuation_complete=False)
                    logger.warning(
                        "auto_trader_paper_valuation_failed",
                        extra={"error_type": type(exc).__name__},
                    )
                event = f"valuation:{ledger.revision}:{now.isoformat()}"
                if await evaluator.repository.save(scope, row.wallet, ledger, event=event, now=now):
                    await service.record_performance(
                        scope, row.wallet, ledger.performance(now, event), source="paper"
                    )

    @staticmethod
    async def _invalidate(
        scope: AutoTraderScope, wallet: str, evaluator: PaperCopyEvaluator, now: datetime
    ) -> None:
        ledger = await evaluator.repository.load(scope, wallet)
        if ledger is not None and ledger.evidence_complete:
            await evaluator.repository.save(
                scope,
                wallet,
                replace(ledger, evidence_complete=False),
                event=f"gap:{ledger.revision}",
                now=now,
            )
