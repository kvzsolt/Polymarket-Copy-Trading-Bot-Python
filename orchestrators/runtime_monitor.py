"""Leader-monitor orchestration with one exclusive copy execution path."""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from hashlib import blake2b
from typing import Any, TYPE_CHECKING

from domain.trading_context import TradingContext
from orchestrators.trade_pipeline import execute_trades as execute_trade_pipeline
from services.metrics import metrics
from services.tradeMonitor import trade_monitor
from utils.logger import get_logger, safe_error_detail

if TYPE_CHECKING:
    from config.app_config import AppConfig
    from services.healthServer import ReadinessTracker

logger = get_logger("RuntimeMonitor")

LegacyExecutor = Callable[
    [TradingContext, str, list[dict[str, Any]], float],
    Awaitable[object],
]
V1BatchSink = Callable[[str, list[dict[str, Any]]], Awaitable[int]]
MonotonicClock = Callable[[], float]


@dataclass(frozen=True, slots=True)
class LeaderDispatchFailure:
    """One follower-specific legacy pipeline failure."""

    context: TradingContext
    error: Exception


class LeaderBatchDispatcher:
    """Select exactly one active copy pipeline for each leader batch."""

    def __init__(
        self,
        *,
        leader: str,
        contexts: Iterable[TradingContext],
        legacy_executor: LegacyExecutor,
        v1_sink: V1BatchSink | None = None,
        clock: MonotonicClock = time.time,
    ) -> None:
        self._leader = str(leader)
        self._contexts = tuple(contexts)
        self._legacy_executor = legacy_executor
        self._v1_sink = v1_sink
        self._clock = clock

    async def dispatch(
        self,
        trades: list[dict[str, Any]],
    ) -> tuple[LeaderDispatchFailure, ...]:
        """Send a batch to v1 or legacy, never both.

        Returns:
            Follower-specific failures from the legacy fan-out. The v1 sink
            owns its own bounded worker error isolation and either accepts the
            batch or raises to the surrounding monitor worker.
        """

        if self._v1_sink is not None:
            await self._v1_sink(self._leader, trades)
            return ()
        now_ts = self._clock()
        results = await asyncio.gather(
            *(
                self._legacy_executor(context, self._leader, trades, now_ts)
                for context in self._contexts
            ),
            return_exceptions=True,
        )
        return tuple(
            LeaderDispatchFailure(context=context, error=result)
            for context, result in zip(self._contexts, results, strict=True)
            if isinstance(result, Exception)
        )


async def run_leader_monitor(
    leader_wallet: str,
    contexts: list[TradingContext],
    readiness_tracker: ReadinessTracker,
    config: AppConfig,
    *,
    fetch_interval: float,
    monitored_leader_count: int = 1,
    research_capture_service: Any | None = None,
    v1_sink: V1BatchSink | None = None,
) -> None:
    """Poll one leader and route each batch to v1 or legacy execution.

    Args:
        leader_wallet: Source wallet monitored by the polling adapter.
        contexts: Legacy per-follower contexts; optional for the v1 path.
        readiness_tracker: Shared process readiness authority.
        config: Immutable application configuration.
        fetch_interval: Poll cadence in seconds.
        monitored_leader_count: Aggregate monitored leader count.
        research_capture_service: Optional read-only research capture service.
        v1_sink: Atomic v1 batch admission callback. When supplied, legacy
            execution is never invoked.
    """

    if not contexts and v1_sink is None:
        logger.warning(
            "no_leaders_monitored", extra={"leader_id": leader_wallet, "reason": "no_followers"}
        )
        metrics.inc("no_leaders_monitored_total")
        return

    trade_queue: asyncio.Queue[list[dict]] = asyncio.Queue(
        maxsize=max(1, int(config.detection.trade_queue_max_batches))
    )
    dispatcher = LeaderBatchDispatcher(
        leader=leader_wallet,
        contexts=contexts,
        legacy_executor=execute_trade_pipeline,
        v1_sink=v1_sink,
    )

    if v1_sink is not None:

        async def on_v1_trade(trades: list[dict]) -> None:
            await dispatcher.dispatch(trades)

        await trade_monitor(
            {
                "leaderWallet": leader_wallet,
                "FETCH_INTERVAL": fetch_interval,
                "MONITORED_LEADER_COUNT": monitored_leader_count,
            },
            on_v1_trade,
            readiness_tracker=readiness_tracker,
            config=config,
            research_capture_service=research_capture_service,
            aggregate_signals=False,
        )
        return

    async def worker() -> None:
        while True:
            trades = await trade_queue.get()
            try:
                failures = await dispatcher.dispatch(trades)
                for failure in failures:
                    logger.error(
                        "trade_pipeline_exception",
                        extra={
                            "leader_id": leader_wallet,
                            "user": failure.context.user_id,
                            "error": safe_error_detail(failure.error),
                            "error_type": type(failure.error).__name__,
                        },
                    )
                    metrics.inc("trade_pipeline_crashes_total")
            finally:
                trade_queue.task_done()
                metrics.set_gauge(
                    "trade_queue_depth",
                    float(trade_queue.qsize()),
                    labels={"leader_id": leader_wallet},
                )

    async def on_trade(trades: list[dict]) -> None:
        batch = trades
        if trade_queue.full():
            oldest = trade_queue.get_nowait()
            trade_queue.task_done()
            batch = _coalesce_trade_batches(oldest, trades)
            metrics.inc(
                "trade_queue_batches_coalesced_total",
                labels={"leader_id": leader_wallet},
            )
        trade_queue.put_nowait(batch)
        metrics.set_gauge(
            "trade_queue_depth",
            float(trade_queue.qsize()),
            labels={"leader_id": leader_wallet},
        )

    worker_task = asyncio.create_task(worker())
    try:
        await trade_monitor(
            {
                "leaderWallet": leader_wallet,
                "FETCH_INTERVAL": fetch_interval,
                "MONITORED_LEADER_COUNT": monitored_leader_count,
            },
            on_trade,
            readiness_tracker=readiness_tracker,
            config=config,
            research_capture_service=research_capture_service,
        )
    finally:
        worker_task.cancel()
        await asyncio.gather(worker_task, return_exceptions=True)


def _stable_row_fingerprint(trade: dict) -> str:
    """Deterministic digest of a whole trade row.

    Used only as the identity of last resort. ``sort_keys`` keeps the digest
    independent of dict ordering and ``default=str`` tolerates Decimal/None
    payload values, so the same row always fingerprints the same way within
    and across runs.
    """
    try:
        canonical = json.dumps(trade, sort_keys=True, default=str)
    except (TypeError, ValueError):
        canonical = repr(sorted(trade.items(), key=lambda item: str(item[0])))
    return blake2b(canonical.encode("utf-8"), digest_size=16).hexdigest()


def _trade_identity(trade: dict) -> tuple[str, str, str, str]:
    tx_hash = str(trade.get("transactionHash") or trade.get("txHash") or "")
    trade_id = str(
        trade.get("tradeId")
        or trade.get("tradeID")
        or trade.get("trade_id")
        or trade.get("id")
        or ""
    )
    if not tx_hash and not trade_id:
        # No venue identity to dedupe on. Keying these rows on (asset, side)
        # alone would collapse two different anonymous fills of the same token
        # into one and silently drop a leader trade. Fall back to the whole
        # row so distinct fills stay distinct while a row echoed by both
        # batches still dedupes to one.
        trade_id = f"row:{_stable_row_fingerprint(trade)}"
    return (
        tx_hash,
        trade_id,
        str(trade.get("asset") or trade.get("tokenID") or ""),
        str(trade.get("side") or "").upper(),
    )


def _coalesce_trade_batches(
    first: list[dict],
    second: list[dict],
) -> list[dict]:
    by_identity: dict[tuple[str, str, str, str], dict] = {}
    for trade in [*first, *second]:
        by_identity[_trade_identity(trade)] = trade
    return sorted(
        by_identity.values(),
        key=lambda trade: float(trade.get("timestamp") or 0),
    )
