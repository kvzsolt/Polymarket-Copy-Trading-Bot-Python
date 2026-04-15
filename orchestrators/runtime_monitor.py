from __future__ import annotations

import asyncio
import time
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


async def run_leader_monitor(
    leader_wallet: str,
    contexts: list[TradingContext],
    readiness_tracker: ReadinessTracker,
    config: AppConfig,
    *,
    fetch_interval: int,
    monitored_leader_count: int = 1,
    research_capture_service: Any | None = None,
) -> None:
    if not contexts:
        logger.warning("no_leaders_monitored", extra={"leader_id": leader_wallet, "reason": "no_followers"})
        await metrics.inc("no_leaders_monitored_total")
        return

    trade_queue: asyncio.Queue[list[dict]] = asyncio.Queue(maxsize=200)

    async def worker() -> None:
        while True:
            trades = await trade_queue.get()
            try:
                now_ts = time.time()
                tasks = [
                    execute_trade_pipeline(context, leader_wallet, trades, now_ts)
                    for context in contexts
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for idx, result in enumerate(results):
                    if isinstance(result, Exception):
                        context = contexts[idx]
                        logger.error(
                            "trade_pipeline_exception",
                            extra={
                                "leader_id": leader_wallet,
                                "user": context.user_id,
                                "error": safe_error_detail(result),
                                "error_type": type(result).__name__,
                            },
                        )
                        await metrics.inc("trade_pipeline_crashes_total")
            finally:
                trade_queue.task_done()

    async def on_trade(trades: list[dict]) -> None:
        await trade_queue.put(trades)

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
