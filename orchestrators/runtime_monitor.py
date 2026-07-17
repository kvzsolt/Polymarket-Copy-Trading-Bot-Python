from __future__ import annotations

import asyncio
import json
import time
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


async def run_leader_monitor(
    leader_wallet: str,
    contexts: list[TradingContext],
    readiness_tracker: ReadinessTracker,
    config: AppConfig,
    *,
    fetch_interval: float,
    monitored_leader_count: int = 1,
    research_capture_service: Any | None = None,
) -> None:
    if not contexts:
        logger.warning("no_leaders_monitored", extra={"leader_id": leader_wallet, "reason": "no_followers"})
        metrics.inc("no_leaders_monitored_total")
        return

    trade_queue: asyncio.Queue[list[dict]] = asyncio.Queue(
        maxsize=max(1, int(config.polling.trade_queue_max_batches))
    )

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
