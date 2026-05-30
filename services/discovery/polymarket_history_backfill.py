from __future__ import annotations

import time
from dataclasses import dataclass

from services.discovery.ingestion_service import IngestionService
from utils.logger import get_logger

logger = get_logger("PolymarketHistoryBackfill")


@dataclass(frozen=True)
class BackfillResult:
    wallet: str
    inserted: int
    skipped: int
    fetched: int
    earliest_ts: int | None
    latest_ts: int | None
    errors: int = 0


class PolymarketHistoryBackfill:
    def __init__(self, *, client, ingestion: IngestionService, config):
        self.client = client
        self.ingestion = ingestion
        self.config = config

    async def backfill_wallet(self, wallet: str) -> BackfillResult:
        now_ts = int(time.time())
        lookback_days = int(self.config.discovery.polymarket_history_lookback_days)
        window_days = int(self.config.discovery.polymarket_history_window_days)
        earliest_allowed = now_ts - lookback_days * 86400

        total_inserted = 0
        total_skipped = 0
        total_fetched = 0
        total_errors = 0
        earliest_seen: int | None = None
        latest_seen: int | None = None

        end_ts = now_ts
        while end_ts > earliest_allowed:
            start_ts = max(earliest_allowed, end_ts - window_days * 86400)
            offset = 0
            while offset <= int(self.config.discovery.polymarket_activity_max_offset):
                try:
                    page = await self.client.fetch_activity_page(
                        wallet=wallet,
                        start_ts=start_ts,
                        end_ts=end_ts,
                        offset=offset,
                        limit=500,
                    )
                except Exception as exc:
                    total_errors += 1
                    logger.warning(
                        "polymarket_history_page_failed | wallet=%s start_ts=%s end_ts=%s offset=%s error_type=%s error=%r",
                        wallet,
                        start_ts,
                        end_ts,
                        offset,
                        type(exc).__name__,
                        exc,
                        extra={
                            "wallet": wallet,
                            "start_ts": start_ts,
                            "end_ts": end_ts,
                            "offset": offset,
                            "error_type": type(exc).__name__,
                            "error": repr(exc),
                        },
                    )
                    break
                if not page.rows:
                    break

                inserted, skipped = await self.ingestion.ingest_activity_batch(page.rows)
                total_inserted += inserted
                total_skipped += skipped
                total_fetched += len(page.rows)

                for row in page.rows:
                    ts = _ts(row.get("timestamp"))
                    if ts is not None:
                        earliest_seen = ts if earliest_seen is None else min(earliest_seen, ts)
                        latest_seen = ts if latest_seen is None else max(latest_seen, ts)

                if len(page.rows) < 500:
                    break
                offset += 500

            end_ts = start_ts - 1

        return BackfillResult(
            wallet=wallet.lower(),
            inserted=total_inserted,
            skipped=total_skipped,
            fetched=total_fetched,
            earliest_ts=earliest_seen,
            latest_ts=latest_seen,
            errors=total_errors,
        )


def _ts(value) -> int | None:
    try:
        ts = int(float(value))
    except (TypeError, ValueError):
        return None
    if ts > 1_000_000_000_000:
        ts = int(ts / 1000)
    return ts
