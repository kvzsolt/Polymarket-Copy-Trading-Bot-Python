from __future__ import annotations

import time
from dataclasses import dataclass

from services.discovery.ingestion_service import IngestionService
from utils.logger import get_logger

logger = get_logger("PolymarketHistoryBackfill")
POLYMARKET_ACTIVITY_OFFSET_CAP = 3000


@dataclass(frozen=True)
class BackfillResult:
    wallet: str
    inserted: int
    skipped: int
    fetched: int
    earliest_ts: int | None
    latest_ts: int | None
    errors: int = 0


@dataclass(frozen=True)
class _WindowResult:
    inserted: int = 0
    skipped: int = 0
    fetched: int = 0
    errors: int = 0
    earliest_ts: int | None = None
    latest_ts: int | None = None


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
        configured_max_offset = int(self.config.discovery.polymarket_activity_max_offset)
        max_offset = max(0, min(configured_max_offset, POLYMARKET_ACTIVITY_OFFSET_CAP))
        if configured_max_offset > POLYMARKET_ACTIVITY_OFFSET_CAP:
            logger.info(
                "polymarket_history_offset_cap_hit",
                extra={
                    "wallet": wallet,
                    "configured_max_offset": configured_max_offset,
                    "effective_max_offset": max_offset,
                },
            )

        end_ts = now_ts
        while end_ts > earliest_allowed:
            start_ts = max(earliest_allowed, end_ts - window_days * 86400)
            result = await self._backfill_window(
                wallet=wallet,
                start_ts=start_ts,
                end_ts=end_ts,
                max_offset=max_offset,
            )
            total_inserted += result.inserted
            total_skipped += result.skipped
            total_fetched += result.fetched
            total_errors += result.errors
            if result.earliest_ts is not None:
                earliest_seen = (
                    result.earliest_ts
                    if earliest_seen is None
                    else min(earliest_seen, result.earliest_ts)
                )
            if result.latest_ts is not None:
                latest_seen = (
                    result.latest_ts
                    if latest_seen is None
                    else max(latest_seen, result.latest_ts)
                )

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

    async def _backfill_window(
        self,
        *,
        wallet: str,
        start_ts: int,
        end_ts: int,
        max_offset: int,
    ) -> _WindowResult:
        rows: list[dict] = []
        errors = 0
        saturated = False

        offset = 0
        while offset <= max_offset:
            try:
                page = await self.client.fetch_activity_page(
                    wallet=wallet,
                    start_ts=start_ts,
                    end_ts=end_ts,
                    offset=offset,
                    limit=500,
                )
            except Exception as exc:
                errors += 1
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

            rows.extend(page.rows)
            if offset == max_offset and len(page.rows) >= 500:
                logger.info(
                    "polymarket_history_offset_cap_hit",
                    extra={
                        "wallet": wallet,
                        "start_ts": start_ts,
                        "end_ts": end_ts,
                        "offset": offset,
                        "effective_max_offset": max_offset,
                    },
                )
                saturated = True
                break
            if len(page.rows) < 500:
                break
            offset += 500

        min_window_seconds = int(
            getattr(self.config.discovery, "polymarket_history_min_window_seconds", 21600)
            or 21600
        )
        if saturated and (end_ts - start_ts) > min_window_seconds:
            mid_ts = start_ts + ((end_ts - start_ts) // 2)
            logger.info(
                "polymarket_history_window_split",
                extra={
                    "wallet": wallet,
                    "start_ts": start_ts,
                    "end_ts": end_ts,
                    "mid_ts": mid_ts,
                    "max_offset": max_offset,
                },
            )
            left = await self._backfill_window(
                wallet=wallet,
                start_ts=start_ts,
                end_ts=mid_ts,
                max_offset=max_offset,
            )
            right = await self._backfill_window(
                wallet=wallet,
                start_ts=mid_ts + 1,
                end_ts=end_ts,
                max_offset=max_offset,
            )
            return _combine_window_results(left, right, extra_errors=errors)

        if saturated:
            logger.warning(
                "polymarket_history_window_too_dense",
                extra={
                    "wallet": wallet,
                    "start_ts": start_ts,
                    "end_ts": end_ts,
                    "max_offset": max_offset,
                    "rows_kept": len(rows),
                },
            )

        return await self._ingest_window_rows(rows, errors=errors)

    async def _ingest_window_rows(self, rows: list[dict], *, errors: int) -> _WindowResult:
        if not rows:
            return _WindowResult(errors=errors)
        inserted, skipped = await self.ingestion.ingest_activity_batch(rows)
        earliest_seen: int | None = None
        latest_seen: int | None = None
        for row in rows:
            ts = _ts(row.get("timestamp"))
            if ts is not None:
                earliest_seen = ts if earliest_seen is None else min(earliest_seen, ts)
                latest_seen = ts if latest_seen is None else max(latest_seen, ts)
        return _WindowResult(
            inserted=inserted,
            skipped=skipped,
            fetched=len(rows),
            errors=errors,
            earliest_ts=earliest_seen,
            latest_ts=latest_seen,
        )


def _ts(value) -> int | None:
    try:
        ts = int(float(value))
    except (TypeError, ValueError):
        return None
    if ts > 1_000_000_000_000:
        ts = int(ts / 1000)
    return ts


def _combine_window_results(
    left: _WindowResult,
    right: _WindowResult,
    *,
    extra_errors: int = 0,
) -> _WindowResult:
    earliest_values = [v for v in (left.earliest_ts, right.earliest_ts) if v is not None]
    latest_values = [v for v in (left.latest_ts, right.latest_ts) if v is not None]
    return _WindowResult(
        inserted=left.inserted + right.inserted,
        skipped=left.skipped + right.skipped,
        fetched=left.fetched + right.fetched,
        errors=left.errors + right.errors + extra_errors,
        earliest_ts=min(earliest_values) if earliest_values else None,
        latest_ts=max(latest_values) if latest_values else None,
    )
