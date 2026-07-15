from __future__ import annotations

import asyncio
import math
import time
from collections import defaultdict
from collections.abc import Mapping
from datetime import date, datetime, time as datetime_time, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from integrations.falcon.client import (
    FalconClient,
    leaderboard_history,
    realized_pnl,
    row_value,
    wallet_trades,
)
from services.discovery.longitudinal import rolling_sharpe
from storage.postgres.discovery_repo import DiscoveryRepo
from utils.logger import get_logger, redact_error_detail

logger = get_logger("DiscoveryBackfill")


class LongitudinalBackfillService:
    """Seed realized-PnL, trade-count, rank, and metric time series once."""

    def __init__(
        self,
        client: FalconClient,
        repository: DiscoveryRepo,
        config: Any,
        config_hash: str,
    ) -> None:
        self.client = client
        self.repository = repository
        self.config = config
        self.config_hash = config_hash
        self._semaphore = asyncio.Semaphore(config.backfill_concurrency)

    async def ensure_wallet(self, wallet: str) -> bool:
        if await self.repository.has_completed_backfill(wallet):
            return False
        async with self._semaphore:
            # Re-check after acquiring: two concurrent funnels may have raced.
            if await self.repository.has_completed_backfill(wallet):
                return False
            try:
                await self._backfill(wallet)
                await self.repository.mark_backfill_complete(
                    wallet,
                    self.config_hash,
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "longitudinal_backfill_failed",
                    extra={
                        "wallet": wallet,
                        "error": redact_error_detail(exc),
                    },
                )
                return False
            return True

    async def _backfill(self, wallet: str) -> None:
        today = datetime.now(timezone.utc).date()
        start = today - timedelta(days=int(self.config.pnl_backfill_days))
        start_epoch = int(
            datetime.combine(start, datetime_time.min, tzinfo=timezone.utc).timestamp()
        )
        end_epoch = int(time.time())
        pnl_rows, trades, ranks = await asyncio.gather(
            realized_pnl(
                self.client,
                wallet,
                start_date=start.isoformat(),
                end_date=today.isoformat(),
                max_pages=self.config.pnl_backfill_max_pages,
            ),
            wallet_trades(
                self.client,
                wallet,
                start_time=start_epoch,
                end_time=end_epoch,
                page_size=200,
                max_pages=self.config.trade_backfill_max_pages,
            ),
            leaderboard_history(
                self.client,
                wallet,
                tuple(self.config.leaderboard_windows),
            ),
        )

        daily_pnl = _daily_pnl(pnl_rows)
        await self.repository.upsert_pnl_daily(
            wallet,
            [
                (pnl_date, amount, payload)
                for pnl_date, (amount, payload) in sorted(daily_pnl.items())
            ],
        )

        now = time.time()
        rank_records: list[tuple[float, int, int, float, Mapping[str, Any]]] = []
        rank_by_window: dict[int, int] = {}
        for window, payload in ranks:
            rank = max(
                0,
                int(_number(row_value(payload, "rank", "leaderboard_rank", default=0))),
            )
            rank_by_window[int(window)] = rank
            rank_records.append(
                (now, int(window), rank, _rank_score(rank), payload)
            )
        await self.repository.upsert_rank_history(wallet, rank_records)

        metrics = _metric_rows(
            daily_pnl=daily_pnl,
            trades=trades,
            fallback_rank=rank_by_window.get(30, rank_by_window.get(7, 0)),
        )
        await self.repository.upsert_metric_timeseries(
            wallet,
            metrics,
            self.config_hash,
        )


def _daily_pnl(
    rows: list[dict[str, Any]],
) -> dict[date, tuple[Decimal, dict[str, Any]]]:
    output: dict[date, tuple[Decimal, dict[str, Any]]] = {}
    for row in rows:
        pnl_date = _date(
            row_value(row, "date", "pnl_date", "day", "timestamp", default="")
        )
        if pnl_date is None:
            continue
        amount = _decimal(
            row_value(
                row,
                "realized_pnl",
                "realizedPnl",
                "pnl",
                "profit",
                default="0",
            )
        )
        previous, _ = output.get(pnl_date, (Decimal("0"), {}))
        output[pnl_date] = (previous + amount, dict(row))
    return output


def _metric_rows(
    *,
    daily_pnl: dict[date, tuple[Decimal, dict[str, Any]]],
    trades: list[dict[str, Any]],
    fallback_rank: int,
) -> list[dict[str, Any]]:
    trades_by_day: defaultdict[date, int] = defaultdict(int)
    notional_by_day: defaultdict[date, float] = defaultdict(float)
    for trade in trades:
        trade_date = _date(
            row_value(
                trade,
                "timestamp",
                "event_ts",
                "created_at",
                "matched_at",
                default="",
            )
        )
        if trade_date is None:
            continue
        trades_by_day[trade_date] += 1
        notional_by_day[trade_date] += abs(
            _number(
                row_value(
                    trade,
                    "usdc_size",
                    "notional",
                    "amount",
                    default=0.0,
                )
            )
        )

    output: list[dict[str, Any]] = []
    pnl_values: list[float] = []
    cumulative_pnl = 0.0
    cumulative_notional = 0.0
    cumulative_trades = 0
    peak = 0.0
    winning_days = 0
    for pnl_date, (amount, payload) in sorted(daily_pnl.items()):
        pnl = float(amount)
        pnl_values.append(pnl)
        cumulative_pnl += pnl
        cumulative_notional += notional_by_day[pnl_date]
        cumulative_trades += trades_by_day[pnl_date]
        winning_days += int(pnl > 0)
        peak = max(peak, cumulative_pnl)
        drawdown = max(0.0, peak - cumulative_pnl)
        raw_rank = int(
            _number(
                row_value(payload, "rank", "leaderboard_rank", default=fallback_rank)
            )
        )
        raw_roi = row_value(payload, "roi", "roi_pct", "roi_15d", default=None)
        roi = (
            _number(raw_roi)
            if raw_roi is not None
            else 100.0 * cumulative_pnl / max(cumulative_notional, 1.0)
        )
        raw_win_rate = row_value(
            payload,
            "win_rate",
            "win_rate_pct",
            default=None,
        )
        win_rate = (
            _rate(raw_win_rate)
            if raw_win_rate is not None
            else winning_days / len(pnl_values)
        )
        output.append(
            {
                "ts": datetime.combine(
                    pnl_date,
                    datetime_time(hour=12),
                    tzinfo=timezone.utc,
                ).timestamp(),
                "sharpe": rolling_sharpe(pnl_values[-30:]),
                "roi": roi,
                "win_rate": win_rate,
                "rank": raw_rank,
                # Realized-only proxy. It intentionally does not claim to
                # measure unrealized open-position drawdown.
                "drawdown_pct": 100.0
                * drawdown
                / max(cumulative_notional, abs(peak), 1.0),
                "trade_count": cumulative_trades,
                "realized_only": True,
            }
        )
    return output


def _date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).date()
    if isinstance(value, date):
        return value
    if isinstance(value, (int, float)):
        timestamp = float(value)
        if timestamp > 1_000_000_000_000:
            timestamp /= 1000.0
        try:
            return datetime.fromtimestamp(timestamp, tz=timezone.utc).date()
        except (OSError, OverflowError, ValueError):
            return None
    text = str(value).strip()
    if not text:
        return None
    try:
        numeric_timestamp = float(text)
    except ValueError:
        pass
    else:
        return _date(numeric_timestamp)
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
        except ValueError:
            return None


def _decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return Decimal("0")


def _number(value: Any) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return 0.0
    return parsed if math.isfinite(parsed) else 0.0


def _rate(value: Any) -> float:
    parsed = _number(value)
    return parsed / 100.0 if parsed > 1.0 else parsed


def _rank_score(rank: int) -> float:
    if rank <= 0:
        return 0.0
    return 1.0 / float(rank)


__all__ = ["LongitudinalBackfillService"]
