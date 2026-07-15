"""PostgreSQL persistence for the single trader-discovery pipeline."""

from __future__ import annotations

import time
import uuid
from collections.abc import Iterable, Mapping, Sequence
from datetime import date
from decimal import Decimal
from typing import Any

import asyncpg
import orjson

from services.discovery.models import Candidate


def _json(value: Any) -> str:
    return orjson.dumps(value, option=orjson.OPT_SORT_KEYS).decode("utf-8")


def _decoded(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, memoryview):
        value = value.tobytes()
    if isinstance(value, (bytes, bytearray, str)):
        try:
            return orjson.loads(value)
        except orjson.JSONDecodeError:
            return default
    return default


class DiscoveryRepo:
    """Batched cycle, score, roster-input, and time-series persistence."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def begin_cycle(self, config_hash: str) -> str:
        cycle_id = str(uuid.uuid4())
        async with self._pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO discovery_cycles
                    (cycle_id, started_at, status, config_hash)
                VALUES ($1, $2, 'running', $3)
                """,
                cycle_id,
                time.time(),
                config_hash,
            )
        return cycle_id

    async def persist_cycle(
        self,
        cycle_id: str,
        candidates: list[Candidate],
        config_hash: str,
    ) -> None:
        if not candidates:
            return
        cycle_ts = time.time()
        snapshot_rows: list[tuple[Any, ...]] = []
        score_rows: list[tuple[Any, ...]] = []
        wallet360_rows: list[tuple[Any, ...]] = []
        metric_rows: list[tuple[Any, ...]] = []
        for candidate in candidates:
            snapshot_rows.append(
                (
                    cycle_id,
                    candidate.wallet,
                    cycle_ts,
                    _json(candidate.hscore),
                    _json(candidate.wallet360),
                    _json(candidate.recent_trades),
                    _json(candidate.market_quality),
                    _json(candidate.social),
                    float(candidate.base_score),
                    candidate.longitudinal_score,
                    float(candidate.score),
                    _json(candidate.sub_scores),
                    candidate.rejected_reason,
                    config_hash,
                )
            )
            for window, payload in candidate.wallet360.items():
                wallet360_rows.append(
                    (
                        candidate.wallet,
                        cycle_ts,
                        str(window),
                        _json(payload),
                        config_hash,
                    )
                )
            if candidate.rejected_reason is not None:
                continue
            score_rows.append(
                (
                    candidate.wallet,
                    cycle_ts,
                    float(candidate.base_score),
                    candidate.longitudinal_score,
                    float(candidate.score),
                    _json(candidate.sub_scores),
                    config_hash,
                )
            )
            metrics = candidate.metrics
            metric_rows.append(
                (
                    candidate.wallet,
                    cycle_ts,
                    float(metrics.get("sharpe", 0.0)),
                    float(metrics.get("roi", 0.0)),
                    float(metrics.get("win_rate", 0.0)),
                    int(metrics.get("rank", 0) or 0),
                    float(metrics.get("drawdown_pct", 0.0)),
                    int(metrics.get("trade_count", 0) or 0),
                    _json(metrics),
                    config_hash,
                )
            )

        async with self._pool.acquire() as connection:
            async with connection.transaction():
                await connection.executemany(
                    """
                    INSERT INTO discovery_candidate_snapshots (
                        cycle_id, wallet, cycle_ts, hscore_json, wallet360_json,
                        recent_trades_json, market_quality_json, social_json,
                        base_score, longitudinal_score, final_score,
                        sub_scores_json, rejected_reason, config_hash
                    ) VALUES (
                        $1, $2, $3, $4::jsonb, $5::jsonb, $6::jsonb, $7::jsonb,
                        $8::jsonb, $9, $10, $11, $12::jsonb, $13, $14
                    )
                    ON CONFLICT (cycle_id, wallet) DO NOTHING
                    """,
                    snapshot_rows,
                )
                if score_rows:
                    await connection.executemany(
                        """
                        INSERT INTO discovery_score_history (
                            wallet, ts, base_score, longitudinal_score,
                            final_score, sub_scores_json, config_hash
                        ) VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)
                        ON CONFLICT (wallet, ts) DO UPDATE SET
                            base_score = excluded.base_score,
                            longitudinal_score = excluded.longitudinal_score,
                            final_score = excluded.final_score,
                            sub_scores_json = excluded.sub_scores_json,
                            config_hash = excluded.config_hash
                        """,
                        score_rows,
                    )
                if wallet360_rows:
                    await connection.executemany(
                        """
                        INSERT INTO wallet360_snapshots (
                            wallet, ts, window_days, payload_json, config_hash
                        ) VALUES ($1, $2, $3, $4::jsonb, $5)
                        ON CONFLICT (wallet, ts, window_days) DO NOTHING
                        """,
                        wallet360_rows,
                    )
                if metric_rows:
                    await connection.executemany(
                        """
                        INSERT INTO wallet_metric_timeseries (
                            wallet, ts, sharpe, roi, win_rate, rank,
                            drawdown_pct, trade_count, metrics_json, config_hash
                        ) VALUES (
                            $1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10
                        )
                        ON CONFLICT (wallet, ts) DO UPDATE SET
                            sharpe = excluded.sharpe,
                            roi = excluded.roi,
                            win_rate = excluded.win_rate,
                            rank = excluded.rank,
                            drawdown_pct = excluded.drawdown_pct,
                            trade_count = excluded.trade_count,
                            metrics_json = excluded.metrics_json,
                            config_hash = excluded.config_hash
                        """,
                        metric_rows,
                    )

    async def complete_cycle(
        self,
        cycle_id: str,
        *,
        status: str,
        candidate_count: int,
        kept_count: int,
        error: str | None = None,
    ) -> None:
        async with self._pool.acquire() as connection:
            await connection.execute(
                """
                UPDATE discovery_cycles
                SET completed_at = $1, status = $2, candidate_count = $3,
                    kept_count = $4, error_detail = $5
                WHERE cycle_id = $6
                """,
                time.time(),
                status,
                int(candidate_count),
                int(kept_count),
                error,
                cycle_id,
            )

    async def score_history(
        self,
        wallet: str,
        *,
        since_ts: float,
    ) -> list[Mapping[str, Any]]:
        async with self._pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT ts, sharpe, roi, win_rate, rank, drawdown_pct,
                       trade_count, metrics_json
                FROM wallet_metric_timeseries
                WHERE wallet = $1 AND ts >= $2
                ORDER BY ts ASC
                """,
                wallet.lower(),
                float(since_ts),
            )
        return [dict(row) for row in rows]

    async def latest_candidates(self) -> list[Candidate]:
        async with self._pool.acquire() as connection:
            rows = await connection.fetch(
                """
                WITH latest_cycle AS (
                    SELECT cycle_id
                    FROM discovery_cycles
                    WHERE status = 'completed'
                    ORDER BY completed_at DESC NULLS LAST
                    LIMIT 1
                )
                SELECT snapshots.*
                FROM discovery_candidate_snapshots AS snapshots
                JOIN latest_cycle USING (cycle_id)
                WHERE snapshots.rejected_reason IS NULL
                ORDER BY snapshots.final_score DESC, snapshots.wallet ASC
                """
            )
        output: list[Candidate] = []
        for row in rows:
            candidate = Candidate(
                wallet=str(row["wallet"]),
                hscore=_decoded(row["hscore_json"], {}),
                wallet360=_decoded(row["wallet360_json"], {}),
                recent_trades=_decoded(row["recent_trades_json"], []),
                market_quality=_decoded(row["market_quality_json"], {}),
                social=_decoded(row["social_json"], {}),
                sub_scores={
                    str(key): float(value)
                    for key, value in _decoded(row["sub_scores_json"], {}).items()
                },
                base_score=float(row["base_score"]),
                longitudinal_score=(
                    float(row["longitudinal_score"])
                    if row["longitudinal_score"] is not None
                    else None
                ),
                score=float(row["final_score"]),
            )
            candidate.metrics = _candidate_metrics(candidate)
            output.append(candidate)
        return output

    async def persist_rescore(
        self,
        candidates: Sequence[Candidate],
        config_hash: str,
    ) -> None:
        if not candidates:
            return
        timestamp = time.time()
        rows = [
            (
                candidate.wallet,
                timestamp,
                float(candidate.base_score),
                candidate.longitudinal_score,
                float(candidate.score),
                _json(candidate.sub_scores),
                config_hash,
            )
            for candidate in candidates
        ]
        async with self._pool.acquire() as connection:
            await connection.executemany(
                """
                INSERT INTO discovery_score_history (
                    wallet, ts, base_score, longitudinal_score, final_score,
                    sub_scores_json, config_hash
                ) VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)
                ON CONFLICT (wallet, ts) DO UPDATE SET
                    longitudinal_score = excluded.longitudinal_score,
                    final_score = excluded.final_score,
                    sub_scores_json = excluded.sub_scores_json,
                    config_hash = excluded.config_hash
                """,
                rows,
            )

    async def list_snapshots(self, *, limit: int = 100) -> list[dict[str, Any]]:
        async with self._pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT cycle_id, wallet, cycle_ts, base_score,
                       longitudinal_score, final_score, rejected_reason,
                       sub_scores_json, config_hash
                FROM discovery_candidate_snapshots
                ORDER BY cycle_ts DESC, final_score DESC
                LIMIT $1
                """,
                max(1, int(limit)),
            )
        return [
            {
                **dict(row),
                "sub_scores": _decoded(row["sub_scores_json"], {}),
            }
            for row in rows
        ]

    async def upsert_pnl_daily(
        self,
        wallet: str,
        rows: Iterable[tuple[date, Decimal, Mapping[str, Any]]],
    ) -> None:
        timestamp = time.time()
        records = [
            (wallet.lower(), pnl_date, pnl, _json(payload), timestamp)
            for pnl_date, pnl, payload in rows
        ]
        if not records:
            return
        async with self._pool.acquire() as connection:
            await connection.executemany(
                """
                INSERT INTO wallet_pnl_daily (
                    wallet, pnl_date, realized_pnl, payload_json, updated_at
                ) VALUES ($1, $2, $3, $4::jsonb, $5)
                ON CONFLICT (wallet, pnl_date) DO UPDATE SET
                    realized_pnl = excluded.realized_pnl,
                    payload_json = excluded.payload_json,
                    updated_at = excluded.updated_at
                """,
                records,
            )

    async def upsert_rank_history(
        self,
        wallet: str,
        rows: Iterable[tuple[float, int, int, float, Mapping[str, Any]]],
    ) -> None:
        records = [
            (wallet.lower(), ts, window, rank, score, _json(payload))
            for ts, window, rank, score, payload in rows
        ]
        if not records:
            return
        async with self._pool.acquire() as connection:
            await connection.executemany(
                """
                INSERT INTO wallet_rank_history (
                    wallet, ts, window_days, rank, score, payload_json
                ) VALUES ($1, $2, $3, $4, $5, $6::jsonb)
                ON CONFLICT (wallet, ts, window_days) DO UPDATE SET
                    rank = excluded.rank,
                    score = excluded.score,
                    payload_json = excluded.payload_json
                """,
                records,
            )

    async def has_metric_history(self, wallet: str) -> bool:
        async with self._pool.acquire() as connection:
            return bool(
                await connection.fetchval(
                    """
                    SELECT 1
                    FROM wallet_metric_timeseries
                    WHERE wallet = $1
                    LIMIT 1
                    """,
                    wallet.lower(),
                )
            )

    async def upsert_metric_timeseries(
        self,
        wallet: str,
        rows: Iterable[Mapping[str, Any]],
        config_hash: str,
    ) -> None:
        records = [
            (
                wallet.lower(),
                float(row["ts"]),
                float(row.get("sharpe", 0.0)),
                float(row.get("roi", 0.0)),
                float(row.get("win_rate", 0.0)),
                int(row.get("rank", 0) or 0),
                float(row.get("drawdown_pct", 0.0)),
                int(row.get("trade_count", 0) or 0),
                _json(dict(row)),
                config_hash,
            )
            for row in rows
        ]
        if not records:
            return
        async with self._pool.acquire() as connection:
            await connection.executemany(
                """
                INSERT INTO wallet_metric_timeseries (
                    wallet, ts, sharpe, roi, win_rate, rank,
                    drawdown_pct, trade_count, metrics_json, config_hash
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10)
                ON CONFLICT (wallet, ts) DO UPDATE SET
                    sharpe = excluded.sharpe,
                    roi = excluded.roi,
                    win_rate = excluded.win_rate,
                    rank = excluded.rank,
                    drawdown_pct = excluded.drawdown_pct,
                    trade_count = excluded.trade_count,
                    metrics_json = excluded.metrics_json,
                    config_hash = excluded.config_hash
                """,
                records,
            )


def _candidate_metrics(candidate: Candidate) -> dict[str, float | int]:
    row = candidate.hscore
    window = candidate.wallet360.get("15", {})
    return {
        "sharpe": _number(row.get("sharpe_15d", row.get("sharpe_ratio_15d", 0.0))),
        "roi": _number(row.get("roi_15d", row.get("roi_pct_15d", 0.0))),
        "win_rate": _number(row.get("win_rate_15d", row.get("win_rate_pct_15d", 0.0))),
        "rank": int(_number(row.get("leaderboard_rank", row.get("rank", 0)))),
        "drawdown_pct": _number(
            window.get("max_drawdown_pct", window.get("max_drawdown", 0.0))
        ),
        "trade_count": int(
            _number(row.get("total_trades_15d", row.get("trade_count_15d", 0)))
        ),
    }


def _number(value: Any) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return 0.0
    return parsed if parsed == parsed and abs(parsed) != float("inf") else 0.0


__all__ = ["DiscoveryRepo"]
