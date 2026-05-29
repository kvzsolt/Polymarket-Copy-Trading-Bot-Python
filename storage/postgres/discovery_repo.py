"""Async asyncpg port of storage/sqlite/discovery_repo.py.

Translation notes:
  * jsonb columns (``meta_json``, ``tokens_json``, ``raw_json``,
    ``bids_json``, ``asks_json``, ``context_buckets_json``) require
    explicit ``$N::jsonb`` casts on write. Reads tolerate either
    str-encoded or dict-decoded payloads (codec-dependent) — we
    json.loads strings, pass dicts through.
  * SQLite ``sqlite3.IntegrityError`` on UNIQUE constraint →
    ``asyncpg.exceptions.UniqueViolationError`` — caller-visible
    return value (None) preserved.
  * ``cursor.lastrowid`` → ``INSERT ... RETURNING id`` + ``fetchval``.
  * ``IN (?, ?, ?)`` placeholder expansion → ``= ANY($N::text[])``
    — eliminates the dynamic ``placeholders = ','.join(...)`` pattern.
  * Dynamic WHERE building keeps the same shape but tracks the
    parameter index (``$1``, ``$2``, ...) explicitly via ``len(params)+1``.
  * Boolean columns (``active``, ``resolved``, ``neg_risk``) accept
    Python bool directly (migration uses ``boolean``).
"""

from __future__ import annotations

import json
import time
from decimal import Decimal
from typing import Any

import asyncpg

from domain.discovery_models import (
    ClosedPositionSnapshot,
    ContextBucket,
    Episode,
    EpisodeEvent,
    EpisodeState,
    MarketMeta,
    ObservedTrade,
    OrderbookSnapshot,
    PositionSnapshotRecord,
    TraderRecord,
)
from domain.ports.repositories import AbstractDiscoveryRepository
from services.discovery.candidate_scoring import FalconDiscoveryEvaluation

PositionKey = tuple[str, str, str]


def _decode_jsonb(value: Any) -> Any:
    """asyncpg returns jsonb as a JSON-encoded string by default."""
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    return json.loads(value)


def _ensure_json_str(value: Any) -> str:
    """Ensure a value lands on the wire as a JSON-encoded string,
    suitable for ``$N::jsonb`` casting. Accepts ``str`` (already
    encoded), ``dict``, ``list``, or ``None``.

    cubic P2: ``None`` returns the JSON null literal rather than ``{}``
    so an absent array-typed field doesn't end up stored as the wrong
    shape (an empty object). Empty strings still default to ``{}`` so
    DB columns whose schema expects an object are not surprised by a
    naked null literal where they used to receive ``{}``.
    """
    if value is None:
        return "null"
    if isinstance(value, str):
        return value or "{}"
    return json.dumps(value, default=str)


class DiscoveryRepo(AbstractDiscoveryRepository):
    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    # ------------------------------------------------------------------
    # Traders
    # ------------------------------------------------------------------

    async def upsert_trader(self, t: TraderRecord) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO discovery_traders
                    (wallet, first_seen_ts, last_seen_ts, trade_count, source, meta_json)
                VALUES ($1, $2, $3, $4, $5, $6::jsonb)
                ON CONFLICT(wallet) DO UPDATE SET
                    last_seen_ts = GREATEST(excluded.last_seen_ts, discovery_traders.last_seen_ts),
                    trade_count  = excluded.trade_count,
                    source       = excluded.source,
                    meta_json    = excluded.meta_json
                """,
                t.wallet,
                t.first_seen_ts,
                t.last_seen_ts,
                t.trade_count,
                t.source,
                _ensure_json_str(t.meta),
            )

    async def get_trader(self, wallet: str) -> TraderRecord | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM discovery_traders WHERE wallet = $1", wallet
            )
        if row is None:
            return None
        return TraderRecord(
            wallet=row["wallet"],
            first_seen_ts=row["first_seen_ts"],
            last_seen_ts=row["last_seen_ts"],
            trade_count=row["trade_count"],
            source=row["source"],
            meta=_decode_jsonb(row["meta_json"]) or {},
        )

    async def list_traders(
        self, *, limit: int = 100, offset: int = 0
    ) -> list[TraderRecord]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM discovery_traders ORDER BY last_seen_ts DESC LIMIT $1 OFFSET $2",
                limit,
                offset,
            )
        return [
            TraderRecord(
                wallet=r["wallet"],
                first_seen_ts=r["first_seen_ts"],
                last_seen_ts=r["last_seen_ts"],
                trade_count=r["trade_count"],
                source=r["source"],
                meta=_decode_jsonb(r["meta_json"]) or {},
            )
            for r in rows
        ]

    # ------------------------------------------------------------------
    # Markets
    # ------------------------------------------------------------------

    async def upsert_market(self, m: MarketMeta) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO discovery_markets
                    (condition_id, question, slug, event_slug, category, end_date_ts,
                     active, resolved, neg_risk, yes_token_id, no_token_id,
                     tokens_json, meta_json, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12::jsonb, $13::jsonb, $14)
                ON CONFLICT(condition_id) DO UPDATE SET
                    question     = excluded.question,
                    slug         = excluded.slug,
                    event_slug   = excluded.event_slug,
                    category     = excluded.category,
                    end_date_ts  = excluded.end_date_ts,
                    active       = excluded.active,
                    resolved     = excluded.resolved,
                    neg_risk     = excluded.neg_risk,
                    yes_token_id = excluded.yes_token_id,
                    no_token_id  = excluded.no_token_id,
                    tokens_json  = excluded.tokens_json,
                    meta_json    = excluded.meta_json,
                    updated_at   = excluded.updated_at
                """,
                m.condition_id,
                m.question,
                m.slug,
                m.event_slug,
                m.category,
                m.end_date_ts,
                bool(m.active),
                bool(m.resolved),
                bool(m.neg_risk),
                m.yes_token_id,
                m.no_token_id,
                _ensure_json_str(m.tokens_json),
                _ensure_json_str(m.meta_json),
                int(time.time()),
            )

    async def get_market(self, condition_id: str) -> MarketMeta | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM discovery_markets WHERE condition_id = $1",
                condition_id,
            )
        if row is None:
            return None
        return MarketMeta(
            condition_id=row["condition_id"],
            question=row["question"],
            slug=row["slug"],
            event_slug=row["event_slug"],
            category=row["category"],
            end_date_ts=row["end_date_ts"],
            active=bool(row["active"]),
            resolved=bool(row["resolved"]),
            neg_risk=bool(row["neg_risk"]),
            yes_token_id=row["yes_token_id"],
            no_token_id=row["no_token_id"],
            tokens_json=(
                json.dumps(row["tokens_json"])
                if not isinstance(row["tokens_json"], str)
                else row["tokens_json"]
            ),
            meta_json=(
                json.dumps(row["meta_json"])
                if not isinstance(row["meta_json"], str)
                else row["meta_json"]
            ),
        )

    # ------------------------------------------------------------------
    # Observed Trades
    # ------------------------------------------------------------------

    async def insert_trade(self, t: ObservedTrade) -> int | None:
        try:
            async with self._pool.acquire() as conn:
                new_id = await conn.fetchval(
                    """
                    INSERT INTO discovery_trades
                        (trader_wallet, condition_id, token_id, side, size, price,
                         usdc_size, tx_hash, event_ts, trade_id, fee, outcome, title,
                         market_slug, event_slug, raw_json, source, created_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
                            $14, $15, $16::jsonb, $17, $18)
                    RETURNING id
                    """,
                    t.trader_wallet,
                    t.condition_id,
                    t.token_id,
                    t.side,
                    str(t.size),
                    str(t.price),
                    str(t.usdc_size),
                    t.tx_hash,
                    t.event_ts,
                    t.trade_id,
                    str(t.fee),
                    t.outcome,
                    t.title,
                    t.market_slug,
                    t.event_slug,
                    _ensure_json_str(t.raw_json),
                    t.source,
                    time.time(),
                )
            return int(new_id) if new_id is not None else None
        except asyncpg.exceptions.UniqueViolationError:
            return None

    async def list_trades(
        self,
        trader_wallet: str,
        *,
        condition_id: str | None = None,
        token_id: str | None = None,
        since_ts: int | None = None,
        limit: int = 1000,
    ) -> list[ObservedTrade]:
        clauses = ["trader_wallet = $1"]
        params: list[Any] = [trader_wallet]
        if condition_id:
            params.append(condition_id)
            clauses.append(f"condition_id = ${len(params)}")
        if token_id:
            params.append(token_id)
            clauses.append(f"token_id = ${len(params)}")
        if since_ts is not None:
            params.append(since_ts)
            clauses.append(f"event_ts >= ${len(params)}")
        where = " AND ".join(clauses)
        params.append(limit)
        limit_idx = len(params)

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"SELECT * FROM discovery_trades WHERE {where} "
                f"ORDER BY event_ts ASC, id ASC LIMIT ${limit_idx}",
                *params,
            )
        return [self._row_to_trade(r) for r in rows]

    def _row_to_trade(self, r: asyncpg.Record) -> ObservedTrade:
        return ObservedTrade(
            trader_wallet=r["trader_wallet"],
            condition_id=r["condition_id"],
            token_id=r["token_id"],
            side=r["side"],
            size=Decimal(r["size"]),
            price=Decimal(r["price"]),
            usdc_size=Decimal(r["usdc_size"]),
            tx_hash=r["tx_hash"],
            event_ts=r["event_ts"],
            trade_id=r["trade_id"],
            fee=Decimal(r["fee"]),
            outcome=r["outcome"],
            title=r["title"],
            market_slug=r["market_slug"],
            event_slug=r["event_slug"],
            raw_json=(
                json.dumps(r["raw_json"])
                if not isinstance(r["raw_json"], str)
                else r["raw_json"]
            ),
            source=r["source"],
        )

    # ------------------------------------------------------------------
    # Falcon discovery evaluations
    # ------------------------------------------------------------------

    async def upsert_evaluation(self, e: FalconDiscoveryEvaluation) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO falcon_discovery_evaluations (
                    wallet, status, final_score, falcon_quality_score,
                    copyability_score, recent_state_score, market_fit_score,
                    h_score, roi_15d, win_rate_15d, sharpe_15d, pnl_15d,
                    total_trades_15d, markets_traded_15d, trajectory,
                    total_grouped_signals, meaningful_signals, copyable_signals,
                    copyable_signal_ratio, copyable_notional_ratio,
                    sports_esports_ratio, fast_crypto_signal_ratio,
                    fast_crypto_notional_ratio, pnl_7d, pnl_3d,
                    rejection_reason, eligible_live, eligible_shadow,
                    falcon_raw_json, replay_json, updated_at
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                    $11, $12, $13, $14, $15, $16, $17, $18,
                    $19, $20, $21, $22, $23, $24, $25, $26,
                    $27, $28, $29::jsonb, $30::jsonb, $31
                )
                ON CONFLICT(wallet) DO UPDATE SET
                    status = excluded.status,
                    final_score = excluded.final_score,
                    falcon_quality_score = excluded.falcon_quality_score,
                    copyability_score = excluded.copyability_score,
                    recent_state_score = excluded.recent_state_score,
                    market_fit_score = excluded.market_fit_score,
                    h_score = excluded.h_score,
                    roi_15d = excluded.roi_15d,
                    win_rate_15d = excluded.win_rate_15d,
                    sharpe_15d = excluded.sharpe_15d,
                    pnl_15d = excluded.pnl_15d,
                    total_trades_15d = excluded.total_trades_15d,
                    markets_traded_15d = excluded.markets_traded_15d,
                    trajectory = excluded.trajectory,
                    total_grouped_signals = excluded.total_grouped_signals,
                    meaningful_signals = excluded.meaningful_signals,
                    copyable_signals = excluded.copyable_signals,
                    copyable_signal_ratio = excluded.copyable_signal_ratio,
                    copyable_notional_ratio = excluded.copyable_notional_ratio,
                    sports_esports_ratio = excluded.sports_esports_ratio,
                    fast_crypto_signal_ratio = excluded.fast_crypto_signal_ratio,
                    fast_crypto_notional_ratio = excluded.fast_crypto_notional_ratio,
                    pnl_7d = excluded.pnl_7d,
                    pnl_3d = excluded.pnl_3d,
                    rejection_reason = excluded.rejection_reason,
                    eligible_live = excluded.eligible_live,
                    eligible_shadow = excluded.eligible_shadow,
                    falcon_raw_json = excluded.falcon_raw_json,
                    replay_json = excluded.replay_json,
                    updated_at = excluded.updated_at
                """,
                e.wallet.lower(),
                e.status,
                float(e.final_score),
                float(e.falcon_quality_score),
                float(e.copyability_score),
                float(e.recent_state_score),
                float(e.market_fit_score),
                float(e.h_score),
                float(e.roi_15d),
                float(e.win_rate_15d),
                float(e.sharpe_15d),
                float(e.pnl_15d),
                int(e.total_trades_15d),
                int(e.markets_traded_15d),
                e.trajectory,
                int(e.total_grouped_signals),
                int(e.meaningful_signals),
                int(e.copyable_signals),
                float(e.copyable_signal_ratio),
                float(e.copyable_notional_ratio),
                float(e.sports_esports_ratio),
                float(e.fast_crypto_signal_ratio),
                float(e.fast_crypto_notional_ratio),
                str(e.pnl_7d),
                str(e.pnl_3d),
                e.rejection_reason,
                bool(e.eligible_live),
                bool(e.eligible_shadow),
                _ensure_json_str(e.falcon_raw_json),
                _ensure_json_str(e.replay_json),
                float(e.updated_at),
            )

    async def get_evaluation(self, wallet: str) -> FalconDiscoveryEvaluation | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM falcon_discovery_evaluations WHERE wallet = $1",
                wallet.lower(),
            )
        return self._row_to_evaluation(row) if row is not None else None

    async def list_evaluations(
        self,
        *,
        statuses: list[str] | None = None,
        eligible_live: bool | None = None,
        limit: int = 50,
    ) -> list[FalconDiscoveryEvaluation]:
        clauses: list[str] = []
        params: list[Any] = []
        if statuses:
            params.append(statuses)
            clauses.append(f"status = ANY(${len(params)}::text[])")
        if eligible_live is not None:
            params.append(bool(eligible_live))
            clauses.append(f"eligible_live = ${len(params)}")
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(int(limit))
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT * FROM falcon_discovery_evaluations
                {where}
                ORDER BY final_score DESC, wallet ASC
                LIMIT ${len(params)}
                """,
                *params,
            )
        return [self._row_to_evaluation(r) for r in rows]

    def _row_to_evaluation(self, r: asyncpg.Record) -> FalconDiscoveryEvaluation:
        return FalconDiscoveryEvaluation(
            wallet=str(r["wallet"]),
            status=str(r["status"]),
            final_score=float(r["final_score"] or 0.0),
            falcon_quality_score=float(r["falcon_quality_score"] or 0.0),
            copyability_score=float(r["copyability_score"] or 0.0),
            recent_state_score=float(r["recent_state_score"] or 0.0),
            market_fit_score=float(r["market_fit_score"] or 0.0),
            h_score=float(r["h_score"] or 0.0),
            roi_15d=float(r["roi_15d"] or 0.0),
            win_rate_15d=float(r["win_rate_15d"] or 0.0),
            sharpe_15d=float(r["sharpe_15d"] or 0.0),
            pnl_15d=float(r["pnl_15d"] or 0.0),
            total_trades_15d=int(r["total_trades_15d"] or 0),
            markets_traded_15d=int(r["markets_traded_15d"] or 0),
            trajectory=str(r["trajectory"] or ""),
            total_grouped_signals=int(r["total_grouped_signals"] or 0),
            meaningful_signals=int(r["meaningful_signals"] or 0),
            copyable_signals=int(r["copyable_signals"] or 0),
            copyable_signal_ratio=float(r["copyable_signal_ratio"] or 0.0),
            copyable_notional_ratio=float(r["copyable_notional_ratio"] or 0.0),
            sports_esports_ratio=float(r["sports_esports_ratio"] or 0.0),
            fast_crypto_signal_ratio=float(r["fast_crypto_signal_ratio"] or 0.0),
            fast_crypto_notional_ratio=float(r["fast_crypto_notional_ratio"] or 0.0),
            pnl_7d=Decimal(r["pnl_7d"] or "0"),
            pnl_3d=Decimal(r["pnl_3d"] or "0"),
            rejection_reason=r["rejection_reason"],
            eligible_live=bool(r["eligible_live"]),
            eligible_shadow=bool(r["eligible_shadow"]),
            falcon_raw_json=_decode_jsonb(r["falcon_raw_json"]) or {},
            replay_json=_decode_jsonb(r["replay_json"]) or {},
            updated_at=float(r["updated_at"] or 0.0),
        )

    # ------------------------------------------------------------------
    # Position Snapshots
    # ------------------------------------------------------------------

    async def insert_position_snapshot(
        self,
        s: PositionSnapshotRecord,
        *,
        wallet_role: str = "leader",
    ) -> int | None:
        if wallet_role not in {"leader", "own"}:
            raise ValueError(
                f"wallet_role must be 'leader' or 'own'; got {wallet_role!r}"
            )
        try:
            async with self._pool.acquire() as conn:
                new_id = await conn.fetchval(
                    """
                    INSERT INTO discovery_position_snapshots
                        (trader_wallet, condition_id, token_id, size, avg_price,
                         current_value, current_price, realized_pnl, snapshot_ts,
                         raw_json, wallet_role)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11)
                    RETURNING id
                    """,
                    s.trader_wallet,
                    s.condition_id,
                    s.token_id,
                    str(s.size),
                    str(s.avg_price),
                    str(s.current_value),
                    str(s.current_price),
                    str(s.realized_pnl),
                    s.snapshot_ts,
                    _ensure_json_str(s.raw_json),
                    wallet_role,
                )
            return int(new_id) if new_id is not None else None
        except asyncpg.exceptions.UniqueViolationError:
            return None

    async def latest_position_snapshot(
        self,
        trader_wallet: str,
        condition_id: str,
        *,
        token_id: str | None = None,
    ) -> PositionSnapshotRecord | None:
        clauses = ["trader_wallet = $1", "condition_id = $2"]
        params: list[Any] = [trader_wallet, condition_id]
        if token_id is not None:
            params.append(token_id)
            clauses.append(f"token_id = ${len(params)}")
        where = " AND ".join(clauses)
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT * FROM discovery_position_snapshots
                WHERE {where}
                ORDER BY snapshot_ts DESC LIMIT 1
                """,
                *params,
            )
        if row is None:
            return None
        return self._row_to_position_snapshot(row)

    async def list_latest_position_snapshots(
        self,
        trader_wallet: str,
        *,
        condition_id: str | None = None,
        token_id: str | None = None,
        wallet_role: str | None = None,
    ) -> list[PositionSnapshotRecord]:
        clauses = ["trader_wallet = $1"]
        params: list[Any] = [trader_wallet]
        if condition_id:
            params.append(condition_id)
            clauses.append(f"condition_id = ${len(params)}")
        if token_id:
            params.append(token_id)
            clauses.append(f"token_id = ${len(params)}")
        if wallet_role is not None:
            params.append(wallet_role)
            clauses.append(f"wallet_role = ${len(params)}")
        where = " AND ".join(clauses)

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT snapshots.*
                FROM discovery_position_snapshots AS snapshots
                INNER JOIN (
                    SELECT trader_wallet, condition_id, token_id,
                           MAX(snapshot_ts) AS snapshot_ts
                    FROM discovery_position_snapshots
                    WHERE {where}
                    GROUP BY trader_wallet, condition_id, token_id
                ) AS latest
                    ON snapshots.trader_wallet = latest.trader_wallet
                    AND snapshots.condition_id = latest.condition_id
                    AND snapshots.token_id    = latest.token_id
                    AND snapshots.snapshot_ts = latest.snapshot_ts
                ORDER BY snapshots.condition_id ASC, snapshots.token_id ASC
                """,
                *params,
            )
        return [self._row_to_position_snapshot(row) for row in rows]

    def _row_to_position_snapshot(
        self, row: asyncpg.Record
    ) -> PositionSnapshotRecord:
        return PositionSnapshotRecord(
            trader_wallet=row["trader_wallet"],
            condition_id=row["condition_id"],
            token_id=row["token_id"],
            size=Decimal(row["size"]),
            avg_price=Decimal(row["avg_price"]),
            current_value=Decimal(row["current_value"]),
            current_price=Decimal(row["current_price"]),
            realized_pnl=Decimal(row["realized_pnl"]),
            snapshot_ts=row["snapshot_ts"],
            raw_json=(
                json.dumps(row["raw_json"])
                if not isinstance(row["raw_json"], str)
                else row["raw_json"]
            ),
        )

    # ------------------------------------------------------------------
    # Closed Positions
    # ------------------------------------------------------------------

    async def insert_closed_position(
        self, c: ClosedPositionSnapshot
    ) -> int | None:
        try:
            async with self._pool.acquire() as conn:
                new_id = await conn.fetchval(
                    """
                    INSERT INTO discovery_closed_positions
                        (trader_wallet, condition_id, token_id, entry_size,
                         entry_avg_price, exit_avg_price, realized_pnl,
                         closed_ts, close_reason, raw_json)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb)
                    RETURNING id
                    """,
                    c.trader_wallet,
                    c.condition_id,
                    c.token_id,
                    str(c.entry_size),
                    str(c.entry_avg_price),
                    str(c.exit_avg_price),
                    str(c.realized_pnl),
                    c.closed_ts,
                    c.close_reason,
                    _ensure_json_str(c.raw_json),
                )
            return int(new_id) if new_id is not None else None
        except asyncpg.exceptions.UniqueViolationError:
            return None

    # ------------------------------------------------------------------
    # Orderbook Snapshots
    # ------------------------------------------------------------------

    async def insert_orderbook_snapshot(
        self, o: OrderbookSnapshot
    ) -> int | None:
        try:
            async with self._pool.acquire() as conn:
                new_id = await conn.fetchval(
                    """
                    INSERT INTO discovery_orderbook_snapshots
                        (token_id, condition_id, snapshot_ts, trader_wallet,
                         reference_tx_hash, reference_trade_id, reference_event_ts,
                         bids_json, asks_json, best_bid, best_ask, mid_price, spread)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9::jsonb, $10, $11, $12, $13)
                    RETURNING id
                    """,
                    o.token_id,
                    o.condition_id,
                    o.snapshot_ts,
                    o.trader_wallet,
                    o.reference_tx_hash,
                    o.reference_trade_id,
                    o.reference_event_ts,
                    _ensure_json_str(o.bids_json),
                    _ensure_json_str(o.asks_json),
                    str(o.best_bid) if o.best_bid is not None else None,
                    str(o.best_ask) if o.best_ask is not None else None,
                    str(o.mid_price) if o.mid_price is not None else None,
                    str(o.spread) if o.spread is not None else None,
                )
            return int(new_id) if new_id is not None else None
        except asyncpg.exceptions.UniqueViolationError:
            return None

    async def list_orderbook_snapshots(
        self,
        token_id: str,
        *,
        reference_tx_hash: str | None = None,
        limit: int = 100,
    ) -> list[OrderbookSnapshot]:
        clauses = ["token_id = $1"]
        params: list[Any] = [token_id]
        if reference_tx_hash:
            params.append(reference_tx_hash)
            clauses.append(f"reference_tx_hash = ${len(params)}")
        params.append(limit)
        limit_idx = len(params)
        where = " AND ".join(clauses)

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"SELECT * FROM discovery_orderbook_snapshots WHERE {where} "
                f"ORDER BY snapshot_ts DESC LIMIT ${limit_idx}",
                *params,
            )
        return [
            OrderbookSnapshot(
                token_id=row["token_id"],
                condition_id=row["condition_id"],
                snapshot_ts=row["snapshot_ts"],
                trader_wallet=row["trader_wallet"],
                reference_tx_hash=row["reference_tx_hash"],
                reference_trade_id=row["reference_trade_id"],
                reference_event_ts=row["reference_event_ts"],
                bids_json=(
                    json.dumps(row["bids_json"])
                    if not isinstance(row["bids_json"], str)
                    else row["bids_json"]
                ),
                asks_json=(
                    json.dumps(row["asks_json"])
                    if not isinstance(row["asks_json"], str)
                    else row["asks_json"]
                ),
                best_bid=Decimal(row["best_bid"]) if row["best_bid"] is not None else None,
                best_ask=Decimal(row["best_ask"]) if row["best_ask"] is not None else None,
                mid_price=Decimal(row["mid_price"]) if row["mid_price"] is not None else None,
                spread=Decimal(row["spread"]) if row["spread"] is not None else None,
            )
            for row in rows
        ]

    # ------------------------------------------------------------------
    # Episodes
    # ------------------------------------------------------------------

    async def upsert_episode(self, ep: Episode) -> None:
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    """
                    INSERT INTO discovery_episodes
                        (episode_id, trader_wallet, condition_id, token_id, state,
                         direction, entry_size, entry_avg_price, exit_size,
                         exit_avg_price, realized_pnl, opened_ts, closed_ts,
                         context_buckets_json)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14::jsonb)
                    ON CONFLICT(episode_id) DO UPDATE SET
                        state              = excluded.state,
                        direction          = excluded.direction,
                        entry_size         = excluded.entry_size,
                        entry_avg_price    = excluded.entry_avg_price,
                        exit_size          = excluded.exit_size,
                        exit_avg_price     = excluded.exit_avg_price,
                        realized_pnl       = excluded.realized_pnl,
                        closed_ts          = excluded.closed_ts,
                        context_buckets_json = excluded.context_buckets_json
                    """,
                    ep.episode_id,
                    ep.trader_wallet,
                    ep.condition_id,
                    ep.token_id,
                    ep.state.value,
                    ep.direction,
                    str(ep.entry_size),
                    str(ep.entry_avg_price),
                    str(ep.exit_size),
                    str(ep.exit_avg_price),
                    str(ep.realized_pnl),
                    ep.opened_ts,
                    ep.closed_ts,
                    _ensure_json_str(ep.context_buckets),
                )
                for ev in ep.events:
                    await conn.execute(
                        """
                        INSERT INTO discovery_episode_events
                            (episode_id, trade_id, side, size, price, usdc_size, ts, event_type)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                        ON CONFLICT (episode_id, trade_id) DO NOTHING
                        """,
                        ep.episode_id,
                        ev.trade_id,
                        ev.side,
                        str(ev.size),
                        str(ev.price),
                        str(ev.usdc_size),
                        ev.ts,
                        ev.event_type,
                    )

    async def get_episode(self, episode_id: str) -> Episode | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM discovery_episodes WHERE episode_id = $1",
                episode_id,
            )
            if row is None:
                return None
            events = await conn.fetch(
                "SELECT * FROM discovery_episode_events WHERE episode_id = $1 ORDER BY ts ASC",
                episode_id,
            )
        return self._row_to_episode(row, events)

    async def list_episodes(
        self,
        trader_wallet: str,
        *,
        state: str | None = None,
        condition_id: str | None = None,
        limit: int = 100,
    ) -> list[Episode]:
        clauses = ["trader_wallet = $1"]
        params: list[Any] = [trader_wallet]
        if state:
            params.append(state)
            clauses.append(f"state = ${len(params)}")
        if condition_id:
            params.append(condition_id)
            clauses.append(f"condition_id = ${len(params)}")
        where = " AND ".join(clauses)
        params.append(limit)
        limit_idx = len(params)

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"SELECT * FROM discovery_episodes WHERE {where} "
                f"ORDER BY opened_ts DESC LIMIT ${limit_idx}",
                *params,
            )
            if not rows:
                return []
            ep_ids = [r["episode_id"] for r in rows]
            # IN(?,?,?) → = ANY($1::text[]) — single param, no
            # dynamic placeholder string assembly.
            ev_rows = await conn.fetch(
                "SELECT * FROM discovery_episode_events "
                "WHERE episode_id = ANY($1::text[]) ORDER BY ts ASC",
                ep_ids,
            )
        events_by_ep: dict[str, list] = {}
        for ev in ev_rows:
            events_by_ep.setdefault(ev["episode_id"], []).append(ev)
        return [
            self._row_to_episode(r, events_by_ep.get(r["episode_id"], []))
            for r in rows
        ]

    def _row_to_episode(
        self, row: asyncpg.Record, events: list[asyncpg.Record]
    ) -> Episode:
        return Episode(
            episode_id=row["episode_id"],
            trader_wallet=row["trader_wallet"],
            condition_id=row["condition_id"],
            token_id=row["token_id"],
            state=EpisodeState(row["state"]),
            direction=row["direction"],
            entry_size=Decimal(row["entry_size"]),
            entry_avg_price=Decimal(row["entry_avg_price"]),
            exit_size=Decimal(row["exit_size"]),
            exit_avg_price=Decimal(row["exit_avg_price"]),
            realized_pnl=Decimal(row["realized_pnl"]),
            opened_ts=row["opened_ts"],
            closed_ts=row["closed_ts"],
            context_buckets=_decode_jsonb(row["context_buckets_json"]) or {},
            events=[
                EpisodeEvent(
                    trade_id=e["trade_id"],
                    side=e["side"],
                    size=Decimal(e["size"]),
                    price=Decimal(e["price"]),
                    usdc_size=Decimal(e["usdc_size"]),
                    ts=e["ts"],
                    event_type=e["event_type"],
                )
                for e in events
            ],
        )

    # ------------------------------------------------------------------
    # Context Buckets
    # ------------------------------------------------------------------

    async def set_context_bucket(
        self, entity_type: str, entity_id: str, bucket: ContextBucket
    ) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO discovery_context_buckets
                    (entity_type, entity_id, dimension, bucket, raw_value)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT(entity_type, entity_id, dimension) DO UPDATE SET
                    bucket    = excluded.bucket,
                    raw_value = excluded.raw_value
                """,
                entity_type,
                entity_id,
                bucket.dimension,
                bucket.bucket,
                bucket.raw_value,
            )

    async def get_context_buckets(
        self, entity_type: str, entity_id: str
    ) -> list[ContextBucket]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM discovery_context_buckets "
                "WHERE entity_type = $1 AND entity_id = $2",
                entity_type,
                entity_id,
            )
        return [
            ContextBucket(
                dimension=r["dimension"],
                bucket=r["bucket"],
                raw_value=r["raw_value"],
            )
            for r in rows
        ]
