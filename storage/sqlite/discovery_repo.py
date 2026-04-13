"""Repository for the leader discovery subsystem.

Follows the existing repo pattern: constructor takes ``db_path``,
each public method opens a connection via ``create_connection()``.
"""
from __future__ import annotations

import json
import time
from decimal import Decimal
from typing import Any

from domain.ports.repositories import AbstractDiscoveryRepository
from storage.sqlite.connection import create_connection

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

PositionKey = tuple[str, str, str]


class DiscoveryRepo(AbstractDiscoveryRepository):
    def __init__(self, db_path: str):
        self.db_path = db_path

    # ------------------------------------------------------------------
    # Traders
    # ------------------------------------------------------------------

    def upsert_trader(self, t: TraderRecord) -> None:
        with create_connection(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO discovery_traders
                    (wallet, first_seen_ts, last_seen_ts, trade_count, source, meta_json)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(wallet) DO UPDATE SET
                    last_seen_ts = MAX(excluded.last_seen_ts, discovery_traders.last_seen_ts),
                    trade_count  = excluded.trade_count,
                    source       = excluded.source,
                    meta_json    = excluded.meta_json
                """,
                (t.wallet, t.first_seen_ts, t.last_seen_ts, t.trade_count,
                 t.source, json.dumps(t.meta, default=str)),
            )
            conn.commit()

    def get_trader(self, wallet: str) -> TraderRecord | None:
        with create_connection(self.db_path) as conn:
            row = conn.execute(
                "SELECT * FROM discovery_traders WHERE wallet = ?", (wallet,)
            ).fetchone()
        if row is None:
            return None
        return TraderRecord(
            wallet=row["wallet"],
            first_seen_ts=row["first_seen_ts"],
            last_seen_ts=row["last_seen_ts"],
            trade_count=row["trade_count"],
            source=row["source"],
            meta=json.loads(row["meta_json"]),
        )

    def list_traders(self, *, limit: int = 100, offset: int = 0) -> list[TraderRecord]:
        with create_connection(self.db_path) as conn:
            rows = conn.execute(
                "SELECT * FROM discovery_traders ORDER BY last_seen_ts DESC LIMIT ? OFFSET ?",
                (limit, offset),
            ).fetchall()
        return [
            TraderRecord(
                wallet=r["wallet"],
                first_seen_ts=r["first_seen_ts"],
                last_seen_ts=r["last_seen_ts"],
                trade_count=r["trade_count"],
                source=r["source"],
                meta=json.loads(r["meta_json"]),
            )
            for r in rows
        ]

    # ------------------------------------------------------------------
    # Markets
    # ------------------------------------------------------------------

    def upsert_market(self, m: MarketMeta) -> None:
        with create_connection(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO discovery_markets
                    (condition_id, question, slug, event_slug, category, end_date_ts,
                     active, resolved, neg_risk, yes_token_id, no_token_id,
                     tokens_json, meta_json, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(condition_id) DO UPDATE SET
                    question    = excluded.question,
                    slug        = excluded.slug,
                    event_slug  = excluded.event_slug,
                    category    = excluded.category,
                    end_date_ts = excluded.end_date_ts,
                    active      = excluded.active,
                    resolved    = excluded.resolved,
                    neg_risk    = excluded.neg_risk,
                    yes_token_id = excluded.yes_token_id,
                    no_token_id  = excluded.no_token_id,
                    tokens_json = excluded.tokens_json,
                    meta_json   = excluded.meta_json,
                    updated_at  = excluded.updated_at
                """,
                (
                    m.condition_id,
                    m.question,
                    m.slug,
                    m.event_slug,
                    m.category,
                    m.end_date_ts,
                    int(m.active),
                    int(m.resolved),
                    int(m.neg_risk),
                    m.yes_token_id,
                    m.no_token_id,
                    m.tokens_json,
                    m.meta_json,
                    int(time.time()),
                ),
            )
            conn.commit()

    def get_market(self, condition_id: str) -> MarketMeta | None:
        with create_connection(self.db_path) as conn:
            row = conn.execute(
                "SELECT * FROM discovery_markets WHERE condition_id = ?",
                (condition_id,),
            ).fetchone()
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
            tokens_json=row["tokens_json"],
            meta_json=row["meta_json"],
        )

    # ------------------------------------------------------------------
    # Observed Trades
    # ------------------------------------------------------------------

    def insert_trade(self, t: ObservedTrade) -> int | None:
        """Insert a trade.  Returns rowid, or None on duplicate."""
        with create_connection(self.db_path) as conn:
            try:
                cur = conn.execute(
                    """
                    INSERT INTO discovery_trades
                        (trader_wallet, condition_id, token_id, side, size, price,
                         usdc_size, tx_hash, event_ts, trade_id, fee, outcome, title,
                         market_slug, event_slug, raw_json, source, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
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
                        t.raw_json,
                        t.source,
                        time.time(),
                    ),
                )
                conn.commit()
                return int(cur.lastrowid)
            except Exception:
                # UNIQUE constraint → duplicate
                return None

    def list_trades(
        self,
        trader_wallet: str,
        *,
        condition_id: str | None = None,
        token_id: str | None = None,
        since_ts: int | None = None,
        limit: int = 1000,
    ) -> list[ObservedTrade]:
        clauses = ["trader_wallet = ?"]
        params: list[Any] = [trader_wallet]
        if condition_id:
            clauses.append("condition_id = ?")
            params.append(condition_id)
        if token_id:
            clauses.append("token_id = ?")
            params.append(token_id)
        if since_ts is not None:
            clauses.append("event_ts >= ?")
            params.append(since_ts)
        where = " AND ".join(clauses)
        params.append(limit)

        with create_connection(self.db_path) as conn:
            rows = conn.execute(
                f"SELECT * FROM discovery_trades WHERE {where} "
                "ORDER BY event_ts ASC, id ASC LIMIT ?",
                params,
            ).fetchall()
        return [self._row_to_trade(r) for r in rows]

    def _row_to_trade(self, r: Any) -> ObservedTrade:
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
            raw_json=r["raw_json"],
            source=r["source"],
        )

    # ------------------------------------------------------------------
    # Position Snapshots
    # ------------------------------------------------------------------

    def insert_position_snapshot(self, s: PositionSnapshotRecord) -> int | None:
        with create_connection(self.db_path) as conn:
            try:
                cur = conn.execute(
                    """
                    INSERT INTO discovery_position_snapshots
                        (trader_wallet, condition_id, token_id, size, avg_price,
                         current_value, current_price, realized_pnl, snapshot_ts, raw_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (s.trader_wallet, s.condition_id, s.token_id,
                     str(s.size), str(s.avg_price), str(s.current_value),
                     str(s.current_price), str(s.realized_pnl), s.snapshot_ts, s.raw_json),
                )
                conn.commit()
                return int(cur.lastrowid)
            except Exception:
                return None

    def latest_position_snapshot(
        self, trader_wallet: str, condition_id: str, *, token_id: str | None = None
    ) -> PositionSnapshotRecord | None:
        clauses = ["trader_wallet = ?", "condition_id = ?"]
        params: list[Any] = [trader_wallet, condition_id]
        if token_id is not None:
            clauses.append("token_id = ?")
            params.append(token_id)
        where = " AND ".join(clauses)
        with create_connection(self.db_path) as conn:
            row = conn.execute(
                f"""
                SELECT * FROM discovery_position_snapshots
                WHERE {where}
                ORDER BY snapshot_ts DESC LIMIT 1
                """,
                params,
            ).fetchone()
        if row is None:
            return None
        return self._row_to_position_snapshot(row)

    def list_latest_position_snapshots(
        self,
        trader_wallet: str,
        *,
        condition_id: str | None = None,
        token_id: str | None = None,
    ) -> list[PositionSnapshotRecord]:
        clauses = ["trader_wallet = ?"]
        params: list[Any] = [trader_wallet]
        if condition_id:
            clauses.append("condition_id = ?")
            params.append(condition_id)
        if token_id:
            clauses.append("token_id = ?")
            params.append(token_id)
        where = " AND ".join(clauses)

        with create_connection(self.db_path) as conn:
            rows = conn.execute(
                f"""
                SELECT snapshots.*
                FROM discovery_position_snapshots AS snapshots
                INNER JOIN (
                    SELECT trader_wallet, condition_id, token_id, MAX(snapshot_ts) AS snapshot_ts
                    FROM discovery_position_snapshots
                    WHERE {where}
                    GROUP BY trader_wallet, condition_id, token_id
                ) AS latest
                    ON snapshots.trader_wallet = latest.trader_wallet
                    AND snapshots.condition_id = latest.condition_id
                    AND snapshots.token_id = latest.token_id
                    AND snapshots.snapshot_ts = latest.snapshot_ts
                ORDER BY snapshots.condition_id ASC, snapshots.token_id ASC
                """,
                params,
            ).fetchall()
        return [self._row_to_position_snapshot(row) for row in rows]

    def _row_to_position_snapshot(self, row: Any) -> PositionSnapshotRecord:
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
            raw_json=row["raw_json"],
        )

    # ------------------------------------------------------------------
    # Closed Positions
    # ------------------------------------------------------------------

    def insert_closed_position(self, c: ClosedPositionSnapshot) -> int | None:
        with create_connection(self.db_path) as conn:
            try:
                cur = conn.execute(
                    """
                    INSERT INTO discovery_closed_positions
                        (trader_wallet, condition_id, token_id, entry_size,
                         entry_avg_price, exit_avg_price, realized_pnl,
                         closed_ts, close_reason, raw_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (c.trader_wallet, c.condition_id, c.token_id,
                     str(c.entry_size), str(c.entry_avg_price),
                     str(c.exit_avg_price), str(c.realized_pnl),
                     c.closed_ts, c.close_reason, c.raw_json),
                )
                conn.commit()
                return int(cur.lastrowid)
            except Exception:
                return None

    # ------------------------------------------------------------------
    # Orderbook Snapshots  (schema-ready, wiring TBD)
    # ------------------------------------------------------------------

    def insert_orderbook_snapshot(self, o: OrderbookSnapshot) -> int | None:
        with create_connection(self.db_path) as conn:
            try:
                cur = conn.execute(
                    """
                    INSERT INTO discovery_orderbook_snapshots
                        (token_id, condition_id, snapshot_ts, trader_wallet,
                         reference_tx_hash, reference_trade_id, reference_event_ts,
                         bids_json, asks_json, best_bid, best_ask, mid_price, spread)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (o.token_id, o.condition_id, o.snapshot_ts, o.trader_wallet,
                     o.reference_tx_hash, o.reference_trade_id, o.reference_event_ts,
                     o.bids_json, o.asks_json,
                     str(o.best_bid) if o.best_bid is not None else None,
                     str(o.best_ask) if o.best_ask is not None else None,
                     str(o.mid_price) if o.mid_price is not None else None,
                     str(o.spread) if o.spread is not None else None),
                )
                conn.commit()
                return int(cur.lastrowid)
            except Exception:
                return None

    def list_orderbook_snapshots(
        self,
        token_id: str,
        *,
        reference_tx_hash: str | None = None,
        limit: int = 100,
    ) -> list[OrderbookSnapshot]:
        clauses = ["token_id = ?"]
        params: list[Any] = [token_id]
        if reference_tx_hash:
            clauses.append("reference_tx_hash = ?")
            params.append(reference_tx_hash)
        params.append(limit)
        where = " AND ".join(clauses)
        with create_connection(self.db_path) as conn:
            rows = conn.execute(
                f"SELECT * FROM discovery_orderbook_snapshots WHERE {where} "
                "ORDER BY snapshot_ts DESC LIMIT ?",
                params,
            ).fetchall()
        return [
            OrderbookSnapshot(
                token_id=row["token_id"],
                condition_id=row["condition_id"],
                snapshot_ts=row["snapshot_ts"],
                trader_wallet=row["trader_wallet"],
                reference_tx_hash=row["reference_tx_hash"],
                reference_trade_id=row["reference_trade_id"],
                reference_event_ts=row["reference_event_ts"],
                bids_json=row["bids_json"],
                asks_json=row["asks_json"],
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

    def upsert_episode(self, ep: Episode) -> None:
        with create_connection(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO discovery_episodes
                    (episode_id, trader_wallet, condition_id, token_id, state,
                     direction, entry_size, entry_avg_price, exit_size,
                     exit_avg_price, realized_pnl, opened_ts, closed_ts,
                     context_buckets_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(episode_id) DO UPDATE SET
                    state             = excluded.state,
                    direction         = excluded.direction,
                    entry_size        = excluded.entry_size,
                    entry_avg_price   = excluded.entry_avg_price,
                    exit_size         = excluded.exit_size,
                    exit_avg_price    = excluded.exit_avg_price,
                    realized_pnl      = excluded.realized_pnl,
                    closed_ts         = excluded.closed_ts,
                    context_buckets_json = excluded.context_buckets_json
                """,
                (ep.episode_id, ep.trader_wallet, ep.condition_id, ep.token_id,
                 ep.state.value, ep.direction,
                 str(ep.entry_size), str(ep.entry_avg_price),
                 str(ep.exit_size), str(ep.exit_avg_price),
                 str(ep.realized_pnl), ep.opened_ts, ep.closed_ts,
                 json.dumps(ep.context_buckets, default=str)),
            )
            # Upsert events
            for ev in ep.events:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO discovery_episode_events
                        (episode_id, trade_id, side, size, price, usdc_size, ts, event_type)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (ep.episode_id, ev.trade_id, ev.side, str(ev.size),
                     str(ev.price), str(ev.usdc_size), ev.ts, ev.event_type),
                )
            conn.commit()

    def get_episode(self, episode_id: str) -> Episode | None:
        with create_connection(self.db_path) as conn:
            row = conn.execute(
                "SELECT * FROM discovery_episodes WHERE episode_id = ?",
                (episode_id,),
            ).fetchone()
            if row is None:
                return None
            events = conn.execute(
                "SELECT * FROM discovery_episode_events WHERE episode_id = ? ORDER BY ts ASC",
                (episode_id,),
            ).fetchall()
        return self._row_to_episode(row, events)

    def list_episodes(
        self,
        trader_wallet: str,
        *,
        state: str | None = None,
        condition_id: str | None = None,
        limit: int = 100,
    ) -> list[Episode]:
        clauses = ["trader_wallet = ?"]
        params: list[Any] = [trader_wallet]
        if state:
            clauses.append("state = ?")
            params.append(state)
        if condition_id:
            clauses.append("condition_id = ?")
            params.append(condition_id)
        where = " AND ".join(clauses)
        params.append(limit)

        with create_connection(self.db_path) as conn:
            rows = conn.execute(
                f"SELECT * FROM discovery_episodes WHERE {where} "
                "ORDER BY opened_ts DESC LIMIT ?",
                params,
            ).fetchall()
            if not rows:
                return []
            ep_ids = [r["episode_id"] for r in rows]
            placeholders = ",".join("?" for _ in ep_ids)
            ev_rows = conn.execute(
                f"SELECT * FROM discovery_episode_events "
                f"WHERE episode_id IN ({placeholders}) ORDER BY ts ASC",
                ep_ids,
            ).fetchall()
        events_by_ep: dict[str, list] = {}
        for ev in ev_rows:
            events_by_ep.setdefault(ev["episode_id"], []).append(ev)
        return [self._row_to_episode(r, events_by_ep.get(r["episode_id"], []))
                for r in rows]

    def _row_to_episode(self, row: Any, events: list) -> Episode:
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
            context_buckets=json.loads(row["context_buckets_json"]),
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

    def set_context_bucket(
        self, entity_type: str, entity_id: str, bucket: ContextBucket
    ) -> None:
        with create_connection(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO discovery_context_buckets
                    (entity_type, entity_id, dimension, bucket, raw_value)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(entity_type, entity_id, dimension) DO UPDATE SET
                    bucket    = excluded.bucket,
                    raw_value = excluded.raw_value
                """,
                (entity_type, entity_id, bucket.dimension, bucket.bucket, bucket.raw_value),
            )
            conn.commit()

    def get_context_buckets(
        self, entity_type: str, entity_id: str
    ) -> list[ContextBucket]:
        with create_connection(self.db_path) as conn:
            rows = conn.execute(
                "SELECT * FROM discovery_context_buckets "
                "WHERE entity_type = ? AND entity_id = ?",
                (entity_type, entity_id),
            ).fetchall()
        return [
            ContextBucket(
                dimension=r["dimension"],
                bucket=r["bucket"],
                raw_value=r["raw_value"],
            )
            for r in rows
        ]
