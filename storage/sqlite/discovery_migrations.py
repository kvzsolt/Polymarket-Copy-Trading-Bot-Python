"""SQLite schema migrations for the leader discovery subsystem."""
from __future__ import annotations

import sqlite3


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {str(r[1]) for r in rows}


def _ensure_column(conn: sqlite3.Connection, table: str, column_def: str) -> None:
    column_name = column_def.split()[0]
    if column_name in _table_columns(conn, table):
        return
    conn.execute(f"ALTER TABLE {table} ADD COLUMN {column_def}")


def _ensure_discovery_traders(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS discovery_traders (
            wallet TEXT PRIMARY KEY,
            first_seen_ts INTEGER NOT NULL,
            last_seen_ts INTEGER NOT NULL,
            trade_count INTEGER NOT NULL DEFAULT 0,
            source TEXT NOT NULL DEFAULT 'activity_api',
            meta_json TEXT NOT NULL DEFAULT '{}'
        )
        """
    )


def _ensure_discovery_markets(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS discovery_markets (
            condition_id TEXT PRIMARY KEY,
            question TEXT NOT NULL DEFAULT '',
            slug TEXT NOT NULL DEFAULT '',
            event_slug TEXT NOT NULL DEFAULT '',
            category TEXT NOT NULL DEFAULT '',
            end_date_ts INTEGER,
            active INTEGER NOT NULL DEFAULT 1,
            resolved INTEGER NOT NULL DEFAULT 0,
            neg_risk INTEGER NOT NULL DEFAULT 0,
            yes_token_id TEXT NOT NULL DEFAULT '',
            no_token_id TEXT NOT NULL DEFAULT '',
            tokens_json TEXT NOT NULL DEFAULT '[]',
            meta_json TEXT NOT NULL DEFAULT '{}',
            updated_at INTEGER NOT NULL
        )
        """
    )
    _ensure_column(conn, "discovery_markets", "event_slug TEXT NOT NULL DEFAULT ''")
    _ensure_column(conn, "discovery_markets", "resolved INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "discovery_markets", "neg_risk INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "discovery_markets", "yes_token_id TEXT NOT NULL DEFAULT ''")
    _ensure_column(conn, "discovery_markets", "no_token_id TEXT NOT NULL DEFAULT ''")


def _ensure_discovery_trades(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS discovery_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trader_wallet TEXT NOT NULL,
            condition_id TEXT NOT NULL,
            token_id TEXT NOT NULL,
            side TEXT NOT NULL,
            size TEXT NOT NULL,
            price TEXT NOT NULL,
            usdc_size TEXT NOT NULL,
            tx_hash TEXT NOT NULL,
            event_ts INTEGER NOT NULL,
            trade_id TEXT NOT NULL DEFAULT '',
            fee TEXT NOT NULL DEFAULT '0',
            outcome TEXT NOT NULL DEFAULT '',
            title TEXT NOT NULL DEFAULT '',
            market_slug TEXT NOT NULL DEFAULT '',
            event_slug TEXT NOT NULL DEFAULT '',
            raw_json TEXT NOT NULL DEFAULT '{}',
            source TEXT NOT NULL DEFAULT 'activity_api',
            created_at REAL NOT NULL,
            UNIQUE(trader_wallet, token_id, tx_hash, trade_id, side, event_ts)
        )
        """
    )
    _ensure_column(conn, "discovery_trades", "outcome TEXT NOT NULL DEFAULT ''")
    _ensure_column(conn, "discovery_trades", "title TEXT NOT NULL DEFAULT ''")
    _ensure_column(conn, "discovery_trades", "market_slug TEXT NOT NULL DEFAULT ''")
    _ensure_column(conn, "discovery_trades", "event_slug TEXT NOT NULL DEFAULT ''")
    _ensure_column(conn, "discovery_trades", "raw_json TEXT NOT NULL DEFAULT '{}'")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_disc_trades_wallet_cond "
        "ON discovery_trades(trader_wallet, condition_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_disc_trades_wallet_ts "
        "ON discovery_trades(trader_wallet, event_ts)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_disc_trades_cond "
        "ON discovery_trades(condition_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_disc_trades_tx "
        "ON discovery_trades(tx_hash)"
    )


def _ensure_discovery_position_snapshots(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS discovery_position_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trader_wallet TEXT NOT NULL,
            condition_id TEXT NOT NULL,
            token_id TEXT NOT NULL,
            size TEXT NOT NULL,
            avg_price TEXT NOT NULL,
            current_value TEXT NOT NULL DEFAULT '0',
            current_price TEXT NOT NULL DEFAULT '0',
            realized_pnl TEXT NOT NULL DEFAULT '0',
            snapshot_ts INTEGER NOT NULL,
            raw_json TEXT NOT NULL DEFAULT '{}',
            UNIQUE(trader_wallet, condition_id, token_id, snapshot_ts)
        )
        """
    )
    _ensure_column(conn, "discovery_position_snapshots", "current_value TEXT NOT NULL DEFAULT '0'")
    _ensure_column(conn, "discovery_position_snapshots", "current_price TEXT NOT NULL DEFAULT '0'")
    _ensure_column(conn, "discovery_position_snapshots", "realized_pnl TEXT NOT NULL DEFAULT '0'")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_disc_pos_snap_wallet "
        "ON discovery_position_snapshots(trader_wallet)"
    )


def _ensure_discovery_closed_positions(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS discovery_closed_positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trader_wallet TEXT NOT NULL,
            condition_id TEXT NOT NULL,
            token_id TEXT NOT NULL,
            entry_size TEXT NOT NULL,
            entry_avg_price TEXT NOT NULL,
            exit_avg_price TEXT NOT NULL,
            realized_pnl TEXT NOT NULL,
            closed_ts INTEGER NOT NULL,
            close_reason TEXT NOT NULL DEFAULT 'trade',
            raw_json TEXT NOT NULL DEFAULT '{}',
            UNIQUE(trader_wallet, condition_id, closed_ts)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_disc_closed_wallet "
        "ON discovery_closed_positions(trader_wallet)"
    )


def _ensure_discovery_orderbook_snapshots(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS discovery_orderbook_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            token_id TEXT NOT NULL,
            condition_id TEXT NOT NULL,
            snapshot_ts INTEGER NOT NULL,
            trader_wallet TEXT NOT NULL DEFAULT '',
            reference_tx_hash TEXT NOT NULL DEFAULT '',
            reference_trade_id TEXT NOT NULL DEFAULT '',
            reference_event_ts INTEGER NOT NULL DEFAULT 0,
            bids_json TEXT NOT NULL DEFAULT '[]',
            asks_json TEXT NOT NULL DEFAULT '[]',
            best_bid TEXT,
            best_ask TEXT,
            mid_price TEXT,
            spread TEXT,
            UNIQUE(token_id, snapshot_ts, reference_tx_hash)
        )
        """
    )
    _ensure_column(conn, "discovery_orderbook_snapshots", "trader_wallet TEXT NOT NULL DEFAULT ''")
    _ensure_column(conn, "discovery_orderbook_snapshots", "reference_tx_hash TEXT NOT NULL DEFAULT ''")
    _ensure_column(conn, "discovery_orderbook_snapshots", "reference_trade_id TEXT NOT NULL DEFAULT ''")
    _ensure_column(conn, "discovery_orderbook_snapshots", "reference_event_ts INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "discovery_orderbook_snapshots", "best_bid TEXT")
    _ensure_column(conn, "discovery_orderbook_snapshots", "best_ask TEXT")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_disc_ob_token "
        "ON discovery_orderbook_snapshots(token_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_disc_ob_ref_tx "
        "ON discovery_orderbook_snapshots(reference_tx_hash)"
    )


def _ensure_discovery_episodes(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS discovery_episodes (
            episode_id TEXT PRIMARY KEY,
            trader_wallet TEXT NOT NULL,
            condition_id TEXT NOT NULL,
            token_id TEXT NOT NULL,
            state TEXT NOT NULL DEFAULT 'open',
            direction TEXT NOT NULL DEFAULT '',
            entry_size TEXT NOT NULL DEFAULT '0',
            entry_avg_price TEXT NOT NULL DEFAULT '0',
            exit_size TEXT NOT NULL DEFAULT '0',
            exit_avg_price TEXT NOT NULL DEFAULT '0',
            realized_pnl TEXT NOT NULL DEFAULT '0',
            opened_ts INTEGER NOT NULL,
            closed_ts INTEGER,
            context_buckets_json TEXT NOT NULL DEFAULT '{}'
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_disc_ep_wallet "
        "ON discovery_episodes(trader_wallet)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_disc_ep_cond "
        "ON discovery_episodes(condition_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_disc_ep_state "
        "ON discovery_episodes(state)"
    )


def _ensure_discovery_episode_events(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS discovery_episode_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            episode_id TEXT NOT NULL REFERENCES discovery_episodes(episode_id),
            trade_id TEXT NOT NULL,
            side TEXT NOT NULL,
            size TEXT NOT NULL,
            price TEXT NOT NULL,
            usdc_size TEXT NOT NULL,
            ts INTEGER NOT NULL,
            event_type TEXT NOT NULL DEFAULT ''
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_disc_ep_ev_episode "
        "ON discovery_episode_events(episode_id)"
    )


def _ensure_discovery_context_buckets(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS discovery_context_buckets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entity_type TEXT NOT NULL,
            entity_id TEXT NOT NULL,
            dimension TEXT NOT NULL,
            bucket TEXT NOT NULL,
            raw_value TEXT NOT NULL DEFAULT '',
            UNIQUE(entity_type, entity_id, dimension)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_disc_ctx_entity "
        "ON discovery_context_buckets(entity_type, entity_id)"
    )


def apply_discovery_migrations(conn: sqlite3.Connection) -> None:
    """Create/migrate all discovery tables.  Idempotent."""
    _ensure_discovery_traders(conn)
    _ensure_discovery_markets(conn)
    _ensure_discovery_trades(conn)
    _ensure_discovery_position_snapshots(conn)
    _ensure_discovery_closed_positions(conn)
    _ensure_discovery_orderbook_snapshots(conn)
    _ensure_discovery_episodes(conn)
    _ensure_discovery_episode_events(conn)
    _ensure_discovery_context_buckets(conn)
    conn.commit()
