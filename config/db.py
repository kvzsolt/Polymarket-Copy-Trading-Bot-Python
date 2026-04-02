from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

from config.env import ENV
from utils.logger import get_logger

logger = get_logger("Database")

_LOCK = threading.Lock()
_INITIALIZED = False
_DB_PATH = Path(ENV.get("BOT_STATE_DB", "data/bot_state.db"))


def _connect() -> sqlite3.Connection:
    _DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(_DB_PATH, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _init_schema() -> None:
    global _INITIALIZED
    if _INITIALIZED:
        return

    with _LOCK:
        if _INITIALIZED:
            return

        with _connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS orders (
                    idempotency_key TEXT PRIMARY KEY,
                    request_json TEXT NOT NULL,
                    response_json TEXT NOT NULL,
                    order_id TEXT,
                    created_at REAL NOT NULL,
                    status TEXT
                );

                CREATE TABLE IF NOT EXISTS fills (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_id TEXT NOT NULL,
                    fill_json TEXT NOT NULL,
                    created_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS leader_cursors (
                    leader_wallet TEXT PRIMARY KEY,
                    last_timestamp REAL NOT NULL,
                    updated_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS discovered_leaders (
                    wallet TEXT PRIMARY KEY,
                    name TEXT,
                    overall_gain REAL,
                    win_amount REAL,
                    loss_amount REAL,
                    win_rate REAL,
                    total_positions INTEGER,
                    active_positions INTEGER,
                    event_count INTEGER,
                    win_count INTEGER,
                    rank INTEGER,
                    total_current_value REAL,
                    tags TEXT,
                    composite_score REAL,
                    updated_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS arb_baskets (
                    basket_id TEXT PRIMARY KEY,
                    condition_id TEXT NOT NULL,
                    state TEXT NOT NULL,
                    data_json TEXT NOT NULL,
                    updated_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS user_positions (
                    wallet_address TEXT NOT NULL,
                    condition_id TEXT NOT NULL,
                    data_json TEXT NOT NULL,
                    updated_at REAL NOT NULL,
                    PRIMARY KEY (wallet_address, condition_id)
                );

                CREATE TABLE IF NOT EXISTS user_activities (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    wallet_address TEXT NOT NULL,
                    external_id TEXT,
                    transaction_hash TEXT,
                    data_json TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS trading_results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    leader_wallet TEXT,
                    transaction_hash TEXT,
                    condition_id TEXT,
                    side TEXT,
                    title TEXT,
                    outcome TEXT,
                    price REAL,
                    size REAL,
                    usdc_size REAL,
                    event_timestamp REAL,
                    result TEXT,
                    reason TEXT,
                    recorded_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS signals (
                    signal_id TEXT PRIMARY KEY,
                    payload_json TEXT NOT NULL,
                    created_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS accounting_fills (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT NOT NULL,
                    order_id TEXT,
                    condition_id TEXT,
                    token_id TEXT,
                    side TEXT,
                    size_tokens TEXT,
                    price TEXT,
                    usdc_value TEXT,
                    leader_id TEXT,
                    category TEXT,
                    reason TEXT,
                    payload_json TEXT,
                    created_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS positions_ledger (
                    user_id TEXT NOT NULL,
                    condition_id TEXT NOT NULL,
                    token_id TEXT NOT NULL,
                    size_tokens TEXT NOT NULL,
                    avg_entry_price TEXT NOT NULL,
                    leader_id TEXT,
                    category TEXT,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    PRIMARY KEY (user_id, condition_id)
                );

                CREATE TABLE IF NOT EXISTS daily_equity_snapshots (
                    user_id TEXT NOT NULL,
                    snapshot_date TEXT NOT NULL,
                    equity_usdc TEXT NOT NULL,
                    free_cash_usdc TEXT NOT NULL,
                    reserved_cash_usdc TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    PRIMARY KEY (user_id, snapshot_date)
                );

                CREATE TABLE IF NOT EXISTS redemptions (
                    user_id TEXT NOT NULL,
                    condition_id TEXT NOT NULL,
                    attempt_id TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    PRIMARY KEY (user_id, condition_id, attempt_id)
                );
                """
            )
            conn.commit()

        _INITIALIZED = True


async def connect_db() -> None:
    _init_schema()
    logger.info(f"SQLite ready at {_DB_PATH}")


def get_client() -> sqlite3.Connection:
    _init_schema()
    return _connect()


def get_database() -> sqlite3.Connection:
    _init_schema()
    return _connect()
