from __future__ import annotations

import sqlite3

from storage.sqlite.discovery_migrations import apply_discovery_migrations


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {str(r[1]) for r in rows}


def _exec_each(conn: sqlite3.Connection, script: str) -> None:
    """Run each ``;``-terminated statement in *script* via ``conn.execute``.

    ``conn.executescript`` issues an implicit COMMIT before executing,
    which breaks any outer transaction (e.g. the BEGIN IMMEDIATE wrapping
    ``apply_migrations``). Splitting and dispatching per-statement keeps
    the outer transaction intact so the whole migration is atomic.
    """
    # Naive ``;`` split is safe for our DDL — no embedded semicolons in
    # string literals here. If that ever changes, switch to a real parser.
    for stmt in script.split(";"):
        if stmt.strip():
            conn.execute(stmt)


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table,),
    ).fetchone()
    return row is not None


def _ensure_column(conn: sqlite3.Connection, table: str, column_def: str) -> None:
    column_name = column_def.split()[0]
    if column_name in _table_columns(conn, table):
        return
    conn.execute(f"ALTER TABLE {table} ADD COLUMN {column_def}")


def _ensure_orders_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS orders (
            idempotency_key TEXT PRIMARY KEY,
            user_id TEXT NOT NULL DEFAULT '',
            leader_id TEXT,
            order_id TEXT,
            status TEXT NOT NULL DEFAULT 'created',
            request_json TEXT NOT NULL DEFAULT '{}',
            response_json TEXT,
            attempt_count INTEGER NOT NULL DEFAULT 1,
            last_error TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    _ensure_column(conn, "orders", "user_id TEXT NOT NULL DEFAULT ''")
    _ensure_column(conn, "orders", "leader_id TEXT")
    _ensure_column(conn, "orders", "status TEXT NOT NULL DEFAULT 'created'")
    _ensure_column(conn, "orders", "attempt_count INTEGER NOT NULL DEFAULT 1")
    _ensure_column(conn, "orders", "last_error TEXT")
    _ensure_column(conn, "orders", "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
    _ensure_column(conn, "orders", "matched_total TEXT DEFAULT '0'")  # issue-71: reconciler delta tracking

    # Reconciliation joins by venue order_id every cycle. Without this
    # partial unique index the lookup full-scans the orders table and a
    # duplicate order_id can land silently (wrong-row read on the
    # ORDER BY updated_at DESC LIMIT 1 query). Partial WHERE order_id IS
    # NOT NULL because rows in pending/INTENT_PENDING state carry NULL
    # until the venue ack arrives.
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_order_id "
        "ON orders(order_id) WHERE order_id IS NOT NULL"
    )

    # Backfill user/leader from request payload where possible.
    conn.execute(
        """
        UPDATE orders
        SET
            user_id = CASE
                WHEN COALESCE(user_id, '') = '' THEN COALESCE(json_extract(request_json, '$.user_id'), '')
                ELSE user_id
            END,
            leader_id = CASE
                WHEN COALESCE(leader_id, '') = '' THEN COALESCE(json_extract(request_json, '$.leader_id'), json_extract(request_json, '$.leaderWallet'))
                ELSE leader_id
            END,
            status = COALESCE(status, json_extract(response_json, '$.status'), 'created'),
            updated_at = COALESCE(updated_at, CURRENT_TIMESTAMP)
        """
    )



def _rebuild_legacy_fills_table(conn: sqlite3.Connection) -> None:
    info = conn.execute("PRAGMA table_info(fills)").fetchall()
    if not info:
        return

    columns = {str(row[1]): row for row in info}
    order_id_is_not_null = bool(columns.get("order_id") and int(columns["order_id"][3]))
    fill_json_is_not_null = bool(columns.get("fill_json") and int(columns["fill_json"][3]))
    created_at_is_not_null = bool(columns.get("created_at") and int(columns["created_at"][3]))

    legacy_layout = {
        "id",
        "order_id",
        "fill_json",
        "created_at",
    }.issubset(columns.keys())

    if not legacy_layout and not order_id_is_not_null and not fill_json_is_not_null and not created_at_is_not_null:
        return

    conn.execute("ALTER TABLE fills RENAME TO fills_legacy")
    conn.execute(
        """
        CREATE TABLE fills (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT,
            leader_id TEXT,
            order_id TEXT,
            condition_id TEXT,
            token_id TEXT,
            side TEXT,
            size_tokens TEXT,
            price TEXT,
            fee TEXT,
            fill_ts INTEGER,
            fill_json TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    def _legacy_expr(column: str, *, default_sql: str = "NULL") -> str:
        return column if column in columns else default_sql

    conn.execute(
        f"""
        INSERT INTO fills (
            id, user_id, leader_id, order_id, condition_id, token_id, side,
            size_tokens, price, fee, fill_ts, fill_json, created_at
        )
        SELECT
            {_legacy_expr('id')},
            {_legacy_expr('user_id')},
            {_legacy_expr('leader_id')},
            {_legacy_expr('order_id')},
            {_legacy_expr('condition_id')},
            {_legacy_expr('token_id')},
            {_legacy_expr('side')},
            {_legacy_expr('size_tokens')},
            {_legacy_expr('price')},
            {_legacy_expr('fee')},
            {_legacy_expr('fill_ts')},
            {_legacy_expr('fill_json')},
            {_legacy_expr('created_at', default_sql='CURRENT_TIMESTAMP')}
        FROM fills_legacy
        """
    )
    conn.execute("DROP TABLE fills_legacy")


def _ensure_fills_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fills (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT,
            leader_id TEXT,
            order_id TEXT,
            condition_id TEXT,
            token_id TEXT,
            side TEXT,
            size_tokens TEXT,
            price TEXT,
            fee TEXT,
            fill_ts INTEGER,
            fill_json TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    _rebuild_legacy_fills_table(conn)

    _ensure_column(conn, "fills", "user_id TEXT")
    _ensure_column(conn, "fills", "leader_id TEXT")
    _ensure_column(conn, "fills", "condition_id TEXT")
    _ensure_column(conn, "fills", "token_id TEXT")
    _ensure_column(conn, "fills", "side TEXT")
    _ensure_column(conn, "fills", "size_tokens TEXT")
    _ensure_column(conn, "fills", "price TEXT")
    _ensure_column(conn, "fills", "fee TEXT")
    _ensure_column(conn, "fills", "fill_ts INTEGER")
    _ensure_column(conn, "fills", "fill_json TEXT")

    conn.execute("CREATE INDEX IF NOT EXISTS idx_fills_user_condition ON fills(user_id, condition_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_fills_order ON fills(order_id)")

    # Uniqueness: prevent duplicate fill records for the same user/order/condition/side/timestamp
    # Dedup existing rows first to avoid bricking upgrades
    conn.execute("""
        DELETE FROM fills
        WHERE rowid NOT IN (
            SELECT MIN(rowid)
            FROM fills
            WHERE order_id IS NOT NULL
            GROUP BY user_id, order_id, condition_id, side, fill_ts
        )
          AND order_id IS NOT NULL
    """)
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_fills_unique ON fills(user_id, order_id, condition_id, side, fill_ts)")

    # Migrate legacy retry_pending_<n> and retryable_failed_<n> statuses:
    # 1. Extract embedded counter into retry_attempt column
    # 2. Normalize status to clean "retryable_failed" (no counter suffix)
    try:
        # Handle retryable_failed_<n> rows
        conn.execute("""
            UPDATE processed_trades
            SET retry_attempt = CAST(REPLACE(status, 'retryable_failed_', '') AS INTEGER),
                status = 'retryable_failed'
            WHERE status LIKE 'retryable_failed_%'
        """)
        # Handle legacy retry_pending_<n> rows
        conn.execute("""
            UPDATE processed_trades
            SET retry_attempt = CAST(REPLACE(status, 'retry_pending_', '') AS INTEGER),
                status = 'retryable_failed'
            WHERE status LIKE 'retry_pending_%'
        """)
    except Exception:
        pass  # Table/column may not exist yet


def _positions_pk_columns(conn: sqlite3.Connection) -> list[str]:
    """Return the primary-key column names of the positions table (empty if absent)."""
    if not _table_exists(conn, "positions"):
        return []
    # sqlite_master sql may be None for internal tables; fall back to PRAGMA.
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='positions' LIMIT 1"
    ).fetchone()
    if row and row[0]:
        sql = str(row[0]).upper()
        if "PRIMARY KEY (USER_ID, CONDITION_ID, TOKEN_ID)" in sql:
            return ["user_id", "condition_id", "token_id"]
        if "PRIMARY KEY (USER_ID, CONDITION_ID)" in sql:
            return ["user_id", "condition_id"]
    # Fallback: inspect index_list / table_info
    pks: list[str] = []
    for col in conn.execute("PRAGMA table_info(positions)").fetchall():
        if col[5]:  # pk flag
            pks.append(str(col[1]))
    return pks


def _ensure_positions_table(conn: sqlite3.Connection) -> None:
    pk_cols = _positions_pk_columns(conn)

    if pk_cols == ["user_id", "condition_id", "token_id"]:
        # Already on the new 3-column PK — nothing to do.
        return

    if pk_cols == ["user_id", "condition_id"]:
        # Migrate: rename old table, create with correct PK, copy data over.
        conn.execute("DROP TABLE IF EXISTS _positions_legacy")
        conn.execute("ALTER TABLE positions RENAME TO _positions_legacy")
        conn.execute(
            """
            CREATE TABLE positions (
                user_id TEXT NOT NULL,
                condition_id TEXT NOT NULL,
                token_id TEXT NOT NULL,
                leader_id TEXT,
                size_tokens TEXT NOT NULL,
                avg_entry_price TEXT NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (user_id, condition_id, token_id)
            )
            """
        )
        conn.execute(
            """
            INSERT OR IGNORE INTO positions (user_id, condition_id, token_id, leader_id, size_tokens, avg_entry_price, updated_at)
            SELECT user_id, condition_id, token_id, leader_id, size_tokens, avg_entry_price, updated_at
            FROM _positions_legacy
            """
        )
        conn.execute("DROP TABLE _positions_legacy")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_positions_user_condition ON positions(user_id, condition_id)")
        return

    # Fresh create (table doesn't exist yet).
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS positions (
            user_id TEXT NOT NULL,
            condition_id TEXT NOT NULL,
            token_id TEXT NOT NULL,
            leader_id TEXT,
            size_tokens TEXT NOT NULL,
            avg_entry_price TEXT NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (user_id, condition_id, token_id)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_positions_user_condition ON positions(user_id, condition_id)")

    # Backfill from old positions_ledger when present.
    if _table_exists(conn, "positions_ledger"):
        conn.execute(
            """
            INSERT OR IGNORE INTO positions (user_id, condition_id, token_id, leader_id, size_tokens, avg_entry_price, updated_at)
            SELECT user_id, condition_id, token_id, leader_id, size_tokens, avg_entry_price,
                   COALESCE(updated_at, created_at, strftime('%s','now'))
            FROM positions_ledger
            """
        )


def _ensure_processed_trades_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS processed_trades (
            trade_key TEXT PRIMARY KEY,
            user_id TEXT NOT NULL DEFAULT '',
            leader_id TEXT,
            tx_hash TEXT,
            side TEXT,
            asset TEXT,
            condition_id TEXT,
            status TEXT NOT NULL,
            processed_at REAL NOT NULL
        )
        """
    )
    _ensure_column(conn, "processed_trades", "user_id TEXT NOT NULL DEFAULT ''")
    _ensure_column(conn, "processed_trades", "leader_id TEXT")
    _ensure_column(conn, "processed_trades", "retry_attempt INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "processed_trades", "order_id TEXT")


def _ensure_cursors_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS leader_cursors (
            leader_wallet TEXT PRIMARY KEY,
            last_timestamp REAL NOT NULL DEFAULT 0,
            cursor_tx_hash TEXT,
            cursor_trade_id TEXT,
            updated_at REAL NOT NULL
        )
        """
    )
    _ensure_column(conn, "leader_cursors", "cursor_tx_hash TEXT")
    _ensure_column(conn, "leader_cursors", "cursor_trade_id TEXT")


def _ensure_leaders_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
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
            active INTEGER NOT NULL DEFAULT 0,
            rejection_reason TEXT,
            source TEXT,
            updated_at REAL NOT NULL
        )
        """
    )
    _ensure_column(conn, "discovered_leaders", "active INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "discovered_leaders", "rejection_reason TEXT")
    _ensure_column(conn, "discovered_leaders", "source TEXT")
    _ensure_column(conn, "discovered_leaders", "follow_tier TEXT NOT NULL DEFAULT 'watchlist'")
    _ensure_column(conn, "discovered_leaders", "tier_changed_at REAL")


def _drop_redemptions_table(conn: sqlite3.Connection) -> None:
    """Destructive migration: removes the legacy ``redemptions`` table.

    Issue-78 replaces on-chain CTF redemption with NearOneExitService, which
    sells the position at near-par via the normal CLOB SELL path. The table
    no longer has any writer and is dropped to prevent confusion. Rollback
    requires restoring the table schema from this commit's parent and
    repopulating from external audit logs — there is no in-tree downgrade.
    """
    conn.execute("DROP TABLE IF EXISTS redemptions")


def _ensure_market_structure_events_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS market_structure_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT NOT NULL,
            condition_id TEXT NOT NULL,
            old_token_id TEXT,
            new_token_id TEXT,
            ratio TEXT,
            event_ts REAL NOT NULL,
            payload_json TEXT NOT NULL,
            remapped INTEGER NOT NULL DEFAULT 0,
            created_at REAL NOT NULL
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_market_structure_condition ON market_structure_events(condition_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_market_structure_pending ON market_structure_events(remapped, event_type)")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS market_structure_event_applied (
            user_id TEXT NOT NULL,
            event_id INTEGER NOT NULL REFERENCES market_structure_events(id),
            applied_at REAL NOT NULL,
            PRIMARY KEY (user_id, event_id)
        )
        """
    )


def _ensure_risk_blocks_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS risk_blocks (
            user_id TEXT NOT NULL,
            condition_id TEXT NOT NULL,
            token_id TEXT NOT NULL,
            reason TEXT NOT NULL,
            source TEXT NOT NULL,
            triggered_at REAL NOT NULL,
            expires_at REAL,
            cleared_at REAL,
            clear_reason TEXT,
            leader_anchor TEXT,
            metadata_json TEXT NOT NULL DEFAULT '{}',
            PRIMARY KEY (user_id, condition_id, token_id)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_risk_blocks_cleared ON risk_blocks(cleared_at)")


def _ensure_risk_triggers_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS risk_triggers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            condition_id TEXT NOT NULL,
            token_id TEXT NOT NULL,
            reason TEXT NOT NULL,
            source TEXT NOT NULL DEFAULT 'risk_monitor',
            triggered_at REAL NOT NULL,
            fill_order_id TEXT,
            fill_status TEXT NOT NULL DEFAULT 'pending',
            metadata_json TEXT NOT NULL DEFAULT '{}'
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_risk_triggers_position "
        "ON risk_triggers(user_id, condition_id, token_id)"
    )


def _ensure_legacy_tables(conn: sqlite3.Connection) -> None:
    """Keep legacy tables available while active path is migrated."""
    _exec_each(
        conn,
        """
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
            -- Money columns stored as TEXT to round-trip Decimal losslessly.
            -- SQLite's REAL is IEEE-754 double and corrupts cent precision
            -- once any Decimal arithmetic happens upstream.
            price TEXT,
            size TEXT,
            usdc_size TEXT,
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
            PRIMARY KEY (user_id, condition_id, token_id)
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

        CREATE TABLE IF NOT EXISTS arb_baskets (
            basket_id TEXT PRIMARY KEY,
            condition_id TEXT NOT NULL,
            state TEXT NOT NULL,
            data_json TEXT NOT NULL,
            updated_at REAL NOT NULL
        );
        """
    )


def _ensure_order_reservations_table(conn: sqlite3.Connection) -> None:
    _exec_each(
        conn,
        """
        CREATE TABLE IF NOT EXISTS order_reservations (
            id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            order_intent_key TEXT NOT NULL,
            condition_id TEXT NOT NULL,
            token_id TEXT NOT NULL,
            side TEXT NOT NULL,
            reason TEXT NOT NULL,
            reserved_usdc TEXT NOT NULL DEFAULT '0',
            reserved_size TEXT NOT NULL DEFAULT '0',
            status TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            expires_at INTEGER NOT NULL,
            order_id TEXT,
            venue_order_id TEXT,
            metadata_json TEXT
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_order_reservations_unique_active
        ON order_reservations(user_id, order_intent_key)
        WHERE status = 'active';

        CREATE INDEX IF NOT EXISTS idx_order_reservations_status
        ON order_reservations(status);

        CREATE INDEX IF NOT EXISTS idx_order_reservations_user_token
        ON order_reservations(user_id, token_id);
        """
    )


def _ensure_positions_ledger_pk(conn: sqlite3.Connection) -> None:
    """Upgrade positions_ledger PK to (user_id, condition_id, token_id).

    Pre-fix PK was (user_id, condition_id) which silently overwrote the
    YES outcome with NO (or vice-versa) for the same condition because
    binary markets share a condition_id across two distinct token_ids.

    Idempotent: returns early if the 3-column PK is already present, or
    if the table does not yet exist (fresh DBs get the correct schema
    via _ensure_legacy_tables).
    """
    info = conn.execute("PRAGMA table_info(positions_ledger)").fetchall()
    if not info:
        return
    pk_cols = sorted(str(row[1]) for row in info if int(row[5] or 0) > 0)
    if pk_cols == ["condition_id", "token_id", "user_id"]:
        return  # already migrated

    conn.execute("ALTER TABLE positions_ledger RENAME TO positions_ledger_pk2_legacy")
    conn.execute(
        """
        CREATE TABLE positions_ledger (
            user_id TEXT NOT NULL,
            condition_id TEXT NOT NULL,
            token_id TEXT NOT NULL,
            size_tokens TEXT NOT NULL,
            avg_entry_price TEXT NOT NULL,
            leader_id TEXT,
            category TEXT,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL,
            PRIMARY KEY (user_id, condition_id, token_id)
        )
        """
    )
    # Legacy rows had at most one row per (user, condition) under the old
    # 2-col PK, so a direct copy never violates the new 3-col PK. Rows
    # whose token_id was NULL/'' would silently collide; defensively
    # fall back to '__unknown__' so the upgrade never fails mid-startup.
    conn.execute(
        """
        INSERT INTO positions_ledger
          (user_id, condition_id, token_id, size_tokens, avg_entry_price,
           leader_id, category, created_at, updated_at)
        SELECT
          user_id,
          condition_id,
          CASE WHEN token_id IS NULL OR token_id = '' THEN '__unknown__' ELSE token_id END,
          size_tokens,
          avg_entry_price,
          leader_id,
          category,
          created_at,
          updated_at
        FROM positions_ledger_pk2_legacy
        """
    )
    conn.execute("DROP TABLE positions_ledger_pk2_legacy")


def apply_migrations(conn: sqlite3.Connection) -> None:
    """Apply the full schema migration atomically.

    Wrapped in BEGIN IMMEDIATE so the whole pipeline is one transaction:
    if any step raises, every CREATE / ALTER / INSERT from this call is
    rolled back. A half-applied schema is impossible — a crashed boot
    leaves the DB in its pre-call state, and the next boot starts the
    pipeline from scratch.
    """
    # sqlite3 in PEP 249 mode opens an implicit transaction on the first
    # write; flush anything pending so our BEGIN IMMEDIATE is clean.
    if conn.in_transaction:
        conn.commit()
    conn.execute("BEGIN IMMEDIATE")
    try:
        _ensure_order_reservations_table(conn)
        _ensure_orders_table(conn)
        _ensure_fills_table(conn)
        _ensure_positions_table(conn)
        _ensure_processed_trades_table(conn)
        _ensure_cursors_table(conn)
        _ensure_leaders_table(conn)
        _drop_redemptions_table(conn)
        _ensure_market_structure_events_table(conn)
        _ensure_risk_blocks_table(conn)
        _ensure_risk_triggers_table(conn)
        _ensure_legacy_tables(conn)
        _ensure_positions_ledger_pk(conn)
        apply_discovery_migrations(conn)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
