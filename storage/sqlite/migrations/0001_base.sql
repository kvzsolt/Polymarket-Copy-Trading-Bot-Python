-- 0001_base.sql: Core trading tables extracted from migrations.py
-- These are the foundational tables required for the copy-trading bot.

-- Orders: tracks all order submissions with idempotency
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
);

-- Fills: records individual fill events for each order
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
);

-- Positions: current open positions per user/condition/token
CREATE TABLE IF NOT EXISTS positions (
    user_id TEXT NOT NULL,
    condition_id TEXT NOT NULL,
    token_id TEXT NOT NULL,
    leader_id TEXT,
    size_tokens TEXT NOT NULL,
    avg_entry_price TEXT NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, condition_id, token_id)
);

-- Processed trades: deduplication ledger for leader trade signals
CREATE TABLE IF NOT EXISTS processed_trades (
    trade_key TEXT PRIMARY KEY,
    user_id TEXT NOT NULL DEFAULT '',
    leader_id TEXT,
    tx_hash TEXT,
    side TEXT,
    asset TEXT,
    condition_id TEXT,
    status TEXT NOT NULL,
    retry_attempt INTEGER NOT NULL DEFAULT 0,
    order_id TEXT,
    processed_at REAL NOT NULL
);

-- Leader cursors: tracks last-processed timestamp per leader wallet
CREATE TABLE IF NOT EXISTS leader_cursors (
    leader_wallet TEXT PRIMARY KEY,
    last_timestamp REAL NOT NULL DEFAULT 0,
    cursor_tx_hash TEXT,
    cursor_trade_id TEXT,
    updated_at REAL NOT NULL
);

-- Discovered leaders: leader discovery and scoring
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
    follow_tier TEXT NOT NULL DEFAULT 'watchlist',
    tier_changed_at REAL,
    updated_at REAL NOT NULL
);

-- Risk blocks: prevents trading on flagged condition/token pairs
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
);

-- Indexes for core tables
CREATE INDEX IF NOT EXISTS idx_fills_user_condition ON fills(user_id, condition_id);
CREATE INDEX IF NOT EXISTS idx_fills_order ON fills(order_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_fills_unique ON fills(user_id, order_id, condition_id, side, fill_ts);
CREATE INDEX IF NOT EXISTS idx_positions_user_condition ON positions(user_id, condition_id);
CREATE INDEX IF NOT EXISTS idx_risk_blocks_cleared ON risk_blocks(cleared_at);
