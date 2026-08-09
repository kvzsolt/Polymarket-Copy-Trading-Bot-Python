-- Polymarket Copy Trader v1.0.0 copy tasks and post-protective-exit locks.
-- Additive and idempotent. Existing position/order tables remain authoritative.

CREATE TABLE IF NOT EXISTS copy_tasks (
    task_id TEXT PRIMARY KEY,
    name TEXT NOT NULL CHECK (BTRIM(name) <> ''),
    leader_wallet TEXT NOT NULL CHECK (leader_wallet ~ '^0x[0-9a-f]{40}$'),
    follower_account TEXT NOT NULL CHECK (BTRIM(follower_account) <> ''),
    status TEXT NOT NULL CHECK (status IN ('active', 'paused', 'deleted')),
    monitoring_mode TEXT NOT NULL CHECK (monitoring_mode IN ('normal', 'priority')),
    sizing_mode TEXT NOT NULL CHECK (BTRIM(sizing_mode) <> ''),
    notifications_muted BOOLEAN NOT NULL DEFAULT FALSE,
    version INTEGER NOT NULL DEFAULT 1 CHECK (version > 0),
    configuration JSONB NOT NULL CHECK (jsonb_typeof(configuration) = 'object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_copy_tasks_leader_status
    ON copy_tasks (leader_wallet, status, task_id);
CREATE INDEX IF NOT EXISTS idx_copy_tasks_follower_status
    ON copy_tasks (follower_account, status, task_id);

CREATE TABLE IF NOT EXISTS market_reentry_locks (
    follower_account TEXT NOT NULL CHECK (BTRIM(follower_account) <> ''),
    task_id TEXT NOT NULL REFERENCES copy_tasks(task_id) ON DELETE RESTRICT,
    leader_wallet TEXT NOT NULL CHECK (leader_wallet ~ '^0x[0-9a-f]{40}$'),
    market_id TEXT NOT NULL CHECK (BTRIM(market_id) <> ''),
    reason TEXT NOT NULL CHECK (
        reason IN (
            'stop_loss',
            'take_profit',
            'trailing_stop',
            'layered_stop_loss',
            'layered_take_profit'
        )
    ),
    fill_id TEXT NOT NULL CHECK (BTRIM(fill_id) <> ''),
    locked_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (follower_account, task_id, leader_wallet, market_id),
    UNIQUE (fill_id)
);

CREATE INDEX IF NOT EXISTS idx_reentry_locks_market
    ON market_reentry_locks (market_id, locked_at DESC);
CREATE INDEX IF NOT EXISTS idx_reentry_locks_leader
    ON market_reentry_locks (leader_wallet, locked_at DESC);

CREATE TABLE IF NOT EXISTS copy_task_budget_reservations (
    reservation_id TEXT PRIMARY KEY CHECK (BTRIM(reservation_id) <> ''),
    follower_account TEXT NOT NULL CHECK (BTRIM(follower_account) <> ''),
    task_id TEXT NOT NULL REFERENCES copy_tasks(task_id) ON DELETE RESTRICT,
    market_id TEXT NOT NULL CHECK (BTRIM(market_id) <> ''),
    sports_market_type TEXT NOT NULL DEFAULT '',
    amount NUMERIC(38, 18) NOT NULL CHECK (amount > 0),
    reserved_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL CHECK (expires_at > reserved_at),
    status TEXT NOT NULL CHECK (
        status IN ('reserved', 'committed', 'released', 'expired')
    ),
    fill_id TEXT,
    committed_notional NUMERIC(38, 18),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (
        (status = 'committed' AND fill_id IS NOT NULL AND committed_notional > 0)
        OR
        (status <> 'committed' AND fill_id IS NULL AND committed_notional IS NULL)
    )
);

ALTER TABLE copy_task_budget_reservations
    ADD COLUMN IF NOT EXISTS sports_market_type TEXT NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS idx_task_budget_reservations_task_status
    ON copy_task_budget_reservations (task_id, status, expires_at);
CREATE INDEX IF NOT EXISTS idx_task_budget_reservations_account_market
    ON copy_task_budget_reservations (
        follower_account,
        market_id,
        status,
        expires_at
    );

CREATE TABLE IF NOT EXISTS copy_task_fill_ledger (
    fill_id TEXT PRIMARY KEY CHECK (BTRIM(fill_id) <> ''),
    reservation_id TEXT NOT NULL UNIQUE
        REFERENCES copy_task_budget_reservations(reservation_id) ON DELETE RESTRICT,
    follower_account TEXT NOT NULL CHECK (BTRIM(follower_account) <> ''),
    task_id TEXT NOT NULL REFERENCES copy_tasks(task_id) ON DELETE RESTRICT,
    market_id TEXT NOT NULL CHECK (BTRIM(market_id) <> ''),
    sports_market_type TEXT NOT NULL DEFAULT '',
    notional NUMERIC(38, 18) NOT NULL CHECK (notional > 0),
    occurred_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE copy_task_fill_ledger
    ADD COLUMN IF NOT EXISTS sports_market_type TEXT NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS idx_copy_task_fill_ledger_task_time
    ON copy_task_fill_ledger (task_id, occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_copy_task_fill_ledger_account_market
    ON copy_task_fill_ledger (follower_account, market_id, occurred_at DESC);

CREATE INDEX IF NOT EXISTS idx_task_budget_reservations_sports_type
    ON copy_task_budget_reservations (
        task_id,
        sports_market_type,
        status,
        expires_at
    )
    WHERE sports_market_type <> '';

CREATE INDEX IF NOT EXISTS idx_copy_task_fill_ledger_sports_type
    ON copy_task_fill_ledger (task_id, sports_market_type, occurred_at DESC)
    WHERE sports_market_type <> '';
