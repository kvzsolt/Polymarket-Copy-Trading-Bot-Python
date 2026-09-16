-- Extend v1 task accounting with non-expiring POST state, multiple fills,
-- and restart-safe per-fill protective-risk lots.

ALTER TABLE copy_task_budget_reservations
    ADD COLUMN IF NOT EXISTS execution_state TEXT NOT NULL DEFAULT 'pre_post'
    CHECK (execution_state IN ('pre_post', 'retained'));

ALTER TABLE copy_task_budget_reservations
    ADD COLUMN IF NOT EXISTS settled_notional NUMERIC(38, 18) NOT NULL DEFAULT 0
    CHECK (settled_notional >= 0 AND settled_notional <= amount);

ALTER TABLE copy_task_fill_ledger
    DROP CONSTRAINT IF EXISTS copy_task_fill_ledger_reservation_id_key;

CREATE INDEX IF NOT EXISTS idx_copy_task_fill_ledger_reservation
    ON copy_task_fill_ledger (reservation_id, occurred_at, fill_id);

CREATE TABLE IF NOT EXISTS copy_task_risk_lots (
    lot_id TEXT PRIMARY KEY CHECK (BTRIM(lot_id) <> ''),
    entry_fill_id TEXT NOT NULL UNIQUE CHECK (BTRIM(entry_fill_id) <> ''),
    follower_account TEXT NOT NULL CHECK (BTRIM(follower_account) <> ''),
    task_id TEXT NOT NULL REFERENCES copy_tasks(task_id) ON DELETE RESTRICT,
    leader_wallet TEXT NOT NULL CHECK (leader_wallet ~ '^0x[0-9a-f]{40}$'),
    market_id TEXT NOT NULL CHECK (BTRIM(market_id) <> ''),
    outcome_id TEXT NOT NULL CHECK (BTRIM(outcome_id) <> ''),
    entry_price NUMERIC(38, 18) NOT NULL CHECK (entry_price > 0 AND entry_price <= 1),
    original_shares NUMERIC(38, 18) NOT NULL CHECK (original_shares > 0),
    remaining_shares NUMERIC(38, 18) NOT NULL CHECK (
        remaining_shares >= 0 AND remaining_shares <= original_shares
    ),
    peak_price NUMERIC(38, 18) NOT NULL CHECK (peak_price > 0 AND peak_price <= 1),
    policy_snapshot JSONB NOT NULL CHECK (jsonb_typeof(policy_snapshot) = 'object'),
    triggered_levels JSONB NOT NULL DEFAULT '[]'::jsonb
        CHECK (jsonb_typeof(triggered_levels) = 'array'),
    status TEXT NOT NULL CHECK (status IN ('open', 'closed')),
    opened_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    closed_at TIMESTAMPTZ,
    version INTEGER NOT NULL DEFAULT 1 CHECK (version > 0),
    CHECK (
        (status = 'open' AND remaining_shares > 0 AND closed_at IS NULL)
        OR
        (status = 'closed' AND remaining_shares = 0 AND closed_at IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS idx_copy_task_risk_lots_open_follower
    ON copy_task_risk_lots (follower_account, status, opened_at, lot_id);

CREATE INDEX IF NOT EXISTS idx_copy_task_risk_lots_market
    ON copy_task_risk_lots (market_id, outcome_id, status, opened_at);

CREATE TABLE IF NOT EXISTS copy_task_protective_exit_fill_ledger (
    fill_id TEXT PRIMARY KEY CHECK (BTRIM(fill_id) <> ''),
    lot_id TEXT NOT NULL REFERENCES copy_task_risk_lots(lot_id) ON DELETE RESTRICT,
    shares NUMERIC(38, 18) NOT NULL CHECK (shares > 0),
    price NUMERIC(38, 18) NOT NULL CHECK (price > 0 AND price <= 1),
    level_id TEXT,
    occurred_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_copy_task_exit_fills_lot_time
    ON copy_task_protective_exit_fill_ledger (lot_id, occurred_at, fill_id);
