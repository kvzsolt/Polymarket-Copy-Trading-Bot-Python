-- 0002_execution_lifecycle.sql: Order reservation tracking for execution lifecycle
-- Ensures USDC/size is reserved before submission and released on terminal state.

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
    metadata_json TEXT,
    UNIQUE(user_id, order_intent_key)
);

CREATE INDEX IF NOT EXISTS idx_order_reservations_status
ON order_reservations(status);

CREATE INDEX IF NOT EXISTS idx_order_reservations_user_token
ON order_reservations(user_id, token_id);
