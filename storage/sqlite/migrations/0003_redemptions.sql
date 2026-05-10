-- 0003_redemptions.sql: Redemption tracking for market resolution payouts
-- Records each redemption attempt from creation through on-chain confirmation.

CREATE TABLE IF NOT EXISTS redemptions (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    condition_id TEXT NOT NULL,
    token_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'created',
    tx_hash TEXT,
    amount TEXT NOT NULL DEFAULT '0',
    submitted_at INTEGER,
    confirmed_at INTEGER,
    reverted_at INTEGER,
    error_message TEXT,
    metadata_json TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_redemptions_status
ON redemptions(status);

CREATE INDEX IF NOT EXISTS idx_redemptions_user_condition
ON redemptions(user_id, condition_id);
