CREATE TABLE IF NOT EXISTS redemption_attempts (
    authorization_digest text PRIMARY KEY,
    wallet text NOT NULL CHECK (wallet ~ '^0x[0-9a-f]{40}$'),
    state text NOT NULL CHECK (state IN ('pending', 'confirmed', 'failed', 'expired', 'cleared')),
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS redemption_one_pending_wallet
    ON redemption_attempts (wallet) WHERE state = 'pending';
