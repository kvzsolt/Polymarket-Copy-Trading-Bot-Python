-- Follower evidence commits atomically with fill/position accounting. Remote
-- publisher availability never participates in this local transaction.
CREATE TABLE IF NOT EXISTS evidence_transaction_outbox (
    seq bigserial PRIMARY KEY,
    event_id text NOT NULL UNIQUE CHECK (length(event_id) BETWEEN 1 AND 200),
    digest text NOT NULL CHECK (digest ~ '^[0-9a-f]{64}$'),
    payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    created_at timestamptz NOT NULL DEFAULT now(),
    drained_at timestamptz
);
CREATE INDEX IF NOT EXISTS evidence_transaction_outbox_undrained
    ON evidence_transaction_outbox(seq) WHERE drained_at IS NULL;
