-- Minimal operator control-plane persistence. This is audit/projection state,
-- not an event-sourcing substrate and never owns trading-domain decisions.

CREATE TABLE IF NOT EXISTS control_plane_audit_log (
    audit_sequence BIGSERIAL PRIMARY KEY,
    event_id TEXT NOT NULL UNIQUE CHECK (BTRIM(event_id) <> ''),
    occurred_at TIMESTAMPTZ NOT NULL,
    actor TEXT NOT NULL CHECK (BTRIM(actor) <> ''),
    action TEXT NOT NULL CHECK (BTRIM(action) <> ''),
    target_type TEXT NOT NULL CHECK (BTRIM(target_type) <> ''),
    target_id TEXT NOT NULL CHECK (BTRIM(target_id) <> ''),
    follower_account TEXT,
    outcome TEXT NOT NULL CHECK (BTRIM(outcome) <> ''),
    reason_code TEXT NOT NULL CHECK (BTRIM(reason_code) <> ''),
    details_json JSONB NOT NULL DEFAULT '{}'::jsonb
        CHECK (jsonb_typeof(details_json) = 'object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_control_plane_audit_scope_time
    ON control_plane_audit_log (
        follower_account,
        occurred_at DESC,
        audit_sequence DESC
    );

CREATE INDEX IF NOT EXISTS idx_control_plane_audit_target_time
    ON control_plane_audit_log (
        target_type,
        target_id,
        occurred_at DESC,
        audit_sequence DESC
    );

-- The canonical trading-control row predates the control-plane revision
-- contract and could have started at generation zero.  Normalize that one
-- legacy value so every optimistic command uses the same positive revision
-- invariant as the HTTP contract.
UPDATE trading_control
SET generation = 1
WHERE singleton_id = 1
  AND generation < 1;

CREATE SEQUENCE IF NOT EXISTS market_reentry_lock_revision_seq AS BIGINT;

ALTER TABLE market_reentry_locks
    ADD COLUMN IF NOT EXISTS revision BIGINT;

ALTER SEQUENCE market_reentry_lock_revision_seq
    OWNED BY market_reentry_locks.revision;

ALTER TABLE market_reentry_locks
    ALTER COLUMN revision
    SET DEFAULT nextval('market_reentry_lock_revision_seq');

UPDATE market_reentry_locks
    SET revision = nextval('market_reentry_lock_revision_seq')
    WHERE revision IS NULL;

ALTER TABLE market_reentry_locks
    ALTER COLUMN revision SET NOT NULL;

ALTER TABLE market_reentry_locks
    DROP CONSTRAINT IF EXISTS market_reentry_locks_revision_positive;

ALTER TABLE market_reentry_locks
    ADD CONSTRAINT market_reentry_locks_revision_positive CHECK (revision > 0);
