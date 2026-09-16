-- Durable scoped automatic membership, exact evidence, and bounded discovery jobs.
CREATE TABLE IF NOT EXISTS auto_trader_rosters (
    follower_account text NOT NULL,
    trading_mode text NOT NULL CHECK (trading_mode IN ('paper', 'live')),
    revision bigint NOT NULL CHECK (revision > 0),
    snapshot jsonb NOT NULL,
    PRIMARY KEY (follower_account, trading_mode)
);
CREATE TABLE IF NOT EXISTS auto_trader_events (
    id bigserial PRIMARY KEY,
    follower_account text NOT NULL,
    trading_mode text NOT NULL,
    revision bigint NOT NULL,
    event text NOT NULL,
    actor text NOT NULL,
    snapshot jsonb NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (follower_account, trading_mode, revision)
);
CREATE INDEX IF NOT EXISTS auto_trader_events_scope
    ON auto_trader_events (follower_account, trading_mode, id DESC);
CREATE TABLE IF NOT EXISTS auto_trader_receipts (
    follower_account text NOT NULL,
    trading_mode text NOT NULL,
    receipt_key text NOT NULL,
    snapshot jsonb NOT NULL,
    PRIMARY KEY (follower_account, trading_mode, receipt_key)
);
CREATE TABLE IF NOT EXISTS auto_trader_discovery_jobs (
    follower_account text NOT NULL,
    trading_mode text NOT NULL,
    event text NOT NULL,
    status text NOT NULL CHECK (status IN ('running', 'completed', 'failed')),
    attempts integer NOT NULL DEFAULT 1,
    lease_until timestamptz NOT NULL,
    PRIMARY KEY (follower_account, trading_mode, event)
);
CREATE TABLE IF NOT EXISTS auto_trader_paper_portfolios (
    follower_account text NOT NULL,
    trading_mode text NOT NULL,
    leader_wallet text NOT NULL,
    revision bigint NOT NULL DEFAULT 1,
    snapshot jsonb NOT NULL,
    PRIMARY KEY (follower_account, trading_mode, leader_wallet)
);
CREATE TABLE IF NOT EXISTS auto_trader_paper_events (
    follower_account text NOT NULL,
    trading_mode text NOT NULL,
    leader_wallet text NOT NULL,
    event_key text NOT NULL,
    snapshot jsonb NOT NULL,
    recorded_at timestamptz NOT NULL,
    PRIMARY KEY (follower_account, trading_mode, leader_wallet, event_key)
);
