-- 0004_market_structure.sql: Market structure metadata cache
-- Stores market question, type, active/closed/archived state, and token layout.

CREATE TABLE IF NOT EXISTS market_structure (
    condition_id TEXT PRIMARY KEY,
    question_id TEXT,
    question TEXT,
    description TEXT,
    market_type TEXT,
    active INTEGER NOT NULL DEFAULT 1,
    closed INTEGER NOT NULL DEFAULT 0,
    archived INTEGER NOT NULL DEFAULT 0,
    min_order_size TEXT NOT NULL DEFAULT '0',
    tick_size TEXT NOT NULL DEFAULT '0.01',
    end_date_iso TEXT,
    tokens_json TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_market_structure_active
ON market_structure(active);

CREATE INDEX IF NOT EXISTS idx_market_structure_question_id
ON market_structure(question_id);
