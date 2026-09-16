-- Preserve existing inventory as unclassified until a validated venue snapshot arrives.
ALTER TABLE positions ADD COLUMN IF NOT EXISTS lifecycle text NOT NULL DEFAULT 'unknown'
    CHECK (lifecycle IN ('unknown', 'active', 'settled'));
ALTER TABLE positions ADD COLUMN IF NOT EXISTS settlement_price text
    CHECK (settlement_price IS NULL OR (
        lifecycle = 'settled' AND settlement_price::numeric >= 0
        AND settlement_price::numeric <= 1
    ));
