-- Historical effective order keys remain readable by their economic intent.
CREATE INDEX IF NOT EXISTS idx_orders_account_source_intent
    ON orders (user_id, (request_json->>'signal_id'));
