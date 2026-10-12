-- A02: complete order identity and explicit sizing units on `orders`.
--
-- Additive columns only: every statement is IF NOT EXISTS, no column is
-- renamed or dropped, and legacy rows keep NULL identity — the recovery
-- reader treats a missing unit/token identity as fail-closed (the row
-- stays duplicate-blocking) rather than fabricating one.
--
-- token_id              raw outcome token the submitted order trades
-- amount_unit           unit of the submitted amount: 'quote' (BUY USDC
--                       spend) or 'shares' (SELL outcome shares)
-- order_kind            submission kind ('market' for the FAK/FOK money
--                       path); NULL for legacy rows
-- effective_price_limit explicit venue price limit the request carried
-- correlation_ids       available correlation identities (signal_id,
--                       source_promotion_id, ...) as a jsonb object
ALTER TABLE orders ADD COLUMN IF NOT EXISTS token_id text;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS amount_unit text
    CHECK (amount_unit IS NULL OR amount_unit IN ('quote', 'shares'));
ALTER TABLE orders ADD COLUMN IF NOT EXISTS order_kind text
    CHECK (order_kind IS NULL OR order_kind IN ('market', 'limit'));
ALTER TABLE orders ADD COLUMN IF NOT EXISTS effective_price_limit text;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS correlation_ids jsonb;
