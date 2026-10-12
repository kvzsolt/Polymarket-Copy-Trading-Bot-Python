-- Persist outstanding targets without reinterpreting legacy completed levels.
ALTER TABLE copy_task_risk_lots
    ADD COLUMN IF NOT EXISTS pending_exits jsonb NOT NULL DEFAULT '[]'::jsonb;
ALTER TABLE copy_task_protective_exit_fill_ledger
    ADD COLUMN IF NOT EXISTS level_target_shares numeric;
