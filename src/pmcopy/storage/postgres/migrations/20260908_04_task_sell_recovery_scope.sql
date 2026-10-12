-- Legacy targets keep explicit unscoped ownership. Do not infer attribution.
ALTER TABLE leader_sell_targets ADD COLUMN task_id text NOT NULL DEFAULT '';
ALTER TABLE leader_sell_targets DROP CONSTRAINT leader_sell_targets_pkey;
ALTER TABLE leader_sell_targets ADD PRIMARY KEY
    (user_id, leader_wallet, condition_id, token_id, task_id);
ALTER TABLE leader_sell_target_redrives ADD COLUMN task_id text NOT NULL DEFAULT '';
ALTER TABLE leader_sell_target_redrives DROP CONSTRAINT leader_sell_target_redrives_pkey;
ALTER TABLE leader_sell_target_redrives ADD PRIMARY KEY
    (user_id, leader_wallet, condition_id, token_id, task_id, generation, released_reservation_id);
