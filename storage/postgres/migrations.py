"""Postgres schema — fresh-create translation of storage/sqlite/migrations.py.

Per the migration plan: start fresh, no SQLite→PG data export. So the
schema here is the FINAL shape only — no _ensure_column ALTER paths,
no rebuild-legacy backfills, no in-place PK upgrades. A fresh DB gets
the correct schema in one shot.

SQLite idioms swapped to Postgres equivalents:

  INTEGER PRIMARY KEY AUTOINCREMENT  → BIGSERIAL PRIMARY KEY
  TIMESTAMP DEFAULT CURRENT_TIMESTAMP → timestamptz DEFAULT now()
  TEXT (JSON columns)                → jsonb (default '{}'::jsonb / '[]'::jsonb)
  REAL (timestamps, ratios)          → double precision
  REAL (money — already TEXT post-audit) → text (unchanged)
  INTEGER (booleans)                 → boolean
  INTEGER (counts / epochs)          → bigint
  UNIQUE(...) inside CREATE TABLE    → identical
  CREATE INDEX … WHERE x IS NOT NULL → identical (partial index)
  CREATE UNIQUE INDEX … WHERE status = 'active' → identical (partial)
  ON CONFLICT(k) DO UPDATE …         → identical
  json_extract(col, '$.field')       → col->>'field' (consumer-side change)
  strftime('%s','now')               → extract(epoch from now())::bigint
  BEGIN IMMEDIATE                    → async with conn.transaction()
  lastrowid                          → INSERT … RETURNING id
"""

from __future__ import annotations

import asyncpg


# ---------------------------------------------------------------------------
# Core tables (translated from storage/sqlite/migrations.py)
# ---------------------------------------------------------------------------

_CORE_DDL = [
    # ----- orders ---------------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS orders (
        idempotency_key text PRIMARY KEY,
        user_id         text NOT NULL DEFAULT '',
        leader_id       text,
        order_id        text,
        status          text NOT NULL DEFAULT 'created',
        request_json    jsonb NOT NULL DEFAULT '{}'::jsonb,
        response_json   jsonb,
        attempt_count   integer NOT NULL DEFAULT 1,
        last_error      text,
        matched_total   text NOT NULL DEFAULT '0',
        created_at      timestamptz NOT NULL DEFAULT now(),
        updated_at      timestamptz NOT NULL DEFAULT now()
    )
    """,
    # Partial unique index — pending rows (order_id NULL) coexist.
    """
    CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_order_id
    ON orders(order_id) WHERE order_id IS NOT NULL
    """,

    # ----- fills ----------------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS fills (
        id           bigserial PRIMARY KEY,
        user_id      text,
        leader_id    text,
        order_id     text,
        condition_id text,
        token_id     text,
        side         text,
        size_tokens  text,
        price        text,
        fee          text,
        fill_ts      bigint,
        fill_json    jsonb,
        created_at   timestamptz NOT NULL DEFAULT now()
    )
    """,
    # greptile P1: ON CONFLICT in ``OrdersRepo.mark_fill`` requires a
    # uniqueness target. A unique index on the fill identity tuple makes
    # the insert idempotent so reconciler-triggered duplicate fills no
    # longer raise — they silently skip via ``ON CONFLICT DO NOTHING``.
    """
    CREATE UNIQUE INDEX IF NOT EXISTS idx_fills_dedup
    ON fills(user_id, order_id, condition_id, side, fill_ts)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_fills_user_condition
    ON fills(user_id, condition_id)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_fills_order ON fills(order_id)
    """,
    # DL-03: ``idx_fills_unique`` was byte-identical to ``idx_fills_dedup``
    # above. The duplicate index doubled the per-INSERT index-maintenance
    # cost and made ``ON CONFLICT ON CONSTRAINT`` ambiguous. Drop it on
    # both fresh and redeployed databases. ``idx_fills_dedup`` remains the
    # canonical conflict target referenced by orders_repo.mark_fill.
    """DROP INDEX IF EXISTS idx_fills_unique""",

    # ----- positions (canonical 3-col PK) ---------------------------------
    """
    CREATE TABLE IF NOT EXISTS positions (
        user_id         text NOT NULL,
        condition_id    text NOT NULL,
        token_id        text NOT NULL,
        leader_id       text,
        size_tokens     text NOT NULL,
        avg_entry_price text NOT NULL,
        updated_at      timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (user_id, condition_id, token_id)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_positions_user_condition
    ON positions(user_id, condition_id)
    """,

    # ----- processed_trades ----------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS processed_trades (
        trade_key     text PRIMARY KEY,
        user_id       text NOT NULL DEFAULT '',
        leader_id     text,
        tx_hash       text,
        side          text,
        asset         text,
        condition_id  text,
        status        text NOT NULL,
        retry_attempt integer NOT NULL DEFAULT 0,
        order_id      text,
        processed_at  double precision NOT NULL
    )
    """,

    # ----- leader_cursors -------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS leader_cursors (
        leader_wallet    text PRIMARY KEY,
        last_timestamp   double precision NOT NULL DEFAULT 0,
        cursor_tx_hash   text,
        cursor_trade_id  text,
        updated_at       double precision NOT NULL
    )
    """,

    # ----- discovered_leaders --------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS discovered_leaders (
        wallet              text PRIMARY KEY,
        name                text,
        overall_gain        text,
        win_amount          text,
        loss_amount         text,
        win_rate            double precision,
        total_positions     integer,
        active_positions    integer,
        event_count         integer,
        win_count           integer,
        rank                integer,
        total_current_value text,
        tags                text,
        composite_score     double precision,
        active              boolean NOT NULL DEFAULT FALSE,
        rejection_reason    text,
        source              text,
        follow_tier         text NOT NULL DEFAULT 'watchlist',
        tier_changed_at     double precision,
        updated_at          double precision NOT NULL
    )
    """,

    # ----- market_structure_events ---------------------------------------
    """
    CREATE TABLE IF NOT EXISTS market_structure_events (
        id            bigserial PRIMARY KEY,
        event_type    text NOT NULL,
        condition_id  text NOT NULL,
        old_token_id  text,
        new_token_id  text,
        ratio         text,
        event_ts      double precision NOT NULL,
        payload_json  jsonb NOT NULL,
        remapped      boolean NOT NULL DEFAULT FALSE,
        created_at    double precision NOT NULL
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_market_structure_condition
    ON market_structure_events(condition_id)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_market_structure_pending
    ON market_structure_events(remapped, event_type)
    """,
    """
    CREATE TABLE IF NOT EXISTS market_structure_event_applied (
        user_id    text NOT NULL,
        event_id   bigint NOT NULL REFERENCES market_structure_events(id),
        applied_at double precision NOT NULL,
        PRIMARY KEY (user_id, event_id)
    )
    """,

    # ----- risk_blocks ----------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS risk_blocks (
        user_id       text NOT NULL,
        condition_id  text NOT NULL,
        token_id      text NOT NULL,
        reason        text NOT NULL,
        source        text NOT NULL,
        triggered_at  double precision NOT NULL,
        expires_at    double precision,
        cleared_at    double precision,
        clear_reason  text,
        leader_anchor text,
        metadata_json jsonb NOT NULL DEFAULT '{}'::jsonb,
        PRIMARY KEY (user_id, condition_id, token_id)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_risk_blocks_cleared
    ON risk_blocks(cleared_at)
    """,

    # ----- risk_triggers --------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS risk_triggers (
        id            bigserial PRIMARY KEY,
        user_id       text NOT NULL,
        condition_id  text NOT NULL,
        token_id      text NOT NULL,
        reason        text NOT NULL,
        source        text NOT NULL DEFAULT 'risk_monitor',
        triggered_at  double precision NOT NULL,
        fill_order_id text,
        fill_status   text NOT NULL DEFAULT 'pending',
        metadata_json jsonb NOT NULL DEFAULT '{}'::jsonb
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_risk_triggers_position
    ON risk_triggers(user_id, condition_id, token_id)
    """,

    # ----- order_reservations --------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS order_reservations (
        id               text PRIMARY KEY,
        user_id          text NOT NULL,
        order_intent_key text NOT NULL,
        condition_id     text NOT NULL,
        token_id         text NOT NULL,
        side             text NOT NULL,
        reason           text NOT NULL,
        reserved_usdc    text NOT NULL DEFAULT '0',
        reserved_size    text NOT NULL DEFAULT '0',
        status           text NOT NULL,
        created_at       bigint NOT NULL,
        updated_at       bigint NOT NULL,
        expires_at       bigint NOT NULL,
        order_id         text,
        venue_order_id   text,
        metadata_json    jsonb
    )
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS idx_order_reservations_unique_active
    ON order_reservations(user_id, order_intent_key) WHERE status = 'active'
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_order_reservations_status
    ON order_reservations(status)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_order_reservations_user_token
    ON order_reservations(user_id, token_id)
    """,

    # ----- legacy / supporting tables -------------------------------------
    # Still actively read/written by accountingService, dayEquity, etc.
    """
    CREATE TABLE IF NOT EXISTS user_positions (
        wallet_address text NOT NULL,
        condition_id   text NOT NULL,
        data_json      jsonb NOT NULL,
        updated_at     double precision NOT NULL,
        PRIMARY KEY (wallet_address, condition_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS user_activities (
        id              bigserial PRIMARY KEY,
        wallet_address  text NOT NULL,
        external_id     text,
        transaction_hash text,
        data_json       jsonb NOT NULL,
        created_at      double precision NOT NULL,
        updated_at      double precision NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS trading_results (
        id               bigserial PRIMARY KEY,
        leader_wallet    text,
        transaction_hash text,
        condition_id     text,
        side             text,
        title            text,
        outcome          text,
        price            text,
        size             text,
        usdc_size        text,
        event_timestamp  double precision,
        result           text,
        reason           text,
        recorded_at      double precision NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS signals (
        signal_id    text PRIMARY KEY,
        payload_json jsonb NOT NULL,
        created_at   double precision NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS accounting_fills (
        id            bigserial PRIMARY KEY,
        user_id       text NOT NULL,
        order_id      text,
        condition_id  text,
        token_id      text,
        side          text,
        size_tokens   text,
        price         text,
        usdc_value    text,
        leader_id     text,
        category      text,
        reason        text,
        payload_json  jsonb,
        created_at    double precision NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS positions_ledger (
        user_id         text NOT NULL,
        condition_id    text NOT NULL,
        token_id        text NOT NULL,
        size_tokens     text NOT NULL,
        avg_entry_price text NOT NULL,
        leader_id       text,
        category        text,
        created_at      double precision NOT NULL,
        updated_at      double precision NOT NULL,
        PRIMARY KEY (user_id, condition_id, token_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS daily_equity_snapshots (
        user_id            text NOT NULL,
        snapshot_date      text NOT NULL,
        equity_usdc        text NOT NULL,
        free_cash_usdc     text NOT NULL,
        reserved_cash_usdc text NOT NULL,
        created_at         double precision NOT NULL,
        updated_at         double precision NOT NULL,
        PRIMARY KEY (user_id, snapshot_date)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS arb_baskets (
        basket_id    text PRIMARY KEY,
        condition_id text NOT NULL,
        state        text NOT NULL,
        data_json    jsonb NOT NULL,
        updated_at   double precision NOT NULL
    )
    """,
]


# ---------------------------------------------------------------------------
# Discovery tables (translated from storage/sqlite/discovery_migrations.py)
# ---------------------------------------------------------------------------

_DISCOVERY_DDL = [
    """
    CREATE TABLE IF NOT EXISTS discovery_traders (
        wallet        text PRIMARY KEY,
        first_seen_ts bigint NOT NULL,
        last_seen_ts  bigint NOT NULL,
        trade_count   integer NOT NULL DEFAULT 0,
        source        text NOT NULL DEFAULT 'activity_api',
        meta_json     jsonb NOT NULL DEFAULT '{}'::jsonb
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS discovery_markets (
        condition_id  text PRIMARY KEY,
        question      text NOT NULL DEFAULT '',
        slug          text NOT NULL DEFAULT '',
        event_slug    text NOT NULL DEFAULT '',
        category      text NOT NULL DEFAULT '',
        end_date_ts   bigint,
        active        boolean NOT NULL DEFAULT TRUE,
        resolved      boolean NOT NULL DEFAULT FALSE,
        neg_risk      boolean NOT NULL DEFAULT FALSE,
        yes_token_id  text NOT NULL DEFAULT '',
        no_token_id   text NOT NULL DEFAULT '',
        tokens_json   jsonb NOT NULL DEFAULT '[]'::jsonb,
        meta_json     jsonb NOT NULL DEFAULT '{}'::jsonb,
        updated_at    bigint NOT NULL
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS discovery_trades (
        id            bigserial PRIMARY KEY,
        trader_wallet text NOT NULL,
        condition_id  text NOT NULL,
        token_id      text NOT NULL,
        side          text NOT NULL,
        size          text NOT NULL,
        price         text NOT NULL,
        usdc_size     text NOT NULL,
        tx_hash       text NOT NULL,
        event_ts      bigint NOT NULL,
        trade_id      text NOT NULL DEFAULT '',
        fee           text NOT NULL DEFAULT '0',
        outcome       text NOT NULL DEFAULT '',
        title         text NOT NULL DEFAULT '',
        market_slug   text NOT NULL DEFAULT '',
        event_slug    text NOT NULL DEFAULT '',
        raw_json      jsonb NOT NULL DEFAULT '{}'::jsonb,
        source        text NOT NULL DEFAULT 'activity_api',
        created_at    double precision NOT NULL,
        UNIQUE(trader_wallet, token_id, tx_hash, trade_id, side, event_ts)
    )
    """,
    """CREATE INDEX IF NOT EXISTS idx_disc_trades_wallet_cond
       ON discovery_trades(trader_wallet, condition_id)""",
    """CREATE INDEX IF NOT EXISTS idx_disc_trades_wallet_ts
       ON discovery_trades(trader_wallet, event_ts)""",
    """CREATE INDEX IF NOT EXISTS idx_disc_trades_cond
       ON discovery_trades(condition_id)""",
    """CREATE INDEX IF NOT EXISTS idx_disc_trades_tx
       ON discovery_trades(tx_hash)""",

    """
    CREATE TABLE IF NOT EXISTS discovery_position_snapshots (
        id             bigserial PRIMARY KEY,
        trader_wallet  text NOT NULL,
        condition_id   text NOT NULL,
        token_id       text NOT NULL,
        size           text NOT NULL,
        avg_price      text NOT NULL,
        current_value  text NOT NULL DEFAULT '0',
        current_price  text NOT NULL DEFAULT '0',
        realized_pnl   text NOT NULL DEFAULT '0',
        snapshot_ts    bigint NOT NULL,
        raw_json       jsonb NOT NULL DEFAULT '{}'::jsonb,
        -- Distinguishes leader-wallet snapshots from the bot's own
        -- wallet snapshots so the bootup banner + analytics can split
        -- them cleanly. Both share the same shape so the indexer
        -- doesn't fork.
        wallet_role    text NOT NULL DEFAULT 'leader'
        -- cubic P2: uniqueness intentionally NOT inline. We add a
        -- ``wallet_role``-aware unique index further down so a
        -- leader-side snapshot and an own-side snapshot at the same
        -- (trader_wallet, condition_id, token_id, snapshot_ts) tuple
        -- can coexist without one silently dropping the other.
    )
    """,
    """CREATE INDEX IF NOT EXISTS idx_disc_pos_snap_wallet
       ON discovery_position_snapshots(trader_wallet)""",
    # cubic P2: drop the legacy non-role-aware UNIQUE constraint
    # (autogenerated name from the prior inline ``UNIQUE(...)``).
    # Idempotent — silently no-ops on fresh DBs where the constraint
    # was never inlined to begin with.
    """
    DO $$
    DECLARE
      cname text;
    BEGIN
      SELECT conname INTO cname
      FROM pg_constraint
      WHERE conrelid = 'discovery_position_snapshots'::regclass
        AND contype = 'u'
        AND pg_get_constraintdef(oid) =
            'UNIQUE (trader_wallet, condition_id, token_id, snapshot_ts)';
      IF cname IS NOT NULL THEN
        EXECUTE format(
          'ALTER TABLE discovery_position_snapshots DROP CONSTRAINT %I', cname
        );
      END IF;
    END$$
    """,
    # New unique index keyed on the role too so leader/own snapshots
    # at the same identity tuple don't collide.
    """CREATE UNIQUE INDEX IF NOT EXISTS idx_disc_pos_snap_unique_v2
       ON discovery_position_snapshots(trader_wallet, condition_id,
                                       token_id, snapshot_ts, wallet_role)""",
    # cubic P1: idempotent column-add MUST run before the role-keyed
    # index. On a DB created before ``wallet_role`` existed, creating the
    # index first would fail because the column it references isn't
    # there yet. ``ADD COLUMN IF NOT EXISTS`` is a no-op on fresh DBs
    # where the column was created with the table at line 458.
    """ALTER TABLE discovery_position_snapshots
       ADD COLUMN IF NOT EXISTS wallet_role text NOT NULL DEFAULT 'leader'""",
    """CREATE INDEX IF NOT EXISTS idx_disc_pos_snap_role_ts
       ON discovery_position_snapshots(wallet_role, snapshot_ts DESC)""",

    """
    CREATE TABLE IF NOT EXISTS discovery_closed_positions (
        id              bigserial PRIMARY KEY,
        trader_wallet   text NOT NULL,
        condition_id    text NOT NULL,
        token_id        text NOT NULL,
        entry_size      text NOT NULL,
        entry_avg_price text NOT NULL,
        exit_avg_price  text NOT NULL,
        realized_pnl    text NOT NULL,
        closed_ts       bigint NOT NULL,
        close_reason    text NOT NULL DEFAULT 'trade',
        raw_json        jsonb NOT NULL DEFAULT '{}'::jsonb,
        UNIQUE(trader_wallet, condition_id, closed_ts)
    )
    """,
    """CREATE INDEX IF NOT EXISTS idx_disc_closed_wallet
       ON discovery_closed_positions(trader_wallet)""",

    """
    CREATE TABLE IF NOT EXISTS discovery_orderbook_snapshots (
        id                   bigserial PRIMARY KEY,
        token_id             text NOT NULL,
        condition_id         text NOT NULL,
        snapshot_ts          bigint NOT NULL,
        trader_wallet        text NOT NULL DEFAULT '',
        reference_tx_hash    text NOT NULL DEFAULT '',
        reference_trade_id   text NOT NULL DEFAULT '',
        reference_event_ts   bigint NOT NULL DEFAULT 0,
        bids_json            jsonb NOT NULL DEFAULT '[]'::jsonb,
        asks_json            jsonb NOT NULL DEFAULT '[]'::jsonb,
        best_bid             text,
        best_ask             text,
        mid_price            text,
        spread               text,
        UNIQUE(token_id, snapshot_ts, reference_tx_hash)
    )
    """,
    """CREATE INDEX IF NOT EXISTS idx_disc_ob_token
       ON discovery_orderbook_snapshots(token_id)""",
    """CREATE INDEX IF NOT EXISTS idx_disc_ob_ref_tx
       ON discovery_orderbook_snapshots(reference_tx_hash)""",

    """
    CREATE TABLE IF NOT EXISTS discovery_episodes (
        episode_id            text PRIMARY KEY,
        trader_wallet         text NOT NULL,
        condition_id          text NOT NULL,
        token_id              text NOT NULL,
        state                 text NOT NULL DEFAULT 'open',
        direction             text NOT NULL DEFAULT '',
        entry_size            text NOT NULL DEFAULT '0',
        entry_avg_price       text NOT NULL DEFAULT '0',
        exit_size             text NOT NULL DEFAULT '0',
        exit_avg_price        text NOT NULL DEFAULT '0',
        realized_pnl          text NOT NULL DEFAULT '0',
        opened_ts             bigint NOT NULL,
        closed_ts             bigint,
        context_buckets_json  jsonb NOT NULL DEFAULT '{}'::jsonb
    )
    """,
    """CREATE INDEX IF NOT EXISTS idx_disc_ep_wallet
       ON discovery_episodes(trader_wallet)""",
    """CREATE INDEX IF NOT EXISTS idx_disc_ep_cond
       ON discovery_episodes(condition_id)""",
    """CREATE INDEX IF NOT EXISTS idx_disc_ep_state
       ON discovery_episodes(state)""",

    """
    CREATE TABLE IF NOT EXISTS discovery_episode_events (
        id          bigserial PRIMARY KEY,
        episode_id  text NOT NULL REFERENCES discovery_episodes(episode_id),
        trade_id    text NOT NULL,
        side        text NOT NULL,
        size        text NOT NULL,
        price       text NOT NULL,
        usdc_size   text NOT NULL,
        ts          bigint NOT NULL,
        event_type  text NOT NULL DEFAULT ''
    )
    """,
    """CREATE INDEX IF NOT EXISTS idx_disc_ep_ev_episode
       ON discovery_episode_events(episode_id)""",
    # Unique constraint backing ``ON CONFLICT (episode_id, trade_id) DO
    # NOTHING`` in ``discovery_repo.upsert_episode``. Without it that
    # clause is a no-op against the table's bigserial PK and duplicates
    # are silently inserted, skewing leader-event counts.
    #
    # Review fix (cubic P1 on PR #110): a redeployed environment that
    # ran the prior ``ON CONFLICT DO NOTHING`` clause (which silently
    # inserted duplicates) will already hold duplicate rows. Creating
    # the unique index without first deduplicating would fail the
    # migration. Keep the lowest-``id`` row per ``(episode_id, trade_id)``
    # — that matches the "first event observed" semantic the table was
    # always intended to hold — then add the unique index. Both
    # statements are idempotent: the DELETE is a no-op on a clean DB,
    # and ``CREATE UNIQUE INDEX IF NOT EXISTS`` skips on re-run.
    """
    DELETE FROM discovery_episode_events a
    USING discovery_episode_events b
    WHERE a.id > b.id
      AND a.episode_id = b.episode_id
      AND a.trade_id   = b.trade_id
    """,
    """CREATE UNIQUE INDEX IF NOT EXISTS idx_disc_ep_ev_unique
       ON discovery_episode_events(episode_id, trade_id)""",

    """
    CREATE TABLE IF NOT EXISTS discovery_context_buckets (
        id          bigserial PRIMARY KEY,
        entity_type text NOT NULL,
        entity_id   text NOT NULL,
        dimension   text NOT NULL,
        bucket      text NOT NULL,
        raw_value   text NOT NULL DEFAULT '',
        UNIQUE(entity_type, entity_id, dimension)
    )
    """,
    """CREATE INDEX IF NOT EXISTS idx_disc_ctx_entity
       ON discovery_context_buckets(entity_type, entity_id)""",

    """
    CREATE TABLE IF NOT EXISTS falcon_discovery_evaluations (
        wallet text PRIMARY KEY,
        status text NOT NULL DEFAULT 'shadow',
        final_score double precision NOT NULL DEFAULT 0,
        falcon_quality_score double precision NOT NULL DEFAULT 0,
        copyability_score double precision NOT NULL DEFAULT 0,
        recent_state_score double precision NOT NULL DEFAULT 0,
        market_fit_score double precision NOT NULL DEFAULT 0,

        h_score double precision,
        roi_15d double precision,
        win_rate_15d double precision,
        sharpe_15d double precision,
        pnl_15d double precision,
        total_trades_15d integer,
        markets_traded_15d integer,
        trajectory text,

        total_grouped_signals integer NOT NULL DEFAULT 0,
        meaningful_signals integer NOT NULL DEFAULT 0,
        copyable_signals integer NOT NULL DEFAULT 0,
        copyable_signal_ratio double precision NOT NULL DEFAULT 0,
        copyable_notional_ratio double precision NOT NULL DEFAULT 0,

        sports_esports_ratio double precision NOT NULL DEFAULT 0,
        fast_crypto_signal_ratio double precision NOT NULL DEFAULT 0,
        fast_crypto_notional_ratio double precision NOT NULL DEFAULT 0,

        pnl_7d text NOT NULL DEFAULT '0',
        pnl_3d text NOT NULL DEFAULT '0',
        rejection_reason text,
        eligible_live boolean NOT NULL DEFAULT false,
        eligible_shadow boolean NOT NULL DEFAULT false,

        balance_usdc text NOT NULL DEFAULT '0',
        config_hash text NOT NULL DEFAULT '',
        evaluation_context_json jsonb NOT NULL DEFAULT '{}'::jsonb,
        falcon_raw_json jsonb NOT NULL DEFAULT '{}'::jsonb,
        replay_json jsonb NOT NULL DEFAULT '{}'::jsonb,
        updated_at double precision NOT NULL
    )
    """,
    """ALTER TABLE falcon_discovery_evaluations
       ADD COLUMN IF NOT EXISTS balance_usdc text NOT NULL DEFAULT '0'""",
    """ALTER TABLE falcon_discovery_evaluations
       ADD COLUMN IF NOT EXISTS config_hash text NOT NULL DEFAULT ''""",
    """ALTER TABLE falcon_discovery_evaluations
       ADD COLUMN IF NOT EXISTS evaluation_context_json jsonb NOT NULL DEFAULT '{}'::jsonb""",
    """CREATE INDEX IF NOT EXISTS idx_falcon_eval_status_score
       ON falcon_discovery_evaluations(status, final_score DESC)""",
    """CREATE INDEX IF NOT EXISTS idx_falcon_eval_live
       ON falcon_discovery_evaluations(eligible_live, final_score DESC)""",
]


async def apply_migrations(conn: asyncpg.Connection) -> None:
    """Apply the full Postgres schema migration atomically.

    All CREATE TABLE / INDEX statements run inside a single asyncpg
    transaction so a partial-apply on crash is impossible. Mirrors the
    SQLite BEGIN IMMEDIATE wrap from the audit work.

    Idempotent: every statement is ``CREATE TABLE IF NOT EXISTS`` /
    ``CREATE INDEX IF NOT EXISTS`` so re-running on a populated DB is
    a no-op.
    """
    async with conn.transaction():
        for stmt in _CORE_DDL:
            await conn.execute(stmt)
        for stmt in _DISCOVERY_DDL:
            await conn.execute(stmt)
