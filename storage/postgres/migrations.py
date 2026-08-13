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

from storage.postgres.v1_migration_runner import MigrationConnection, apply_v1_migrations


# ---------------------------------------------------------------------------
# Core tables (translated from storage/sqlite/migrations.py)
# ---------------------------------------------------------------------------

_CORE_DDL = [
    # ----- operator trading control --------------------------------------
    # Singleton, database-backed kill switch.  It defaults paused and is
    # checked immediately before every live venue submission.  Database
    # unavailability is also interpreted as paused by TradingPermissionService.
    """
    CREATE TABLE IF NOT EXISTS trading_control (
        singleton_id smallint PRIMARY KEY CHECK (singleton_id = 1),
        enabled      boolean NOT NULL DEFAULT false,
        generation   bigint NOT NULL DEFAULT 1,
        reason       text NOT NULL DEFAULT 'initially_paused',
        updated_at   timestamptz NOT NULL DEFAULT now(),
        updated_by   text NOT NULL DEFAULT 'migration'
    )
    """,
    """
    INSERT INTO trading_control(singleton_id, enabled, generation, reason, updated_by)
    VALUES (1, false, 1, 'initially_paused', 'migration')
    ON CONFLICT (singleton_id) DO NOTHING
    """,
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
    # P0-1: stable reconciler fill-dedup. The wall-clock ``fill_ts`` discriminator
    # let a re-applied settlement (after a crash / swallowed watermark error left
    # the reconciler recomputing the same delta) insert a second fill row in a
    # later second and double-apply the position delta. The reconciler now writes
    # ``dedup_key`` = the cumulative-matched total, unique per
    # (user_id, order_id, condition_id, side). ADD COLUMN before the index that
    # references it (cf. wallet_role ordering, P1-h). Partial index so
    # non-reconciler fills (dedup_key NULL) keep their fill_ts dedup unchanged.
    """ALTER TABLE fills ADD COLUMN IF NOT EXISTS dedup_key text""",
    # P0-1: make the legacy wall-clock fill_ts dedup PARTIAL so it governs only
    # non-reconciler fills (dedup_key IS NULL). Reconciler fills carry a stable
    # dedup_key and must NOT be subject to fill_ts uniqueness — two *distinct*
    # settlements in the same wall-clock second would otherwise falsely collide
    # and under-apply. Drop + recreate so existing DBs pick up the WHERE clause.
    """DROP INDEX IF EXISTS idx_fills_dedup""",
    """
    CREATE UNIQUE INDEX IF NOT EXISTS idx_fills_dedup
    ON fills(user_id, order_id, condition_id, side, fill_ts)
    WHERE dedup_key IS NULL
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS idx_fills_dedup_v2
    ON fills(user_id, order_id, condition_id, side, dedup_key)
    WHERE dedup_key IS NOT NULL
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_fills_user_condition
    ON fills(user_id, condition_id)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_fills_order ON fills(order_id)
    """,
    # ----- source transaction confirmation state ------------------------
    # One complete immutable JSON image keeps source reservations and
    # follower-scoped consumption claims crash-safe. The application owns
    # all mutations behind one asyncio lock and replaces this singleton in
    # one statement, so no check-then-mutate split exists across services.
    """
    CREATE TABLE IF NOT EXISTS source_transaction_state (
        singleton_id smallint PRIMARY KEY CHECK (singleton_id = 1),
        snapshot_json jsonb NOT NULL,
        updated_at    timestamptz NOT NULL DEFAULT now()
    )
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
# Trader discovery — single-generation schema
# ---------------------------------------------------------------------------

_DISCOVERY_RESET_ID = "trader_discovery_single_generation_20260713"

# This reset is deliberately scoped to data produced by the superseded
# discovery engines. Trading, order, fill, position, reservation, cursor, and
# control tables are never touched.
_LEGACY_DISCOVERY_TABLES = (
    "leader_discovery_run_candidates",
    "leader_discovery_runs",
    "falcon_discovery_evaluations",
    "discovery_context_buckets",
    "discovery_episode_events",
    "discovery_episodes",
    "discovery_orderbook_snapshots",
    "discovery_closed_positions",
    "discovery_position_snapshots",
    "discovery_trades",
    "discovery_markets",
    "discovery_traders",
    "discovered_leaders",
)

_DISCOVERY_DDL = [
    """
    CREATE TABLE IF NOT EXISTS discovery_cycles (
        cycle_id        text PRIMARY KEY,
        started_at      double precision NOT NULL,
        completed_at    double precision,
        status          text NOT NULL,
        config_hash     text NOT NULL,
        candidate_count integer NOT NULL DEFAULT 0,
        kept_count      integer NOT NULL DEFAULT 0,
        error_detail    text
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS discovery_candidate_snapshots (
        cycle_id              text NOT NULL REFERENCES discovery_cycles(cycle_id)
                              ON DELETE CASCADE,
        wallet                text NOT NULL,
        cycle_ts              double precision NOT NULL,
        hscore_json            jsonb NOT NULL,
        wallet360_json         jsonb NOT NULL,
        recent_trades_json     jsonb NOT NULL,
        market_quality_json    jsonb NOT NULL,
        social_json            jsonb NOT NULL,
        base_score             double precision NOT NULL,
        longitudinal_score     double precision,
        final_score            double precision NOT NULL,
        sub_scores_json        jsonb NOT NULL,
        rejected_reason        text,
        config_hash            text NOT NULL,
        PRIMARY KEY (cycle_id, wallet)
    )
    """,
    """CREATE INDEX IF NOT EXISTS idx_discovery_candidate_wallet_ts
       ON discovery_candidate_snapshots(wallet, cycle_ts DESC)""",
    """CREATE INDEX IF NOT EXISTS idx_discovery_candidate_score
       ON discovery_candidate_snapshots(final_score DESC, cycle_ts DESC)
       WHERE rejected_reason IS NULL""",
    """
    CREATE TABLE IF NOT EXISTS discovery_score_history (
        wallet             text NOT NULL,
        ts                 double precision NOT NULL,
        base_score         double precision NOT NULL,
        longitudinal_score double precision,
        final_score        double precision NOT NULL,
        sub_scores_json    jsonb NOT NULL,
        config_hash        text NOT NULL,
        PRIMARY KEY (wallet, ts)
    )
    """,
    """CREATE INDEX IF NOT EXISTS idx_discovery_score_history_ts
       ON discovery_score_history(ts DESC)""",
    """
    CREATE TABLE IF NOT EXISTS leader_roster (
        wallet             text PRIMARY KEY,
        tier               text NOT NULL DEFAULT 'watchlist'
                           CHECK (tier IN ('watchlist', 'active')),
        score              double precision NOT NULL DEFAULT 0,
        base_score         double precision NOT NULL DEFAULT 0,
        longitudinal_score double precision,
        cycles_in_tier     integer NOT NULL DEFAULT 0,
        promote_streak     integer NOT NULL DEFAULT 0,
        demote_streak      integer NOT NULL DEFAULT 0,
        tier_changed_at    double precision NOT NULL,
        first_promoted_at  double precision,
        last_seen_at       double precision,
        config_hash        text NOT NULL DEFAULT '',
        updated_at         double precision NOT NULL
    )
    """,
    """CREATE INDEX IF NOT EXISTS idx_leader_roster_tier_score
       ON leader_roster(tier, score DESC, wallet ASC)""",
    """
    CREATE TABLE IF NOT EXISTS wallet360_snapshots (
        wallet       text NOT NULL,
        ts           double precision NOT NULL,
        window_days  text NOT NULL CHECK (window_days IN ('1', '3', '7', '15')),
        payload_json jsonb NOT NULL,
        config_hash  text NOT NULL,
        PRIMARY KEY (wallet, ts, window_days)
    )
    """,
    """CREATE INDEX IF NOT EXISTS idx_wallet360_wallet_ts
       ON wallet360_snapshots(wallet, ts DESC)""",
    """
    CREATE TABLE IF NOT EXISTS wallet_pnl_daily (
        wallet       text NOT NULL,
        pnl_date     date NOT NULL,
        realized_pnl numeric NOT NULL,
        payload_json jsonb NOT NULL,
        updated_at   double precision NOT NULL,
        PRIMARY KEY (wallet, pnl_date)
    )
    """,
    """CREATE INDEX IF NOT EXISTS idx_wallet_pnl_daily_wallet_date
       ON wallet_pnl_daily(wallet, pnl_date DESC)""",
    """
    CREATE TABLE IF NOT EXISTS wallet_rank_history (
        wallet       text NOT NULL,
        ts           double precision NOT NULL,
        window_days  integer NOT NULL,
        rank          integer NOT NULL,
        score         double precision NOT NULL,
        payload_json jsonb NOT NULL,
        PRIMARY KEY (wallet, ts, window_days)
    )
    """,
    """CREATE INDEX IF NOT EXISTS idx_wallet_rank_wallet_ts
       ON wallet_rank_history(wallet, ts DESC)""",
    """
    CREATE TABLE IF NOT EXISTS wallet_metric_timeseries (
        wallet               text NOT NULL,
        ts                   double precision NOT NULL,
        sharpe               double precision NOT NULL,
        roi                  double precision NOT NULL,
        win_rate             double precision NOT NULL,
        rank                 integer NOT NULL,
        drawdown_pct         double precision NOT NULL,
        trade_count          integer NOT NULL,
        metrics_json         jsonb NOT NULL,
        config_hash          text NOT NULL,
        PRIMARY KEY (wallet, ts)
    )
    """,
    """CREATE INDEX IF NOT EXISTS idx_wallet_metrics_wallet_ts
       ON wallet_metric_timeseries(wallet, ts DESC)""",
    """
    CREATE TABLE IF NOT EXISTS wallet_backfill_state (
        wallet       text PRIMARY KEY,
        completed_at double precision NOT NULL,
        config_hash  text NOT NULL
    )
    """,
]


async def apply_migrations(conn: MigrationConnection) -> None:
    """Apply core schema and the one-time legacy-discovery reset atomically."""
    async with conn.transaction():
        for statement in _CORE_DDL:
            await conn.execute(statement)

        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS app_schema_migrations (
                migration_id text PRIMARY KEY,
                applied_at   timestamptz NOT NULL DEFAULT now()
            )
            """
        )
        already_reset = await conn.fetchval(
            "SELECT 1 FROM app_schema_migrations WHERE migration_id = $1",
            _DISCOVERY_RESET_ID,
        )
        if not already_reset:
            for table_name in _LEGACY_DISCOVERY_TABLES:
                await conn.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")
            await conn.execute(
                "INSERT INTO app_schema_migrations (migration_id) VALUES ($1)",
                _DISCOVERY_RESET_ID,
            )

        for statement in _DISCOVERY_DDL:
            await conn.execute(statement)

    # The v1 copy-task migration owns its own transaction (and records its
    # id in ``app_schema_migrations``), so it runs after the core DDL commits
    # rather than nested inside it. Both runners are idempotent; running the
    # v1 slice at every startup keeps deployed schemas complete without a
    # separate operator step. Referenced via the module global so tests can
    # observe/patch the seam.
    await apply_v1_migrations(conn)


__all__ = ["apply_migrations", "apply_v1_migrations"]
