"""Release gate: Postgres schema migration produces the expected tables.

Spins up an ephemeral ``postgres:16-alpine`` container via
testcontainers, applies ``storage.postgres.migrations.apply_migrations``,
and asserts every table from the previous SQLite schema exists with
the right shape.

Pins the schema contract so a future refactor that drops a table or
renames a column breaks the gate immediately.

Skips automatically if Docker is unavailable (developer machine without
the daemon running) so the test stays runnable locally without
requiring docker for every PR.
"""

from __future__ import annotations

import pytest

# Skip the whole module if Docker is unreachable. ``testcontainers`` will
# raise on first container startup, but we want a clean skip message
# rather than a stacktrace in CI environments where Docker is genuinely
# absent.
pytest.importorskip("testcontainers.postgres")


_EXPECTED_TABLES = {
    # Core
    "orders",
    "trading_control",
    "fills",
    "positions",
    "processed_trades",
    "leader_cursors",
    "market_structure_events",
    "market_structure_event_applied",
    "risk_blocks",
    "risk_triggers",
    "order_reservations",
    # Legacy / supporting (still read by accountingService etc.)
    "user_positions",
    "user_activities",
    "trading_results",
    "signals",
    "accounting_fills",
    "positions_ledger",
    "daily_equity_snapshots",
    "arb_baskets",
    # Discovery
    "discovery_cycles",
    "discovery_candidate_snapshots",
    "discovery_score_history",
    "leader_roster",
    "wallet360_snapshots",
    "wallet_pnl_daily",
    "wallet_rank_history",
    "wallet_metric_timeseries",
    "wallet_backfill_state",
    "app_schema_migrations",
}

_REMOVED_DISCOVERY_TABLES = {
    "discovered_leaders",
    "discovery_traders",
    "discovery_markets",
    "discovery_trades",
    "discovery_position_snapshots",
    "discovery_closed_positions",
    "discovery_orderbook_snapshots",
    "discovery_episodes",
    "discovery_episode_events",
    "discovery_context_buckets",
}


@pytest.fixture(scope="module")
def _pg_container():
    try:
        from testcontainers.postgres import PostgresContainer
        ctr = PostgresContainer("postgres:16-alpine")
        ctr.start()
    except Exception as exc:
        pytest.skip(f"Docker / postgres image unavailable: {exc!r}")
    try:
        yield ctr
    finally:
        ctr.stop()


@pytest.mark.asyncio
async def test_migration_creates_every_expected_table(_pg_container) -> None:
    import asyncpg

    from storage.postgres.migrations import apply_migrations

    dsn = _pg_container.get_connection_url(driver="asyncpg").replace(
        "postgresql+asyncpg://", "postgresql://"
    )
    conn = await asyncpg.connect(dsn)
    try:
        await apply_migrations(conn)
        rows = await conn.fetch(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'public'"
        )
        present = {r["table_name"] for r in rows}
        missing = _EXPECTED_TABLES - present
        assert not missing, (
            f"PG migration missing expected tables: {sorted(missing)}"
        )
        assert present.isdisjoint(_REMOVED_DISCOVERY_TABLES)
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_migration_is_idempotent(_pg_container) -> None:
    """Re-applying must not raise — every statement is IF NOT EXISTS."""
    import asyncpg

    from storage.postgres.migrations import apply_migrations

    dsn = _pg_container.get_connection_url(driver="asyncpg").replace(
        "postgresql+asyncpg://", "postgresql://"
    )
    conn = await asyncpg.connect(dsn)
    try:
        await apply_migrations(conn)
        # Second apply on a populated DB must be a no-op.
        await apply_migrations(conn)
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_discovery_reset_preserves_existing_trading_rows(_pg_container) -> None:
    """The destructive rewrite is scoped to superseded discovery data."""
    import asyncpg

    from storage.postgres.migrations import apply_migrations

    dsn = _pg_container.get_connection_url(driver="asyncpg").replace(
        "postgresql+asyncpg://", "postgresql://"
    )
    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute("DROP SCHEMA public CASCADE")
        await conn.execute("CREATE SCHEMA public")
        await conn.execute(
            """
            CREATE TABLE positions (
                user_id text NOT NULL,
                condition_id text NOT NULL,
                token_id text NOT NULL,
                leader_id text,
                size_tokens text NOT NULL,
                avg_entry_price text NOT NULL,
                updated_at timestamptz NOT NULL DEFAULT now(),
                PRIMARY KEY (user_id, condition_id, token_id)
            )
            """
        )
        await conn.execute(
            """
            INSERT INTO positions (
                user_id, condition_id, token_id, size_tokens, avg_entry_price
            ) VALUES ('user', 'condition', 'token', '5', '0.4')
            """
        )
        await conn.execute("CREATE TABLE discovery_traders (wallet text)")
        await conn.execute("INSERT INTO discovery_traders VALUES ('legacy')")

        await apply_migrations(conn)
        assert await conn.fetchval(
            "SELECT size_tokens FROM positions WHERE user_id = 'user'"
        ) == "5"
        assert await conn.fetchval(
            "SELECT to_regclass('public.discovery_traders')"
        ) is None
        assert await conn.fetchval(
            "SELECT to_regclass('public.leader_roster')"
        ) == "leader_roster"

        await apply_migrations(conn)
        assert await conn.fetchval("SELECT count(*) FROM positions") == 1
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_positions_table_has_3col_pk(_pg_container) -> None:
    """The canonical PositionKey identity (user_id, condition_id, token_id)
    must be the positions table PK — Batch 1's data-correctness invariant
    carries over to PG."""
    import asyncpg

    from storage.postgres.migrations import apply_migrations

    dsn = _pg_container.get_connection_url(driver="asyncpg").replace(
        "postgresql+asyncpg://", "postgresql://"
    )
    conn = await asyncpg.connect(dsn)
    try:
        await apply_migrations(conn)
        rows = await conn.fetch(
            """
            SELECT a.attname AS column_name
            FROM pg_constraint c
            JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY(c.conkey)
            WHERE c.contype = 'p'
              AND c.conrelid = 'positions'::regclass
            ORDER BY a.attnum
            """
        )
        # cubic P2: the SQL already orders by ``a.attnum`` (definition
        # order). ``sorted()`` here destroyed that ordering and turned
        # the assertion into a set-equality check that would pass even
        # if the PK columns were defined out of order (which changes the
        # composite index storage). Compare against the canonical
        # ``(user_id, condition_id, token_id)`` order instead.
        pk_cols = [r["column_name"] for r in rows]
        assert pk_cols == ["user_id", "condition_id", "token_id"], (
            f"positions PK must be (user_id, condition_id, token_id); got {pk_cols}"
        )
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_orders_order_id_unique_partial_index_present(_pg_container) -> None:
    """The partial unique index landed in Batch 1 (#40) must survive the
    Postgres translation."""
    import asyncpg

    from storage.postgres.migrations import apply_migrations

    dsn = _pg_container.get_connection_url(driver="asyncpg").replace(
        "postgresql+asyncpg://", "postgresql://"
    )
    conn = await asyncpg.connect(dsn)
    try:
        await apply_migrations(conn)
        idx_def = await conn.fetchval(
            "SELECT indexdef FROM pg_indexes WHERE indexname = 'idx_orders_order_id'"
        )
        assert idx_def is not None, "idx_orders_order_id index missing"
        assert "UNIQUE" in idx_def.upper()
        assert "ORDER_ID IS NOT NULL" in idx_def.upper(), (
            f"index must be partial on order_id IS NOT NULL; got {idx_def}"
        )
    finally:
        await conn.close()
