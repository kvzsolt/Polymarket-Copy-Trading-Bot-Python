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
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
        )
        present = {r["table_name"] for r in rows}
        missing = _EXPECTED_TABLES - present
        assert not missing, f"PG migration missing expected tables: {sorted(missing)}"
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
        assert (
            await conn.fetchval("SELECT size_tokens FROM positions WHERE user_id = 'user'") == "5"
        )
        assert await conn.fetchval("SELECT to_regclass('public.discovery_traders')") is None
        assert await conn.fetchval("SELECT to_regclass('public.leader_roster')") == "leader_roster"

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


@pytest.mark.asyncio
async def test_position_lifecycle_upgrade_preserves_holdings_and_exact_claims(_pg_container):
    """An additive upgrade retains old rows and distinguishes active from settled."""
    import asyncpg
    from decimal import Decimal
    from domain.position_lifecycle import PositionLifecycle
    from storage.postgres.migrations import apply_migrations
    from storage.postgres.positions_repo import PositionRecord, PositionsRepo

    dsn = _pg_container.get_connection_url(driver="asyncpg").replace(
        "postgresql+asyncpg://", "postgresql://"
    )
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=2)
    try:
        async with pool.acquire() as conn:
            await apply_migrations(conn)
            await conn.execute("DELETE FROM positions")
            await conn.execute(
                "INSERT INTO positions(user_id, condition_id, token_id, size_tokens, avg_entry_price) VALUES ('lifecycle', 'legacy', 'legacy', '10', '0.4')"
            )
            await apply_migrations(conn)
        repo = PositionsRepo(pool)
        legacy = await repo.list_open_positions("lifecycle")
        assert len(legacy) == 1
        assert legacy[0].lifecycle is PositionLifecycle.UNKNOWN
        assert legacy[0].size_tokens == Decimal("10")
        await repo.upsert_position(
            PositionRecord(
                "lifecycle",
                "settled",
                "claim",
                None,
                Decimal("3.123456"),
                Decimal("0.4"),
                PositionLifecycle.SETTLED,
                Decimal("1"),
            )
        )
        assert len(await repo.list_open_positions("lifecycle")) == 1
        held = await repo.list_held_positions("lifecycle")
        claim = next(p for p in held if p.condition_id == "settled")
        assert claim.settlement_price == Decimal("1")
        assert claim.size_tokens == Decimal("3.123456")
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_guarded_prune_uses_durable_order_scope_and_retains_pending_orders(_pg_container):
    """Completed orders permit convergence; an unresolved order owns its inventory."""
    import asyncpg
    import json
    from decimal import Decimal
    from storage.postgres.migrations import apply_migrations
    from storage.postgres.positions_repo import PositionRecord, PositionsRepo
    from tests.test_positions_repo_set_position_size import VALID_TOKEN

    dsn = _pg_container.get_connection_url(driver="asyncpg").replace(
        "postgresql+asyncpg://", "postgresql://"
    )
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=2)
    try:
        async with pool.acquire() as conn:
            await apply_migrations(conn)
            await conn.execute("DELETE FROM positions WHERE user_id='prune'")
            await conn.execute("DELETE FROM orders WHERE user_id='prune'")
            await conn.execute(
                "INSERT INTO orders (idempotency_key,user_id,status,request_json) VALUES ('prune','prune','submitted_unconfirmed',$1::jsonb)",
                json.dumps({"condition_id": "condition", "token_id": VALID_TOKEN}),
            )
        repo = PositionsRepo(pool)
        await repo.upsert_position(
            PositionRecord("prune", "condition", VALID_TOKEN, None, Decimal("10"), Decimal("0.4"))
        )
        assert (
            await repo.delete_position_if_unchanged(
                "prune", "condition", token_id=VALID_TOKEN, expected_size=Decimal("10")
            )
            is False
        )
        async with pool.acquire() as conn:
            await conn.execute("UPDATE orders SET status='filled_reconciled' WHERE user_id='prune'")
        assert (
            await repo.delete_position_if_unchanged(
                "prune", "condition", token_id=VALID_TOKEN, expected_size=Decimal("9")
            )
            is False
        )
        assert (
            await repo.delete_position_if_unchanged(
                "prune", "condition", token_id=VALID_TOKEN, expected_size=Decimal("10")
            )
            is True
        )
        assert await repo.list_held_positions("prune") == []
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_active_sql_consumers_exclude_settled_and_retain_unknown(_pg_container):
    """Actual query results and market capacity agree on settled holdings."""
    import asyncpg
    from decimal import Decimal
    from domain.position_lifecycle import PositionLifecycle
    from storage.postgres.migrations import apply_migrations
    from storage.postgres.positions_repo import PositionRecord, PositionsRepo
    from storage.postgres.control_plane_query_repository import PostgresControlPlaneQueryRepository
    from storage.postgres.task_buy_capacity import PostgresTaskBuyCapacity
    from services.control_plane.contracts import PageRequest
    from domain.order_lifecycle import OrderIntent, OrderIntentKey, IntentSide, IntentReason
    from tests.test_active_position_query_contract import FixedClock

    dsn = _pg_container.get_connection_url(driver="asyncpg").replace(
        "postgresql+asyncpg://", "postgresql://"
    )
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=2)
    try:
        async with pool.acquire() as conn:
            await apply_migrations(conn)
        repo = PositionsRepo(pool)
        for state in PositionLifecycle:
            await repo.upsert_position(
                PositionRecord(
                    "mixed",
                    state.value,
                    state.value,
                    "mixed-leader",
                    Decimal("10"),
                    Decimal("0.4"),
                    state,
                    Decimal("1") if state is PositionLifecycle.SETTLED else None,
                )
            )
        reads = PostgresControlPlaneQueryRepository(pool, clock=FixedClock())
        page = await reads.list_positions("mixed", PageRequest(limit=10))
        assert len(page.items) == 2
        assert await reads.get_position("mixed", "settled") is None
        assert (await reads.leader_open_position_counts())["mixed-leader"] == 2
        intent = OrderIntent(
            key=OrderIntentKey(
                user_id="mixed",
                leader_wallet="mixed-leader",
                condition_id="new",
                token_id="new",
                side=IntentSide.BUY,
                reason=IntentReason.COPY_BUY,
            ),
            size=Decimal("1"),
            price=Decimal("0.5"),
            max_slippage_bps=100,
            created_at=FixedClock().now(),
            max_market_notional=Decimal("10"),
            max_open_markets=3,
        )
        async with pool.acquire() as conn:
            assert await PostgresTaskBuyCapacity.permits(conn, intent, Decimal("1"))
    finally:
        await pool.close()
