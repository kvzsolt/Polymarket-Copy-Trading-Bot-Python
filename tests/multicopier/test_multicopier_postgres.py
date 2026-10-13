"""§9 money-path scenarios against disposable Postgres (run explicitly):

    python -m pytest tests/multicopier/test_multicopier_postgres.py -m integration

Two copier accounts are composed through the REAL production bootstrap
(``build_user_state`` → ``build_context_maps``) over one migrated Postgres
schema, each with its own fake venue gateway, and driven through the REAL
``orchestrators.trade_pipeline.execute_trades`` entry point. Proves the SQL
faces of the addon's acceptance matrix:

* bullet 1 — one leader event → independently sized intents, DISTINCT
  persisted reservations per account, per-account claim/dedup (no
  cross-account dedup, no cross-account replay suppression);
* bullet 3 — two leaders concurrently targeting one wallet's same token stay
  inside the account's shared exposure limit (one coordination/reservation
  domain, no overlapping second action);
* bullet 4 — A's insufficient balance never consumes B's budget or attaches
  B's orders;
* bullet 7 — a duplicate source event and a replayed private fill never
  double-account or double-submit for one copier;
* bullet 8 — restart restores both accounts independently: one with a lost
  POST response (no venue id, recovered from its own private history), one
  with a partial fill that keeps accumulating;
* bullet 14 — startup reconciliation with no authoritative evidence never
  releases an unresolved no-ID reservation (composition twin of the T9 SQL
  tests in ``tests/test_reservation_expiry_no_id_release_postgres.py``).
"""

from __future__ import annotations

import asyncio
import contextlib
import tempfile
import time
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from pmcopy.domain.execution import ExecutionMetadata
from pmcopy.integrations.polymarket.models import VenueFill, VenueSubmitResult
from pmcopy.orchestrators.runtime_bootstrap import (
    build_context_maps,
    build_trading_context,
    build_user_state,
    startup_reconcile_unconfirmed,
)
from pmcopy.orchestrators.trade_pipeline import (
    execute_trades,
    get_reconciliation_service,
)
from pmcopy.services.execution.pending_order_reconciliation import (
    PendingOrderReconciliationQueryService,
)
from pmcopy.storage.postgres.migrations import apply_migrations

from tests.multicopier.conftest import (
    CONDITION,
    FUNDER_ALPHA,
    FUNDER_BETA,
    LEADER_1,
    LEADER_2,
    TOKEN,
    USER_ALPHA,
    USER_BETA,
    follow,
    make_config,
    recording_gateway,
    runtime_user,
)


def leader_buy_event(*, tx_hash: str, usdc: str = "100", price: str = "0.5") -> dict:
    # Fresh source timestamp: the copy freshness policy rejects old signals,
    # so every event is minted at the current instant (deterministic relative
    # to the test's own clock).
    return {
        "transactionHash": tx_hash,
        "timestamp": int(time.time()),
        "type": "TRADE",
        "side": "BUY",
        "asset": TOKEN,
        "conditionId": CONDITION,
        "size": str(Decimal(usdc) / Decimal(price)),
        "price": price,
        "usdcSize": usdc,
        "outcome": "YES",
    }


@contextlib.asynccontextmanager
async def _pg_pool():
    try:
        from tests.integration_postgres import PostgresContainer
        import asyncpg
    except Exception as exc:  # pragma: no cover
        pytest.skip(f"Docker / postgres image unavailable: {exc!r}")
    ctr = PostgresContainer("postgres:16-alpine")
    try:
        ctr.start()
    except Exception as exc:  # pragma: no cover
        pytest.skip(f"Docker / postgres image unavailable: {exc!r}")
    try:
        dsn = ctr.get_connection_url(driver="asyncpg").replace(
            "postgresql+asyncpg://", "postgresql://"
        )
        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=8)
        async with pool.acquire() as conn:
            await apply_migrations(conn)
        try:
            yield pool
        finally:
            await pool.close()
    finally:
        ctr.stop()


@pytest.fixture(autouse=True)
def _hermetic_balances(monkeypatch):
    """Deterministic per-wallet balances; never touch an RPC."""

    balances = {FUNDER_ALPHA: Decimal("1000"), FUNDER_BETA: Decimal("1000")}

    async def _fake_balance(wallet, provider_runtime=None):
        return balances.get(wallet)

    import pmcopy.orchestrators.trade_pipeline as trade_pipeline

    monkeypatch.setattr(trade_pipeline, "get_my_balance", _fake_balance)
    yield balances


_COPIERS_FILE_BODY = """schema_version: 2
copiers: []
"""


def _config():
    """The production schema-v2 deployment shape.

    With ``app.copiers_config_file`` set, the legacy global wallet funder is
    unset (the v2 loader never merges legacy wallet env), so every
    per-account ``_funder_wallet_address`` resolution falls through to the
    binding's own ``context.wallet_address`` — exactly the multi-copier
    composition the addon specifies. A real (empty) roster file backs the
    reference so AppConfig's paper-mode validation passes.
    """
    target = Path(tempfile.mkdtemp(prefix="multicopier-copiers-")) / "copiers.yml"
    target.write_text(_COPIERS_FILE_BODY, encoding="utf-8")
    return make_config(
        **{
            "app": {"copiers_config_file": str(target)},
            "wallet": {"funder_address": ""},
            "execution": {
                "trading_mode": "paper",
                "reservations_enabled": True,
                # Default per-position regime cap (20) would reject the
                # larger copier's sized intent; raise it for the matrix.
                "max_position_size_usdc": 1000,
            },
            # Resolve lost-POST rows immediately in tests: no grace wait, one
            # absence scan would still be required for the ABSENCE path (the
            # match path used here needs none).
            "reconciliation": {"ambiguous_recovery": {"grace_seconds": 0}},
        }
    )


# ---------------------------------------------------------------------------
# F5-4 — the copiers roster tempdir must not leak (cleaned fixture contracts)
# ---------------------------------------------------------------------------


def test_config_registers_its_copiers_tmpdir_for_cleanup(_hermetic_balances):
    """Every ``_config()`` tempdir is registered so teardown removes it.

    ``_config()`` historically called ``tempfile.mkdtemp`` with no cleanup:
    every test leaked a ``multicopier-copiers-*`` directory on the host.
    The registry below is drained by this module's autouse fixture after
    each test.
    """
    config = _config()
    copiers_path = Path(config.app.copiers_config_file)
    assert copiers_path.is_file()
    assert any(Path(entry.name) == copiers_path.parent for entry in _tmp_config_dirs()), (
        "every _config() tempdir must be registered for fixture teardown"
    )


def test_registered_copiers_tmpdirs_are_removed_on_drain(_hermetic_balances):
    config = _config()
    copiers_dir = Path(config.app.copiers_config_file).parent
    assert copiers_dir.is_dir()
    # The exact operation the autouse fixture performs at teardown:
    _drain_tmp_config_dirs()
    assert not copiers_dir.exists(), "draining the registry must remove the tempdir"
    assert _tmp_config_dirs() == []


def _gateway(order_prefix: str):
    gateway = recording_gateway()
    submit_result = VenueSubmitResult(
        success=True, order_id=f"{order_prefix}-ord-1", status="submitted"
    )

    async def _record_and_submit(request):
        gateway.submissions.append(request)
        return submit_result

    gateway.submit_marketable_order = _record_and_submit
    gateway.get_price = AsyncMock(return_value={"price": "0.50"})
    gateway.get_fills = AsyncMock(return_value=[])
    gateway.get_execution_metadata = AsyncMock(
        return_value=ExecutionMetadata(
            token_id=TOKEN,
            tick_size=Decimal("0.01"),
            min_order_size=Decimal("0.001"),
            neg_risk=False,
        )
    )
    return gateway


async def _compose_two_accounts(pool, config, *, follows_alpha, follows_beta, balances):
    alpha = runtime_user(USER_ALPHA, FUNDER_ALPHA, follows_alpha)
    beta = runtime_user(USER_BETA, FUNDER_BETA, follows_beta)
    gateway_a = _gateway("alpha")
    gateway_b = _gateway("beta")
    state_a = build_user_state(alpha, None, pool, config, clob_gateway=gateway_a)
    state_b = build_user_state(beta, None, pool, config, clob_gateway=gateway_b)

    # The reservation service's balance gate reads the wallet's USDC via the
    # production closure; pin it to the deterministic per-wallet map.
    async def _balance_for(wallet):
        return balances.get(wallet)

    for state in (state_a, state_b):
        state.execution_coordinator.reservation_service.get_balance_func = _balance_for
    contexts_by_leader, risk_contexts = await build_context_maps(
        [alpha, beta],
        {USER_ALPHA: state_a, USER_BETA: state_b},
        sorted(set(follows_alpha) | set(follows_beta)),
        config,
    )
    return {
        "users": (alpha, beta),
        "gateways": (gateway_a, gateway_b),
        "states": (state_a, state_b),
        "by_leader": contexts_by_leader,
        "risk": risk_contexts,
    }


async def _reservations_rows(pool, user_id: str | None = None):
    async with pool.acquire() as conn:
        if user_id is None:
            return await conn.fetch("SELECT * FROM order_reservations ORDER BY id")
        return await conn.fetch(
            "SELECT * FROM order_reservations WHERE user_id = $1 ORDER BY id", user_id
        )


async def _orders_rows(pool, user_id: str):
    async with pool.acquire() as conn:
        return await conn.fetch(
            "SELECT * FROM orders WHERE user_id = $1 ORDER BY created_at", user_id
        )


async def _reconcile_all(state, context, *, confirm):
    pending = PendingOrderReconciliationQueryService(state.orders_repo)
    records = await pending.list_records(user_id=context.user_id, limit=50)
    reconciler = get_reconciliation_service(context)
    applied = []
    for side, record in records:
        outcome = await reconciler.reconcile_trade_record(record, side, confirm_fill=confirm)
        applied.append((side, outcome))
    return applied


def _confirm(status: str, matched: str, avg: str):
    return AsyncMock(return_value={"status": status, "size_matched": matched, "avg_price": avg})


# ---------------------------------------------------------------------------
# §9 bullet 1 — one event, two copiers: independent intents + reservations
# ---------------------------------------------------------------------------


async def test_same_leader_event_yields_independent_intents_and_reservations(
    _hermetic_balances,
):
    async with _pg_pool() as pool:
        config = _config()
        world = await _compose_two_accounts(
            pool,
            config,
            balances=_hermetic_balances,
            follows_alpha={
                LEADER_1: follow(USER_ALPHA, LEADER_1, allocation_pct="0.10", max_trade_usdc="25")
            },
            follows_beta={
                LEADER_1: follow(USER_BETA, LEADER_1, allocation_pct="0.50", max_trade_usdc="40")
            },
        )
        ctx_a = world["by_leader"][LEADER_1][0]
        ctx_b = world["by_leader"][LEADER_1][1]
        assert {ctx_a.user_id, ctx_b.user_id} == {USER_ALPHA, USER_BETA}

        event = leader_buy_event(tx_hash="0xtx-one-event")
        now = time.time()
        await execute_trades(ctx_a, LEADER_1, [event], now_ts=now)
        await execute_trades(ctx_b, LEADER_1, [event], now_ts=now)

        # Independently sized intents: alpha 10% of 100 → 10 quote; beta 50%
        # of 100 → 50 quote capped at its 40 follow cap. Each account
        # submitted exactly one order through its OWN gateway.
        gateway_a, gateway_b = world["gateways"]
        assert len(gateway_a.submissions) == 1
        assert len(gateway_b.submissions) == 1
        assert gateway_a.submissions[0].size == Decimal("10")
        assert gateway_b.submissions[0].size == Decimal("40")

        # DISTINCT persisted reservations, each scoped to its own account.
        rows = await _reservations_rows(pool)
        assert len(rows) == 2
        by_user = {row["user_id"]: row for row in rows}
        assert set(by_user) == {USER_ALPHA, USER_BETA}
        assert by_user[USER_ALPHA]["id"] != by_user[USER_BETA]["id"]
        assert Decimal(str(by_user[USER_ALPHA]["reserved_usdc"])) == Decimal("10")
        assert Decimal(str(by_user[USER_BETA]["reserved_usdc"])) == Decimal("40")

        # Per-account claim rows: the same event is claimed once per account —
        # alpha's processed row never suppressed beta's copy.
        async with pool.acquire() as conn:
            claims = await conn.fetch(
                "SELECT user_id, trade_key FROM processed_trades ORDER BY user_id"
            )
        assert len(claims) == 2
        assert {row["user_id"] for row in claims} == {USER_ALPHA, USER_BETA}

        # Applying each account's fill lands on that account's position only.
        state_a, state_b = world["states"]
        await _reconcile_all(state_a, ctx_a, confirm=_confirm("filled", "20", "0.50"))
        await _reconcile_all(state_b, ctx_b, confirm=_confirm("filled", "100", "0.50"))
        pos_a = await state_a.positions_repo.get_position(USER_ALPHA, CONDITION, token_id=TOKEN)
        pos_b = await state_b.positions_repo.get_position(USER_BETA, CONDITION, token_id=TOKEN)
        assert pos_a is not None and pos_a.size_tokens == Decimal("20")
        assert pos_b is not None and pos_b.size_tokens == Decimal("100")

        # Replay: the SAME event again must be a per-account duplicate — no
        # new orders, no new reservations, no second position delta.
        await execute_trades(ctx_a, LEADER_1, [event], now_ts=now)
        await execute_trades(ctx_b, LEADER_1, [event], now_ts=now)
        assert len(gateway_a.submissions) == 1
        assert len(gateway_b.submissions) == 1
        assert len(await _reservations_rows(pool)) == 2
        pos_a2 = await state_a.positions_repo.get_position(USER_ALPHA, CONDITION, token_id=TOKEN)
        assert pos_a2.size_tokens == Decimal("20")


# ---------------------------------------------------------------------------
# §9 bullet 3 — two leaders, one wallet, one token: shared limits hold
# ---------------------------------------------------------------------------


async def test_two_leaders_same_token_serialize_and_respect_shared_limits(
    _hermetic_balances,
):
    """§9 bullet 3, three faces:

    1. CONCURRENT: two leaders targeting one wallet's same token serialize on
       the account-owned coordinator — the venue never sees two overlapping
       submission critical sections for the same scope, and the shared
       reservation domain never double-spends the wallet.
    2. SEQUENTIAL: once positions are confirmed, the shared account-level
       market-exposure limit blocks further BUYs on that market from ANY
       leader (balance-tiered account-cap policy).

    3. COPIER CAP (schema v2 ``limits.max_open_exposure_quote``): the
       explicitly configured per-copier open-exposure cap counts CONFIRMED
       open exposure (existing cost-basis accounting) plus IN-FLIGHT reserved
       quote; its dedicated concurrency proof is
       ``test_copier_exposure_cap_bounds_two_concurrent_leaders`` below.
    """
    async with _pg_pool() as pool:
        # ---- Face 2: shared account-level market limit across leaders. ----
        limit_config = make_config(
            **{
                "execution": {
                    "trading_mode": "paper",
                    "reservations_enabled": True,
                    "max_position_size_usdc": 1000,
                },
                "risk": {
                    "enable_balance_tiered_policy": True,
                    "day_start_equity_usdc": 1000,
                    "max_market_exposure_usdc": 60,
                    "max_category_exposure_usdc": 600,
                    "max_leader_exposure_usdc": 500,
                },
            }
        )
        world = await _compose_two_accounts(
            pool,
            limit_config,
            balances=_hermetic_balances,
            follows_alpha={
                LEADER_1: follow(USER_ALPHA, LEADER_1, allocation_pct="0.50"),
                LEADER_2: follow(USER_ALPHA, LEADER_2, allocation_pct="0.50"),
            },
            follows_beta={LEADER_1: follow(USER_BETA, LEADER_1)},
        )
        state_a = world["states"][0]
        # Pre-existing confirmed position at the account's 60 market cap.
        from pmcopy.storage.postgres.positions_repo import PositionRecord

        await state_a.positions_repo.upsert_position(
            PositionRecord(
                user_id=USER_ALPHA,
                condition_id=CONDITION,
                token_id=TOKEN,
                leader_id=LEADER_1,
                size_tokens=Decimal("120"),
                avg_entry_price=Decimal("0.50"),
            )
        )
        ctx_l1 = world["by_leader"][LEADER_1][0]
        ctx_l2 = world["by_leader"][LEADER_2][0]
        results_l1 = await execute_trades(
            ctx_l1, LEADER_1, [leader_buy_event(tx_hash="0xtx-cap-l1")], now_ts=time.time()
        )
        results_l2 = await execute_trades(
            ctx_l2, LEADER_2, [leader_buy_event(tx_hash="0xtx-cap-l2")], now_ts=time.time()
        )
        assert [r.status for r in results_l1] == ["skipped"]
        assert [r.status for r in results_l2] == ["skipped"]
        assert world["gateways"][0].submissions == []
        assert await _reservations_rows(pool, USER_ALPHA) == []

        # ---- Face 1: concurrent same-scope submissions serialize. ----
        concurrent_config = _config()
        world2 = await _compose_two_accounts(
            pool,
            concurrent_config,
            balances=_hermetic_balances,
            follows_alpha={
                LEADER_1: follow(USER_ALPHA, LEADER_1, allocation_pct="0.50"),
                LEADER_2: follow(USER_ALPHA, LEADER_2, allocation_pct="0.50"),
            },
            follows_beta={LEADER_1: follow(USER_BETA, LEADER_1)},
        )
        gateway_a2 = world2["gateways"][0]
        inflight = {"now": 0, "max": 0}
        inner_submit = gateway_a2.submit_marketable_order

        async def _tracked_submit(request):
            inflight["now"] += 1
            inflight["max"] = max(inflight["max"], inflight["now"])
            try:
                # Yield once: if two same-scope submissions could overlap,
                # the second would enter while the first is still in flight.
                await asyncio.sleep(0)
                return await inner_submit(request)
            finally:
                inflight["now"] -= 1

        gateway_a2.submit_marketable_order = _tracked_submit
        ctx2_l1 = world2["by_leader"][LEADER_1][0]
        ctx2_l2 = world2["by_leader"][LEADER_2][0]
        await asyncio.gather(
            execute_trades(
                ctx2_l1, LEADER_1, [leader_buy_event(tx_hash="0xtx-race-l1")], now_ts=time.time()
            ),
            execute_trades(
                ctx2_l2, LEADER_2, [leader_buy_event(tx_hash="0xtx-race-l2")], now_ts=time.time()
            ),
        )
        assert len(gateway_a2.submissions) == 2
        assert inflight["max"] == 1, "same-scope submissions must serialize"

        # The shared reservation domain never double-spends the wallet: at
        # most the two intents' combined quote was ever reserved.
        reserved = [
            row
            for row in await _reservations_rows(pool, USER_ALPHA)
            if str(row["status"]) == "active"
        ]
        assert (
            sum((Decimal(str(row["reserved_usdc"])) for row in reserved), Decimal("0"))
            <= _hermetic_balances[FUNDER_ALPHA]
        )


# ---------------------------------------------------------------------------
# §9 bullet 4 — A's insufficient balance never consumes B's budget
# ---------------------------------------------------------------------------


async def test_alpha_insufficient_balance_leaves_beta_budget_and_orders_untouched(
    _hermetic_balances,
):
    async with _pg_pool() as pool:
        config = _config()
        world = await _compose_two_accounts(
            pool,
            config,
            balances=_hermetic_balances,
            follows_alpha={LEADER_1: follow(USER_ALPHA, LEADER_1, allocation_pct="0.50")},
            follows_beta={LEADER_1: follow(USER_BETA, LEADER_1, allocation_pct="0.50")},
        )
        # Alpha is broke; beta is funded.
        _hermetic_balances[FUNDER_ALPHA] = Decimal("0")
        _hermetic_balances[FUNDER_BETA] = Decimal("1000")

        ctx_a = world["by_leader"][LEADER_1][0]
        ctx_b = world["by_leader"][LEADER_1][1]
        event = leader_buy_event(tx_hash="0xtx-alpha-broke")
        now = time.time()
        await execute_trades(ctx_a, LEADER_1, [event], now_ts=now)
        await execute_trades(ctx_b, LEADER_1, [event], now_ts=now)

        gateway_a, gateway_b = world["gateways"]
        assert gateway_a.submissions == [], "insufficient balance must not submit"
        assert len(gateway_b.submissions) == 1, "beta executes normally"

        # Alpha consumed NO budget; beta's reservation is the only one.
        assert await _reservations_rows(pool, USER_ALPHA) == []
        beta_rows = await _reservations_rows(pool, USER_BETA)
        assert len(beta_rows) == 1
        assert Decimal(str(beta_rows[0]["reserved_usdc"])) == Decimal("50")

        # No alpha orders exist; beta's order is attached to beta only.
        assert await _orders_rows(pool, USER_ALPHA) == []
        beta_orders = await _orders_rows(pool, USER_BETA)
        assert len(beta_orders) == 1

        state_b = world["states"][1]
        await _reconcile_all(state_b, ctx_b, confirm=_confirm("filled", "100", "0.50"))
        pos_a = await world["states"][0].positions_repo.get_position(
            USER_ALPHA, CONDITION, token_id=TOKEN
        )
        pos_b = await state_b.positions_repo.get_position(USER_BETA, CONDITION, token_id=TOKEN)
        assert pos_a is None
        assert pos_b is not None and pos_b.size_tokens == Decimal("100")


# ---------------------------------------------------------------------------
# §9 bullet 7 — duplicate source event + replayed fill: no double accounting
# ---------------------------------------------------------------------------


async def test_duplicate_event_and_fill_replay_never_double_account(
    _hermetic_balances,
):
    async with _pg_pool() as pool:
        config = _config()
        world = await _compose_two_accounts(
            pool,
            config,
            balances=_hermetic_balances,
            follows_alpha={LEADER_1: follow(USER_ALPHA, LEADER_1, allocation_pct="0.50")},
            follows_beta={LEADER_1: follow(USER_BETA, LEADER_1, allocation_pct="0.50")},
        )
        ctx_b = world["by_leader"][LEADER_1][1]
        state_b = world["states"][1]
        gateway_b = world["gateways"][1]

        event = leader_buy_event(tx_hash="0xtx-replay")
        now = time.time()
        # The SAME batch delivered twice in immediate succession (source
        # replay), and the row duplicated inside one batch.
        await execute_trades(ctx_b, LEADER_1, [event, dict(event)], now_ts=now)
        await execute_trades(ctx_b, LEADER_1, [event], now_ts=now)
        assert len(gateway_b.submissions) == 1

        # A private fill replay: reconciling the same fill evidence twice
        # applies the position delta exactly once.
        confirm = _confirm("filled", "100", "0.50")
        await _reconcile_all(state_b, ctx_b, confirm=confirm)
        first = await state_b.positions_repo.get_position(USER_BETA, CONDITION, token_id=TOKEN)
        assert first is not None and first.size_tokens == Decimal("100")
        await _reconcile_all(state_b, ctx_b, confirm=confirm)
        again = await state_b.positions_repo.get_position(USER_BETA, CONDITION, token_id=TOKEN)
        assert again.size_tokens == Decimal("100")

        async with pool.acquire() as conn:
            fill_rows = await conn.fetch("SELECT * FROM fills WHERE user_id = $1", USER_BETA)
        assert len(fill_rows) == 1, "fill evidence is deduplicated by stable identity"


# ---------------------------------------------------------------------------
# §9 bullet 8 — restart restores a lost-POST account and a partial-fill account
# ---------------------------------------------------------------------------


async def test_restart_restores_lost_post_and_partial_fill_accounts(
    _hermetic_balances,
):
    async with _pg_pool() as pool:
        config = _config()
        world = await _compose_two_accounts(
            pool,
            config,
            balances=_hermetic_balances,
            follows_alpha={LEADER_1: follow(USER_ALPHA, LEADER_1, allocation_pct="0.05")},
            follows_beta={LEADER_1: follow(USER_BETA, LEADER_1, allocation_pct="0.05")},
        )
        ctx_a = world["by_leader"][LEADER_1][0]
        ctx_b = world["by_leader"][LEADER_1][1]
        gateway_a = world["gateways"][0]

        # Alpha loses the POST response: the transport dies mid-submission,
        # which must persist an ambiguous no-ID row and keep the reservation.
        async def _lost_post(_request):
            raise RuntimeError("connection reset after POST")

        gateway_a.submit_marketable_order = _lost_post
        event = leader_buy_event(tx_hash="0xtx-restart")
        now = time.time()
        await execute_trades(ctx_a, LEADER_1, [event], now_ts=now)
        await execute_trades(ctx_b, LEADER_1, [event], now_ts=now)

        state_a, state_b = world["states"]
        await _reconcile_all(state_b, ctx_b, confirm=_confirm("partially_filled", "2", "0.50"))
        pos_b_partial = await state_b.positions_repo.get_position(
            USER_BETA, CONDITION, token_id=TOKEN
        )
        assert pos_b_partial is not None and pos_b_partial.size_tokens == Decimal("2")

        alpha_orders = await _orders_rows(pool, USER_ALPHA)
        assert len(alpha_orders) == 1
        assert alpha_orders[0]["order_id"] is None
        assert str(alpha_orders[0]["status"]) == "submitted_unconfirmed"
        alpha_res = await _reservations_rows(pool, USER_ALPHA)
        assert len(alpha_res) == 1 and str(alpha_res[0]["status"]) == "active"

        # ---- RESTART: fresh account runtimes over the same database. ----
        alpha2 = runtime_user(
            USER_ALPHA,
            FUNDER_ALPHA,
            {LEADER_1: follow(USER_ALPHA, LEADER_1, allocation_pct="0.05")},
        )
        beta2 = runtime_user(
            USER_BETA,
            FUNDER_BETA,
            {LEADER_1: follow(USER_BETA, LEADER_1, allocation_pct="0.05")},
        )
        # Alpha's own private history shows the lost order filled at its
        # exact parameters (5.00 quote = 10 shares at 0.50).
        gateway_a2 = _gateway("alpha")
        gateway_a2.get_fills = AsyncMock(
            return_value=[
                VenueFill(
                    fill_id="fill-alpha-recovered",
                    order_id="alpha-recovered-order",
                    token_id=TOKEN,
                    side="BUY",
                    size=Decimal("10"),
                    price=Decimal("0.50"),
                    fee=Decimal("0"),
                    timestamp=int(now) - 5,
                    status="MATCHED",
                    trader_side="TAKER",
                )
            ]
        )
        gateway_b2 = _gateway("beta")
        state_a2 = build_user_state(alpha2, None, pool, config, clob_gateway=gateway_a2)
        state_b2 = build_user_state(beta2, None, pool, config, clob_gateway=gateway_b2)

        # Keep the recovery scan's position probe off the network: the
        # composed service's DataApi shares reader becomes an in-memory one.
        async def _local_venue_shares(_wallet, _token):
            return Decimal("0")

        state_a2.ambiguous_recovery._evidence_source._shares_reader = SimpleNamespace(
            shares=_local_venue_shares
        )
        contexts_by_leader2, _ = await build_context_maps(
            [alpha2, beta2],
            {USER_ALPHA: state_a2, USER_BETA: state_b2},
            [LEADER_1],
            config,
        )
        ctx_b2 = next(ctx for ctx in contexts_by_leader2[LEADER_1] if ctx.user_id == USER_BETA)
        ctx_a2 = next(ctx for ctx in contexts_by_leader2[LEADER_1] if ctx.user_id == USER_ALPHA)

        failures: list[str] = []
        await startup_reconcile_unconfirmed(
            list(contexts_by_leader2[LEADER_1]), failures_out=failures
        )

        # Alpha's lost POST resolves ONLY from its own private history through
        # the production no-ID recovery loop body (grace is zero in this
        # config, so the row is immediately eligible).
        results = await state_a2.ambiguous_recovery.recover_once()
        assert results, "restart recovery must process the lost-POST row"
        recovered = await _orders_rows(pool, USER_ALPHA)
        assert recovered[0]["order_id"] == "alpha-recovered-order"

        # Beta's partial fill keeps accumulating to the full size,
        # independently of alpha's recovery.
        applied_b = await _reconcile_all(state_b2, ctx_b2, confirm=_confirm("filled", "10", "0.50"))

        pos_b_final = await state_b2.positions_repo.get_position(
            USER_BETA, CONDITION, token_id=TOKEN
        )
        assert applied_b, "the restart reconciler must still see beta's partial order"
        assert pos_b_final.size_tokens == Decimal("10")

        # Alpha's recovered fill flows through the standard reconciliation
        # path into its own position.
        await _reconcile_all(
            state_a2,
            ctx_a2,
            confirm=AsyncMock(
                return_value={
                    "status": "filled",
                    "size_matched": "10",
                    "avg_price": "0.50",
                    "order_id": "alpha-recovered-order",
                }
            ),
        )
        pos_a_final = await state_a2.positions_repo.get_position(
            USER_ALPHA, CONDITION, token_id=TOKEN
        )
        assert pos_a_final is not None and pos_a_final.size_tokens == Decimal("10")


# ---------------------------------------------------------------------------
# §9 bullet 14 — startup with no evidence never releases a no-ID reservation
# ---------------------------------------------------------------------------


async def test_startup_without_evidence_keeps_unresolved_no_id_reservation(
    _hermetic_balances,
):
    async with _pg_pool() as pool:
        config = _config()
        world = await _compose_two_accounts(
            pool,
            config,
            balances=_hermetic_balances,
            follows_alpha={LEADER_1: follow(USER_ALPHA, LEADER_1, allocation_pct="0.05")},
            follows_beta={LEADER_1: follow(USER_BETA, LEADER_1, allocation_pct="0.05")},
        )
        ctx_a = world["by_leader"][LEADER_1][0]
        gateway_a = world["gateways"][0]

        async def _lost_post(_request):
            raise RuntimeError("connection reset after POST")

        gateway_a.submit_marketable_order = _lost_post
        await execute_trades(
            ctx_a, LEADER_1, [leader_buy_event(tx_hash="0xtx-keep-reserved")], now_ts=time.time()
        )

        # A fresh restart pass with NO evidence available (private history
        # outage on the restart path): the row and its reservation must stay
        # exactly as they were — never released without authoritative
        # account-correct proof.
        alpha2 = runtime_user(
            USER_ALPHA,
            FUNDER_ALPHA,
            {LEADER_1: follow(USER_ALPHA, LEADER_1, allocation_pct="0.05")},
        )
        gateway_a2 = _gateway("alpha")
        gateway_a2.get_fills = AsyncMock(side_effect=RuntimeError("private history unavailable"))
        state_a2 = build_user_state(alpha2, None, pool, config, clob_gateway=gateway_a2)

        async def _local_venue_shares(_wallet, _token):
            return Decimal("0")

        state_a2.ambiguous_recovery._evidence_source._shares_reader = SimpleNamespace(
            shares=_local_venue_shares
        )
        context = await build_trading_context(state_a2, alpha2.follows[LEADER_1], config)

        failures: list[str] = []
        await startup_reconcile_unconfirmed([context], failures_out=failures)
        await state_a2.ambiguous_recovery.recover_once()

        rows = await _reservations_rows(pool, USER_ALPHA)
        assert len(rows) == 1
        assert str(rows[0]["status"]) == "active", (
            "an unresolved no-ID submission must never release reserved capacity"
        )
        order = (await _orders_rows(pool, USER_ALPHA))[0]
        assert order["order_id"] is None
        assert str(order["status"]) == "submitted_unconfirmed"


# ---------------------------------------------------------------------------
# Task 24 fix — schema-v2 copier open-exposure cap enforcement (behavioral)
# ---------------------------------------------------------------------------


async def _capped_world(pool, balances, *, alpha_cap: str | None, beta_cap: str | None):
    """Compose two copiers; a non-None cap is an EXPLICIT schema-v2 cap."""
    config = _config()
    follows_alpha = {
        LEADER_1: follow(
            USER_ALPHA,
            LEADER_1,
            allocation_pct="0.50",
            **(
                {"max_market_exposure_usdc": alpha_cap, "explicit_exposure_cap": True}
                if alpha_cap is not None
                else {}
            ),
        )
    }
    follows_beta = {
        LEADER_1: follow(
            USER_BETA,
            LEADER_1,
            allocation_pct="0.50",
            **(
                {"max_market_exposure_usdc": beta_cap, "explicit_exposure_cap": True}
                if beta_cap is not None
                else {}
            ),
        )
    }
    return await _compose_two_accounts(
        pool, config, balances=balances, follows_alpha=follows_alpha, follows_beta=follows_beta
    )


async def test_copier_exposure_cap_rejects_entry_beyond_cap_without_reserving(
    _hermetic_balances,
):
    """Beyond the configured cap: skipped with the cap reason, no reservation."""
    async with _pg_pool() as pool:
        world = await _capped_world(pool, _hermetic_balances, alpha_cap="40", beta_cap=None)
        state_a = world["states"][0]
        from pmcopy.storage.postgres.positions_repo import PositionRecord

        # Confirmed open position already at 40 quote of cost basis.
        await state_a.positions_repo.upsert_position(
            PositionRecord(
                user_id=USER_ALPHA,
                condition_id=CONDITION,
                token_id=TOKEN,
                leader_id=LEADER_1,
                size_tokens=Decimal("80"),
                avg_entry_price=Decimal("0.50"),
            )
        )
        ctx_a = world["by_leader"][LEADER_1][0]
        results = await execute_trades(
            ctx_a, LEADER_1, [leader_buy_event(tx_hash="0xtx-cap-reject")], now_ts=time.time()
        )
        assert [r.status for r in results] == ["skipped"]
        assert results[0].reason == "copier_open_exposure_cap"
        # No submission, no reservation consumed, claim row marks the skip.
        assert world["gateways"][0].submissions == []
        assert await _reservations_rows(pool, USER_ALPHA) == []
        async with pool.acquire() as conn:
            claim = await conn.fetchrow(
                "SELECT status FROM processed_trades WHERE user_id = $1", USER_ALPHA
            )
        assert str(claim["status"]) == "skipped_copier_open_exposure_cap"


async def test_copier_exposure_cap_admits_entry_within_cap(_hermetic_balances):
    async with _pg_pool() as pool:
        world = await _capped_world(pool, _hermetic_balances, alpha_cap="60", beta_cap=None)
        ctx_a = world["by_leader"][LEADER_1][0]
        results = await execute_trades(
            ctx_a, LEADER_1, [leader_buy_event(tx_hash="0xtx-cap-admit")], now_ts=time.time()
        )
        assert [r.status for r in results] == ["submitted_unconfirmed"]
        assert len(world["gateways"][0].submissions) == 1


async def test_copier_exposure_cap_counts_inflight_reservations(_hermetic_balances):
    """An unfilled first BUY's reservation consumes the cap for the second."""
    async with _pg_pool() as pool:
        world = await _capped_world(pool, _hermetic_balances, alpha_cap="60", beta_cap=None)
        ctx_a = world["by_leader"][LEADER_1][0]
        # First entry: 50 quote, unfilled (reservation stays active).
        first = await execute_trades(
            ctx_a, LEADER_1, [leader_buy_event(tx_hash="0xtx-cap-inflight-1")], now_ts=time.time()
        )
        assert first[0].status == "submitted_unconfirmed"
        # Second entry on ANOTHER market condition would want another 50:
        # reserved 50 leaves 10 headroom → clamped down, never above the cap.
        other_event = leader_buy_event(tx_hash="0xtx-cap-inflight-2")
        other_event["conditionId"] = "0x" + "cd" * 32
        second = await execute_trades(ctx_a, LEADER_1, [other_event], now_ts=time.time())
        assert second, "the second entry must be admitted (clamped, not dropped)"
        gateway = world["gateways"][0]
        assert len(gateway.submissions) == 2
        assert Decimal(str(gateway.submissions[1].size)) <= Decimal("10")
        assert Decimal(str(gateway.submissions[1].size)) > Decimal("0")
        active = [
            row
            for row in await _reservations_rows(pool, USER_ALPHA)
            if str(row["status"]) == "active"
        ]
        assert sum((Decimal(str(r["reserved_usdc"])) for r in active), Decimal("0")) <= Decimal(
            "60"
        )


async def test_copier_exposure_cap_never_blocks_protective_exits(_hermetic_balances):
    """A protective SELL/reduction is never blocked by an exposure cap."""
    async with _pg_pool() as pool:
        world = await _capped_world(pool, _hermetic_balances, alpha_cap="10", beta_cap=None)
        state_a = world["states"][0]
        ctx_a = world["by_leader"][LEADER_1][0]
        # Held position far beyond the tiny cap, on the copied condition.
        from pmcopy.storage.postgres.positions_repo import PositionRecord

        await state_a.positions_repo.upsert_position(
            PositionRecord(
                user_id=USER_ALPHA,
                condition_id=CONDITION,
                token_id=TOKEN,
                leader_id=LEADER_1,
                size_tokens=Decimal("100"),
                avg_entry_price=Decimal("0.50"),
            )
        )
        sell_event = leader_buy_event(tx_hash="0xtx-cap-sell")
        sell_event["side"] = "SELL"
        sell_event["_leader_sell_fraction"] = 1.0

        # Deterministic live-holdings reader so the exit path runs to
        # submission (the holdings probe itself is not under test).
        class _Holdings:
            async def shares(self, _wallet, _token):
                return Decimal("100")

        ctx_a.holdings_reader = _Holdings()
        results = await execute_trades(ctx_a, LEADER_1, [sell_event], now_ts=time.time())
        # The exit is admitted — never skipped for the exposure cap.
        assert all(r.reason != "copier_open_exposure_cap" for r in results), results
        assert all(r.status != "skipped" for r in results), results
        assert world["gateways"][0].submissions, "the protective exit must submit"


async def test_copier_exposure_cap_legacy_paths_stay_unchanged(_hermetic_balances):
    """Legacy bindings (cap value present but NOT explicit) never reject."""
    async with _pg_pool() as pool:
        world = await _capped_world(pool, _hermetic_balances, alpha_cap=None, beta_cap=None)
        # Even with open exposure beyond the numeric default (300), the
        # legacy value is inert exactly as before the fix.
        state_a = world["states"][0]
        from pmcopy.storage.postgres.positions_repo import PositionRecord

        await state_a.positions_repo.upsert_position(
            PositionRecord(
                user_id=USER_ALPHA,
                condition_id=CONDITION,
                token_id=TOKEN,
                leader_id=LEADER_1,
                size_tokens=Decimal("1000"),
                avg_entry_price=Decimal("0.50"),
            )
        )
        ctx_a = world["by_leader"][LEADER_1][0]
        results = await execute_trades(
            ctx_a, LEADER_1, [leader_buy_event(tx_hash="0xtx-cap-legacy")], now_ts=time.time()
        )
        assert all(r.reason != "copier_open_exposure_cap" for r in results), results


async def test_copier_exposure_cap_isolates_accounts(_hermetic_balances):
    """A capped copier's rejection never affects an uncapped copier."""
    async with _pg_pool() as pool:
        world = await _capped_world(pool, _hermetic_balances, alpha_cap="10", beta_cap=None)
        state_a = world["states"][0]
        from pmcopy.storage.postgres.positions_repo import PositionRecord

        await state_a.positions_repo.upsert_position(
            PositionRecord(
                user_id=USER_ALPHA,
                condition_id=CONDITION,
                token_id=TOKEN,
                leader_id=LEADER_1,
                size_tokens=Decimal("40"),
                avg_entry_price=Decimal("0.50"),
            )
        )
        ctx_a = world["by_leader"][LEADER_1][0]
        ctx_b = world["by_leader"][LEADER_1][1]
        await execute_trades(
            ctx_a, LEADER_1, [leader_buy_event(tx_hash="0xtx-cap-iso-a")], now_ts=time.time()
        )
        results_b = await execute_trades(
            ctx_b, LEADER_1, [leader_buy_event(tx_hash="0xtx-cap-iso-b")], now_ts=time.time()
        )
        assert world["gateways"][0].submissions == []
        assert world["gateways"][1].submissions, "uncapped beta still enters"
        assert results_b[0].status == "submitted_unconfirmed"
        assert await _reservations_rows(pool, USER_BETA), "beta's budget is consumed only by beta"


async def test_copier_exposure_cap_bounds_two_concurrent_leaders(_hermetic_balances):
    """Concurrency twin: two leaders race one copier's token under a 60 cap.

    The account-owned scope lock serializes the two admissions; because the
    cap counts IN-FLIGHT reserved quote (not just confirmed fills), the
    second admission sees the first's reservation and clamps, so submitted
    and reserved quote can never collectively exceed the configured cap —
    even with zero confirmed positions yet.
    """
    async with _pg_pool() as pool:
        world = await _capped_world(pool, _hermetic_balances, alpha_cap="60", beta_cap=None)
        # The capped copier follows BOTH leaders on the same token.
        alpha = world["users"][0]
        alpha.follows[LEADER_2] = follow(
            USER_ALPHA,
            LEADER_2,
            allocation_pct="0.50",
            max_market_exposure_usdc="60",
            explicit_exposure_cap=True,
        )
        contexts_by_leader, _ = await build_context_maps(
            [alpha], {USER_ALPHA: world["states"][0]}, [LEADER_1, LEADER_2], _config()
        )
        gateway = world["gateways"][0]
        await asyncio.gather(
            execute_trades(
                contexts_by_leader[LEADER_1][0],
                LEADER_1,
                [leader_buy_event(tx_hash="0xtx-caprace-l1")],
                now_ts=time.time(),
            ),
            execute_trades(
                contexts_by_leader[LEADER_2][0],
                LEADER_2,
                [leader_buy_event(tx_hash="0xtx-caprace-l2")],
                now_ts=time.time(),
            ),
        )
        submitted = sum((Decimal(str(req.size)) for req in gateway.submissions), Decimal("0"))
        assert submitted == Decimal("60"), (
            f"expected 50 + clamped 10 under the 60 cap, got "
            f"{[str(r.size) for r in gateway.submissions]}"
        )
        active = sum(
            (
                Decimal(str(row["reserved_usdc"]))
                for row in await _reservations_rows(pool, USER_ALPHA)
                if str(row["status"]) == "active"
            ),
            Decimal("0"),
        )
        assert active == Decimal("60")


async def test_copier_exposure_cap_holds_across_a_cross_scope_race(_hermetic_balances):
    """Fix round 2: the reservation TRANSACTION is cap-authoritative.

    Two DIFFERENT markets (distinct condition + token, so the per-scope
    coordinator locks do NOT serialize them) race one capped copier: the
    pipeline-level cap read alone cannot stop both admissions (each reads
    reserved=0), so the atomic checked-reservation transaction under the
    per-user advisory lock must be the authoritative gate — exactly ONE
    reserves and submits; the other is rejected with the cap reason and
    leaves no reservation behind.
    """
    async with _pg_pool() as pool:
        world = await _capped_world(pool, _hermetic_balances, alpha_cap="60", beta_cap=None)
        gateway = world["gateways"][0]
        # Per-token execution metadata so both markets resolve admission.
        from pmcopy.domain.execution import ExecutionMetadata

        async def _metadata_for(token_id, condition_id=None):
            return ExecutionMetadata(
                token_id=str(token_id),
                tick_size=Decimal("0.01"),
                min_order_size=Decimal("0.001"),
                neg_risk=False,
            )

        gateway.get_execution_metadata = AsyncMock(side_effect=_metadata_for)

        event_1 = leader_buy_event(tx_hash="0xtx-xscope-1")
        event_2 = leader_buy_event(tx_hash="0xtx-xscope-2")
        event_2["conditionId"] = "0x" + "cd" * 32
        event_2["asset"] = TOKEN + "1"

        results = await asyncio.gather(
            execute_trades(
                world["by_leader"][LEADER_1][0], LEADER_1, [event_1], now_ts=time.time()
            ),
            execute_trades(
                world["by_leader"][LEADER_1][0], LEADER_1, [event_2], now_ts=time.time()
            ),
        )
        flat = [r for batch in results for r in batch]

        # The INVARIANT holds under every interleaving: whatever the event
        # loop races, submitted and reserved quote never exceed the cap.
        submitted = sum((Decimal(str(req.size)) for req in gateway.submissions), Decimal("0"))
        active_quote = sum(
            (
                Decimal(str(row["reserved_usdc"]))
                for row in await _reservations_rows(pool, USER_ALPHA)
                if str(row["status"]) == "active"
            ),
            Decimal("0"),
        )
        assert submitted <= Decimal("60"), [str(r.size) for r in gateway.submissions]
        assert active_quote <= Decimal("60")
        if len(gateway.submissions) == 1:
            # The loser lost INSIDE the authoritative transaction and is
            # rejected with the cap reason — never a silent pass.
            reasons = {str(r.reason) for r in flat}
            assert any("copier_open_exposure_cap" in reason for reason in reasons), reasons
        else:
            # The second admission read the first's reservation at the
            # pipeline fast path and was clamped: 50 + 10 == cap exactly.
            assert submitted == Decimal("60")


async def test_reservation_transaction_enforces_the_cap_across_scopes(
    _hermetic_balances,
):
    """Deterministic transactional proof: two concurrent reserve_for_intent
    calls on DIFFERENT intent keys under a 60 cap — exactly one reservation
    commits, the other raises CopierOpenExposureCapExceeded."""
    from datetime import datetime, timezone

    from pmcopy.domain.order_lifecycle import (
        IntentReason,
        IntentSide,
        OrderIntent,
        OrderIntentKey,
    )
    from pmcopy.services.execution.reservation_service import (
        CopierOpenExposureCapExceeded,
    )

    async with _pg_pool() as pool:
        world = await _capped_world(pool, _hermetic_balances, alpha_cap="60", beta_cap=None)
        state = world["states"][0]
        service = state.execution_coordinator.reservation_service
        assert service.open_exposure_cap == Decimal("60")

        async def _reserve(market: str):
            intent = OrderIntent(
                key=OrderIntentKey(
                    user_id=USER_ALPHA,
                    leader_wallet=LEADER_1,
                    condition_id=market,
                    token_id=f"{TOKEN}:{market}",
                    side=IntentSide.BUY,
                    reason=IntentReason.COPY_BUY,
                    source_event_id=f"0xsig-{market}",
                ),
                size=Decimal("100"),
                price=Decimal("0.50"),
                max_slippage_bps=200,
                created_at=datetime.now(timezone.utc),
            )
            return await service.reserve_for_intent(intent)

        outcomes = await asyncio.gather(
            _reserve("0x" + "aa" * 32), _reserve("0x" + "bb" * 32), return_exceptions=True
        )
        winners = [o for o in outcomes if not isinstance(o, BaseException)]
        losers = [o for o in outcomes if isinstance(o, BaseException)]
        assert len(winners) == 1, outcomes
        assert len(losers) == 1, outcomes
        assert isinstance(losers[0], CopierOpenExposureCapExceeded)

        active = [
            row
            for row in await _reservations_rows(pool, USER_ALPHA)
            if str(row["status"]) == "active"
        ]
        assert len(active) == 1
        assert Decimal(str(active[0]["reserved_usdc"])) == Decimal("50")
