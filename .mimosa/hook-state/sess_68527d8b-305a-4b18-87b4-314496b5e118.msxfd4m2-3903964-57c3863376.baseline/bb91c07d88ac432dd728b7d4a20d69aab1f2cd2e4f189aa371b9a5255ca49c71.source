"""Startup health checks under PG (PG-A6 port).

Behavioural tests run against a per-test asyncpg testcontainer pool.
Pure-config tests (log payload shape, print_startup_summary) keep their
in-memory shape assertions without DB access.
"""

from __future__ import annotations

import contextlib
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from orchestrators.runtime_summary import print_startup_summary
from orchestrators.startup_health import (
    OrderStartupCleanupResult,
    PositionSyncResult,
    StartupHealthCheckItem,
    StartupHealthReport,
    _position_sync_check,
    normalize_startup_order_statuses,
    perform_startup_health_check,
    reconcile_persisted_positions_to_live,
)
from storage.postgres.orders_repo import OrderSubmissionRecord, OrdersRepo
from storage.postgres.positions_repo import PositionRecord, PositionsRepo


@contextlib.asynccontextmanager
async def _pg_pool():
    try:
        from testcontainers.postgres import PostgresContainer
        import asyncpg
        from storage.postgres.migrations import apply_migrations as pg_apply
    except Exception as exc:
        pytest.skip(f"Docker / postgres image unavailable: {exc!r}")
    ctr = PostgresContainer("postgres:16-alpine")
    try:
        ctr.start()
    except Exception as exc:
        pytest.skip(f"Docker / postgres image unavailable: {exc!r}")
    try:
        dsn = ctr.get_connection_url(driver="asyncpg").replace(
            "postgresql+asyncpg://", "postgresql://"
        )
        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=3)
        async with pool.acquire() as conn:
            await pg_apply(conn)
        try:
            yield pool
        finally:
            await pool.close()
    finally:
        ctr.stop()


def _summary_env() -> dict[str, str]:
    return {
        "PRIVATE_KEY": "0x" + "a" * 64,
        "FUNDER_ADDRESS": "0x" + "b" * 40,
        "USER_ADDRESS": "0x" + "c" * 40,
        "RPC_URL": "https://polygon-rpc.example.com",
        "CHAIN_ID": "137",
        "LEADER_WALLETS": "0x" + "d" * 40,
    }


@pytest.mark.asyncio
async def test_reconcile_persisted_positions_to_live_prunes_stale_rows():
    async with _pg_pool() as pool:
        repo = PositionsRepo(pool)
        await repo.upsert_position(
            PositionRecord(
                user_id="u1",
                condition_id="cond-live",
                token_id="11111111111111111111",
                leader_id="leader-1",
                size_tokens=Decimal("5"),
                avg_entry_price=Decimal("0.5"),
            )
        )
        await repo.upsert_position(
            PositionRecord(
                user_id="u1",
                condition_id="cond-stale",
                token_id="22222222222222222222",
                leader_id="leader-1",
                size_tokens=Decimal("7"),
                avg_entry_price=Decimal("0.7"),
            )
        )

        sync = await reconcile_persisted_positions_to_live(
            repo,
            user_id="u1",
            live_positions=[
                {
                    "conditionId": "cond-live",
                    "asset": "11111111111111111111",
                    "size": 5,
                    "currentValue": 2.5,
                }
            ],
        )

        assert sync.persisted_before == 2
        assert sync.persisted_after == 1
        assert sync.pruned_positions == 1
        assert (
            await repo.get_position("u1", "cond-live", token_id="11111111111111111111") is not None
        )
        assert await repo.get_position("u1", "cond-stale", token_id="22222222222222222222") is None


@pytest.mark.asyncio
async def test_normalize_startup_order_statuses_preserves_order_id_rows():
    # P0-5: a pending row with no order_id is normalized + purged, but a delayed
    # row carrying a venue order_id is PRESERVED for startup reconciliation —
    # deleting it would lose a venue-accepted order as an untracked off-bot fill.
    async with _pg_pool() as pool:
        orders_repo = OrdersRepo(pool)
        await orders_repo.save_submission(
            OrderSubmissionRecord(
                idempotency_key="idem-pending",
                user_id="u1",
                leader_id="leader-1",
                order_id=None,
                status="pending",
                request_payload={"foo": "bar"},
                response_payload=None,
            )
        )
        await orders_repo.save_submission(
            OrderSubmissionRecord(
                idempotency_key="idem-delayed",
                user_id="u1",
                leader_id="leader-1",
                order_id="order-delayed",
                status="delayed",
                request_payload={"foo": "bar"},
                response_payload={"status": "delayed"},
            )
        )

        cleanup = await normalize_startup_order_statuses(pool, user_id="u1")

        assert cleanup.normalized_pending == 1
        assert cleanup.normalized_delayed == 0  # delayed carries an order_id -> preserved
        assert cleanup.normalized_retry_pending == 0
        assert cleanup.purged_failed == 1  # only the normalized (no-order_id) pending row
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT status, COUNT(*) AS count FROM orders WHERE user_id = $1 GROUP BY status",
                "u1",
            )
        counts = {str(row["status"]): int(row["count"]) for row in rows}
        # The order_id-bearing delayed row survives for reconciliation.
        assert counts == {"delayed": 1}


def test_startup_health_report_log_payload_includes_reconciliation_state() -> None:
    report = StartupHealthReport(
        healthy=True,
        status="warning",
        timestamp=1713870000,
        wallet="0xabc",
        checks={
            "positionSync": StartupHealthCheckItem(
                status="warning",
                message="pruned 1 stale persisted position(s) to match 2 live position(s)",
                details={"pruned_positions": 1},
            ),
            "orders": StartupHealthCheckItem(
                status="warning",
                message="orders successful=0 failed=1 | startup_purged_failed=1",
                details={"purged_failed": 1},
            ),
        },
        position_sync=PositionSyncResult(
            persisted_before=3,
            persisted_after=2,
            live_count=2,
            pruned_positions=1,
            pruned_condition_ids=("cond-stale",),
            pruned_token_ids=("token-stale",),
        ),
        order_status_counts={"terminal_failed": 1},
        order_summary={"successful": 0, "failed": 1},
        order_cleanup=OrderStartupCleanupResult(
            normalized_pending=0,
            normalized_delayed=0,
            normalized_retry_pending=0,
            normalized_total=0,
            purged_failed=1,
        ),
    )

    payload = report.as_log_payload()

    assert payload["type"] == "startup_health_check"
    assert payload["healthy"] is True
    assert payload["status"] == "warning"
    assert payload["wallet"] == "0xabc"
    assert payload["positionSync"]["persisted_before"] == 3
    assert payload["positionSync"]["persisted_after"] == 2
    assert payload["positionSync"]["pruned_positions"] == 1
    assert payload["positionSync"]["pruned_condition_ids"] == ("cond-stale",)
    assert payload["positionSync"]["pruned_token_ids"] == ("token-stale",)
    assert payload["orderStatusCounts"] == {"terminal_failed": 1}
    assert payload["orderSummary"] == {"successful": 0, "failed": 1}
    assert payload["orderCleanup"]["purged_failed"] == 1
    assert payload["checks"]["positionSync"]["details"] == {"pruned_positions": 1}
    assert payload["checks"]["orders"]["message"].endswith("startup_purged_failed=1")


def test_print_startup_summary_emits_health_payload_with_reconciliation_state(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    env = _summary_env()
    monkeypatch.setattr(
        "config.app_config.sys.version_info",
        SimpleNamespace(major=3, minor=12),
    )
    from tests.config_helpers import load_test_config

    _config = load_test_config(env)
    app = SimpleNamespace(config=_config)
    report = StartupHealthReport(
        healthy=True,
        status="warning",
        timestamp=1713870000,
        wallet=env["FUNDER_ADDRESS"],
        checks={
            "positionSync": StartupHealthCheckItem(
                status="warning",
                message="pruned 1 stale persisted position(s) to match 1 live position(s)",
                details={"pruned_positions": 1},
            ),
            "orders": StartupHealthCheckItem(
                status="warning",
                message="orders successful=0 failed=1 | startup_purged_failed=1",
                details={"purged_failed": 1},
            ),
        },
        position_sync=PositionSyncResult(
            persisted_before=2,
            persisted_after=1,
            live_count=1,
            pruned_positions=1,
            pruned_condition_ids=("cond-stale",),
            pruned_token_ids=("token-stale",),
        ),
        order_status_counts={"terminal_failed": 1},
        order_summary={"successful": 0, "failed": 1},
        order_cleanup=OrderStartupCleanupResult(
            normalized_pending=0,
            normalized_delayed=0,
            normalized_retry_pending=0,
            normalized_total=0,
            purged_failed=1,
        ),
    )

    with (
        patch("orchestrators.runtime_summary.console.print"),
        patch("orchestrators.runtime_summary.logger.info") as log_info,
    ):
        print_startup_summary(
            wallet=env["FUNDER_ADDRESS"],
            available_cash=12.34,
            total_portfolio=34.56,
            positions=[
                {
                    "title": "Market A",
                    "conditionId": "cond-live",
                    "tokenID": "token-live",
                    "outcome": "YES",
                    "size": 3,
                    "currentValue": 1.5,
                }
            ],
            active_leaders=[env["LEADER_WALLETS"]],
            config=app.config,
            health_report=report,
        )

    extra = log_info.call_args.kwargs["extra"]
    assert extra["type"] == "startup_summary"
    assert extra["health"]["type"] == "startup_health_check"
    assert extra["health"]["positionSync"]["pruned_positions"] == 1
    assert extra["health"]["orderCleanup"]["purged_failed"] == 1
    assert extra["openPositionCount"] == 1
    assert extra["activeLeadersCount"] == 1


@pytest.mark.asyncio
async def test_perform_startup_health_check_warns_after_self_healing(monkeypatch):
    async with _pg_pool() as pool:
        positions_repo = PositionsRepo(pool)
        orders_repo = OrdersRepo(pool)

        await positions_repo.upsert_position(
            PositionRecord(
                user_id="u1",
                condition_id="cond-stale",
                token_id="22222222222222222222",
                leader_id="leader-1",
                size_tokens=Decimal("7"),
                avg_entry_price=Decimal("0.7"),
            )
        )
        await orders_repo.save_submission(
            OrderSubmissionRecord(
                idempotency_key="idem-1",
                user_id="u1",
                leader_id="leader-1",
                order_id=None,
                status="pending",
                request_payload={"foo": "bar"},
                response_payload=None,
            )
        )

        async def _ok_rpc(_rpc_url: str):
            return StartupHealthCheckItem(status="ok", message="RPC endpoint responding")

        async def _ok_api():
            return StartupHealthCheckItem(status="ok", message="Polymarket API responding")

        monkeypatch.setattr("orchestrators.startup_health._check_rpc", _ok_rpc)
        monkeypatch.setattr("orchestrators.startup_health._check_polymarket_api", _ok_api)

        report = await perform_startup_health_check(
            wallet="0xabc",
            user_id="u1",
            pool=pool,
            positions_repo=positions_repo,
            orders_repo=orders_repo,
            live_positions=[],
            available_cash=3.25,
            database_ok=True,
            rpc_url="https://polygon-rpc.example.com",
        )

        assert report.healthy is True
        assert report.status == "warning"
        assert report.position_sync.pruned_positions == 1
        assert report.checks["positionSync"].status == "warning"
        assert report.checks["balance"].status == "warning"
        assert report.checks["orders"].status == "warning"
        assert report.checks["orders"].message.endswith("startup_purged_failed=1")
        assert report.order_summary == {"successful": 0, "failed": 0}
        assert report.order_cleanup.normalized_pending == 1
        assert report.order_cleanup.normalized_total == 1
        assert report.order_cleanup.purged_failed == 1


@pytest.mark.asyncio
async def test_perform_startup_health_check_fails_when_orders_repo_missing(monkeypatch):
    """PR #105 review: ``orders_repo=None`` must mark the run unhealthy."""
    async with _pg_pool() as pool:
        positions_repo = PositionsRepo(pool)
        await positions_repo.upsert_position(
            PositionRecord(
                user_id="u1",
                condition_id="cond-stale",
                token_id="22222222222222222222",
                leader_id="leader-1",
                size_tokens=Decimal("7"),
                avg_entry_price=Decimal("0.7"),
            )
        )

        async def _ok_rpc(_rpc_url: str):
            return StartupHealthCheckItem(status="ok", message="RPC endpoint responding")

        async def _ok_api():
            return StartupHealthCheckItem(status="ok", message="Polymarket API responding")

        monkeypatch.setattr("orchestrators.startup_health._check_rpc", _ok_rpc)
        monkeypatch.setattr("orchestrators.startup_health._check_polymarket_api", _ok_api)

        report = await perform_startup_health_check(
            wallet="0xabc",
            user_id="u1",
            pool=pool,
            positions_repo=positions_repo,
            # orders_repo intentionally omitted.
            live_positions=[],
            available_cash=100.0,
            database_ok=True,
            rpc_url="https://polygon-rpc.example.com",
        )

        assert report.checks["atomicLock"].status == "error"
        assert report.status == "error"
        assert report.healthy is False
        assert (
            await positions_repo.get_position("u1", "cond-stale", token_id="22222222222222222222")
            is None
        )


# ---------------------------------------------------------------------------
# P0-3: a transient live-position fetch failure must NOT delete local state.
# These run without Docker (fake repo) so the fail-closed invariant is checked
# in the unit tier, not only behind a testcontainer.
# ---------------------------------------------------------------------------


class _FakePositionsRepo:
    """Minimal PositionsRepo stand-in: records delete calls, never touches a DB."""

    def __init__(self, records: list[PositionRecord]):
        self._records = list(records)
        self.deleted: list[tuple[str, str, str | None]] = []

    async def list_open_positions(self, user_id: str) -> list[PositionRecord]:
        return [r for r in self._records if r.user_id == user_id]

    async def delete_position(self, user_id, condition_id, token_id=None):
        self.deleted.append((user_id, condition_id, token_id))


def _rec(condition_id: str, token_id: str) -> PositionRecord:
    return PositionRecord(
        user_id="u1",
        condition_id=condition_id,
        token_id=token_id,
        leader_id="leader-1",
        size_tokens=Decimal("5"),
        avg_entry_price=Decimal("0.5"),
    )


@pytest.mark.asyncio
async def test_reconcile_skips_prune_when_live_snapshot_fetch_failed():
    """live_positions=None (fetch failed) → fail closed: no prune, no deletes."""
    repo = _FakePositionsRepo(
        [_rec("cond-a", "11111111111111111111"), _rec("cond-b", "22222222222222222222")]
    )

    sync = await reconcile_persisted_positions_to_live(repo, user_id="u1", live_positions=None)

    assert sync.fetch_failed is True
    assert sync.pruned_positions == 0
    assert sync.persisted_before == 2
    assert sync.persisted_after == 2
    assert repo.deleted == []  # nothing deleted — the ledger is preserved


@pytest.mark.asyncio
async def test_reconcile_still_prunes_on_genuinely_empty_snapshot():
    """live_positions=[] (genuinely empty, fetch OK) → prune proceeds as before."""
    repo = _FakePositionsRepo([_rec("cond-a", "11111111111111111111")])

    sync = await reconcile_persisted_positions_to_live(repo, user_id="u1", live_positions=[])

    assert sync.fetch_failed is False
    assert sync.pruned_positions == 1
    assert repo.deleted == [("u1", "cond-a", "11111111111111111111")]


def test_position_sync_check_fetch_failed_reports_error():
    sync = PositionSyncResult(
        persisted_before=3,
        persisted_after=3,
        live_count=0,
        pruned_positions=0,
        pruned_condition_ids=(),
        pruned_token_ids=(),
        fetch_failed=True,
    )
    item = _position_sync_check(sync)
    assert item.status == "error"
