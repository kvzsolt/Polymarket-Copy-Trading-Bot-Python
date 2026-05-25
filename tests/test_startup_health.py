from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from bootstrap import build_app
from orchestrators.runtime_summary import print_startup_summary
from orchestrators.startup_health import (
    StartupHealthCheckItem,
    OrderStartupCleanupResult,
    PositionSyncResult,
    StartupHealthReport,
    normalize_startup_order_statuses,
    perform_startup_health_check,
    reconcile_persisted_positions_to_live,
)
from storage.sqlite.connection import create_connection
from storage.sqlite.migrations import apply_migrations
from storage.sqlite.orders_repo import OrderSubmissionRecord, OrdersRepo
from storage.sqlite.positions_repo import PositionRecord, PositionsRepo


@pytest.fixture()
def db_path(tmp_path):
    path = tmp_path / "state.db"
    with create_connection(str(path)) as conn:
        apply_migrations(conn)
    return str(path)


def _summary_env(db_path: str) -> dict[str, str]:
    return {
        "PRIVATE_KEY": "0x" + "a" * 64,
        "FUNDER_ADDRESS": "0x" + "b" * 40,
        "USER_ADDRESS": "0x" + "c" * 40,
        "RPC_URL": "https://polygon-rpc.example.com",
        "CHAIN_ID": "137",
        "BOT_STATE_DB": db_path,
        "LEADER_WALLETS": "0x" + "d" * 40,
    }


def test_reconcile_persisted_positions_to_live_prunes_stale_rows(db_path: str) -> None:
    repo = PositionsRepo(db_path)
    repo.upsert_position(
        PositionRecord(
            user_id="u1",
            condition_id="cond-live",
            token_id="11111111111111111111",
            leader_id="leader-1",
            size_tokens=Decimal("5"),
            avg_entry_price=Decimal("0.5"),
        )
    )
    repo.upsert_position(
        PositionRecord(
            user_id="u1",
            condition_id="cond-stale",
            token_id="22222222222222222222",
            leader_id="leader-1",
            size_tokens=Decimal("7"),
            avg_entry_price=Decimal("0.7"),
        )
    )

    sync = reconcile_persisted_positions_to_live(
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
    assert repo.get_position("u1", "cond-live", token_id="11111111111111111111") is not None
    assert repo.get_position("u1", "cond-stale", token_id="22222222222222222222") is None


def test_normalize_startup_order_statuses_collapses_pending_and_delayed(db_path: str) -> None:
    orders_repo = OrdersRepo(db_path)
    orders_repo.save_submission(
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
    orders_repo.save_submission(
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

    cleanup = normalize_startup_order_statuses(db_path, user_id="u1")

    assert cleanup.normalized_pending == 1
    assert cleanup.normalized_delayed == 1
    assert cleanup.normalized_retry_pending == 0
    assert cleanup.purged_failed == 2
    with create_connection(db_path) as conn:
        rows = conn.execute(
            "SELECT status, COUNT(*) AS count FROM orders WHERE user_id = ? GROUP BY status",
            ("u1",),
        ).fetchall()
    counts = {str(row["status"]): int(row["count"]) for row in rows}
    assert counts == {}


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
    env = _summary_env(str(tmp_path / "summary.db"))
    monkeypatch.setattr(
        "config.app_config.sys.version_info",
        SimpleNamespace(major=3, minor=12),
    )
    # PG-A5: ``build_app`` is async and now brings the asyncpg pool
    # online. This test only reads ``app.config``, so call
    # ``load_config`` directly to avoid the testcontainer requirement
    # for what is fundamentally a config-shape assertion.
    from config.app_config import load_config

    _config = load_config(env)
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

    with patch("orchestrators.runtime_summary.console.print"), patch(
        "orchestrators.runtime_summary.logger.info"
    ) as log_info:
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
async def test_perform_startup_health_check_warns_after_self_healing(monkeypatch, db_path: str) -> None:
    positions_repo = PositionsRepo(db_path)
    orders_repo = OrdersRepo(db_path)

    positions_repo.upsert_position(
        PositionRecord(
            user_id="u1",
            condition_id="cond-stale",
            token_id="22222222222222222222",
            leader_id="leader-1",
            size_tokens=Decimal("7"),
            avg_entry_price=Decimal("0.7"),
        )
    )
    orders_repo.save_submission(
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
        db_path=db_path,
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
async def test_perform_startup_health_check_fails_when_orders_repo_missing(
    monkeypatch, db_path: str
) -> None:
    """PR #105 review: ``orders_repo=None`` must mark the run unhealthy.

    The atomic-submission lock check is the gate against duplicate orders in
    live mode. If the caller forgot to wire ``orders_repo`` through, the
    previous behavior was a silent warning — which left the live report
    looking healthy with no atomic-lock verification. Audit it as an error.
    """
    positions_repo = PositionsRepo(db_path)

    async def _ok_rpc(_rpc_url: str):
        return StartupHealthCheckItem(status="ok", message="RPC endpoint responding")

    async def _ok_api():
        return StartupHealthCheckItem(status="ok", message="Polymarket API responding")

    monkeypatch.setattr("orchestrators.startup_health._check_rpc", _ok_rpc)
    monkeypatch.setattr("orchestrators.startup_health._check_polymarket_api", _ok_api)

    report = await perform_startup_health_check(
        wallet="0xabc",
        user_id="u1",
        db_path=db_path,
        positions_repo=positions_repo,
        # orders_repo intentionally omitted to exercise the fail-closed gate.
        live_positions=[],
        available_cash=100.0,
        database_ok=True,
        rpc_url="https://polygon-rpc.example.com",
    )

    assert report.checks["atomicLock"].status == "error"
    assert report.status == "error"
    assert report.healthy is False
    assert positions_repo.get_position("u1", "cond-stale", token_id="22222222222222222222") is None
