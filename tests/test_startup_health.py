from __future__ import annotations

from decimal import Decimal

import pytest

from orchestrators.startup_health import (
    StartupHealthCheckItem,
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
    assert positions_repo.get_position("u1", "cond-stale", token_id="22222222222222222222") is None
