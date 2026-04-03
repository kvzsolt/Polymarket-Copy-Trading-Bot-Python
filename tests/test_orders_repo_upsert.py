from __future__ import annotations

from storage.sqlite.connection import create_connection
from storage.sqlite.migrations import apply_migrations
from storage.sqlite.orders_repo import OrderSubmissionRecord, OrdersRepo


def test_order_repo_upsert_replaces_failed_submission(tmp_path):
    db_path = tmp_path / "state.db"
    with create_connection(str(db_path)) as conn:
        apply_migrations(conn)

    repo = OrdersRepo(str(db_path))
    key = "abc123"

    repo.save_submission(
        OrderSubmissionRecord(
            idempotency_key=key,
            user_id="u1",
            leader_id="l1",
            order_id=None,
            status="submission_failed",
            request_payload={"x": 1},
            response_payload={"error": "timeout"},
            last_error="timeout",
        )
    )

    repo.save_submission(
        OrderSubmissionRecord(
            idempotency_key=key,
            user_id="u1",
            leader_id="l1",
            order_id="ord-789",
            status="submitted",
            request_payload={"x": 1},
            response_payload={"orderID": "ord-789"},
            last_error=None,
        )
    )

    with create_connection(str(db_path)) as conn:
        row = conn.execute(
            "SELECT order_id, status, attempt_count FROM orders WHERE idempotency_key = ?",
            (key,),
        ).fetchone()

    assert row is not None
    assert row["order_id"] == "ord-789"
    assert row["status"] == "submitted"
    assert int(row["attempt_count"]) >= 2
