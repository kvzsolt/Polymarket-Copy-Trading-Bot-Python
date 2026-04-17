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


def test_persist_execution_atomically_scopes_position_by_token_id(tmp_path):
    db_path = tmp_path / "state.db"
    with create_connection(str(db_path)) as conn:
        apply_migrations(conn)
        conn.execute(
            """
            INSERT INTO positions (user_id, condition_id, token_id, leader_id, size_tokens, avg_entry_price, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            ("u1", "cond-1", "token-a", "leader-a", "10", "0.40", 1.0),
        )
        conn.execute(
            """
            INSERT INTO positions (user_id, condition_id, token_id, leader_id, size_tokens, avg_entry_price, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            ("u1", "cond-1", "token-b", "leader-b", "4", "0.60", 1.0),
        )
        conn.commit()

    repo = OrdersRepo(str(db_path))
    persisted = repo.persist_execution_atomically(
        fill={"order_id": "ord-buy-b", "fill_id": "fill-1"},
        user_id="u1",
        leader_id="leader-b",
        condition_id="cond-1",
        token_id="token-b",
        side="BUY",
        fill_size="3",
        fill_price="0.70",
        fill_fee="0.01",
        fill_ts=123456,
        fill_json='{"order_id":"ord-buy-b","fill_id":"fill-1"}',
        trade_key="trade-key-b",
        tx_hash="0xabc",
        asset="token-b",
        trade_status="executed",
    )

    assert persisted is True

    with create_connection(str(db_path)) as conn:
        rows = conn.execute(
            "SELECT token_id, size_tokens, avg_entry_price FROM positions WHERE user_id = ? AND condition_id = ? ORDER BY token_id",
            ("u1", "cond-1"),
        ).fetchall()

    assert len(rows) == 2
    assert rows[0]["token_id"] == "token-a"
    assert rows[0]["size_tokens"] == "10"
    assert rows[0]["avg_entry_price"] == "0.40"
    assert rows[1]["token_id"] == "token-b"
    assert rows[1]["size_tokens"] == "7"
    assert rows[1]["avg_entry_price"] == "0.6428571428571428571428571429"
