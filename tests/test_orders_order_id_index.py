"""Regression: orders.order_id must be uniquely indexed.

Pre-fix: orders only had idempotency_key as PRIMARY KEY. Reconciliation
queries by venue order_id (`WHERE order_id = ?`) did a full-table scan
on every cycle and, if two rows ever shared an order_id, the
`ORDER BY updated_at DESC LIMIT 1` query could return the wrong row
silently.

This test pins:
  * A unique index exists on orders(order_id) WHERE order_id IS NOT NULL
  * Query plan for `WHERE order_id = ?` uses that index
  * Inserting two rows with the same non-NULL order_id raises
    IntegrityError (so a duplicate cannot land silently)
"""
from __future__ import annotations

import sqlite3

import pytest

from storage.sqlite.connection import create_connection
from storage.sqlite.migrations import apply_migrations


@pytest.fixture()
def fresh_db(tmp_path):
    db_path = str(tmp_path / "orders_idx.db")
    with create_connection(db_path) as conn:
        apply_migrations(conn)
        conn.commit()
    return db_path


def test_orders_has_unique_index_on_order_id(fresh_db):
    with create_connection(fresh_db) as conn:
        rows = conn.execute(
            "SELECT name, sql FROM sqlite_master "
            "WHERE type='index' AND tbl_name='orders'"
        ).fetchall()
    names = {str(r[0]) for r in rows}
    assert "idx_orders_order_id" in names, f"expected idx_orders_order_id; got {names}"

    sql = next(str(r[1]) for r in rows if str(r[0]) == "idx_orders_order_id")
    assert "UNIQUE" in sql.upper(), f"index must be UNIQUE; got {sql}"
    # Partial index on non-NULL — NULL order_ids can repeat during pending state.
    assert "WHERE" in sql.upper() and "ORDER_ID" in sql.upper() and "NOT NULL" in sql.upper(), (
        f"index must be partial on order_id IS NOT NULL; got {sql}"
    )


def test_query_plan_uses_order_id_index(fresh_db):
    with create_connection(fresh_db) as conn:
        plan = conn.execute(
            "EXPLAIN QUERY PLAN SELECT * FROM orders WHERE order_id = ?",
            ("venue_123",),
        ).fetchall()
    detail = " ".join(str(row[-1]) for row in plan).upper()
    assert "IDX_ORDERS_ORDER_ID" in detail, (
        f"WHERE order_id=? must use the unique index; plan was:\n{plan}"
    )


def test_duplicate_order_id_insert_raises(fresh_db):
    with create_connection(fresh_db) as conn:
        conn.execute(
            "INSERT INTO orders (idempotency_key, order_id, request_json) "
            "VALUES (?, ?, ?)",
            ("idem-a", "venue-X", "{}"),
        )
        conn.commit()
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO orders (idempotency_key, order_id, request_json) "
                "VALUES (?, ?, ?)",
                ("idem-b", "venue-X", "{}"),
            )
            conn.commit()


def test_null_order_ids_allowed_to_repeat(fresh_db):
    """Multiple pending rows with NULL order_id must coexist (partial index)."""
    with create_connection(fresh_db) as conn:
        conn.execute(
            "INSERT INTO orders (idempotency_key, order_id, request_json) "
            "VALUES (?, ?, ?)",
            ("idem-pending-1", None, "{}"),
        )
        conn.execute(
            "INSERT INTO orders (idempotency_key, order_id, request_json) "
            "VALUES (?, ?, ?)",
            ("idem-pending-2", None, "{}"),
        )
        conn.commit()
        count = conn.execute(
            "SELECT COUNT(*) FROM orders WHERE order_id IS NULL"
        ).fetchone()[0]
        assert count == 2
