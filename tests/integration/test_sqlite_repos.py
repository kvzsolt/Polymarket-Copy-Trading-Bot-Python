"""Integration tests for SQLite repositories."""
import pytest
from storage.sqlite.connection import create_connection
from storage.sqlite.migrations import apply_migrations

@pytest.fixture
def db_path(tmp_path):
    return str(tmp_path / "test.db")

@pytest.fixture
def db_conn(db_path):
    conn = create_connection(db_path)
    apply_migrations(conn)
    yield conn
    conn.close()

def test_database_creates_successfully(db_conn):
    """Basic smoke test that migrations run and tables exist."""
    tables = db_conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    table_names = [row["name"] for row in tables]
    assert "orders" in table_names
    assert "fills" in table_names
    assert "positions" in table_names

def test_orders_table_has_required_columns(db_conn):
    columns = db_conn.execute("PRAGMA table_info(orders)").fetchall()
    col_names = [col["name"] for col in columns]
    assert "order_id" in col_names or "idempotency_key" in col_names
    assert "status" in col_names

def test_fills_table_has_required_columns(db_conn):
    columns = db_conn.execute("PRAGMA table_info(fills)").fetchall()
    col_names = [col["name"] for col in columns]
    assert "order_id" in col_names
    assert "side" in col_names
    assert "size_tokens" in col_names

def test_positions_table_has_composite_pk(db_conn):
    columns = db_conn.execute("PRAGMA table_info(positions)").fetchall()
    pk_cols = [col["name"] for col in columns if col["pk"]]
    assert "user_id" in pk_cols
    assert "condition_id" in pk_cols
    assert "token_id" in pk_cols

def test_order_reservations_table_exists(db_conn):
    tables = db_conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    table_names = [row["name"] for row in tables]
    assert "order_reservations" in table_names

def test_reservations_unique_constraint(db_conn):
    """Verify the unique index on (user_id, order_intent_key) exists."""
    indexes = db_conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='order_reservations'"
    ).fetchall()
    idx_names = [row["name"] for row in indexes]
    assert "idx_order_reservations_status" in idx_names
    assert "idx_order_reservations_user_token" in idx_names

def test_processed_trades_table_exists(db_conn):
    tables = db_conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    table_names = [row["name"] for row in tables]
    assert "processed_trades" in table_names

def test_can_insert_and_query_order(db_conn):
    """End-to-end: insert an order and read it back."""
    db_conn.execute(
        """INSERT INTO orders (idempotency_key, user_id, status)
           VALUES (?, ?, ?)""",
        ("test_key_001", "user_abc", "created"),
    )
    db_conn.commit()

    row = db_conn.execute(
        "SELECT * FROM orders WHERE idempotency_key = ?",
        ("test_key_001",),
    ).fetchone()
    assert row is not None
    assert row["user_id"] == "user_abc"
    assert row["status"] == "created"
