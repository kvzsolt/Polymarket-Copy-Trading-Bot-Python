"""Issue #5 tests — Decimal precision, DB oversell clamp, atomic persistence."""

import time
from decimal import Decimal
from pathlib import Path

import pytest

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.positionManager import PositionManager
from storage.sqlite.connection import create_connection
from storage.sqlite.migrations import apply_migrations
from storage.sqlite.orders_repo import OrdersRepo
from storage.sqlite.positions_repo import PositionsRepo


def _make_pm() -> PositionManager:
    return PositionManager()


def _prepare_db(path: Path) -> None:
    with create_connection(str(path)) as conn:
        apply_migrations(conn)
        conn.commit()


# ---------------------------------------------------------------------------
# Decimal precision in PositionManager
# ---------------------------------------------------------------------------

class TestDecimalPrecision:
    def test_repeating_decimal_buy_sell_no_drift(self):
        """Buy at 0.333333, sell at 0.666667 — Decimal should avoid float drift."""
        pm = _make_pm()
        pm.track_buy("cond-d1", "asset-1", 0.333333, 100.0, 33.3333)
        result = pm.track_sell("cond-d1", 0.666667, 100.0)
        assert result is not None  # position closed
        # PnL should be (0.666667 - 0.333333) * 100 = 33.3334
        assert result.realized_pnl == pytest.approx(33.3334, rel=1e-4)

    def test_weighted_average_decimal_precision(self):
        """Multiple buys at different prices — weighted avg should be exact."""
        pm = _make_pm()
        pm.track_buy("cond-d2", "asset-1", 0.10, 100.0, 10.0)
        pm.track_buy("cond-d2", "asset-1", 0.30, 100.0, 30.0)
        pos = pm.get_position("cond-d2")
        # Weighted avg = (0.10*100 + 0.30*100) / 200 = 40/200 = 0.20
        assert pos.entry_price == pytest.approx(0.20, abs=1e-6)
        assert pos.size_tokens == pytest.approx(200.0)

    def test_partial_sell_preserves_avg_entry(self):
        """Partial sell should NOT change the average entry price."""
        pm = _make_pm()
        pm.track_buy("cond-d3", "asset-1", 0.40, 100.0, 40.0)
        pm.track_buy("cond-d3", "asset-1", 0.60, 100.0, 60.0)
        pos_before = pm.get_position("cond-d3")
        avg_before = pos_before.entry_price  # 0.50

        # Partial sell 50 tokens
        pm.track_sell("cond-d3", 0.55, 50.0)
        pos_after = pm.get_position("cond-d3")
        assert pos_after is not None
        # Average entry should stay unchanged
        assert pos_after.entry_price == pytest.approx(avg_before, abs=1e-6)
        assert pos_after.size_tokens == pytest.approx(150.0)

    def test_many_buys_decimal_accumulation(self):
        """100 micro-buys should not accumulate float error."""
        pm = _make_pm()
        for i in range(100):
            pm.track_buy("cond-d4", "asset-1", 0.33, 1.0, 0.33)
        pos = pm.get_position("cond-d4")
        # 100 buys at 0.33 → avg should be exactly 0.33
        assert pos.entry_price == pytest.approx(0.33, abs=1e-4)
        assert pos.size_tokens == pytest.approx(100.0)


# ---------------------------------------------------------------------------
# DB oversell clamp in PositionsRepo
# ---------------------------------------------------------------------------

class TestDBOversellClamp:
    def test_sell_more_than_held_clamps_to_held(self):
        """Selling more than held in DB should clamp to held amount."""
        db_path = Path("/tmp/test_oversell_db_" + str(int(time.time())) + ".db")
        try:
            _prepare_db(db_path)
            repo = PositionsRepo(str(db_path))

            # Buy 50 tokens at 0.50
            repo.apply_fill(
                user_id="u1",
                leader_id="l1",
                condition_id="cond-os",
                token_id="21742633143463906290569050155826241533067272736897614950488156847949938836495",
                side="BUY",
                fill_size=Decimal("50"),
                fill_price=Decimal("0.50"),
            )

            pos = repo.get_position("u1", "cond-os")
            assert pos is not None
            assert pos.size_tokens == Decimal("50")

            # Sell 80 tokens when only 50 held — should clamp to 50
            result = repo.apply_fill(
                user_id="u1",
                leader_id="l1",
                condition_id="cond-os",
                token_id="21742633143463906290569050155826241533067272736897614950488156847949938836495",
                side="SELL",
                fill_size=Decimal("80"),
                fill_price=Decimal("0.60"),
            )

            # Position should be deleted (closed)
            assert result is None
            assert repo.get_position("u1", "cond-os") is None
        finally:
            db_path.unlink(missing_ok=True)

    def test_sell_exactly_held_closes(self):
        """Selling exactly the held amount should close the position."""
        db_path = Path("/tmp/test_exact_sell_" + str(int(time.time())) + ".db")
        try:
            _prepare_db(db_path)
            repo = PositionsRepo(str(db_path))

            repo.apply_fill(
                user_id="u1",
                leader_id="l1",
                condition_id="cond-exact",
                token_id="21742633143463906290569050155826241533067272736897614950488156847949938836495",
                side="BUY",
                fill_size=Decimal("100"),
                fill_price=Decimal("0.40"),
            )

            result = repo.apply_fill(
                user_id="u1",
                leader_id="l1",
                condition_id="cond-exact",
                token_id="21742633143463906290569050155826241533067272736897614950488156847949938836495",
                side="SELL",
                fill_size=Decimal("100"),
                fill_price=Decimal("0.60"),
            )

            assert result is None
            assert repo.get_position("u1", "cond-exact") is None
        finally:
            db_path.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Atomic execution persistence
# ---------------------------------------------------------------------------

class TestAtomicExecutionPersistence:
    def test_persist_all_in_one_transaction(self):
        """Fill + position + processed_trade should all succeed atomically."""
        db_path = Path("/tmp/test_atomic_" + str(int(time.time())) + ".db")
        try:
            _prepare_db(db_path)
            repo = OrdersRepo(str(db_path))
            token = "21742633143463906290569050155826241533067272736897614950488156847949938836495"

            ok = repo.persist_execution_atomically(
                fill={"order_id": "ord-1"},
                user_id="u1",
                leader_id="l1",
                condition_id="cond-atom",
                token_id=token,
                side="BUY",
                fill_size="50",
                fill_price="0.40",
                fill_fee="0",
                fill_ts=int(time.time()),
                fill_json='{"test": true}',
                trade_key="trade-atom-1",
                tx_hash=None,
                asset=token,
                trade_status="filled",
            )

            assert ok is True

            # Verify fill was recorded
            with create_connection(str(db_path)) as conn:
                fills = conn.execute("SELECT COUNT(*) FROM fills WHERE condition_id = 'cond-atom'").fetchone()[0]
                assert fills == 1

                # Verify position was created
                pos = conn.execute("SELECT size_tokens, avg_entry_price FROM positions WHERE user_id = 'u1' AND condition_id = 'cond-atom'").fetchone()
                assert pos is not None
                assert pos["size_tokens"] == "50"
                assert pos["avg_entry_price"] == "0.40"

                # Verify processed trade was recorded
                pt = conn.execute("SELECT status FROM processed_trades WHERE trade_key = 'trade-atom-1'").fetchone()
                assert pt is not None
                assert pt["status"] == "filled"
        finally:
            db_path.unlink(missing_ok=True)

    def test_duplicate_fill_rejected(self):
        """Second fill with same unique key should be rejected."""
        db_path = Path("/tmp/test_dup_fill_" + str(int(time.time())) + ".db")
        try:
            _prepare_db(db_path)
            repo = OrdersRepo(str(db_path))
            token = "21742633143463906290569050155826241533067272736897614950488156847949938836495"
            ts = int(time.time())

            ok1 = repo.persist_execution_atomically(
                fill={"order_id": "ord-dup"},
                user_id="u1",
                leader_id="l1",
                condition_id="cond-dup",
                token_id=token,
                side="BUY",
                fill_size="50",
                fill_price="0.40",
                fill_fee="0",
                fill_ts=ts,
                fill_json='{"test": true}',
                trade_key="trade-dup-1",
                tx_hash=None,
                asset=token,
                trade_status="filled",
            )
            assert ok1 is True

            # Duplicate fill (same order_id, condition_id, side, fill_ts)
            ok2 = repo.persist_execution_atomically(
                fill={"order_id": "ord-dup"},
                user_id="u1",
                leader_id="l1",
                condition_id="cond-dup",
                token_id=token,
                side="BUY",
                fill_size="50",
                fill_price="0.40",
                fill_fee="0",
                fill_ts=ts,
                fill_json='{"test": true}',
                trade_key="trade-dup-2",
                tx_hash=None,
                asset=token,
                trade_status="filled",
            )
            assert ok2 is False  # rejected due to uniqueness constraint
        finally:
            db_path.unlink(missing_ok=True)

    def test_sell_closes_position_atomically(self):
        """Atomic SELL should delete position when fully closed."""
        db_path = Path("/tmp/test_atomic_sell_" + str(int(time.time())) + ".db")
        try:
            _prepare_db(db_path)
            repo = OrdersRepo(str(db_path))
            token = "21742633143463906290569050155826241533067272736897614950488156847949938836495"

            # First: buy
            repo.persist_execution_atomically(
                fill={"order_id": "ord-buy"},
                user_id="u1",
                leader_id="l1",
                condition_id="cond-as",
                token_id=token,
                side="BUY",
                fill_size="100",
                fill_price="0.30",
                fill_fee="0",
                fill_ts=int(time.time()),
                fill_json='{}',
                trade_key="trade-as-buy",
                tx_hash=None,
                asset=token,
                trade_status="filled",
            )

            # Second: sell all
            repo.persist_execution_atomically(
                fill={"order_id": "ord-sell"},
                user_id="u1",
                leader_id="l1",
                condition_id="cond-as",
                token_id=token,
                side="SELL",
                fill_size="100",
                fill_price="0.60",
                fill_fee="0",
                fill_ts=int(time.time()),
                fill_json='{}',
                trade_key="trade-as-sell",
                tx_hash=None,
                asset=token,
                trade_status="filled",
            )

            with create_connection(str(db_path)) as conn:
                pos = conn.execute("SELECT * FROM positions WHERE user_id = 'u1' AND condition_id = 'cond-as'").fetchone()
                assert pos is None  # position should be deleted
        finally:
            db_path.unlink(missing_ok=True)
