"""Phase 3 tests — storage adapters implement their domain ports.

The goals of this module are:

* Prove each concrete repository is registered as a subclass of its ABC.
* Round-trip the primary CRUD / upsert paths through a temp sqlite file
  so the ABC surface remains honest (no stale method renames, no
  unintended signature drift).
* Isolate every test with its own on-disk DB (``tmp_path``) — no shared
  state, no reliance on the module-level ``ENV`` / default DB path.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from domain.ports import (
    AbstractCursorRepository,
    AbstractDiscoveryRepository,
    AbstractFillRepository,
    AbstractLeaderRepository,
    AbstractOrderRepository,
    AbstractPositionRepository,
)
from storage.sqlite.connection import create_connection
from storage.sqlite.cursors_repo import CursorsRepo, TradeCursor
from storage.sqlite.discovery_repo import DiscoveryRepo
from storage.sqlite.fills_repo import FillRecord, FillsRepo
from storage.sqlite.leaders_repo import LeaderCandidateRecord, LeadersRepo
from storage.sqlite.migrations import apply_migrations
from storage.sqlite.orders_repo import OrdersRepo, OrderSubmissionRecord
from storage.sqlite.positions_repo import PositionRecord, PositionsRepo


@pytest.fixture()
def db_path(tmp_path):
    path = tmp_path / "state.db"
    with create_connection(str(path)) as conn:
        apply_migrations(conn)
    return str(path)


# ---------------------------------------------------------------------------
# ABC registration — every concrete repo must be a subclass of its port
# ---------------------------------------------------------------------------


class TestAbcRegistration:
    def test_orders_repo(self) -> None:
        assert issubclass(OrdersRepo, AbstractOrderRepository)

    def test_positions_repo(self) -> None:
        assert issubclass(PositionsRepo, AbstractPositionRepository)

    def test_fills_repo(self) -> None:
        assert issubclass(FillsRepo, AbstractFillRepository)

    def test_leaders_repo(self) -> None:
        assert issubclass(LeadersRepo, AbstractLeaderRepository)

    def test_cursors_repo(self) -> None:
        assert issubclass(CursorsRepo, AbstractCursorRepository)

    def test_discovery_repo(self) -> None:
        assert issubclass(DiscoveryRepo, AbstractDiscoveryRepository)


# ---------------------------------------------------------------------------
# Smoke-level CRUD round-trips — confirm the ABC-exposed methods work
# ---------------------------------------------------------------------------


class TestOrdersRepo:
    def test_acquire_then_is_locked(self, db_path: str) -> None:
        repo = OrdersRepo(db_path)
        assert repo.try_acquire_submission_lock("k1", "u1", "l1", {"a": 1}) is True
        assert repo.is_submission_locked("k1") is True
        # Re-acquiring must fail deterministically
        assert repo.try_acquire_submission_lock("k1", "u1", "l1", {"a": 1}) is False

    def test_save_submission_readback(self, db_path: str) -> None:
        repo = OrdersRepo(db_path)
        repo.save_submission(
            OrderSubmissionRecord(
                idempotency_key="k2",
                user_id="u1",
                leader_id="l1",
                order_id="ord-xyz",
                status="submitted",
                request_payload={"a": 1},
                response_payload={"orderID": "ord-xyz"},
            )
        )
        row = repo.get_by_idempotency_key("k2")
        assert row is not None
        assert row["order_id"] == "ord-xyz"
        assert row["status"] == "submitted"

    def test_processed_trade_lifecycle(self, db_path: str) -> None:
        repo = OrdersRepo(db_path)
        assert repo.has_processed_trade("tk1") is False
        repo.mark_processed_trade(
            trade_key="tk1",
            user_id="u1",
            leader_id="l1",
            condition_id="c1",
            side="BUY",
            status="ok",
            tx_hash="0xdead",
            asset="3" * 20,
        )
        assert repo.has_processed_trade("tk1") is True
        assert repo.get_processed_trade("tk1") is not None


class TestPositionsRepo:
    def test_upsert_and_list(self, db_path: str) -> None:
        repo = PositionsRepo(db_path)
        record = PositionRecord(
            user_id="u1",
            condition_id="c1",
            token_id="1" * 20,
            leader_id="l1",
            size_tokens=Decimal("10"),
            avg_entry_price=Decimal("0.5"),
        )
        repo.upsert_position(record)
        rows = repo.list_open_positions("u1")
        assert len(rows) == 1
        assert rows[0].condition_id == "c1"

    def test_delete_position(self, db_path: str) -> None:
        repo = PositionsRepo(db_path)
        repo.upsert_position(
            PositionRecord(
                user_id="u1",
                condition_id="c1",
                token_id="2" * 20,
                leader_id="l1",
                size_tokens=Decimal("5"),
                avg_entry_price=Decimal("0.6"),
            )
        )
        repo.delete_position("u1", "c1")
        assert repo.get_position("u1", "c1") is None


class TestFillsRepo:
    def test_append_and_list(self, db_path: str) -> None:
        repo = FillsRepo(db_path)
        rec = FillRecord(
            user_id="u1",
            leader_id="l1",
            order_id="ord-1",
            condition_id="c1",
            token_id="3" * 20,
            side="BUY",
            size_tokens=Decimal("1"),
            price=Decimal("0.5"),
        )
        fid = repo.append_fill(rec)
        assert isinstance(fid, int)
        assert repo.list_fills_for_user("u1")
        assert repo.list_fills_for_user_condition("u1", "c1")


class TestLeadersRepo:
    def test_upsert_candidates_and_list_active(self, db_path: str) -> None:
        repo = LeadersRepo(db_path)
        repo.upsert_candidates(
            [
                LeaderCandidateRecord(
                    leader_id="0xaaa",
                    score=1.0,
                    recent_trades=5,
                    active=True,
                    source="test",
                ),
                LeaderCandidateRecord(
                    leader_id="0xbbb",
                    score=0.5,
                    recent_trades=1,
                    active=False,
                    source="test",
                ),
            ]
        )
        actives = repo.list_active()
        assert "0xaaa" in actives
        assert "0xbbb" not in actives


class TestCursorsRepo:
    def test_default_cursor_then_roundtrip(self, db_path: str) -> None:
        repo = CursorsRepo(db_path)
        initial = repo.get_cursor("leader-a")
        assert initial.timestamp == 0
        assert initial.tx_hash == ""

        repo.set_cursor(
            "leader-a", TradeCursor(timestamp=1234, tx_hash="0xdead", trade_id="t1")
        )
        reloaded = repo.get_cursor("leader-a")
        assert reloaded.timestamp == 1234
        assert reloaded.tx_hash == "0xdead"


