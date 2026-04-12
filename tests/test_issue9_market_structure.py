"""Issue #9 tests — market structure split/merge/resolution foundation."""

import time
from decimal import Decimal
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.marketStructureService import MarketStructureService
from storage.sqlite.connection import create_connection
from storage.sqlite.market_structure_repo import MarketStructureEvent, MarketStructureRepo
from storage.sqlite.migrations import apply_migrations
from storage.sqlite.positions_repo import PositionsRepo


def _prepare_db(path: Path) -> None:
    with create_connection(str(path)) as conn:
        apply_migrations(conn)
        conn.commit()


def _token(n: int) -> str:
    return str(10**75 + n)


class TestMarketStructureRepo:
    def test_record_and_get_split_event(self, tmp_path):
        db = tmp_path / "ms.db"
        _prepare_db(db)
        repo = MarketStructureRepo(str(db))

        eid = repo.record_event(
            MarketStructureEvent(
                event_type="split",
                condition_id="cond-1",
                old_token_id=_token(1),
                new_token_id=_token(2),
                ratio="2:1",
                timestamp=time.time(),
                payload_json=repo.payload({"kind": "split"}),
            )
        )
        assert eid > 0

        rows = repo.get_events_for_condition("cond-1")
        assert len(rows) == 1
        assert rows[0]["event_type"] == "split"

    def test_record_merge_and_resolution(self, tmp_path):
        db = tmp_path / "ms.db"
        _prepare_db(db)
        repo = MarketStructureRepo(str(db))

        repo.record_event(
            MarketStructureEvent(
                event_type="merge",
                condition_id="cond-2",
                old_token_id=f"{_token(1)},{_token(2)}",
                new_token_id=_token(3),
                ratio=None,
                timestamp=time.time(),
                payload_json=repo.payload({"kind": "merge"}),
            )
        )
        repo.record_event(
            MarketStructureEvent(
                event_type="resolution",
                condition_id="cond-2",
                old_token_id=None,
                new_token_id=None,
                ratio=None,
                timestamp=time.time(),
                payload_json=repo.payload({"kind": "resolution", "winning_outcome": 1}),
            )
        )

        rows = repo.get_events_for_condition("cond-2")
        assert len(rows) == 2
        assert {r["event_type"] for r in rows} == {"merge", "resolution"}

    def test_pending_remappings_and_mark(self, tmp_path):
        db = tmp_path / "ms.db"
        _prepare_db(db)
        repo = MarketStructureRepo(str(db))

        eid = repo.record_event(
            MarketStructureEvent(
                event_type="split",
                condition_id="cond-3",
                old_token_id=_token(1),
                new_token_id=_token(2),
                ratio="2:1",
                timestamp=time.time(),
                payload_json=repo.payload({"kind": "split"}),
            )
        )
        pending = repo.get_pending_remappings()
        assert len(pending) == 1
        assert pending[0]["id"] == eid

        assert repo.mark_remapped(eid) is True
        assert repo.get_pending_remappings() == []


class TestMarketStructureService:
    def test_service_records_events(self, tmp_path):
        db = tmp_path / "ms.db"
        _prepare_db(db)
        svc = MarketStructureService(str(db))

        split_id = svc.handle_split(
            condition_id="cond-a",
            old_token_id=_token(10),
            new_token_id=_token(20),
            ratio="2:1",
        )
        merge_id = svc.handle_merge(
            condition_id="cond-a",
            source_token_ids=[_token(20), _token(21)],
            target_token_id=_token(30),
        )
        res_id = svc.handle_resolution(condition_id="cond-a", winning_outcome=1)

        assert split_id > 0 and merge_id > 0 and res_id > 0

    def test_reconcile_positions_applies_remap(self, tmp_path):
        db = tmp_path / "ms.db"
        _prepare_db(db)
        positions = PositionsRepo(str(db))
        svc = MarketStructureService(str(db))

        old_token = _token(100)
        new_token = _token(200)

        # seed position
        positions.apply_fill(
            user_id="u1",
            leader_id="l1",
            condition_id="cond-x",
            token_id=old_token,
            side="BUY",
            fill_size=Decimal("50"),
            fill_price=Decimal("0.40"),
        )

        # add split remap event
        svc.handle_split(
            condition_id="cond-x",
            old_token_id=old_token,
            new_token_id=new_token,
            ratio="2:1",
        )

        updated = svc.reconcile_positions("u1")
        assert updated == 1

        pos = positions.get_position("u1", "cond-x")
        assert pos is not None
        assert pos.token_id == new_token
