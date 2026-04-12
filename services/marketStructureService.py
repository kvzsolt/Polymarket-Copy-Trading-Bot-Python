from __future__ import annotations

import time
from dataclasses import dataclass

from config.db import get_db_path
from storage.sqlite.connection import create_connection
from storage.sqlite.market_structure_repo import MarketStructureEvent, MarketStructureRepo


@dataclass(frozen=True)
class MarketStructureException(Exception):
    message: str


class MarketStructureService:
    def __init__(self, db_path: str | None = None):
        self.db_path = db_path or get_db_path()
        self.repo = MarketStructureRepo(self.db_path)

    def handle_split(self, *, condition_id: str, old_token_id: str, new_token_id: str, ratio: str) -> int:
        event = MarketStructureEvent(
            event_type="split",
            condition_id=condition_id,
            old_token_id=old_token_id,
            new_token_id=new_token_id,
            ratio=ratio,
            timestamp=time.time(),
            payload_json=self.repo.payload(
                {
                    "kind": "split",
                    "condition_id": condition_id,
                    "old_token_id": old_token_id,
                    "new_token_id": new_token_id,
                    "ratio": ratio,
                }
            ),
        )
        return self.repo.record_event(event)

    def handle_merge(self, *, condition_id: str, source_token_ids: list[str], target_token_id: str) -> int:
        event = MarketStructureEvent(
            event_type="merge",
            condition_id=condition_id,
            old_token_id=",".join(source_token_ids),
            new_token_id=target_token_id,
            ratio=None,
            timestamp=time.time(),
            payload_json=self.repo.payload(
                {
                    "kind": "merge",
                    "condition_id": condition_id,
                    "source_token_ids": source_token_ids,
                    "target_token_id": target_token_id,
                }
            ),
        )
        return self.repo.record_event(event)

    def handle_resolution(self, *, condition_id: str, winning_outcome: int) -> int:
        event = MarketStructureEvent(
            event_type="resolution",
            condition_id=condition_id,
            old_token_id=None,
            new_token_id=None,
            ratio=None,
            timestamp=time.time(),
            payload_json=self.repo.payload(
                {
                    "kind": "resolution",
                    "condition_id": condition_id,
                    "winning_outcome": winning_outcome,
                }
            ),
        )
        return self.repo.record_event(event)

    def get_remap_for_token(self, token_id: str) -> str | None:
        pending = self.repo.get_pending_remappings()
        for event in pending:
            if str(event.get("old_token_id") or "") == token_id:
                return str(event.get("new_token_id") or "") or None
        return None

    def reconcile_positions(self, user_id: str) -> int:
        pending = self.repo.get_pending_remappings()
        updated = 0
        if not pending:
            return 0

        with create_connection(self.db_path) as conn:
            for event in pending:
                old_token = str(event.get("old_token_id") or "")
                new_token = str(event.get("new_token_id") or "")
                condition_id = str(event.get("condition_id") or "")
                if not old_token or not new_token or not condition_id:
                    continue

                cur = conn.execute(
                    """
                    UPDATE positions
                    SET token_id = ?
                    WHERE user_id = ? AND condition_id = ? AND token_id = ?
                    """,
                    (new_token, user_id, condition_id, old_token),
                )
                if cur.rowcount > 0:
                    updated += cur.rowcount
                    conn.execute(
                        "UPDATE market_structure_events SET remapped = 1 WHERE id = ?",
                        (event["id"],),
                    )
            conn.commit()

        return updated
