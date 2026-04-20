from __future__ import annotations

import json
import time

from config.db import get_db_path
from storage.sqlite.connection import create_connection
from storage.sqlite.market_structure_repo import MarketStructureEvent, MarketStructureRepo


class MarketStructureException(Exception):
    """Exception for market structure operation failures."""


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
            old_token_id=None,
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
        pending = self.repo.get_pending_remappings(user_id=user_id)
        updated = 0
        if not pending:
            return 0

        with create_connection(self.db_path) as conn:
            for event in pending:
                event_type = str(event.get("event_type") or "")
                condition_id = str(event.get("condition_id") or "")
                if event_type == "split":
                    old_token = str(event.get("old_token_id") or "")
                    new_token = str(event.get("new_token_id") or "")
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
                    updated += cur.rowcount
                elif event_type == "merge":
                    try:
                        payload_obj = json.loads(str(event.get("payload_json") or "{}"))
                    except Exception:
                        continue
                    source_token_ids = [
                        str(token_id).strip()
                        for token_id in (payload_obj.get("source_token_ids") or [])
                        if str(token_id).strip()
                    ]
                    target_token_id = str(payload_obj.get("target_token_id") or event.get("new_token_id") or "").strip()
                    if not condition_id or not source_token_ids or not target_token_id:
                        continue

                    placeholders = ",".join("?" for _ in source_token_ids)
                    rows = conn.execute(
                        f"""
                        SELECT user_id, condition_id, token_id, leader_id, size_tokens, avg_entry_price
                        FROM positions
                        WHERE user_id = ? AND condition_id = ? AND token_id IN ({placeholders})
                        """,
                        (user_id, condition_id, *source_token_ids),
                    ).fetchall()
                    if not rows:
                        continue
                    total_tokens = 0.0
                    weighted_cost = 0.0
                    leader_id = None
                    for row in rows:
                        size = float(row["size_tokens"] or 0.0)
                        entry = float(row["avg_entry_price"] or 0.0)
                        total_tokens += size
                        weighted_cost += size * entry
                        leader_id = leader_id or row["leader_id"]
                    if total_tokens <= 0:
                        continue
                    avg_entry_price = weighted_cost / total_tokens
                    conn.execute(
                        f"DELETE FROM positions WHERE user_id = ? AND condition_id = ? AND token_id IN ({placeholders})",
                        (user_id, condition_id, *source_token_ids),
                    )
                    conn.execute(
                        """
                        INSERT INTO positions (user_id, condition_id, token_id, leader_id, size_tokens, avg_entry_price, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(user_id, condition_id, token_id) DO UPDATE SET
                            leader_id = excluded.leader_id,
                            size_tokens = excluded.size_tokens,
                            avg_entry_price = excluded.avg_entry_price,
                            updated_at = excluded.updated_at
                        """,
                        (
                            user_id,
                            condition_id,
                            target_token_id,
                            leader_id,
                            str(total_tokens),
                            str(avg_entry_price),
                            time.time(),
                        ),
                    )
                    updated += len(rows)

                conn.execute(
                    """
                    INSERT OR IGNORE INTO market_structure_event_applied (user_id, event_id, applied_at)
                    VALUES (?, ?, ?)
                    """,
                    (user_id, event.get("id"), time.time()),
                )
            conn.commit()

        return updated
