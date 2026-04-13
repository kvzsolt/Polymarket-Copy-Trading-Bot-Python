from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any

from storage.sqlite.connection import create_connection


@dataclass(frozen=True)
class MarketStructureEvent:
    event_type: str  # split|merge|resolution
    condition_id: str
    old_token_id: str | None
    new_token_id: str | None
    ratio: str | None
    timestamp: float
    payload_json: str


class MarketStructureRepo:
    def __init__(self, db_path: str):
        self.db_path = db_path

    def record_event(self, event: MarketStructureEvent) -> int:
        with create_connection(self.db_path) as conn:
            cur = conn.execute(
                """
                INSERT INTO market_structure_events (
                    event_type, condition_id, old_token_id, new_token_id,
                    ratio, event_ts, payload_json, remapped, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?)
                """,
                (
                    event.event_type,
                    event.condition_id,
                    event.old_token_id,
                    event.new_token_id,
                    event.ratio,
                    float(event.timestamp),
                    event.payload_json,
                    time.time(),
                ),
            )
            conn.commit()
            return int(cur.lastrowid)

    def get_events_for_condition(self, condition_id: str) -> list[dict[str, Any]]:
        with create_connection(self.db_path) as conn:
            rows = conn.execute(
                """
                SELECT id, event_type, condition_id, old_token_id, new_token_id,
                       ratio, event_ts, payload_json, remapped, created_at
                FROM market_structure_events
                WHERE condition_id = ?
                ORDER BY event_ts ASC, id ASC
                """,
                (condition_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def get_pending_remappings(self, user_id: str | None = None) -> list[dict[str, Any]]:
        """Fetch unprocessed split events, optionally filtered for a specific user."""
        if user_id is not None:
            with create_connection(self.db_path) as conn:
                rows = conn.execute(
                    """
                    SELECT mse.id, mse.event_type, mse.condition_id, mse.old_token_id,
                           mse.new_token_id, mse.ratio, mse.event_ts, mse.payload_json,
                           mse.remapped, mse.created_at
                    FROM market_structure_events mse
                    LEFT JOIN market_structure_event_applied msea
                        ON mse.id = msea.event_id AND msea.user_id = ?
                    WHERE mse.remapped = 0
                      AND mse.event_type = 'split'
                      AND mse.old_token_id IS NOT NULL
                      AND mse.new_token_id IS NOT NULL
                      AND msea.event_id IS NULL
                    ORDER BY mse.event_ts ASC, mse.id ASC
                    """,
                    (user_id,),
                ).fetchall()
            return [dict(row) for row in rows]
        with create_connection(self.db_path) as conn:
            rows = conn.execute(
                """
                SELECT id, event_type, condition_id, old_token_id, new_token_id,
                       ratio, event_ts, payload_json, remapped, created_at
                FROM market_structure_events
                WHERE remapped = 0
                  AND event_type = 'split'
                  AND old_token_id IS NOT NULL
                  AND new_token_id IS NOT NULL
                ORDER BY event_ts ASC, id ASC
                """
            ).fetchall()
        return [dict(row) for row in rows]

    def mark_remapped(self, event_id: int) -> bool:
        with create_connection(self.db_path) as conn:
            cur = conn.execute(
                "UPDATE market_structure_events SET remapped = 1 WHERE id = ?",
                (event_id,),
            )
            conn.commit()
            return cur.rowcount > 0

    def mark_applied_for_user(self, user_id: str, event_id: int) -> None:
        """Record that a remap event has been applied for a specific user."""
        import time as _t
        with create_connection(self.db_path) as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO market_structure_event_applied (user_id, event_id, applied_at)
                VALUES (?, ?, ?)
                """,
                (user_id, event_id, _t.time()),
            )
            conn.commit()

    @staticmethod
    def payload(data: dict[str, Any]) -> str:
        return json.dumps(data, sort_keys=True, separators=(",", ":"), default=str)
