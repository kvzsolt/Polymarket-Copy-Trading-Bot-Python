from __future__ import annotations

import json
import sqlite3
import time
from typing import Any

from storage.sqlite.connection import create_connection


class RedemptionsRepo:
    def __init__(self, db_path: str):
        self.db_path = db_path

    def save_attempt(self, payload: dict[str, Any]) -> None:
        user_id = str(payload.get("user_id") or "")
        condition_id = str(payload.get("condition_id") or "")
        attempt_id = str(payload.get("attempt_id") or "")
        if not user_id or not condition_id or not attempt_id:
            return

        status = str(payload.get("status") or "")
        if not status:
            raise ValueError("redemption status is required")

        with create_connection(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO redemptions (user_id, condition_id, attempt_id, status, payload_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(user_id, condition_id, attempt_id) DO UPDATE SET
                    status = excluded.status,
                    payload_json = excluded.payload_json
                """,
                (
                    user_id,
                    condition_id,
                    attempt_id,
                    status,
                    json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str),
                    float(payload.get("created_at") or time.time()),
                ),
            )
            conn.commit()

    def list_pending_redemptions(self, user_id: str) -> list[dict[str, Any]]:
        """List all redemption attempts that are still in-flight."""
        with create_connection(self.db_path) as conn:
            rows = conn.execute(
                """
                SELECT attempt_id, user_id, condition_id, status, payload_json, created_at
                FROM redemptions
                WHERE user_id = ? AND status IN ('attempting', 'submitted')
                ORDER BY created_at ASC
                """,
                (user_id,),
            ).fetchall()
        return [
            {
                "attempt_id": r["attempt_id"],
                "user_id": r["user_id"],
                "condition_id": r["condition_id"],
                "status": r["status"],
                "outcome_index": int((json.loads(r["payload_json"] or "{}").get("outcome_index", 0)) or 0),
                "payload": json.loads(r["payload_json"] or "{}"),
                "created_at": r["created_at"],
            }
            for r in rows
        ]

    def mark_status(self, *, attempt_id: str, status: str) -> bool:
        """Update the status of an existing redemption attempt. Returns True if updated."""
        with create_connection(self.db_path) as conn:
            cur = conn.execute(
                "UPDATE redemptions SET status = ? WHERE attempt_id = ?",
                (status, attempt_id),
            )
            conn.commit()
            return cur.rowcount > 0

    def is_attempted(self, *, condition_id: str, user_id: str, outcome_index: int | None = None) -> bool:
        """Check if a confirmed/successful redemption already exists for this condition."""
        with create_connection(self.db_path) as conn:
            row = conn.execute(
                """
                SELECT 1 FROM redemptions
                WHERE user_id = ? AND condition_id = ? AND status = 'confirmed'
                  AND (? IS NULL OR json_extract(payload_json, '$.outcome_index') = ?)
                LIMIT 1
                """,
                (user_id, condition_id, outcome_index, outcome_index),
            ).fetchone()
        return row is not None

    def purge_old_attempts(self, max_age_seconds: float = 30 * 86400) -> int:
        """Delete redemption records older than max_age_seconds. Returns count deleted."""
        cutoff = time.time() - max_age_seconds
        with create_connection(self.db_path) as conn:
            cur = conn.execute(
                "DELETE FROM redemptions WHERE created_at < ?",
                (cutoff,),
            )
            conn.commit()
            return cur.rowcount
