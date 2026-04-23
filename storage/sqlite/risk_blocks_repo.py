import json
import time
from typing import Any

from storage.sqlite.connection import create_connection


class RiskBlocksRepo:
    def __init__(self, db_path: str):
        self.db_path = db_path

    def mark_risk_block(
        self,
        *,
        user_id: str,
        condition_id: str,
        token_id: str,
        reason: str,
        source: str,
        leader_anchor: str | None = None,
        metadata: dict[str, Any] | None = None,
        expires_at: float | None = None,
    ) -> None:
        metadata_json = json.dumps(metadata or {}, sort_keys=True, separators=(",", ":"), default=str)
        now = time.time()
        
        with create_connection(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO risk_blocks (
                    user_id, condition_id, token_id, reason, source, 
                    triggered_at, expires_at, cleared_at, clear_reason, leader_anchor, metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?)
                ON CONFLICT(user_id, condition_id, token_id) DO UPDATE SET
                    reason = excluded.reason,
                    source = excluded.source,
                    triggered_at = excluded.triggered_at,
                    expires_at = excluded.expires_at,
                    cleared_at = NULL,
                    clear_reason = NULL,
                    leader_anchor = COALESCE(excluded.leader_anchor, risk_blocks.leader_anchor),
                    metadata_json = excluded.metadata_json
                """,
                (
                    user_id,
                    condition_id,
                    token_id,
                    reason,
                    source,
                    now,
                    expires_at,
                    leader_anchor,
                    metadata_json,
                ),
            )
            conn.commit()

    def get_active_risk_block(self, *, user_id: str, condition_id: str, token_id: str) -> dict[str, Any] | None:
        now = time.time()
        with create_connection(self.db_path) as conn:
            row = conn.execute(
                """
                SELECT user_id, condition_id, token_id, reason, source,
                       triggered_at, expires_at, cleared_at, clear_reason, leader_anchor, metadata_json
                FROM risk_blocks
                WHERE user_id = ? AND condition_id = ? AND token_id = ?
                  AND cleared_at IS NULL
                  AND (expires_at IS NULL OR expires_at > ?)
                LIMIT 1
                """,
                (user_id, condition_id, token_id, now),
            ).fetchone()
            
        if not row:
            return None
            
        return {
            "user_id": row["user_id"],
            "condition_id": row["condition_id"],
            "token_id": row["token_id"],
            "reason": row["reason"],
            "source": row["source"],
            "triggered_at": row["triggered_at"],
            "expires_at": row["expires_at"],
            "cleared_at": row["cleared_at"],
            "clear_reason": row["clear_reason"],
            "leader_anchor": row["leader_anchor"],
            "metadata": json.loads(row["metadata_json"] or "{}"),
        }

    def clear_risk_block(self, *, user_id: str, condition_id: str, token_id: str, clear_reason: str) -> bool:
        """Clear an active risk block. Returns True if a block was cleared."""
        now = time.time()
        with create_connection(self.db_path) as conn:
            cursor = conn.execute(
                """
                UPDATE risk_blocks
                SET cleared_at = ?,
                    clear_reason = ?
                WHERE user_id = ? AND condition_id = ? AND token_id = ?
                  AND cleared_at IS NULL
                """,
                (now, clear_reason, user_id, condition_id, token_id),
            )
            conn.commit()
            return cursor.rowcount > 0

    def list_active_risk_blocks(self, user_id: str | None = None) -> list[dict[str, Any]]:
        now = time.time()
        query = """
            SELECT user_id, condition_id, token_id, reason, source,
                   triggered_at, expires_at, cleared_at, clear_reason, leader_anchor, metadata_json
            FROM risk_blocks
            WHERE cleared_at IS NULL
              AND (expires_at IS NULL OR expires_at > ?)
        """
        params = [now]
        if user_id is not None:
            query += " AND user_id = ?"
            params.append(user_id)
            
        with create_connection(self.db_path) as conn:
            rows = conn.execute(query, tuple(params)).fetchall()
            
        return [
            {
                "user_id": row["user_id"],
                "condition_id": row["condition_id"],
                "token_id": row["token_id"],
                "reason": row["reason"],
                "source": row["source"],
                "triggered_at": row["triggered_at"],
                "expires_at": row["expires_at"],
                "cleared_at": row["cleared_at"],
                "clear_reason": row["clear_reason"],
                "leader_anchor": row["leader_anchor"],
                "metadata": json.loads(row["metadata_json"] or "{}"),
            }
            for row in rows
        ]
