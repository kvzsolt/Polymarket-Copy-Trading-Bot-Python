import json
import time
from typing import Any

from storage.sqlite.connection import create_connection


class RiskTriggersRepo:
    def __init__(self, db_path: str):
        self.db_path = db_path

    def record_trigger(
        self,
        *,
        user_id: str,
        condition_id: str,
        token_id: str,
        reason: str,
        source: str = "risk_monitor",
        fill_status: str = "pending",
        fill_order_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> int:
        metadata_json = json.dumps(metadata or {}, sort_keys=True, separators=(",", ":"), default=str)
        now = time.time()
        with create_connection(self.db_path) as conn:
            cursor = conn.execute(
                """
                INSERT INTO risk_triggers (
                    user_id, condition_id, token_id, reason, source,
                    triggered_at, fill_order_id, fill_status, metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    user_id,
                    condition_id,
                    token_id,
                    reason,
                    source,
                    now,
                    fill_order_id,
                    fill_status,
                    metadata_json,
                ),
            )
            conn.commit()
            return cursor.lastrowid

    def update_fill_status(
        self,
        trigger_id: int,
        fill_status: str,
        fill_order_id: str | None = None,
    ) -> bool:
        with create_connection(self.db_path) as conn:
            if fill_order_id is not None:
                cursor = conn.execute(
                    """
                    UPDATE risk_triggers
                    SET fill_status = ?, fill_order_id = ?
                    WHERE id = ?
                    """,
                    (fill_status, fill_order_id, trigger_id),
                )
            else:
                cursor = conn.execute(
                    """
                    UPDATE risk_triggers
                    SET fill_status = ?
                    WHERE id = ?
                    """,
                    (fill_status, trigger_id),
                )
            conn.commit()
            return cursor.rowcount > 0

    def list_triggers_for_position(
        self,
        *,
        user_id: str,
        condition_id: str,
        token_id: str,
    ) -> list[dict[str, Any]]:
        with create_connection(self.db_path) as conn:
            rows = conn.execute(
                """
                SELECT id, user_id, condition_id, token_id, reason, source,
                       triggered_at, fill_order_id, fill_status, metadata_json
                FROM risk_triggers
                WHERE user_id = ? AND condition_id = ? AND token_id = ?
                ORDER BY triggered_at DESC
                """,
                (user_id, condition_id, token_id),
            ).fetchall()

        return [
            {
                "id": row["id"],
                "user_id": row["user_id"],
                "condition_id": row["condition_id"],
                "token_id": row["token_id"],
                "reason": row["reason"],
                "source": row["source"],
                "triggered_at": row["triggered_at"],
                "fill_order_id": row["fill_order_id"],
                "fill_status": row["fill_status"],
                "metadata": json.loads(row["metadata_json"] or "{}"),
            }
            for row in rows
        ]

    def get_trigger(self, trigger_id: int) -> dict[str, Any] | None:
        with create_connection(self.db_path) as conn:
            row = conn.execute(
                """
                SELECT id, user_id, condition_id, token_id, reason, source,
                       triggered_at, fill_order_id, fill_status, metadata_json
                FROM risk_triggers
                WHERE id = ?
                """,
                (trigger_id,),
            ).fetchone()

        if not row:
            return None

        return {
            "id": row["id"],
            "user_id": row["user_id"],
            "condition_id": row["condition_id"],
            "token_id": row["token_id"],
            "reason": row["reason"],
            "source": row["source"],
            "triggered_at": row["triggered_at"],
            "fill_order_id": row["fill_order_id"],
            "fill_status": row["fill_status"],
            "metadata": json.loads(row["metadata_json"] or "{}"),
        }
