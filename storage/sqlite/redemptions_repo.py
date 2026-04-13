from __future__ import annotations

import json
import time
from typing import Any

from domain.ports.repositories import AbstractRedemptionRepository
from storage.sqlite.connection import create_connection


class RedemptionsRepo(AbstractRedemptionRepository):
    def __init__(self, db_path: str):
        self.db_path = db_path

    def save_attempt(self, payload: dict[str, Any]) -> None:
        user_id = str(payload.get("user_id") or "")
        condition_id = str(payload.get("condition_id") or "")
        attempt_id = str(payload.get("attempt_id") or "")
        if not user_id or not condition_id or not attempt_id:
            return

        with create_connection(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO redemptions (user_id, condition_id, attempt_id, status, payload_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(user_id, condition_id, attempt_id) DO UPDATE SET
                    status = excluded.status,
                    payload_json = excluded.payload_json,
                    created_at = excluded.created_at
                """,
                (
                    user_id,
                    condition_id,
                    attempt_id,
                    payload.get("status"),
                    json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str),
                    float(payload.get("created_at") or time.time()),
                ),
            )
            conn.commit()
