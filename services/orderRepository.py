from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from config.db import get_database
from utils.logger import get_logger

logger = get_logger("OrderRepository")


def _utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


class OrderRepository:
    """SQLite-backed order persistence."""

    def get_by_idempotency_key(self, key: str) -> dict[str, Any] | None:
        with get_database() as conn:
            row = conn.execute(
                """
                SELECT idempotency_key, request_json, response_json, order_id, created_at, status
                FROM orders
                WHERE idempotency_key = ?
                LIMIT 1
                """,
                (key,),
            ).fetchone()

        if not row:
            return None

        request = json.loads(row["request_json"])
        response = json.loads(row["response_json"])
        return {
            "idempotency_key": row["idempotency_key"],
            "request": request,
            "response": response,
            "order_id": row["order_id"],
            "created_at": row["created_at"],
            "status": row["status"] or response.get("status"),
        }

    def save_submission(
        self,
        *,
        idempotency_key: str,
        request: dict[str, Any],
        response: dict[str, Any],
    ) -> None:
        doc = {
            "idempotency_key": idempotency_key,
            "request": request,
            "response": response,
            "order_id": response.get("orderID"),
            "created_at": _utc_now(),
            "status": response.get("status", "submitted"),
        }
        with get_database() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO orders
                (idempotency_key, request_json, response_json, order_id, created_at, status)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    doc["idempotency_key"],
                    json.dumps(doc["request"], separators=(",", ":"), default=str),
                    json.dumps(doc["response"], separators=(",", ":"), default=str),
                    doc["order_id"],
                    doc["created_at"].timestamp(),
                    doc["status"],
                ),
            )
            conn.commit()

    def mark_fill(self, *, order_id: str, fill: dict[str, Any]) -> None:
        doc = {"order_id": order_id, "fill": fill, "created_at": _utc_now()}
        with get_database() as conn:
            conn.execute(
                """
                INSERT INTO fills (order_id, fill_json, created_at)
                VALUES (?, ?, ?)
                """,
                (
                    doc["order_id"],
                    json.dumps(doc["fill"], separators=(",", ":"), default=str),
                    doc["created_at"].timestamp(),
                ),
            )
            conn.commit()
