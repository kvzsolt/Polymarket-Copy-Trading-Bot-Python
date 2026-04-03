from __future__ import annotations

import json
import time
from dataclasses import dataclass
from decimal import Decimal

from storage.sqlite.connection import create_connection


@dataclass
class FillRecord:
    user_id: str
    leader_id: str | None
    order_id: str | None
    condition_id: str
    token_id: str
    side: str
    size_tokens: Decimal
    price: Decimal
    fee: Decimal = Decimal("0")
    fill_ts: int | None = None


class FillsRepo:
    def __init__(self, db_path: str):
        self.db_path = db_path

    def append_fill(self, record: FillRecord, *, raw_payload: dict | None = None) -> int:
        payload = raw_payload or {
            "user_id": record.user_id,
            "leader_id": record.leader_id,
            "order_id": record.order_id,
            "condition_id": record.condition_id,
            "token_id": record.token_id,
            "side": record.side,
            "size_tokens": str(record.size_tokens),
            "price": str(record.price),
            "fee": str(record.fee),
            "fill_ts": int(record.fill_ts or time.time()),
        }

        with create_connection(self.db_path) as conn:
            cur = conn.execute(
                """
                INSERT INTO fills (
                    user_id, leader_id, order_id, condition_id, token_id, side,
                    size_tokens, price, fee, fill_ts, fill_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                (
                    record.user_id,
                    record.leader_id,
                    record.order_id,
                    record.condition_id,
                    record.token_id,
                    record.side,
                    str(record.size_tokens),
                    str(record.price),
                    str(record.fee),
                    int(record.fill_ts or time.time()),
                    json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str),
                ),
            )
            conn.commit()
            return int(cur.lastrowid)

    def list_fills_for_user(self, user_id: str) -> list[dict]:
        with create_connection(self.db_path) as conn:
            rows = conn.execute(
                """
                SELECT id, user_id, leader_id, order_id, condition_id, token_id, side,
                       size_tokens, price, fee, fill_ts, created_at
                FROM fills
                WHERE user_id = ?
                ORDER BY fill_ts ASC, id ASC
                """,
                (user_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def list_fills_for_user_condition(self, user_id: str, condition_id: str) -> list[dict]:
        with create_connection(self.db_path) as conn:
            rows = conn.execute(
                """
                SELECT id, user_id, leader_id, order_id, condition_id, token_id, side,
                       size_tokens, price, fee, fill_ts, created_at
                FROM fills
                WHERE user_id = ? AND condition_id = ?
                ORDER BY fill_ts ASC, id ASC
                """,
                (user_id, condition_id),
            ).fetchall()
        return [dict(row) for row in rows]
