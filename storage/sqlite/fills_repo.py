from __future__ import annotations

import json
import time
from dataclasses import dataclass
from decimal import Decimal

from domain.ports.repositories import AbstractFillRepository
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


class FillsRepo(AbstractFillRepository):
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
            # ``INSERT OR IGNORE`` against the ``idx_fills_unique`` index
            # on (user_id, order_id, condition_id, side, fill_ts).
            # Pre-fix: bare INSERT raised sqlite3.IntegrityError on a
            # duplicate fill — the persist_execution_atomically path
            # caught it but direct callers of FillsRepo did not. The
            # two fill-writing paths now share consistent dedup
            # semantics: returns 0 when the row already exists,
            # lastrowid otherwise.
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO fills (
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
            # rowcount==0 → duplicate; return 0 so callers can distinguish
            # "newly persisted" from "already known". lastrowid is the
            # auto-id of the row that just landed.
            if cur.rowcount == 0:
                return 0
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
