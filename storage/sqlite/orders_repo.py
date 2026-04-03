from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any

from storage.sqlite.connection import create_connection


_TERMINAL_SUCCESS_STATES = {
    "submitted",
    "submitted_unconfirmed",
    "partially_filled",
    "filled",
    "cancelled",
    "rejected",
    "duplicate_ignored",
}

_FAILURE_STATES = {"failed", "submission_failed", "created"}


@dataclass
class OrderSubmissionRecord:
    idempotency_key: str
    user_id: str
    leader_id: str | None
    order_id: str | None
    status: str
    request_payload: dict[str, Any]
    response_payload: dict[str, Any] | None
    last_error: str | None = None


class OrdersRepo:
    def __init__(self, db_path: str):
        self.db_path = db_path

    def get_by_idempotency_key(self, key: str) -> dict[str, Any] | None:
        with create_connection(self.db_path) as conn:
            row = conn.execute(
                """
                SELECT idempotency_key, user_id, leader_id, order_id, status,
                       request_json, response_json, attempt_count, last_error,
                       created_at, updated_at
                FROM orders
                WHERE idempotency_key = ?
                LIMIT 1
                """,
                (key,),
            ).fetchone()

        if not row:
            return None

        return {
            "idempotency_key": row["idempotency_key"],
            "user_id": row["user_id"],
            "leader_id": row["leader_id"],
            "order_id": row["order_id"],
            "status": row["status"],
            "request": json.loads(row["request_json"] or "{}"),
            "response": json.loads(row["response_json"] or "{}") if row["response_json"] else {},
            "attempt_count": int(row["attempt_count"] or 0),
            "last_error": row["last_error"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def save_submission(self, record: OrderSubmissionRecord) -> None:
        request_json = json.dumps(record.request_payload, sort_keys=True, separators=(",", ":"), default=str)
        response_json = (
            json.dumps(record.response_payload, sort_keys=True, separators=(",", ":"), default=str)
            if record.response_payload is not None
            else None
        )

        with create_connection(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO orders (
                    idempotency_key,
                    user_id,
                    leader_id,
                    order_id,
                    status,
                    request_json,
                    response_json,
                    attempt_count,
                    last_error,
                    created_at,
                    updated_at
                ) VALUES (
                    :idempotency_key,
                    :user_id,
                    :leader_id,
                    :order_id,
                    :status,
                    :request_json,
                    :response_json,
                    1,
                    :last_error,
                    CURRENT_TIMESTAMP,
                    CURRENT_TIMESTAMP
                )
                ON CONFLICT(idempotency_key) DO UPDATE SET
                    user_id = COALESCE(NULLIF(excluded.user_id, ''), orders.user_id),
                    leader_id = COALESCE(excluded.leader_id, orders.leader_id),
                    order_id = COALESCE(excluded.order_id, orders.order_id),
                    status = CASE
                        WHEN orders.status IN ('submitted','submitted_unconfirmed','partially_filled','filled','cancelled','rejected','duplicate_ignored')
                             AND excluded.status IN ('failed','submission_failed','created')
                        THEN orders.status
                        ELSE excluded.status
                    END,
                    request_json = excluded.request_json,
                    response_json = CASE
                        WHEN orders.status IN ('submitted','submitted_unconfirmed','partially_filled','filled','cancelled','rejected','duplicate_ignored')
                             AND excluded.status IN ('failed','submission_failed','created')
                        THEN orders.response_json
                        ELSE excluded.response_json
                    END,
                    attempt_count = COALESCE(orders.attempt_count, 0) + 1,
                    last_error = excluded.last_error,
                    updated_at = CURRENT_TIMESTAMP
                """,
                {
                    "idempotency_key": record.idempotency_key,
                    "user_id": record.user_id,
                    "leader_id": record.leader_id,
                    "order_id": record.order_id,
                    "status": record.status,
                    "request_json": request_json,
                    "response_json": response_json,
                    "last_error": record.last_error,
                },
            )
            conn.commit()

    def save_submission_legacy(self, *, idempotency_key: str, request: dict[str, Any], response: dict[str, Any]) -> None:
        status = str(response.get("status") or ("submitted" if response.get("success") else "failed"))
        record = OrderSubmissionRecord(
            idempotency_key=idempotency_key,
            user_id=str(request.get("user_id") or request.get("userId") or ""),
            leader_id=str(request.get("leader_id") or request.get("leaderWallet") or "") or None,
            order_id=response.get("orderID"),
            status=status,
            request_payload=request,
            response_payload=response,
            last_error=str(response.get("error")) if response.get("error") else None,
        )
        self.save_submission(record)

    def mark_fill(self, *, order_id: str, fill: dict[str, Any], user_id: str | None = None, leader_id: str | None = None) -> None:
        with create_connection(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO fills (
                    user_id, leader_id, order_id, condition_id, token_id, side,
                    size_tokens, price, fee, fill_ts, fill_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                (
                    user_id,
                    leader_id,
                    order_id,
                    fill.get("condition_id"),
                    fill.get("token_id"),
                    fill.get("side"),
                    str(fill.get("filled_amount") or fill.get("size_tokens") or "0"),
                    str(fill.get("avg_price") or fill.get("price") or "0"),
                    str(fill.get("fees_paid") or fill.get("fee") or "0"),
                    int(fill.get("fill_ts") or time.time()),
                    json.dumps(fill, sort_keys=True, separators=(",", ":"), default=str),
                ),
            )
            conn.commit()

    def has_processed_trade(self, trade_key: str) -> bool:
        if not trade_key:
            return False
        with create_connection(self.db_path) as conn:
            row = conn.execute(
                "SELECT 1 FROM processed_trades WHERE trade_key = ? LIMIT 1",
                (trade_key,),
            ).fetchone()
        return row is not None

    def mark_processed_trade(
        self,
        *,
        trade_key: str,
        user_id: str,
        leader_id: str | None,
        tx_hash: str | None,
        side: str | None,
        asset: str | None,
        condition_id: str | None,
        status: str,
    ) -> None:
        if not trade_key:
            return
        with create_connection(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO processed_trades (
                    trade_key, user_id, leader_id, tx_hash, side, asset, condition_id, status, processed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(trade_key) DO UPDATE SET
                    status = excluded.status,
                    processed_at = excluded.processed_at
                """,
                (
                    trade_key,
                    user_id,
                    leader_id,
                    tx_hash,
                    side,
                    asset,
                    condition_id,
                    status,
                    time.time(),
                ),
            )
            conn.commit()
