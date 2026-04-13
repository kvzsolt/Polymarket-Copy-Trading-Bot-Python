from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from decimal import Decimal
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

_FAILURE_STATES = {"failed", "submission_failed", "created", "retryable_failed", "terminal_failed"}
_RETRYABLE_FAILED_STATUS = "retryable_failed"
# Legacy prefixes for backward compat with pre-migration data
_LEGACY_RETRY_PREFIXES = ("retryable_failed_", "retry_pending_")


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

    def try_acquire_submission_lock(self, key: str, user_id: str, leader_id: str | None, request_payload: dict) -> bool:
        """Atomically acquire a submission lock for this idempotency key.

        Uses INSERT ... ON CONFLICT DO NOTHING + rowcount check.
        Returns True if this caller acquired the lock (may proceed to submit).
        Returns False if someone else already holds it (must NOT submit).

        This prevents the race condition where two concurrent execute() calls
        both pass the "existing?" check and both submit distinct signed orders.
        """
        request_json = json.dumps(request_payload, sort_keys=True, separators=(",", ":"), default=str)
        with create_connection(self.db_path) as conn:
            cursor = conn.execute(
                """
                INSERT INTO orders (
                    idempotency_key, user_id, leader_id, order_id, status,
                    request_json, response_json, attempt_count, last_error,
                    created_at, updated_at
                ) VALUES (?, ?, ?, NULL, 'pending_lock', ?, NULL, 0, NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ON CONFLICT(idempotency_key) DO NOTHING
                """,
                (key, user_id, leader_id, request_json),
            )
            conn.commit()
            acquired = cursor.rowcount == 1
        return acquired

    def is_submission_locked(self, key: str) -> bool:
        """Check if a submission lock exists that is still pending (no order_id yet).

        Returns True if locked but not yet submitted (inflight_lock).
        Returns False if no record exists, or if an order_id is present (terminal).
        """
        rec = self.get_by_idempotency_key(key)
        if rec is None:
            return False
        if rec.get("order_id"):
            return False  # has order_id = terminal, not just locked
        return rec.get("status") in ("pending", "pending_lock")

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
                "SELECT status FROM processed_trades WHERE trade_key = ? LIMIT 1",
                (trade_key,),
            ).fetchone()
        if row is None:
            return False
        status = str(row["status"] or "")
        if status == _RETRYABLE_FAILED_STATUS:
            return False
        return not any(status.startswith(p) for p in _LEGACY_RETRY_PREFIXES)

    def get_processed_trade(self, trade_key: str) -> dict[str, Any] | None:
        if not trade_key:
            return None
        with create_connection(self.db_path) as conn:
            row = conn.execute(
                """
                SELECT trade_key, user_id, leader_id, tx_hash, side, asset, condition_id, status, processed_at,
                       COALESCE(retry_attempt, 0) AS retry_attempt
                FROM processed_trades
                WHERE trade_key = ?
                LIMIT 1
                """,
                (trade_key,),
            ).fetchone()
        if row is None:
            return None
        return {
            "trade_key": row["trade_key"],
            "user_id": row["user_id"],
            "leader_id": row["leader_id"],
            "tx_hash": row["tx_hash"],
            "side": row["side"],
            "asset": row["asset"],
            "condition_id": row["condition_id"],
            "status": row["status"],
            "processed_at": row["processed_at"],
            "retry_attempt": row["retry_attempt"],
        }

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
        retry_attempt: int = 0,
    ) -> None:
        if not trade_key:
            return
        with create_connection(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO processed_trades (
                    trade_key, user_id, leader_id, tx_hash, side, asset, condition_id, status, processed_at, retry_attempt
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(trade_key) DO UPDATE SET
                    status = excluded.status,
                    processed_at = excluded.processed_at,
                    retry_attempt = excluded.retry_attempt
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
                    retry_attempt,
                ),
            )
            conn.commit()

    # ── retention / cleanup ──────────────────────────────────────────

    def persist_execution_atomically(
        self,
        *,
        fill: dict[str, Any],
        user_id: str,
        leader_id: str | None,
        condition_id: str,
        token_id: str,
        side: str,
        fill_size: str,
        fill_price: str,
        fill_fee: str,
        fill_ts: int,
        fill_json: str,
        trade_key: str,
        tx_hash: str | None,
        asset: str | None,
        trade_status: str,
    ) -> bool:
        """Persist fill + position update + processed trade in a single transaction.

        Returns True if all three operations succeeded atomically.
        Returns False if the fill was a duplicate (uniqueness constraint).
        """
        try:
            with create_connection(self.db_path) as conn:
                # 1. Insert fill (may fail on uniqueness constraint)
                try:
                    conn.execute(
                        """
                        INSERT INTO fills (
                            user_id, leader_id, order_id, condition_id, token_id, side,
                            size_tokens, price, fee, fill_ts, fill_json, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                        """,
                        (
                            user_id, leader_id, fill.get("order_id"),
                            condition_id, token_id, side,
                            fill_size, fill_price, fill_fee, fill_ts, fill_json,
                        ),
                    )
                except sqlite3.IntegrityError:
                    # Uniqueness constraint violation — duplicate fill
                    import logging
                    logging.getLogger(__name__).info("Atomic fill rejected as duplicate: user=%s order=%s cond=%s", user_id, fill.get("order_id"), condition_id)
                    conn.rollback()
                    return False

                # 2. Update position (apply fill)
                current = conn.execute(
                    "SELECT size_tokens, avg_entry_price FROM positions WHERE user_id = ? AND condition_id = ?",
                    (user_id, condition_id),
                ).fetchone()

                side_u = str(side or "").upper()
                if side_u == "BUY":
                    if current:
                        cur_size = Decimal(str(current["size_tokens"] or "0"))
                        cur_avg = Decimal(str(current["avg_entry_price"] or "0"))
                        new_fill = Decimal(fill_size)
                        new_price = Decimal(fill_price)
                        new_size = cur_size + new_fill
                        if new_size > 0:
                            avg = ((cur_size * cur_avg) + (new_fill * new_price)) / new_size
                        else:
                            avg = new_price
                        conn.execute(
                            """
                            INSERT INTO positions (user_id, condition_id, token_id, leader_id, size_tokens, avg_entry_price, updated_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                            ON CONFLICT(user_id, condition_id) DO UPDATE SET
                                token_id = excluded.token_id,
                                leader_id = excluded.leader_id,
                                size_tokens = excluded.size_tokens,
                                avg_entry_price = excluded.avg_entry_price,
                                updated_at = excluded.updated_at
                            """,
                            (user_id, condition_id, token_id, leader_id, str(new_size), str(avg), time.time()),
                        )
                    else:
                        conn.execute(
                            """
                            INSERT INTO positions (user_id, condition_id, token_id, leader_id, size_tokens, avg_entry_price, updated_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                            ON CONFLICT(user_id, condition_id) DO UPDATE SET
                                token_id = excluded.token_id,
                                leader_id = excluded.leader_id,
                                size_tokens = excluded.size_tokens,
                                avg_entry_price = excluded.avg_entry_price,
                                updated_at = excluded.updated_at
                            """,
                            (user_id, condition_id, token_id, leader_id, fill_size, fill_price, time.time()),
                        )

                elif side_u == "SELL":
                    if current:
                        cur_size = Decimal(str(current["size_tokens"] or "0"))
                        sell_size = min(Decimal(fill_size), cur_size)
                        new_size = cur_size - sell_size
                        if new_size <= Decimal("1e-12"):
                            conn.execute(
                                "DELETE FROM positions WHERE user_id = ? AND condition_id = ?",
                                (user_id, condition_id),
                            )
                        else:
                            conn.execute(
                                """
                                INSERT INTO positions (user_id, condition_id, token_id, leader_id, size_tokens, avg_entry_price, updated_at)
                                VALUES (?, ?, ?, ?, ?, ?, ?)
                                ON CONFLICT(user_id, condition_id) DO UPDATE SET
                                    token_id = excluded.token_id,
                                    size_tokens = excluded.size_tokens,
                                    updated_at = excluded.updated_at
                                """,
                                (user_id, condition_id, token_id, leader_id, str(new_size), str(current["avg_entry_price"]), time.time()),
                            )

                # 3. Mark processed trade
                conn.execute(
                    """
                    INSERT INTO processed_trades (
                        trade_key, user_id, leader_id, tx_hash, side, asset, condition_id, status, processed_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(trade_key) DO UPDATE SET
                        status = excluded.status,
                        processed_at = excluded.processed_at
                    """,
                    (trade_key, user_id, leader_id, tx_hash, side, asset, condition_id, trade_status, time.time()),
                )

                conn.commit()
                return True
        except sqlite3.OperationalError:
            # DB-level failure (locked, missing table, etc.) — let caller handle
            raise

    def purge_old_processed_trades(self, max_age_seconds: float = 30 * 86400) -> int:
        """Delete processed_trades older than *max_age_seconds*. Returns rows deleted."""
        cutoff = time.time() - max_age_seconds
        with create_connection(self.db_path) as conn:
            cur = conn.execute(
                "DELETE FROM processed_trades WHERE processed_at < ?",
                (cutoff,),
            )
            conn.commit()
            return cur.rowcount
