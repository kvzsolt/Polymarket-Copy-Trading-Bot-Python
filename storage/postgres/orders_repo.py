"""Async asyncpg port of storage/sqlite/orders_repo.py.

Behaviour invariants preserved verbatim:
  * ``try_acquire_submission_lock``: single round-trip
    ``INSERT ... ON CONFLICT DO NOTHING RETURNING idempotency_key``
    is atomic — caller treats a None result as "someone else holds
    the lock, do NOT submit" (Batch 3 race fix).
  * ``update_submission_status``: wraps the SELECT-merge-UPDATE in
    ``async with conn.transaction()`` plus a row-level
    ``FOR UPDATE`` on the SELECT, so two writers cannot lost-update
    the merged ``response_json`` (Batch 3 lost-update fix).
  * ``try_claim_processed_trade``: same atomic INSERT-or-RETURNING
    pattern; an ON CONFLICT DO UPDATE WHERE clause only re-claims
    rows whose status is retryable or whose ``in_progress`` claim
    is stale (TTL).
  * ``mark_processed_trade``: never overwrites a row already in a
    terminal status (executed / processed / filled_reconciled /
    duplicate_ignored / terminal_failed).
  * ``persist_execution_atomically``: fill insert + position
    update + processed_trade mark inside one transaction. Returns
    False only when the fill's unique index trips (duplicate).

Translation notes:
  * jsonb columns require explicit ``$N::jsonb`` casts on writes —
    asyncpg will not auto-encode dicts to jsonb without it.
  * timestamptz ``created_at`` / ``updated_at`` use ``now()`` DDL
    default; UPDATE statements set them via ``now()`` literal.
  * ``cursor.rowcount`` → parse asyncpg's ``execute`` status
    string (``"DELETE N"``, ``"UPDATE N"``).
  * ``json_extract(col, '$.side')`` → ``col->>'side'`` for text
    extraction from jsonb (no need for ``upper()`` wrapping —
    Postgres jsonb keys are exact-text).
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import asyncpg

from domain.ports.repositories import AbstractOrderRepository

logger = logging.getLogger(__name__)


_TERMINAL_SUCCESS_STATES = {
    "submitted",
    "submitted_unconfirmed",
    "partially_filled",
    "filled",
    "cancelled",
    "rejected",
    "duplicate_ignored",
}

_FAILURE_STATES = {
    "failed",
    "submission_failed",
    "created",
    "retryable_failed",
    "terminal_failed",
}
_RETRYABLE_FAILED_STATUS = "retryable_failed"
_LEGACY_RETRY_PREFIXES = ("retryable_failed_", "retry_pending_")

_PROCESSED_TRADE_TERMINAL_STATES = [
    "executed",
    "processed",
    "filled_reconciled",
    "duplicate_ignored",
    "terminal_failed",
]


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


def _affected(status: str) -> int:
    if not status:
        return 0
    parts = status.split()
    try:
        return int(parts[-1])
    except (ValueError, IndexError):
        return 0


def _json_str(payload: dict[str, Any] | None) -> str | None:
    if payload is None:
        return None
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)


def _parse_jsonb(value: Any) -> dict[str, Any]:
    """asyncpg returns jsonb as a JSON-encoded string by default; some
    rows may already be dicts if a codec was registered. Tolerate both."""
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    return json.loads(value)


class OrdersRepo(AbstractOrderRepository):
    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    async def try_acquire_submission_lock(
        self,
        key: str,
        user_id: str,
        leader_id: str | None,
        request_payload: dict,
    ) -> bool:
        request_json = _json_str(request_payload) or "{}"
        async with self._pool.acquire() as conn:
            # RETURNING returns the inserted row's key when INSERT happens;
            # when ON CONFLICT DO NOTHING fires there is no returned row.
            returned = await conn.fetchval(
                """
                INSERT INTO orders (
                    idempotency_key, user_id, leader_id, order_id, status,
                    request_json, response_json, attempt_count, last_error,
                    created_at, updated_at
                ) VALUES (
                    $1, $2, $3, NULL, 'pending_lock',
                    $4::jsonb, NULL, 0, NULL,
                    now(), now()
                )
                ON CONFLICT(idempotency_key) DO NOTHING
                RETURNING idempotency_key
                """,
                key,
                user_id,
                leader_id,
                request_json,
            )
        return returned is not None

    async def is_submission_locked(self, key: str) -> bool:
        rec = await self.get_by_idempotency_key(key)
        if rec is None:
            return False
        if rec.get("order_id"):
            return False
        return rec.get("status") in ("pending", "pending_lock")

    @staticmethod
    def _row_to_order_record(row: asyncpg.Record | None) -> dict[str, Any] | None:
        if not row:
            return None
        return {
            "idempotency_key": row["idempotency_key"],
            "user_id": row["user_id"],
            "leader_id": row["leader_id"],
            "order_id": row["order_id"],
            "status": row["status"],
            "request": _parse_jsonb(row["request_json"]),
            "response": _parse_jsonb(row["response_json"]),
            "attempt_count": int(row["attempt_count"] or 0),
            "last_error": row["last_error"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "matched_total": row["matched_total"],
        }

    async def upsert_matched_total(
        self, *, order_id: str, matched: str, committed: bool = True
    ) -> None:
        if not order_id:
            return
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE orders SET matched_total = $1, updated_at = now()
                WHERE order_id = $2
                """,
                matched,
                order_id,
            )
        # ``committed`` kept for API parity with the SQLite version (caller-driven
        # commit); asyncpg execute auto-commits when no transaction is open.

    async def get_by_idempotency_key(
        self, key: str
    ) -> dict[str, Any] | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT idempotency_key, user_id, leader_id, order_id, status,
                       request_json, response_json, attempt_count, last_error,
                       created_at, updated_at,
                       COALESCE(matched_total, '0') AS matched_total
                FROM orders
                WHERE idempotency_key = $1
                LIMIT 1
                """,
                key,
            )
        return self._row_to_order_record(row)

    async def get_by_order_id(self, order_id: str) -> dict[str, Any] | None:
        if not order_id:
            return None
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT idempotency_key, user_id, leader_id, order_id, status,
                       request_json, response_json, attempt_count, last_error,
                       created_at, updated_at,
                       COALESCE(matched_total, '0') AS matched_total
                FROM orders
                WHERE order_id = $1
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                order_id,
            )
        return self._row_to_order_record(row)

    async def save_submission(self, record: OrderSubmissionRecord) -> None:
        request_json = _json_str(record.request_payload) or "{}"
        response_json = _json_str(record.response_payload)
        async with self._pool.acquire() as conn:
            await conn.execute(
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
                    $1, $2, $3, $4, $5,
                    $6::jsonb, $7::jsonb, 1, $8,
                    now(), now()
                )
                ON CONFLICT(idempotency_key) DO UPDATE SET
                    user_id  = COALESCE(NULLIF(excluded.user_id, ''), orders.user_id),
                    leader_id = COALESCE(excluded.leader_id, orders.leader_id),
                    order_id  = COALESCE(excluded.order_id, orders.order_id),
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
                    updated_at = now()
                """,
                record.idempotency_key,
                record.user_id,
                record.leader_id,
                record.order_id,
                record.status,
                request_json,
                response_json,
                record.last_error,
            )

    async def mark_fill(
        self,
        *,
        order_id: str,
        fill: dict[str, Any],
        user_id: str | None = None,
        leader_id: str | None = None,
    ) -> None:
        async with self._pool.acquire() as conn:
            # greptile P1: ON CONFLICT DO NOTHING on the fill identity
            # tuple so a reconciler-triggered second arrival of the same
            # fill silently no-ops instead of raising
            # ``UniqueViolationError``. Pairs with the
            # ``idx_fills_dedup`` unique index in migrations.py.
            await conn.execute(
                """
                INSERT INTO fills (
                    user_id, leader_id, order_id, condition_id, token_id, side,
                    size_tokens, price, fee, fill_ts, fill_json
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb)
                ON CONFLICT (user_id, order_id, condition_id, side, fill_ts) DO NOTHING
                """,
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
            )

    async def has_processed_trade(self, trade_key: str) -> bool:
        if not trade_key:
            return False
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT status FROM processed_trades WHERE trade_key = $1 LIMIT 1",
                trade_key,
            )
        if row is None:
            return False
        status = str(row["status"] or "")
        if status == _RETRYABLE_FAILED_STATUS:
            return False
        return not any(status.startswith(p) for p in _LEGACY_RETRY_PREFIXES)

    async def get_processed_trade(
        self, trade_key: str
    ) -> dict[str, Any] | None:
        if not trade_key:
            return None
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT trade_key, user_id, leader_id, tx_hash, side, asset,
                       condition_id, status, processed_at,
                       COALESCE(retry_attempt, 0) AS retry_attempt
                FROM processed_trades
                WHERE trade_key = $1
                LIMIT 1
                """,
                trade_key,
            )
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

    async def try_claim_processed_trade(
        self,
        *,
        trade_key: str,
        user_id: str,
        leader_id: str | None,
        tx_hash: str | None,
        side: str | None,
        asset: str | None,
        condition_id: str | None,
        status: str = "in_progress",
        # DL-06: the prior 300s default let a reconciler reclaim while a
        # submitter retry was still in flight. Match the database command
        # timeout so a true stall (no progress for the full HTTP / SQL
        # round-trip budget) is required before a reclaim can land. The
        # value is overridable via the keyword for callers that need a
        # tighter or looser window in tests.
        stale_claim_ttl_seconds: float = 30.0,
    ) -> bool:
        if not trade_key:
            return False
        now = time.time()
        stale_cutoff = now - max(0.0, stale_claim_ttl_seconds)
        try:
            async with self._pool.acquire() as conn:
                returned = await conn.fetch(
                    """
                    INSERT INTO processed_trades (
                        trade_key, user_id, leader_id, tx_hash, side, asset,
                        condition_id, status, processed_at, retry_attempt, order_id
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 0, NULL)
                    ON CONFLICT(trade_key) DO UPDATE SET
                        status = excluded.status,
                        processed_at = excluded.processed_at,
                        retry_attempt = COALESCE(processed_trades.retry_attempt, 0) + 1
                    WHERE processed_trades.status = 'retryable_failed'
                       OR processed_trades.status LIKE 'retryable_failed_%'
                       OR processed_trades.status LIKE 'retry_pending_%'
                       OR (processed_trades.status = 'in_progress'
                           AND processed_trades.processed_at < $10)
                    RETURNING trade_key
                    """,
                    trade_key,
                    user_id,
                    leader_id,
                    tx_hash,
                    side,
                    asset,
                    condition_id,
                    status,
                    now,
                    stale_cutoff,
                )
                return len(returned) == 1
        except asyncpg.exceptions.UniqueViolationError:
            return False

    async def mark_processed_trade(
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
        order_id: str | None = None,
    ) -> None:
        if not trade_key:
            return
        async with self._pool.acquire() as conn:
            # Postgres array equality lets us replace SQLite's dynamic
            # ``?, ?, ?`` placeholder string with a single ``= ANY(...)``
            # — eliminates the f-string SQL fragment from the legacy code.
            await conn.execute(
                """
                INSERT INTO processed_trades (
                    trade_key, user_id, leader_id, tx_hash, side, asset,
                    condition_id, status, processed_at, retry_attempt, order_id
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                ON CONFLICT(trade_key) DO UPDATE SET
                    status = excluded.status,
                    processed_at = excluded.processed_at,
                    retry_attempt = excluded.retry_attempt,
                    order_id = COALESCE(excluded.order_id, processed_trades.order_id)
                WHERE NOT (processed_trades.status = ANY($12::text[]))
                """,
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
                order_id,
                _PROCESSED_TRADE_TERMINAL_STATES,
            )

    async def delete_processed_trade(self, trade_key: str) -> None:
        if not trade_key:
            return
        async with self._pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM processed_trades WHERE trade_key = $1", trade_key
            )

    async def update_submission_status(
        self,
        *,
        order_id: str,
        status: str,
        response_patch: dict[str, Any] | None = None,
    ) -> None:
        if not order_id:
            return
        response_patch = response_patch or {}
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                # FOR UPDATE on the SELECT serialises against concurrent
                # writers — Postgres equivalent of the SQLite BEGIN IMMEDIATE
                # writer-lock. Two reconcilers cannot both observe the same
                # response_json snapshot and lose-update each other. The
                # partial unique index ``idx_orders_order_id WHERE
                # order_id IS NOT NULL`` already enforces at most one row
                # per non-null ``order_id``, so no ORDER BY / LIMIT needed.
                row = await conn.fetchrow(
                    """
                    SELECT response_json
                    FROM orders
                    WHERE order_id = $1
                    FOR UPDATE
                    """,
                    order_id,
                )
                if row is None:
                    return
                existing_response = _parse_jsonb(row["response_json"])
                merged_response = {**existing_response, **response_patch, "status": status}
                await conn.execute(
                    """
                    UPDATE orders
                    SET status = $1,
                        response_json = $2::jsonb,
                        updated_at = now()
                    WHERE order_id = $3
                    """,
                    status,
                    json.dumps(
                        merged_response,
                        sort_keys=True,
                        separators=(",", ":"),
                        default=str,
                    ),
                    order_id,
                )

    async def get_unconfirmed_order_submissions(
        self,
        *,
        user_id: str | None = None,
        side: str | None = None,
    ) -> list[dict[str, Any]]:
        query = """
            SELECT idempotency_key, user_id, leader_id, order_id, status,
                   request_json, response_json, attempt_count, last_error,
                   created_at, updated_at,
                   COALESCE(matched_total, '0') AS matched_total
            FROM orders
            WHERE status IN ('submitted_unconfirmed', 'submitted')
              AND order_id IS NOT NULL
        """
        params: list[Any] = []
        if user_id is not None:
            query += f" AND user_id = ${len(params) + 1}"
            params.append(user_id)
        if side is not None:
            # jsonb ->> returns text; pin a parameterised case-insensitive
            # comparison so SQL stays static and the side value is bound.
            query += (
                f" AND UPPER(COALESCE(request_json->>'side', '')) = ${len(params) + 1}"
            )
            params.append(str(side).upper())
        query += " ORDER BY updated_at ASC"

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(query, *params)

        return [
            record for row in rows
            if (record := self._row_to_order_record(row)) is not None
        ]

    async def get_unconfirmed_buys(
        self, max_age_seconds: float = 300.0
    ) -> list[dict]:
        cutoff = time.time() - max_age_seconds
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT trade_key, user_id, leader_id, tx_hash, asset,
                       condition_id, order_id, processed_at
                FROM processed_trades
                WHERE status IN ('submitted_unconfirmed', 'submitted')
                  AND side = 'BUY'
                  AND (order_id IS NOT NULL OR processed_at <= $1)
                ORDER BY processed_at ASC
                """,
                cutoff,
            )
        return [dict(r) for r in rows]

    async def get_unconfirmed_sells(
        self, max_age_seconds: float = 300.0
    ) -> list[dict]:
        cutoff = time.time() - max_age_seconds
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT trade_key, user_id, leader_id, tx_hash, asset,
                       condition_id, order_id, processed_at
                FROM processed_trades
                WHERE status IN ('submitted_unconfirmed', 'submitted')
                  AND side = 'SELL'
                  AND (order_id IS NOT NULL OR processed_at <= $1)
                ORDER BY processed_at ASC
                """,
                cutoff,
            )
        return [dict(r) for r in rows]

    async def persist_execution_atomically(
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
        try:
            async with self._pool.acquire() as conn:
                async with conn.transaction():
                    # 1. Insert fill (may trip uniqueness constraint).
                    try:
                        await conn.execute(
                            """
                            INSERT INTO fills (
                                user_id, leader_id, order_id, condition_id,
                                token_id, side, size_tokens, price, fee,
                                fill_ts, fill_json
                            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb)
                            """,
                            user_id,
                            leader_id,
                            fill.get("order_id"),
                            condition_id,
                            token_id,
                            side,
                            fill_size,
                            fill_price,
                            fill_fee,
                            fill_ts,
                            fill_json,
                        )
                    except asyncpg.exceptions.UniqueViolationError:
                        logger.info(
                            "Atomic fill rejected as duplicate: user=%s order=%s cond=%s",
                            user_id,
                            fill.get("order_id"),
                            condition_id,
                        )
                        # Raise so the transaction context manager rolls back
                        # the partial work and the caller sees False below.
                        raise

                    # 2. Update position (apply fill).
                    # greptile P1 + cubic P1: ``FOR UPDATE`` serialises
                    # concurrent fills against the same position row so
                    # two partial fills cannot both read the same
                    # snapshot and overwrite each other (lost-update).
                    # A first-fill INSERT path is covered by the
                    # ``ON CONFLICT`` UPDATE below, which re-reads the
                    # current row via excluded — the lock here is what
                    # prevents the second fill from issuing a stale
                    # avg_entry_price calculation.
                    current = await conn.fetchrow(
                        "SELECT size_tokens, avg_entry_price FROM positions "
                        "WHERE user_id = $1 AND condition_id = $2 AND token_id = $3 "
                        "FOR UPDATE",
                        user_id,
                        condition_id,
                        token_id,
                    )

                    side_u = str(side or "").upper()
                    if side_u == "BUY":
                        if current:
                            cur_size = Decimal(str(current["size_tokens"] or "0"))
                            cur_avg = Decimal(str(current["avg_entry_price"] or "0"))
                            new_fill = Decimal(fill_size)
                            new_price = Decimal(fill_price)
                            new_size = cur_size + new_fill
                            if new_size > 0:
                                avg = (
                                    (cur_size * cur_avg)
                                    + (new_fill * new_price)
                                ) / new_size
                            else:
                                avg = new_price
                            await conn.execute(
                                """
                                INSERT INTO positions (
                                    user_id, condition_id, token_id, leader_id,
                                    size_tokens, avg_entry_price, updated_at
                                ) VALUES ($1, $2, $3, $4, $5, $6, now())
                                ON CONFLICT(user_id, condition_id, token_id) DO UPDATE SET
                                    token_id        = excluded.token_id,
                                    leader_id       = excluded.leader_id,
                                    size_tokens     = excluded.size_tokens,
                                    avg_entry_price = excluded.avg_entry_price,
                                    updated_at      = excluded.updated_at
                                """,
                                user_id,
                                condition_id,
                                token_id,
                                leader_id,
                                str(new_size),
                                str(avg),
                            )
                        else:
                            await conn.execute(
                                """
                                INSERT INTO positions (
                                    user_id, condition_id, token_id, leader_id,
                                    size_tokens, avg_entry_price, updated_at
                                ) VALUES ($1, $2, $3, $4, $5, $6, now())
                                ON CONFLICT(user_id, condition_id, token_id) DO UPDATE SET
                                    token_id        = excluded.token_id,
                                    leader_id       = excluded.leader_id,
                                    size_tokens     = excluded.size_tokens,
                                    avg_entry_price = excluded.avg_entry_price,
                                    updated_at      = excluded.updated_at
                                """,
                                user_id,
                                condition_id,
                                token_id,
                                leader_id,
                                fill_size,
                                fill_price,
                            )

                    elif side_u == "SELL":
                        if current:
                            cur_size = Decimal(str(current["size_tokens"] or "0"))
                            if Decimal(fill_size) > cur_size:
                                logger.warning(
                                    "Atomic SELL fill exceeds held size; clamping "
                                    "user=%s cond=%s token=%s fill_size=%s current_size=%s",
                                    user_id,
                                    condition_id,
                                    token_id,
                                    fill_size,
                                    current["size_tokens"],
                                )
                            sell_size = min(Decimal(fill_size), cur_size)
                            new_size = cur_size - sell_size
                            if new_size <= Decimal("1e-12"):
                                await conn.execute(
                                    "DELETE FROM positions "
                                    "WHERE user_id = $1 AND condition_id = $2 AND token_id = $3",
                                    user_id,
                                    condition_id,
                                    token_id,
                                )
                            else:
                                await conn.execute(
                                    """
                                    INSERT INTO positions (
                                        user_id, condition_id, token_id, leader_id,
                                        size_tokens, avg_entry_price, updated_at
                                    ) VALUES ($1, $2, $3, $4, $5, $6, now())
                                    ON CONFLICT(user_id, condition_id, token_id) DO UPDATE SET
                                        token_id    = excluded.token_id,
                                        size_tokens = excluded.size_tokens,
                                        updated_at  = excluded.updated_at
                                    """,
                                    user_id,
                                    condition_id,
                                    token_id,
                                    leader_id,
                                    str(new_size),
                                    str(current["avg_entry_price"]),
                                )

                    # 3. Mark processed trade.
                    # greptile P1: terminal-state guard so a later
                    # non-terminal status update cannot overwrite a row
                    # that already reached a terminal state. Mirrors the
                    # legacy SQLite ``mark_processed_trade`` invariant
                    # tracked in PR #105 audit-fix #102.
                    await conn.execute(
                        """
                        INSERT INTO processed_trades (
                            trade_key, user_id, leader_id, tx_hash, side, asset,
                            condition_id, order_id, retry_attempt, status, processed_at
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                        ON CONFLICT(trade_key) DO UPDATE SET
                            order_id      = excluded.order_id,
                            retry_attempt = excluded.retry_attempt,
                            status        = excluded.status,
                            processed_at  = excluded.processed_at
                        WHERE processed_trades.status NOT IN (
                            'processed', 'terminal_failed', 'duplicate_ignored',
                            'skipped_sell_size_zero'
                        )
                        """,
                        trade_key,
                        user_id,
                        leader_id,
                        tx_hash,
                        side,
                        asset,
                        condition_id,
                        fill.get("order_id"),
                        int(fill.get("retry_attempt") or 0),
                        trade_status,
                        time.time(),
                    )

                    return True
        except asyncpg.exceptions.UniqueViolationError:
            # Duplicate fill — the transaction context manager rolled back.
            return False

    async def purge_old_processed_trades(
        self, max_age_seconds: float = 30 * 86400
    ) -> int:
        cutoff = time.time() - max_age_seconds
        async with self._pool.acquire() as conn:
            status = await conn.execute(
                "DELETE FROM processed_trades WHERE processed_at < $1", cutoff
            )
        return _affected(status)
