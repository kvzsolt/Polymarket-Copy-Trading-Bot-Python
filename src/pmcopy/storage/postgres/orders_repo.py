"""Async asyncpg port of storage/sqlite/orders_repo.py.

Behaviour invariants preserved verbatim:
  * ``try_acquire_submission_lock``: source-intent advisory locking plus
    ``INSERT ... ON CONFLICT ... RETURNING idempotency_key``
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
from decimal import Decimal
from typing import Any

import asyncpg

from pmcopy.adapters.time.clock import MonotonicClock, SystemMonotonicClock
from pmcopy.interfaces.repositories import AbstractOrderRepository
from pmcopy.storage.postgres.orders_ambiguous_mixin import _OrdersAmbiguousMixin
from pmcopy.storage.postgres.orders_locks_mixin import _OrdersLocksMixin
from pmcopy.storage.postgres.orders_records import (
    CROSS_SOURCE_CLAIM_STATUS,
    _DEFAULT_QUARANTINE_COUNT_TTL_SECONDS,
    _DEFAULT_STALE_LOCK_TAKEOVER_SECONDS,  # noqa: F401
    _DIRECT_EXPIRABLE_STALE_LOCK_STATUSES,  # noqa: F401
    _FAILURE_STATES,  # noqa: F401
    _LEGACY_RETRY_PREFIXES,
    _PROCESSED_TRADE_TERMINAL_STATES,
    _RETRYABLE_FAILED_STATUS,
    _STALE_SUBMISSION_LOCK_STATUSES,  # noqa: F401
    _TERMINAL_SUCCESS_STATES,  # noqa: F401
    _UNCONFIRMED_SUBMISSION_STATES,
    LegacyOwnershipMappingReport,  # noqa: F401
    OrderSubmissionRecord,
    _affected,
    _bounded_positive,
    _json_str,
    _optional_decimal,  # noqa: F401
    _optional_text,  # noqa: F401
    _order_identity_values,
    _parse_jsonb,
    _row_to_ambiguous_submission,  # noqa: F401
    _submission_amount_unit,  # noqa: F401
    _utc_datetime,  # noqa: F401
)

logger = logging.getLogger(__name__)



class OrdersRepo(_OrdersLocksMixin, _OrdersAmbiguousMixin, AbstractOrderRepository):
    """Persist order and idempotency state in PostgreSQL."""

    def __init__(
        self,
        pool: asyncpg.Pool,
        *,
        quarantine_count_clock: MonotonicClock | None = None,
        quarantine_count_ttl_seconds: float = _DEFAULT_QUARANTINE_COUNT_TTL_SECONDS,
    ):
        self._pool = pool
        # Pure-observability throttle: the legacy blank-user quarantine count
        # (``SELECT count(*)`` + WARNING) used to run on EVERY scoped
        # unconfirmed call — one extra count query per recovery cycle per
        # account for a log line.  At most one count per (scope, TTL window)
        # now; the scoped query RESULTS are never affected because the
        # count feeds only the log, never the returned rows.
        self._quarantine_clock = (
            quarantine_count_clock if quarantine_count_clock is not None else SystemMonotonicClock()
        )
        self._quarantine_ttl_seconds = max(float(quarantine_count_ttl_seconds), 0.0)
        self._quarantine_counted_at: dict[str, float] = {}

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
        """Insert or update matched total."""
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

    async def get_by_idempotency_key(self, key: str) -> dict[str, Any] | None:
        """Return by idempotency key."""
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
        """Return by order id."""
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

    async def get_by_reservation_id(self, reservation_id: str) -> dict[str, Any] | None:
        """Return by reservation id."""
        if not reservation_id:
            return None
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT idempotency_key, user_id, leader_id, order_id, status,
                       request_json, response_json, attempt_count, last_error,
                       created_at, updated_at,
                       COALESCE(matched_total, '0') AS matched_total
                FROM orders
                WHERE request_json->>'reservation_id' = $1
                   OR request_json->'recovery_metadata'->>'reservation_id' = $1
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                reservation_id,
            )
        return self._row_to_order_record(row)


    async def save_submission(self, record: OrderSubmissionRecord) -> None:
        """Persist normalized submission evidence idempotently."""
        request_json = _json_str(record.request_payload) or "{}"
        response_json = _json_str(record.response_payload)
        token_id, amount_unit, order_kind, price_limit, correlation_ids = _order_identity_values(
            record.request_payload
        )
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
                    updated_at,
                    token_id,
                    amount_unit,
                    order_kind,
                    effective_price_limit,
                    correlation_ids
                ) VALUES (
                    $1, $2, $3, $4, $5,
                    $6::jsonb, $7::jsonb, 1, $8,
                    now(), now(),
                    $9, $10, $11, $12, $13::jsonb
                )
                ON CONFLICT(idempotency_key) DO UPDATE SET
                    user_id  = COALESCE(NULLIF(excluded.user_id, ''), orders.user_id),
                    leader_id = COALESCE(excluded.leader_id, orders.leader_id),
                    order_id  = COALESCE(excluded.order_id, orders.order_id),
                    status = CASE
                        WHEN orders.status IN ('submitted','submitted_unconfirmed',
                                               'partially_filled','filled','cancelled',
                                               'rejected','duplicate_ignored')
                             AND excluded.status IN ('failed','submission_failed','created')
                        THEN orders.status
                        ELSE excluded.status
                    END,
                    request_json = excluded.request_json,
                    response_json = CASE
                        WHEN orders.status IN ('submitted','submitted_unconfirmed',
                                               'partially_filled','filled','cancelled',
                                               'rejected','duplicate_ignored')
                             AND excluded.status IN ('failed','submission_failed','created')
                        THEN orders.response_json
                        ELSE excluded.response_json
                    END,
                    attempt_count = COALESCE(orders.attempt_count, 0) + 1,
                    last_error = excluded.last_error,
                    updated_at = now(),
                    token_id = COALESCE(excluded.token_id, orders.token_id),
                    amount_unit = COALESCE(excluded.amount_unit, orders.amount_unit),
                    order_kind = COALESCE(excluded.order_kind, orders.order_kind),
                    effective_price_limit = COALESCE(
                        excluded.effective_price_limit, orders.effective_price_limit),
                    correlation_ids = COALESCE(
                        excluded.correlation_ids, orders.correlation_ids)
                """,
                record.idempotency_key,
                record.user_id,
                record.leader_id,
                record.order_id,
                record.status,
                request_json,
                response_json,
                record.last_error,
                token_id,
                amount_unit,
                order_kind,
                price_limit,
                correlation_ids,
            )

    async def mark_fill(
        self,
        *,
        order_id: str,
        fill: dict[str, Any],
        user_id: str | None = None,
        leader_id: str | None = None,
    ) -> None:
        """Mark fill."""
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
        """Return whether the economic trade identity is already claimed."""
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

    async def get_processed_trade(self, trade_key: str) -> dict[str, Any] | None:
        """Return processed trade.

        ``order_id`` is part of the projection because it is the only durable
        evidence that a copy already reached the venue for this key: the
        cross-source claim release reads it to tell "the pipeline raised
        before submitting anything" from "an order is already out there".
        """
        if not trade_key:
            return None
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT trade_key, user_id, leader_id, tx_hash, side, asset,
                       condition_id, status, order_id, processed_at,
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
            "order_id": row["order_id"],
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
        """Attempt to claim processed trade."""
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
        """Mark processed trade."""
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
        """Delete processed trade."""
        if not trade_key:
            return
        async with self._pool.acquire() as conn:
            await conn.execute("DELETE FROM processed_trades WHERE trade_key = $1", trade_key)

    async def release_cross_source_claim(self, trade_key: str) -> bool:
        """Delete a tx-level cross-source claim row, guarded by its status.

        The on-chain dispatch path inserts an order-less ``processed_trades``
        row carrying ``CROSS_SOURCE_CLAIM_STATUS`` so REST polling cannot copy
        the same fill, and releases it again when the dispatch turned out to
        produce nothing. The status predicate keeps that release incapable of
        removing a row the claim policy no longer owns — a real submission, a
        settled fill, or a polling claim that replaced the sentinel meanwhile.

        Args:
            trade_key: Tx-level processed-trade key to release.

        Returns:
            ``True`` when a sentinel row was removed; ``False`` when no row
            matched, i.e. it was absent or held under a different status.
        """
        if not trade_key:
            return False
        async with self._pool.acquire() as conn:
            status = await conn.execute(
                "DELETE FROM processed_trades WHERE trade_key = $1 AND status = $2",
                trade_key,
                CROSS_SOURCE_CLAIM_STATUS,
            )
        return _affected(status) > 0

    async def update_submission_status(
        self,
        *,
        order_id: str,
        status: str,
        response_patch: dict[str, Any] | None = None,
    ) -> None:
        """Update submission status."""
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

    async def update_submission_status_by_idempotency_key(
        self,
        *,
        idempotency_key: str,
        status: str,
        response_patch: dict[str, Any] | None = None,
    ) -> None:
        """Update submission status by idempotency key (used for no-order-ID rows)."""
        if not idempotency_key:
            return
        response_patch = response_patch or {}
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    """
                    SELECT response_json
                    FROM orders
                    WHERE idempotency_key = $1
                    FOR UPDATE
                    """,
                    idempotency_key,
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
                    WHERE idempotency_key = $3
                    """,
                    status,
                    json.dumps(
                        merged_response,
                        sort_keys=True,
                        separators=(",", ":"),
                        default=str,
                    ),
                    idempotency_key,
                )

    async def get_unconfirmed_order_submissions(
        self,
        *,
        user_id: str | None = None,
        side: str | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        """Return unconfirmed order submissions."""
        bounded_limit = _bounded_positive(
            limit,
            field="limit",
            maximum=1000,
        )
        query = """
            SELECT idempotency_key, user_id, leader_id, order_id, status,
                   request_json, response_json, attempt_count, last_error,
                   created_at, updated_at,
                   COALESCE(matched_total, '0') AS matched_total
            FROM orders
            WHERE status = ANY($1::text[])
              AND order_id IS NOT NULL
        """
        params: list[Any] = [list(_UNCONFIRMED_SUBMISSION_STATES)]
        if user_id is not None:
            query += f" AND user_id = ${len(params) + 1}"
            params.append(user_id)
        if side is not None:
            # jsonb ->> returns text; pin a parameterised case-insensitive
            # comparison so SQL stays static and the side value is bound.
            query += f" AND UPPER(COALESCE(request_json->>'side', '')) = ${len(params) + 1}"
            params.append(str(side).upper())
        params.append(bounded_limit)
        query += f" ORDER BY updated_at ASC LIMIT ${len(params)}"

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(query, *params)

        return [record for row in rows if (record := self._row_to_order_record(row)) is not None]

    async def get_unconfirmed_buys(
        self,
        max_age_seconds: float = 300.0,
        *,
        user_id: str | None = None,
    ) -> list[dict]:
        """Return unconfirmed buys for one account's pending set.

        ``user_id`` is mandatory: a blank/None scope raises ``ValueError``
        (fail-closed) instead of widening to every account's rows.
        """
        return await self._unconfirmed_processed_trades(
            side="BUY", max_age_seconds=max_age_seconds, user_id=user_id
        )

    async def get_unconfirmed_sells(
        self,
        max_age_seconds: float = 300.0,
        *,
        user_id: str | None = None,
    ) -> list[dict]:
        """Return unconfirmed sells for one account's pending set.

        ``user_id`` is mandatory: a blank/None scope raises ``ValueError``
        (fail-closed) instead of widening to every account's rows.
        """
        return await self._unconfirmed_processed_trades(
            side="SELL", max_age_seconds=max_age_seconds, user_id=user_id
        )

    async def _unconfirmed_processed_trades(
        self,
        *,
        side: str,
        max_age_seconds: float,
        user_id: str | None,
    ) -> list[dict]:
        """List one account's unconfirmed processed trades.

        Audit A01: the scope is a strict equality predicate.  Legacy NULL/
        ''-user rows are NOT unioned into every user's pending set; they are
        excluded and counted into a logged quarantine reason pending the
        one-shot provenance mapping helper
        (``map_legacy_unowned_rows_to_provenance_owners``) — an
        observability-only count throttled to at most once per (side, TTL
        window), never a predicate on the returned rows.

        Final-review fix (owed since T24): a blank/None ``user_id`` fails
        closed with ``ValueError`` — mirroring the sister method
        ``list_no_id_ambiguous_submissions`` — instead of silently widening
        the pending set to EVERY account's trades.  Every caller owns one
        account (``TradingContext.user_id`` is validated non-empty by both
        roster loaders); an unscoped maintenance read, if one is ever
        needed, gets its own explicitly-named method rather than a blank
        scope here.
        """

        scoped_owner = str(user_id or "").strip()
        if not scoped_owner:
            raise ValueError(
                "user_id is required to scope unconfirmed processed trades "
                "(a blank scope would return every account's pending rows)"
            )
        cutoff = time.time() - max_age_seconds
        query = """
                SELECT trade_key, user_id, leader_id, tx_hash, asset,
                       condition_id, order_id, processed_at
                FROM processed_trades
                WHERE status = ANY($2::text[])
                  AND side = $3
                  AND (order_id IS NOT NULL OR processed_at <= $1)
        """
        params: list[Any] = [cutoff, list(_UNCONFIRMED_SUBMISSION_STATES), side]
        query += f" AND user_id = ${len(params) + 1}"
        params.append(scoped_owner)
        query += " ORDER BY processed_at ASC"
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(query, *params)
            quarantined_legacy = 0
            if self._quarantine_count_due(f"processed_trades:{side}"):
                quarantined_legacy = await conn.fetchval(
                    """
                    SELECT count(*) FROM processed_trades
                    WHERE status = ANY($1::text[])
                      AND side = $2
                      AND (order_id IS NOT NULL OR processed_at <= $3)
                      AND COALESCE(user_id, '') = ''
                    """,
                    list(_UNCONFIRMED_SUBMISSION_STATES),
                    side,
                    cutoff,
                )
            if quarantined_legacy:
                logger.warning(
                    "unconfirmed_trades_legacy_rows_quarantined | side=%s count=%d "
                    "reason=blank_user_id pending=provenance_mapping",
                    side,
                    int(quarantined_legacy),
                )
        return [dict(r) for r in rows]


    async def has_recent_processed_trade(
        self,
        *,
        user_id: str,
        asset: str,
        side: str | None = None,
        max_age_seconds: float = 300.0,
    ) -> bool:
        """Whether a bot-originated processed trade exists for this scope recently.

        Used by manual-sync to recognise the bot's OWN copy fills before the
        async reconciler settles them. ``processed_trades`` rows are written at
        submit time (see ``mark_processed_trade``), BEFORE reconciliation, so a
        recent row on ``(user_id, asset[, side])`` is the authoritative "the bot
        acted on this scope" signal that closes the manual-sync race window.

        Matches ANY status so both in-flight (``submitted_unconfirmed``) and
        just-settled rows count — except the order-less
        ``CROSS_SOURCE_CLAIM_STATUS`` sentinel written by the on-chain
        cross-source dedup, which is not a bot order and would misclassify a
        genuine manual fill on the same scope. ``processed_at`` is a
        unix-epoch numeric column (same comparison basis as
        ``get_unconfirmed_buys``).
        """
        if not user_id or not asset:
            return False
        cutoff = time.time() - max(0.0, float(max_age_seconds))
        query = """
            SELECT 1 FROM processed_trades
            WHERE user_id = $1
              AND asset = $2
              AND processed_at >= $3
              AND status <> $4
        """
        params: list[Any] = [user_id, asset, cutoff, CROSS_SOURCE_CLAIM_STATUS]
        if side is not None:
            query += f" AND UPPER(side) = ${len(params) + 1}"
            params.append(str(side).upper())
        query += " LIMIT 1"
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(query, *params)
        return row is not None

    async def has_unconfirmed_processed_trade(
        self,
        *,
        user_id: str,
        asset: str,
        side: str | None = None,
    ) -> bool:
        """Whether reconciliation still owns a bot trade for this scope.

        Unlike the activity-classification helper above, this probe has no age
        cutoff. A venue outage can keep a legitimate BUY pending for hours;
        manual sync must not seed the venue's absolute position and later let
        reconciliation add the same fill as a delta.
        """
        if not user_id or not asset:
            return False
        query = """
            SELECT 1 FROM processed_trades
            WHERE user_id = $1
              AND asset = $2
              AND status = ANY($3::text[])
        """
        params: list[Any] = [user_id, asset, list(_UNCONFIRMED_SUBMISSION_STATES)]
        if side is not None:
            query += f" AND UPPER(side) = ${len(params) + 1}"
            params.append(str(side).upper())
        query += " LIMIT 1"
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(query, *params)
        return row is not None

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
        """Persist execution, fill, position, and reservation effects atomically."""
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
                            # P2-r: str()-wrap so a float caller can't inject
                            # binary-float noise into the avg-price math.
                            new_fill = Decimal(str(fill_size))
                            new_price = Decimal(str(fill_price))
                            new_size = cur_size + new_fill
                            if new_size > 0:
                                avg = ((cur_size * cur_avg) + (new_fill * new_price)) / new_size
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
                            # P2-r: str()-wrap (defensive; matches file convention).
                            if Decimal(str(fill_size)) > cur_size:
                                logger.warning(
                                    "Atomic SELL fill exceeds held size; clamping "
                                    "user=%s cond=%s token=%s fill_size=%s current_size=%s",
                                    user_id,
                                    condition_id,
                                    token_id,
                                    fill_size,
                                    current["size_tokens"],
                                )
                            sell_size = min(Decimal(str(fill_size)), cur_size)
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

    async def purge_old_processed_trades(self, max_age_seconds: float = 30 * 86400) -> int:
        """Purge old processed trades."""
        cutoff = time.time() - max_age_seconds
        async with self._pool.acquire() as conn:
            status = await conn.execute(
                "DELETE FROM processed_trades WHERE processed_at < $1", cutoff
            )
        return _affected(status)
