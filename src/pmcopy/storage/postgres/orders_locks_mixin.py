"""Atomic submission-lock acquisition and stale-lock lifecycle mixin."""

from __future__ import annotations

import json
import logging
import time
from typing import Any

import asyncpg

from pmcopy.adapters.time.clock import MonotonicClock
from pmcopy.storage.postgres.orders_records import (
    _DEFAULT_STALE_LOCK_TAKEOVER_SECONDS,
    _DIRECT_EXPIRABLE_STALE_LOCK_STATUSES,
    _STALE_LOCK_AGE_EXPR,
    _STALE_SUBMISSION_LOCK_STATUSES,
    _json_str,
    _order_identity_values,
    _parse_jsonb,
)

logger = logging.getLogger(__name__)


class _OrdersLocksMixin:
    """Submission-lock acquisition, freshness counting, and expiry."""

    _pool: asyncpg.Pool
    _quarantine_clock: MonotonicClock
    _quarantine_ttl_seconds: float
    _quarantine_counted_at: dict[str, float]

    async def get_by_idempotency_key(self, key: str) -> dict[str, Any] | None:
        raise NotImplementedError

    def _quarantine_count_due(self, scope: str) -> bool:
        """Whether the legacy-quarantine count query is due for ``scope``.

        Marks the scope as counted BEFORE issuing the query so a slow count
        cannot re-arm its own window; scopes are keyed independently (the
        ambiguous-orders count and each side's processed-trades count).
        """

        now = self._quarantine_clock.monotonic()
        last = self._quarantine_counted_at.get(scope)
        if last is not None and (now - last) < self._quarantine_ttl_seconds:
            return False
        self._quarantine_counted_at[scope] = now
        return True

    async def try_acquire_submission_lock(
        self,
        key: str,
        user_id: str,
        leader_id: str | None,
        request_payload: dict,
        *,
        stale_takeover_seconds: int = _DEFAULT_STALE_LOCK_TAKEOVER_SECONDS,
    ) -> bool:
        """Attempt to acquire submission lock."""
        request_json = _json_str(request_payload) or "{}"
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                signal_id = request_payload.get("signal_id")
                if isinstance(signal_id, str) and signal_id:
                    recovery = request_payload.get("recovery_metadata")
                    recovery = recovery if isinstance(recovery, dict) else {}
                    promotion_id = recovery.get("source_promotion_id")
                    if not isinstance(promotion_id, str) or not promotion_id:
                        promotion_id = signal_id
                    provisional = recovery.get("source_is_provisional") is True
                    await conn.execute(
                        "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                        json.dumps([user_id, promotion_id], separators=(",", ":")),
                    )
                    existing_intent = await conn.fetchval(
                        "SELECT 1 FROM orders WHERE user_id = $1 "
                        "AND (request_json->>'signal_id' = $2 "
                        "OR request_json->>'signal_id' = $7 "
                        "OR ($8::boolean AND "
                        "request_json->'recovery_metadata'->>'source_promotion_id' = $7)) "
                        "AND request_json->>'token_id' IS NOT DISTINCT FROM $3::text "
                        "AND request_json->>'condition_id' IS NOT DISTINCT FROM $4::text "
                        "AND upper(request_json->>'side') = $5 "
                        "AND idempotency_key <> $6 LIMIT 1",
                        user_id,
                        signal_id,
                        request_payload.get("token_id"),
                        request_payload.get("condition_id"),
                        str(request_payload.get("side", "")).upper(),
                        key,
                        promotion_id,
                        provisional,
                    )
                    if existing_intent:
                        return False
                # RETURNING yields the key when the INSERT happens. P1-g: the
                # ON CONFLICT was previously DO NOTHING, so a task that died holding
                # a ``pending_lock`` row (before it ever POSTed — hence
                # ``order_id IS NULL``) blocked that idempotency key until the next
                # boot, with no runtime auto-release. DO UPDATE ... WHERE makes the
                # lock self-healing: a fresh acquirer takes it over ONLY when the
                # existing row is a never-submitted (``order_id IS NULL``)
                # direct-expirable lock that has been stale longer than the generous
                # takeover TTL — exactly the ``safe_to_expire`` condition the batch
                # reaper uses. A live (recent) or already-submitted (``order_id`` set)
                # lock fails the WHERE, the UPDATE is skipped, RETURNING is empty,
                # and acquisition returns False as before.
                token_id, amount_unit, order_kind, price_limit, correlation_ids = (
                    _order_identity_values(request_payload)
                )
                returned = await conn.fetchval(
                    f"""
                    INSERT INTO orders (
                        idempotency_key, user_id, leader_id, order_id, status,
                        request_json, response_json, attempt_count, last_error,
                        created_at, updated_at,
                        token_id, amount_unit, order_kind, effective_price_limit,
                        correlation_ids
                    ) VALUES (
                        $1, $2, $3, NULL, 'pending_lock',
                        $4::jsonb, NULL, 0, NULL,
                        now(), now(),
                        $7, $8, $9, $10, $11::jsonb
                    )
                    ON CONFLICT(idempotency_key) DO UPDATE SET
                        user_id = excluded.user_id,
                        leader_id = excluded.leader_id,
                        request_json = excluded.request_json,
                        response_json = NULL,
                        status = 'pending_lock',
                        attempt_count = 0,
                        last_error = 'stale_lock_taken_over',
                        created_at = now(),
                        updated_at = now(),
                        token_id = excluded.token_id,
                        amount_unit = excluded.amount_unit,
                        order_kind = excluded.order_kind,
                        effective_price_limit = excluded.effective_price_limit,
                        correlation_ids = excluded.correlation_ids
                    WHERE orders.order_id IS NULL
                      AND orders.status = ANY($5::text[])
                      AND (
                          EXTRACT(EPOCH FROM now())::bigint - {_STALE_LOCK_AGE_EXPR}
                      ) > $6
                    RETURNING idempotency_key
                    """,
                    key,
                    user_id,
                    leader_id,
                    request_json,
                    list(_DIRECT_EXPIRABLE_STALE_LOCK_STATUSES),
                    int(stale_takeover_seconds),
                    token_id,
                    amount_unit,
                    order_kind,
                    price_limit,
                    correlation_ids,
                )
        return returned is not None

    async def is_submission_locked(self, key: str) -> bool:
        """Return whether a fresh submission lock blocks this identity."""
        rec = await self.get_by_idempotency_key(key)
        if rec is None:
            return False
        if rec.get("order_id"):
            return False
        return rec.get("status") in ("pending", "pending_lock")

    @staticmethod
    def _stale_lock_row_to_dict(row: asyncpg.Record) -> dict[str, Any]:
        status = str(row["status"] or "").lower()
        order_id = row["order_id"]
        direct_expire = not order_id and status in _DIRECT_EXPIRABLE_STALE_LOCK_STATUSES
        return {
            "idempotency_key": row["idempotency_key"],
            "user_id": row["user_id"],
            "leader_id": row["leader_id"],
            "order_id": order_id,
            "status": row["status"],
            "attempt_count": row["attempt_count"],
            "last_error": row["last_error"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "pending_since_ts": int(row["pending_since_ts"])
            if row["pending_since_ts"] is not None
            else None,
            "age_seconds": int(row["age_seconds"] or 0),
            "classification": (
                "safe_to_expire" if direct_expire else "deferred_reconciliation_required"
            ),
            "action": "expire" if direct_expire else "defer",
        }

    async def list_stale_submission_locks(
        self,
        *,
        ttl_seconds: int,
        user_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Return stale submission locks."""
        base = f"""
            SELECT idempotency_key, user_id, leader_id, order_id, status,
                   attempt_count, last_error, created_at, updated_at,
                   {_STALE_LOCK_AGE_EXPR} AS pending_since_ts,
                   (
                       EXTRACT(EPOCH FROM now())::bigint
                       - {_STALE_LOCK_AGE_EXPR}
                   ) AS age_seconds
            FROM orders
            WHERE status = ANY($1::text[])
              AND (
                  EXTRACT(EPOCH FROM now())::bigint
                  - {_STALE_LOCK_AGE_EXPR}
              ) > $2
        """
        params: list[Any] = [list(_STALE_SUBMISSION_LOCK_STATUSES), int(ttl_seconds)]
        if user_id is not None:
            params.append(user_id)
            base += f" AND user_id = ${len(params)}"
        base += " ORDER BY age_seconds DESC"

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(base, *params)
        return [self._stale_lock_row_to_dict(row) for row in rows]

    async def count_fresh_submission_locks(
        self,
        *,
        ttl_seconds: int,
        user_id: str | None = None,
    ) -> int:
        """Count fresh submission locks."""
        base = f"""
            SELECT COUNT(*) FROM orders
            WHERE status = ANY($1::text[])
              AND (
                  EXTRACT(EPOCH FROM now())::bigint
                  - {_STALE_LOCK_AGE_EXPR}
              ) <= $2
        """
        params: list[Any] = [list(_STALE_SUBMISSION_LOCK_STATUSES), int(ttl_seconds)]
        if user_id is not None:
            params.append(user_id)
            base += f" AND user_id = ${len(params)}"
        async with self._pool.acquire() as conn:
            count = await conn.fetchval(base, *params)
        return int(count or 0)

    async def expire_stale_submission_lock(
        self,
        *,
        idempotency_key: str,
        ttl_seconds: int,
        reason: str = "stale_lock_resolve",
    ) -> dict[str, Any] | None:
        """Expire stale submission lock."""
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    f"""
                    SELECT idempotency_key, user_id, leader_id, order_id, status,
                           attempt_count, last_error, created_at, updated_at,
                           {_STALE_LOCK_AGE_EXPR} AS pending_since_ts,
                           (
                               EXTRACT(EPOCH FROM now())::bigint
                               - {_STALE_LOCK_AGE_EXPR}
                           ) AS age_seconds,
                           response_json
                    FROM orders
                    WHERE idempotency_key = $1
                    FOR UPDATE
                    """,
                    idempotency_key,
                )
                if row is None:
                    return None
                lock_row = self._stale_lock_row_to_dict(row)
                if lock_row["classification"] != "safe_to_expire" or lock_row["age_seconds"] <= int(
                    ttl_seconds
                ):
                    return None

                pending_since = lock_row["pending_since_ts"]
                pending_expires_at = (
                    pending_since + int(ttl_seconds) if pending_since is not None else None
                )
                response = _parse_jsonb(row["response_json"])
                response.update(
                    {
                        "success": False,
                        "status": "expired",
                        "error": (f"stale submission lock expired after {int(ttl_seconds)}s"),
                        "expired_at": time.time(),
                        "pending_ttl_seconds": int(ttl_seconds),
                        "cleanup_reason": reason,
                    }
                )
                if pending_since is not None:
                    response["pending_since"] = pending_since
                if pending_expires_at is not None:
                    response["pending_expires_at"] = pending_expires_at

                updated = await conn.fetchrow(
                    """
                    UPDATE orders
                    SET status = 'expired',
                        response_json = $2::jsonb,
                        last_error = COALESCE(last_error, $3),
                        updated_at = now()
                    WHERE idempotency_key = $1
                      AND order_id IS NULL
                      AND status = ANY($4::text[])
                    RETURNING idempotency_key, user_id, leader_id, order_id,
                              status, attempt_count, last_error, created_at,
                              updated_at,
                              $5::bigint AS pending_since_ts,
                              $6::bigint AS age_seconds
                    """,
                    idempotency_key,
                    _json_str(response) or "{}",
                    response["error"],
                    list(_DIRECT_EXPIRABLE_STALE_LOCK_STATUSES),
                    pending_since,
                    lock_row["age_seconds"],
                )
        if updated is None:
            return None
        result = self._stale_lock_row_to_dict(updated)
        result["classification"] = "expired"
        result["action"] = "expired"
        return result

    async def resolve_stale_submission_locks(
        self,
        *,
        ttl_seconds: int,
        user_id: str | None = None,
        apply: bool = False,
    ) -> dict[str, Any]:
        """Resolve stale submission locks."""
        stale_rows = await self.list_stale_submission_locks(
            ttl_seconds=ttl_seconds,
            user_id=user_id,
        )
        fresh_count = await self.count_fresh_submission_locks(
            ttl_seconds=ttl_seconds,
            user_id=user_id,
        )
        safe_rows = [row for row in stale_rows if row["classification"] == "safe_to_expire"]
        deferred_rows = [
            row for row in stale_rows if row["classification"] == "deferred_reconciliation_required"
        ]
        expired_rows: list[dict[str, Any]] = []
        if apply:
            for row in safe_rows:
                expired = await self.expire_stale_submission_lock(
                    idempotency_key=row["idempotency_key"],
                    ttl_seconds=ttl_seconds,
                )
                if expired is not None:
                    expired_rows.append(expired)

        return {
            "applied": bool(apply),
            "ttl_seconds": int(ttl_seconds),
            "expired": expired_rows,
            "would_expire": [] if apply else safe_rows,
            "deferred_reconciliation_required": deferred_rows,
            "kept_fresh": fresh_count,
            "summary": {
                "expired": len(expired_rows),
                "would_expire": 0 if apply else len(safe_rows),
                "deferred_reconciliation_required": len(deferred_rows),
                "kept_fresh": fresh_count,
            },
        }
