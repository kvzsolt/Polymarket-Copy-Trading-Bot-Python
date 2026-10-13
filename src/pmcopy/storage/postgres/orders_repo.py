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
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

import asyncpg

from pmcopy.adapters.clock import MonotonicClock, SystemMonotonicClock
from pmcopy.ports.repositories import AbstractOrderRepository
from pmcopy.services.execution.ambiguous_recovery import (
    AmbiguousRecoveryProgress,
    AmbiguousSubmission,
    AuthoritativeRecoveryScan,
    RecoveryMatchSource,
    SubmissionAmountUnit,
    SubmissionSide,
)

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

# Observability-only TTL window for the legacy blank-user quarantine count
# queries: at most one ``SELECT count(*)`` + WARNING per scope per window.
_DEFAULT_QUARANTINE_COUNT_TTL_SECONDS = 60.0

_PROCESSED_TRADE_TERMINAL_STATES = [
    "executed",
    "processed",
    "filled_reconciled",
    "duplicate_ignored",
    "terminal_failed",
]

# Status of the order-less tx-level row the on-chain dispatch path inserts to
# dedup against REST polling (``CrossSourceTradeClaimPolicy``). It records
# "an on-chain source is copying this tx" — never a submitted bot order — so
# activity classification (manual-sync) must not read it as bot activity.
CROSS_SOURCE_CLAIM_STATUS = "cross_source_claimed"

# Order statuses that are *not terminally settled* and therefore still need
# reconciliation — NOT only "never reached the venue". The set deliberately
# includes live/matched states: an order that was ``live`` or ``matched`` when
# the bot stopped can accrue fills while it is down, so startup reconciliation
# must re-poll those orders too. This is safe because every consumer of this
# set is a *reconcile-only* path (``reconcile_unconfirmed_risk_orders``,
# ``reconcile_unconfirmed_*`` in the trade pipeline): they look the order up by
# venue ``order_id`` and apply only the incremental positive delta of the
# cumulative ``matched_total`` — never resubmit, never double-apply. Restricting
# this to truly pre-venue states would silently drop in-flight fills across a
# restart, which is the more dangerous failure mode. Terminal states live in
# ``_PROCESSED_TRADE_TERMINAL_STATES`` / ``_FAILURE_STATES``.
_UNCONFIRMED_SUBMISSION_STATES = (
    "submitted_unconfirmed",
    "submitted",
    "matched",
    "matched_pending_confirmation",
    "order_status_matched",
    "submitted_delayed",
    "delayed",
    "order_live",
    "live",
    "ambiguous_timeout",
    "reconciliation_required",
)

_STALE_SUBMISSION_LOCK_STATUSES = (
    "pending_lock",
    "submitting",
    "pending",
    "submitted_unconfirmed",
    "delayed",
)
_DIRECT_EXPIRABLE_STALE_LOCK_STATUSES = (
    "pending_lock",
    "submitting",
    "pending",
)
# Columns are qualified with ``orders.`` so this expression is unambiguous
# inside an ``INSERT ... ON CONFLICT DO UPDATE ... WHERE`` clause (where both the
# target ``orders`` row and the ``excluded`` pseudo-row expose the same column
# names). The qualification is also valid in the plain ``FROM orders`` SELECTs
# that use this expression (P1-g follow-up: bare names raised AmbiguousColumnError
# in the takeover path).
_STALE_LOCK_AGE_EXPR = """
COALESCE(
    (orders.response_json->>'pending_since')::float8::bigint,
    EXTRACT(EPOCH FROM COALESCE(orders.updated_at, orders.created_at))::bigint
)
"""

# P1-g: generous TTL after which a never-submitted (order_id IS NULL) submission
# lock is considered a dead holder and may be taken over by a fresh acquirer.
# A live submission completes in seconds; a pending_lock row older than this has
# almost certainly lost its holder. Kept well above realistic submit durations
# so a slow-but-alive submitter is never stolen from.
_DEFAULT_STALE_LOCK_TAKEOVER_SECONDS = 300
_MAX_AMBIGUOUS_RECOVERY_BATCH_SIZE = 1_000
_MAX_AMBIGUOUS_EVIDENCE_HISTORY = 100
_MAX_AMBIGUOUS_REQUIRED_SCANS = 100


@dataclass
class OrderSubmissionRecord:
    """Describe durable evidence for one order submission."""

    idempotency_key: str
    user_id: str
    leader_id: str | None
    order_id: str | None
    status: str
    request_payload: dict[str, Any]
    response_payload: dict[str, Any] | None
    last_error: str | None = None


@dataclass(frozen=True)
class LegacyOwnershipMappingReport:
    """Counts from the one-shot legacy-ownership mapping maintenance helper.

    ``mapped`` rows received an unambiguous provenance owner; ``ambiguous``
    rows had conflicting provenance and were skipped; ``unresolved`` rows had
    no usable provenance and stay quarantined from automatic recovery.
    """

    orders_candidates: int = 0
    orders_mapped: int = 0
    orders_ambiguous: int = 0
    orders_unresolved: int = 0
    trades_candidates: int = 0
    trades_mapped: int = 0
    trades_ambiguous: int = 0
    trades_unresolved: int = 0


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


def _optional_decimal(value: Any) -> Decimal | None:
    """Parse an optional finite positive decimal without using binary floats."""

    if value is None or str(value).strip() == "":
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    if not parsed.is_finite() or parsed <= 0:
        return None
    return parsed


def _optional_text(value: Any) -> str | None:
    """Normalize an optional persisted text identity; blank becomes None."""

    if value is None:
        return None
    text = str(value).strip()
    return text or None


# A02: persisted order identity vocabulary.  Values outside these sets are
# written as NULL (never fabricated) and readers surface ``None`` so
# matching fails closed.
_ORDER_AMOUNT_UNITS = frozenset({"quote", "shares"})
_ORDER_KINDS = frozenset({"market", "limit"})


def _submission_amount_unit(value: Any) -> SubmissionAmountUnit | None:
    text = str(value or "").strip().lower()
    if text == SubmissionAmountUnit.QUOTE.value:
        return SubmissionAmountUnit.QUOTE
    if text == SubmissionAmountUnit.SHARES.value:
        return SubmissionAmountUnit.SHARES
    return None


def _order_identity_values(
    request: dict[str, Any],
) -> tuple[str | None, str | None, str | None, str | None, str | None]:
    """Extract the A02 order identity tuple from one sanitized request.

    Returns ``(token_id, amount_unit, order_kind, effective_price_limit,
    correlation_ids_json)``.  Every field is optional: an unrecognized or
    absent value stays ``None`` so legacy rows are never back-filled with a
    guessed identity.
    """

    token_id = _optional_text(request.get("token_id"))
    unit = str(request.get("amount_unit") or "").strip().lower()
    amount_unit = unit if unit in _ORDER_AMOUNT_UNITS else None
    kind = str(request.get("order_kind") or "").strip().lower()
    order_kind = kind if kind in _ORDER_KINDS else None
    price_limit = _optional_text(request.get("price_limit"))
    correlation: dict[str, str] = {}
    signal_id = _optional_text(request.get("signal_id"))
    if signal_id is not None:
        correlation["signal_id"] = signal_id
    recovery = request.get("recovery_metadata")
    if isinstance(recovery, dict):
        promotion_id = _optional_text(recovery.get("source_promotion_id"))
        if promotion_id is not None:
            correlation["source_promotion_id"] = promotion_id
    correlation_ids = (
        json.dumps(correlation, sort_keys=True, separators=(",", ":")) if correlation else None
    )
    return token_id, amount_unit, order_kind, price_limit, correlation_ids


def _utc_datetime(value: Any) -> datetime | None:
    """Coerce persisted epoch/ISO/datetime values to an aware UTC instant."""

    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    try:
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    except (TypeError, ValueError, OverflowError):
        pass
    try:
        parsed = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _bounded_positive(value: int, *, field: str, maximum: int) -> int:
    """Validate caller-supplied persistence bounds before issuing SQL."""

    parsed = int(value)
    if not 1 <= parsed <= maximum:
        raise ValueError(f"{field} must be between 1 and {maximum}")
    return parsed


def _row_to_ambiguous_submission(row: Any) -> AmbiguousSubmission:
    """Build the immutable recovery identity from one persisted order row.

    Raises ``ValueError``/``TypeError`` for rows whose identity cannot be
    represented (blank ``user_id``, undated submission); callers quarantine
    such rows with a logged reason instead of processing them.

    A02 identity columns are authoritative for rows that carry them; the
    sanitized request payload remains the fallback for legacy rows.  A row
    with no amount unit surfaces ``None`` — never a guessed unit — so the
    evidence adapter can fail closed on it.
    """

    request = _parse_jsonb(row["request_json"])
    response = _parse_jsonb(row["response_json"])
    submitted_at = (
        _utc_datetime(response.get("pending_since"))
        or _utc_datetime(row["created_at"])
        or _utc_datetime(row["updated_at"])
    )
    if submitted_at is None:
        # Fail closed: an undated lost-POST row cannot satisfy a grace period
        # and must stay duplicate-blocking for operator review.
        raise ValueError("submission timestamp unavailable")
    raw_side = str(request.get("side") or "").strip().upper()
    side = (
        SubmissionSide(raw_side)
        if raw_side in {SubmissionSide.BUY.value, SubmissionSide.SELL.value}
        else SubmissionSide.UNKNOWN
    )
    row_keys = set(row.keys())
    token_column = row["token_id"] if "token_id" in row_keys else None
    unit_column = row["amount_unit"] if "amount_unit" in row_keys else None
    return AmbiguousSubmission(
        idempotency_key=str(row["idempotency_key"]),
        user_id=str(row["user_id"]),
        leader_id=(str(row["leader_id"]) if row["leader_id"] is not None else None),
        token_id=_optional_text(token_column) or _optional_text(request.get("token_id")),
        condition_id=(
            str(request["condition_id"]) if request.get("condition_id") is not None else None
        ),
        side=side,
        amount=_optional_decimal(request.get("amount")),
        price_limit=_optional_decimal(request.get("price_limit")),
        submitted_at=submitted_at,
        amount_unit=_submission_amount_unit(unit_column or request.get("amount_unit")),
    )


class OrdersRepo(AbstractOrderRepository):
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

    async def list_no_id_ambiguous_submissions(
        self,
        *,
        user_id: str,
        limit: int,
    ) -> list[AmbiguousSubmission]:
        """Return a bounded oldest-first batch of one account's lost-POST rows.

        Only ``submitted_unconfirmed`` rows without a venue identity qualify.
        The ``user_id`` predicate is mandatory and applied BEFORE ``LIMIT`` so
        one account's recovery batch is structurally scoped to its own rows:
        with N follower accounts, N recovery loops can never race over the
        same batch.  Legacy rows with a blank/NULL ``user_id`` are excluded
        by the same predicate and counted into a logged quarantine reason
        pending provenance mapping
        (``map_legacy_unowned_rows_to_provenance_owners``) — an
        observability-only count throttled to at most once per TTL window,
        never a predicate on the returned batch.  No age predicate
        is applied here: the constructor-injected recovery clock owns the
        grace-period decision, which keeps that behavior fully deterministic
        in tests.  ``LIMIT`` still bounds the database and in-memory work for
        every recovery cycle.
        """

        owner = str(user_id).strip()
        if not owner:
            raise ValueError("user_id is required to scope ambiguous recovery")
        bounded_limit = _bounded_positive(
            limit,
            field="limit",
            maximum=_MAX_AMBIGUOUS_RECOVERY_BATCH_SIZE,
        )
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT idempotency_key, user_id, leader_id, order_id, status,
                       request_json, response_json, attempt_count, last_error,
                       created_at, updated_at,
                       COALESCE(matched_total, '0') AS matched_total,
                       token_id, amount_unit
                FROM orders
                WHERE status = 'submitted_unconfirmed'
                  AND order_id IS NULL
                  AND user_id = $1
                ORDER BY created_at ASC, idempotency_key ASC
                LIMIT $2
                """,
                owner,
                bounded_limit,
            )
            quarantined_legacy = 0
            if self._quarantine_count_due("ambiguous_orders"):
                quarantined_legacy = await conn.fetchval(
                    """
                    SELECT count(*) FROM orders
                    WHERE status = 'submitted_unconfirmed'
                      AND order_id IS NULL
                      AND COALESCE(user_id, '') = ''
                    """
                )
        if quarantined_legacy:
            logger.warning(
                "ambiguous_recovery_legacy_rows_quarantined | count=%d "
                "reason=blank_user_id pending=provenance_mapping",
                int(quarantined_legacy),
            )

        submissions: list[AmbiguousSubmission] = []
        for row in rows:
            try:
                submissions.append(_row_to_ambiguous_submission(row))
            except (TypeError, ValueError) as exc:
                # Quarantine a malformed legacy row (e.g. blank ``user_id``)
                # instead of letting it poison the whole batch: the row keeps
                # blocking duplicates while the reason stays countable.
                logger.error(
                    "ambiguous_recovery_row_quarantined | key=%s reason=%s",
                    row["idempotency_key"],
                    type(exc).__name__,
                )
        return submissions

    async def record_ambiguous_scan_evidence(
        self,
        *,
        user_id: str,
        idempotency_key: str,
        scan: AuthoritativeRecoveryScan,
        observed_at: datetime,
        required_scan_count: int,
        evidence_history_limit: int,
    ) -> AmbiguousRecoveryProgress | None:
        """Atomically append a bounded evidence summary to an ambiguous row.

        Only consecutive scans that exhaust both private collections and find
        both account checks consistent with absence advance the proof.  Any
        incomplete or contradictory scan resets the counter to zero.  The
        counter saturates at the configured requirement and the retained scan
        tail is bounded independently, preventing response JSON growth.

        The statement compare-and-sets on the account identity: a scan
        recorded by any other account's recovery loop finds no row and is a
        complete no-op, so a foreign account's incomplete scan can never
        reset the owning account's absence proof.
        """

        owner = str(user_id).strip()
        if not owner:
            raise ValueError("user_id is required to record ambiguous scan evidence")
        required = _bounded_positive(
            required_scan_count,
            field="required_scan_count",
            maximum=_MAX_AMBIGUOUS_REQUIRED_SCANS,
        )
        history_limit = _bounded_positive(
            evidence_history_limit,
            field="evidence_history_limit",
            maximum=_MAX_AMBIGUOUS_EVIDENCE_HISTORY,
        )
        if observed_at.tzinfo is None:
            raise ValueError("observed_at must be timezone-aware")

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    """
                    SELECT response_json
                    FROM orders
                    WHERE idempotency_key = $1
                      AND user_id = $2
                      AND status = 'submitted_unconfirmed'
                      AND order_id IS NULL
                    FOR UPDATE
                    """,
                    idempotency_key,
                    owner,
                )
                if row is None:
                    return None

                response = _parse_jsonb(row["response_json"])
                existing_recovery = response.get("ambiguous_recovery")
                recovery = dict(existing_recovery) if isinstance(existing_recovery, dict) else {}
                existing_scans = recovery.get("scans")
                scans = (
                    [dict(item) for item in existing_scans if isinstance(item, dict)]
                    if isinstance(existing_scans, list)
                    else []
                )
                try:
                    previous_consecutive = max(
                        0,
                        int(recovery.get("consecutive_complete_absence_scans", 0)),
                    )
                except (TypeError, ValueError):
                    previous_consecutive = 0

                qualifies = scan.qualifies_as_complete_absence
                consecutive = min(previous_consecutive + 1, required) if qualifies else 0
                evidence = {
                    "observed_at": observed_at.astimezone(timezone.utc).isoformat(),
                    "private_orders_complete": bool(scan.private_orders_complete),
                    "private_fills_complete": bool(scan.private_fills_complete),
                    "position_check": scan.position_check.value,
                    "balance_check": scan.balance_check.value,
                    "qualifies_as_complete_absence": qualifies,
                }
                if scan.match is not None:
                    evidence["match_source"] = scan.match.source.value
                    evidence["venue_order_id"] = scan.match.venue_order_id
                scans.append(evidence)
                scans = scans[-history_limit:]
                recovery.update(
                    {
                        "version": 1,
                        "consecutive_complete_absence_scans": consecutive,
                        "scans": scans,
                    }
                )
                response["ambiguous_recovery"] = recovery
                response["status"] = "submitted_unconfirmed"
                status = await conn.execute(
                    """
                    UPDATE orders
                    SET response_json = $3::jsonb,
                        updated_at = now()
                    WHERE idempotency_key = $1
                      AND user_id = $2
                      AND status = 'submitted_unconfirmed'
                      AND order_id IS NULL
                    """,
                    idempotency_key,
                    owner,
                    _json_str(response) or "{}",
                )
                if _affected(status) != 1:
                    return None

        return AmbiguousRecoveryProgress(
            consecutive_complete_absence_scans=consecutive,
            retained_evidence_entries=len(scans),
        )

    async def attach_recovered_venue_order_id(
        self,
        *,
        user_id: str,
        idempotency_key: str,
        venue_order_id: str,
        source: RecoveryMatchSource,
        recovered_at: datetime,
    ) -> bool:
        """Atomically attach an authoritative identity without changing state.

        The row remains ``submitted_unconfirmed`` so normal order-id-based
        reconciliation, rather than this recovery service, determines fills
        and terminal status.  A concurrent resolution causes a no-op.  The
        compare-and-set includes the account identity: an identity recovered
        from one account's private history can only ever be attached to that
        same account's row, making cross-account attachment structurally
        impossible at the persistence layer.
        """

        owner = str(user_id).strip()
        if not owner:
            raise ValueError("user_id is required to attach a recovered identity")
        order_id = str(venue_order_id).strip()
        if not order_id:
            raise ValueError("venue_order_id is required")
        if not isinstance(source, RecoveryMatchSource):
            raise TypeError("source must be a RecoveryMatchSource")
        if recovered_at.tzinfo is None:
            raise ValueError("recovered_at must be timezone-aware")

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    """
                    SELECT response_json
                    FROM orders
                    WHERE idempotency_key = $1
                      AND user_id = $2
                      AND status = 'submitted_unconfirmed'
                      AND order_id IS NULL
                    FOR UPDATE
                    """,
                    idempotency_key,
                    owner,
                )
                if row is None:
                    return False
                response = _parse_jsonb(row["response_json"])
                existing_recovery = response.get("ambiguous_recovery")
                recovery = dict(existing_recovery) if isinstance(existing_recovery, dict) else {}
                recovery.update(
                    {
                        "version": 1,
                        "resolution": "recovered_order_id",
                        "match_source": source.value,
                        "recovered_at": recovered_at.astimezone(timezone.utc).isoformat(),
                    }
                )
                response.update(
                    {
                        "status": "submitted_unconfirmed",
                        "order_id": order_id,
                        "orderID": order_id,
                        "ambiguous_recovery": recovery,
                    }
                )
                returned = await conn.fetchval(
                    """
                    UPDATE orders
                    SET order_id = $3,
                        response_json = $4::jsonb,
                        updated_at = now()
                    WHERE idempotency_key = $1
                      AND user_id = $2
                      AND status = 'submitted_unconfirmed'
                      AND order_id IS NULL
                    RETURNING idempotency_key
                    """,
                    idempotency_key,
                    owner,
                    order_id,
                    _json_str(response) or "{}",
                )
        return returned is not None

    async def mark_ambiguous_submission_confirmed_absent(
        self,
        *,
        user_id: str,
        idempotency_key: str,
        required_scan_count: int,
        confirmed_at: datetime,
    ) -> bool:
        """Resolve a no-ID ambiguity only after its persisted absence proof.

        The threshold and latest qualifying evidence are checked while holding
        the row lock, so a concurrent scan, recovered venue identity, or status
        transition cannot race an unsafe ``confirmed_absent`` update.  The
        compare-and-set also carries the account identity, so only the owning
        account's recovery loop can resolve its own row.
        """

        owner = str(user_id).strip()
        if not owner:
            raise ValueError("user_id is required to confirm an ambiguous absence")
        required = _bounded_positive(
            required_scan_count,
            field="required_scan_count",
            maximum=_MAX_AMBIGUOUS_REQUIRED_SCANS,
        )
        if confirmed_at.tzinfo is None:
            raise ValueError("confirmed_at must be timezone-aware")

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    """
                    SELECT response_json
                    FROM orders
                    WHERE idempotency_key = $1
                      AND user_id = $2
                      AND status = 'submitted_unconfirmed'
                      AND order_id IS NULL
                    FOR UPDATE
                    """,
                    idempotency_key,
                    owner,
                )
                if row is None:
                    return False
                response = _parse_jsonb(row["response_json"])
                existing_recovery = response.get("ambiguous_recovery")
                if not isinstance(existing_recovery, dict):
                    return False
                recovery = dict(existing_recovery)
                try:
                    consecutive = int(recovery.get("consecutive_complete_absence_scans", 0))
                except (TypeError, ValueError):
                    return False
                scans = recovery.get("scans")
                latest = scans[-1] if isinstance(scans, list) and scans else None
                if (
                    consecutive < required
                    or not isinstance(latest, dict)
                    or latest.get("qualifies_as_complete_absence") is not True
                ):
                    return False

                recovery.update(
                    {
                        "version": 1,
                        "resolution": "confirmed_absent",
                        "confirmed_at": confirmed_at.astimezone(timezone.utc).isoformat(),
                        "required_scan_count": required,
                    }
                )
                response.update(
                    {
                        "success": False,
                        "status": "confirmed_absent",
                        "ambiguous_recovery": recovery,
                    }
                )
                returned = await conn.fetchval(
                    """
                    UPDATE orders
                    SET status = 'confirmed_absent',
                        response_json = $3::jsonb,
                        updated_at = now()
                    WHERE idempotency_key = $1
                      AND user_id = $2
                      AND status = 'submitted_unconfirmed'
                      AND order_id IS NULL
                    RETURNING idempotency_key
                    """,
                    idempotency_key,
                    owner,
                    _json_str(response) or "{}",
                )
        return returned is not None

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

    async def map_legacy_unowned_rows_to_provenance_owners(
        self,
        *,
        apply: bool = False,
    ) -> LegacyOwnershipMappingReport:
        """One-shot maintenance mapping of legacy unowned rows to their owners.

        Audit A01 quarantines blank/NULL-``user_id`` rows from automatic
        recovery instead of guessing an owner.  This explicit maintenance
        helper resolves owners ONLY from durable provenance, and ONLY when
        every provenance source agrees:

        * ``orders`` rows: the persisted ``request_json->>'user_id'`` and the
          owner of the reservation named by
          ``request_json->'recovery_metadata'->>'reservation_id'`` (when
          present).  Disagreement or absence leaves the row quarantined.
        * ``processed_trades`` rows: the owning account of the single
          ``orders`` row sharing the trade's ``order_id``.

        With ``apply=False`` (default) nothing is written; the returned
        report counts what a subsequent ``apply=True`` call would map, skip
        as ambiguous, or leave unresolved.  Updates are compare-and-set on
        ``COALESCE(user_id, '') = ''`` so a concurrently written owner is
        never overwritten.  This is deliberately NOT wired into any runtime
        loop: an operator decides when (and whether) to run it.
        """

        orders_candidates = orders_mapped = orders_ambiguous = orders_unresolved = 0
        trades_candidates = trades_mapped = trades_ambiguous = trades_unresolved = 0

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                legacy_orders = await conn.fetch(
                    """
                    SELECT idempotency_key,
                           COALESCE(request_json->>'user_id', '') AS request_user_id,
                           COALESCE(
                               request_json->'recovery_metadata'->>'reservation_id', ''
                           ) AS reservation_id
                    FROM orders
                    WHERE COALESCE(user_id, '') = ''
                    ORDER BY created_at ASC
                    """
                )
                for row in legacy_orders:
                    orders_candidates += 1
                    request_owner = str(row["request_user_id"] or "").strip()
                    reservation_owner: str | None = None
                    reservation_id = str(row["reservation_id"] or "").strip()
                    if reservation_id:
                        reservation_owner = await conn.fetchval(
                            """
                            SELECT COALESCE(user_id, '') FROM order_reservations
                            WHERE id = $1
                            """,
                            reservation_id,
                        )
                        reservation_owner = str(reservation_owner or "").strip() or None
                    owners = {
                        owner for owner in (request_owner or None, reservation_owner) if owner
                    }
                    if len(owners) > 1:
                        orders_ambiguous += 1
                        continue
                    if not owners:
                        orders_unresolved += 1
                        continue
                    owner = owners.pop()
                    if apply:
                        status = await conn.execute(
                            """
                            UPDATE orders
                            SET user_id = $2, updated_at = now()
                            WHERE idempotency_key = $1
                              AND COALESCE(user_id, '') = ''
                            """,
                            row["idempotency_key"],
                            owner,
                        )
                        if _affected(status) == 1:
                            orders_mapped += 1
                    else:
                        orders_mapped += 1

                legacy_trades = await conn.fetch(
                    """
                    SELECT trade_key, order_id
                    FROM processed_trades
                    WHERE COALESCE(user_id, '') = ''
                    ORDER BY processed_at ASC
                    """
                )
                for row in legacy_trades:
                    trades_candidates += 1
                    order_id = row["order_id"]
                    if not order_id:
                        trades_unresolved += 1
                        continue
                    owner_rows = await conn.fetch(
                        """
                        SELECT DISTINCT user_id FROM orders
                        WHERE order_id = $1 AND COALESCE(user_id, '') <> ''
                        """,
                        order_id,
                    )
                    owners = {
                        str(record["user_id"]).strip()
                        for record in owner_rows
                        if str(record["user_id"] or "").strip()
                    }
                    if len(owners) > 1:
                        trades_ambiguous += 1
                        continue
                    if not owners:
                        trades_unresolved += 1
                        continue
                    owner = owners.pop()
                    if apply:
                        status = await conn.execute(
                            """
                            UPDATE processed_trades
                            SET user_id = $2
                            WHERE trade_key = $1
                              AND COALESCE(user_id, '') = ''
                            """,
                            row["trade_key"],
                            owner,
                        )
                        if _affected(status) == 1:
                            trades_mapped += 1
                    else:
                        trades_mapped += 1

        return LegacyOwnershipMappingReport(
            orders_candidates=orders_candidates,
            orders_mapped=orders_mapped,
            orders_ambiguous=orders_ambiguous,
            orders_unresolved=orders_unresolved,
            trades_candidates=trades_candidates,
            trades_mapped=trades_mapped,
            trades_ambiguous=trades_ambiguous,
            trades_unresolved=trades_unresolved,
        )

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
