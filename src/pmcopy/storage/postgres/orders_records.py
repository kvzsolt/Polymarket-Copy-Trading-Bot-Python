"""Order submission records, status constants, and PostgreSQL row mappers."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any


from pmcopy.services.execution.ambiguous_recovery import (
    AmbiguousSubmission,
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

