"""Python module exposing the audit event schema constants and required fields.

This module provides the same information as event_schema.json but as
importable Python constants, making it easier for code to reference required
event fields without loading a JSON file at runtime.
"""
from __future__ import annotations

# ---------------------------------------------------------------------------
# Required fields for every execution audit event (per spec section 10.1)
# ---------------------------------------------------------------------------
REQUIRED_EVENT_FIELDS: tuple[str, ...] = (
    "event",
    "user_id",
    "leader_wallet",
    "condition_id",
    "token_id",
    "side",
    "intent_key",
    "order_id",
    "reservation_id",
    "venue_order_id",
    "status",
    "reason",
    "source_signal_id",
    "exception_type",
)

# ---------------------------------------------------------------------------
# Known event types and their specific required fields
# ---------------------------------------------------------------------------
EVENT_TYPES: dict[str, tuple[str, ...]] = {
    "intent_accepted": (
        "intent_key",
        "side",
        "size",
        "price",
        "reason",
    ),
    "reservation_created": (
        "reservation_id",
        "intent_key",
        "reserved_value",
    ),
    "order_submitting": (
        "order_id",
        "reservation_id",
        "intent_key",
    ),
    "order_submitted": (
        "order_id",
        "reservation_id",
        "venue_order_id",
    ),
    "order_submitted_unconfirmed": (
        "order_id",
        "reservation_id",
        "venue_order_id",
        "raw_status",
    ),
    "order_filled": (
        "order_id",
        "reservation_id",
        "venue_order_id",
        "fill_size",
        "fill_price",
    ),
    "order_cancelled": (
        "order_id",
        "reservation_id",
        "venue_order_id",
    ),
    "order_expired": (
        "order_id",
        "reservation_id",
    ),
    "order_rejected": (
        "order_id",
        "reservation_id",
        "venue_order_id",
        "error",
    ),
    "order_terminal_failed": (
        "order_id",
        "reservation_id",
        "error",
    ),
    "order_retryable_failed": (
        "order_id",
        "reservation_id",
        "error",
    ),
    "order_reconciliation_required": (
        "order_id",
        "reservation_id",
        "venue_order_id",
    ),
    "duplicate_intent_suppressed": (
        "intent_key",
        "existing_reservation_id",
    ),
    "reservation_released": (
        "reservation_id",
        "reason",
    ),
    "geoblock_failed_closed": (
        "reason",
        "error",
    ),
    "risk_block_applied": (
        "condition_id",
        "token_id",
        "reason",
        "source",
    ),
}


def validate_event(event_type: str, fields: dict) -> list[str]:
    """Validate that an event dict contains all required fields.

    Returns a list of missing field names (empty if valid).
    """
    missing: list[str] = []

    # Base required fields
    for f in REQUIRED_EVENT_FIELDS:
        if f not in fields and f != "exception_type":
            missing.append(f)

    # Event-specific fields
    specific = EVENT_TYPES.get(event_type, ())
    for f in specific:
        if f not in fields:
            missing.append(f)

    return missing
