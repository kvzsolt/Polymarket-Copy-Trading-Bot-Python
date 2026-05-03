"""Structured audit logger for execution lifecycle events.

Every execution lifecycle event is logged as a structured JSON line
so that operators can trace intent → reservation → submission → fill
through the log stream without needing SQL access.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger("audit")


def utcnow() -> str:
    """Return current UTC timestamp as ISO-8601 string."""
    return datetime.now(timezone.utc).isoformat()


def _audit(event: str, **fields: Any) -> None:
    """Emit a structured audit log line as JSON."""
    record = {"ts": utcnow(), "event": event, **fields}
    logger.info(json.dumps(record, default=str))


def log_intent_accepted(intent_key: str, side: str, size: str, price: str, reason: str) -> None:
    _audit(
        "intent_accepted",
        intent_key=intent_key,
        side=side,
        size=size,
        price=price,
        reason=reason,
    )


def log_reservation_created(reservation_id: str, intent_key: str, reserved_value: str) -> None:
    _audit(
        "reservation_created",
        reservation_id=reservation_id,
        intent_key=intent_key,
        reserved_value=reserved_value,
    )


def log_order_submitted(reservation_id: str, order_id: str, venue_order_id: str) -> None:
    _audit(
        "order_submitted",
        reservation_id=reservation_id,
        order_id=order_id,
        venue_order_id=venue_order_id,
    )


def log_order_filled(reservation_id: str, fill_size: str, fill_price: str) -> None:
    _audit(
        "order_filled",
        reservation_id=reservation_id,
        fill_size=fill_size,
        fill_price=fill_price,
    )


def log_order_cancelled(reservation_id: str, reason: str) -> None:
    _audit(
        "order_cancelled",
        reservation_id=reservation_id,
        reason=reason,
    )


def log_order_rejected(reservation_id: str, reason: str) -> None:
    _audit(
        "order_rejected",
        reservation_id=reservation_id,
        reason=reason,
    )


def log_reservation_released(reservation_id: str, reason: str) -> None:
    _audit(
        "reservation_released",
        reservation_id=reservation_id,
        reason=reason,
    )


def log_reconciliation_result(order_id: str, status: str, details: str) -> None:
    _audit(
        "reconciliation_result",
        order_id=order_id,
        status=status,
        details=details,
    )


def log_duplicate_intent_suppressed(intent_key: str, existing_reservation_id: str) -> None:
    _audit(
        "duplicate_intent_suppressed",
        intent_key=intent_key,
        existing_reservation_id=existing_reservation_id,
    )


def log_geoblock_decision(allowed: bool, reason: str) -> None:
    _audit(
        "geoblock_decision",
        allowed=allowed,
        reason=reason,
    )


def log_wallet_validation(allowed: bool, reason: str, signer: str, funder: str) -> None:
    _audit(
        "wallet_validation",
        allowed=allowed,
        reason=reason,
        signer=signer,
        funder=funder,
    )


def log_kill_switch() -> None:
    _audit("kill_switch_engaged")


def log_startup(mode: str, version: str = "unknown") -> None:
    _audit("bot_startup", mode=mode, version=version)


def log_shutdown(reason: str = "normal") -> None:
    _audit("bot_shutdown", reason=reason)
