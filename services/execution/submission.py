"""Submission sanitization and pending-order tracking helpers."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, TYPE_CHECKING

from .types import (
    _MAX_PERSISTED_ERROR_CHARS,
    _MAX_PERSISTED_FIELD_CHARS,
    _PERSISTED_REQUEST_FIELDS,
    _PERSISTED_RESPONSE_FIELDS,
    _RETRY_BLOCKING_STATUSES,
    _TRUNCATED_SUFFIX,
)

if TYPE_CHECKING:
    from config.app_config import AppConfig


_EXPIRABLE_PRE_POST_STATUSES = frozenset({"pending", "pending_lock", "submitting"})


def truncate_text(value: Any, *, max_chars: int) -> str:
    text = str(value).replace("\x00", " ").replace("\r", " ").replace("\n", " ").replace("\t", " ")
    if len(text) <= max_chars:
        return text
    keep = max(0, max_chars - len(_TRUNCATED_SUFFIX))
    return text[:keep] + _TRUNCATED_SUFFIX


def sanitize_submission_value(value: Any, *, max_chars: int = _MAX_PERSISTED_FIELD_CHARS) -> Any:
    if value is None:
        return None
    if isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        sanitized_dict: dict[str, Any] = {}
        for key, item in value.items():
            sanitized_item = sanitize_submission_value(item, max_chars=max_chars)
            if sanitized_item is not None:
                sanitized_dict[truncate_text(key, max_chars=max_chars)] = sanitized_item
        return sanitized_dict
    if isinstance(value, (list, tuple)):
        return [
            sanitized_item
            for item in value
            if (sanitized_item := sanitize_submission_value(item, max_chars=max_chars)) is not None
        ]
    return truncate_text(value, max_chars=max_chars)


def sanitize_submission_request(request: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(request, dict):
        return {}
    sanitized: dict[str, Any] = {}
    for key in _PERSISTED_REQUEST_FIELDS:
        if key not in request:
            continue
        value = sanitize_submission_value(request.get(key))
        if value is not None:
            sanitized[key] = value
    return sanitized


def coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return False


def coerce_timestamp(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).strip()
    if not text:
        return None

    try:
        return float(text)
    except ValueError:
        pass

    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.timestamp()
    except ValueError:
        pass

    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
        try:
            parsed = datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
            return parsed.timestamp()
        except ValueError:
            continue

    return None


def sanitize_submission_response(response: dict[str, Any] | Any) -> dict[str, Any]:
    if not isinstance(response, dict):
        return {
            "success": False,
            "status": "failed",
            "error": truncate_text(response, max_chars=_MAX_PERSISTED_ERROR_CHARS),
        }

    sanitized: dict[str, Any] = {}
    for key in _PERSISTED_RESPONSE_FIELDS:
        if key not in response:
            continue
        raw_value = response.get(key)
        if key == "success":
            sanitized[key] = coerce_bool(raw_value)
            continue
        if key in {"pending_since", "pending_expires_at", "expired_at"}:
            ts_value = coerce_timestamp(raw_value)
            sanitized[key] = (
                ts_value if ts_value is not None else sanitize_submission_value(raw_value)
            )
            continue
        if key == "error":
            error_value = sanitize_submission_value(raw_value, max_chars=_MAX_PERSISTED_ERROR_CHARS)
            if error_value:
                sanitized[key] = error_value
            continue
        value = sanitize_submission_value(raw_value)
        if value is not None:
            sanitized[key] = value

    if "success" not in sanitized:
        sanitized["success"] = coerce_bool(response.get("success"))

    if "status" not in sanitized:
        sanitized["status"] = "submitted" if sanitized.get("success") else "failed"

    return sanitized


def pending_submission_ttl_seconds(config: "AppConfig | None" = None) -> float:
    if config is not None:
        return max(float(config.execution.pending_submission_ttl_seconds), 0.0)
    return 120.0


def pending_since(existing: dict[str, Any]) -> float | None:
    response_value = existing.get("response")
    response = response_value if isinstance(response_value, dict) else {}
    for candidate in (
        existing.get("pending_since"),
        response.get("pending_since"),
        existing.get("updated_at"),
        existing.get("created_at"),
    ):
        ts = coerce_timestamp(candidate)
        if ts is not None:
            return ts
    return None


def pending_expires_at(existing: dict[str, Any]) -> float | None:
    response_value = existing.get("response")
    response = response_value if isinstance(response_value, dict) else {}
    for candidate in (
        existing.get("pending_expires_at"),
        response.get("pending_expires_at"),
    ):
        ts = coerce_timestamp(candidate)
        if ts is not None:
            return ts
    return None


def build_pending_response(
    *, pending_since: float, ttl_seconds: float | None = None, config: "AppConfig | None" = None
) -> dict[str, Any]:
    ttl = (
        pending_submission_ttl_seconds(config)
        if ttl_seconds is None
        else max(float(ttl_seconds), 0.0)
    )
    return {
        "success": False,
        "status": "pending",
        "pending_since": pending_since,
        "pending_ttl_seconds": ttl,
        "pending_expires_at": pending_since + ttl,
    }


def is_pending_submission_expired(
    existing: dict[str, Any] | None,
    *,
    now_ts: float | None = None,
    pending_ttl_seconds: float | None = None,
    config: "AppConfig | None" = None,
) -> bool:
    if not existing:
        return False
    status = str(existing.get("status") or existing.get("response", {}).get("status") or "").lower()
    if existing.get("order_id"):
        return False
    # Wall-clock takeover is safe only while the row represents a lock whose
    # holder never attempted a venue POST. ``submitted_unconfirmed`` means the
    # request may already be live or filled and therefore remains blocking
    # until reconciliation or an explicit confirmed-absence transition.
    if status not in _EXPIRABLE_PRE_POST_STATUSES:
        return False

    now_value = time.time() if now_ts is None else float(now_ts)
    explicit_expires_at = pending_expires_at(existing)
    if explicit_expires_at is not None:
        return now_value >= explicit_expires_at

    ttl_seconds = (
        pending_submission_ttl_seconds(config)
        if pending_ttl_seconds is None
        else float(pending_ttl_seconds)
    )
    if ttl_seconds <= 0:
        return True

    submitted_at = pending_since(existing)
    if submitted_at is None:
        return True

    return (now_value - submitted_at) >= ttl_seconds


def is_retry_blocking_existing(
    existing: dict[str, Any] | None,
    *,
    now_ts: float | None = None,
    pending_ttl_seconds: float | None = None,
    config: "AppConfig | None" = None,
) -> bool:
    if not existing:
        return False
    status = str(existing.get("status") or existing.get("response", {}).get("status") or "").lower()
    if existing.get("order_id"):
        return True
    if is_pending_submission_expired(
        existing,
        now_ts=now_ts,
        pending_ttl_seconds=pending_ttl_seconds,
        config=config,
    ):
        return False
    return status in _RETRY_BLOCKING_STATUSES
