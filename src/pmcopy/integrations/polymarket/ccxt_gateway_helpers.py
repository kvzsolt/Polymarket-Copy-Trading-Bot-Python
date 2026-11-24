"""Pure response parsing, status normalization, and error classification helpers for CCXT."""

from __future__ import annotations

import asyncio
import inspect
import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from http import HTTPStatus
from typing import Any, Literal, NoReturn

from pmcopy.integrations.polymarket.errors import (
    VenueAuthError,
    VenueInsufficientBalanceError,
    VenueInvalidOrderError,
    VenueOrderNotFoundError,
    VenueOrderbookNotFoundError,
    VenueRateLimitError,
    VenueUnavailableError,
)
from pmcopy.integrations.polymarket.models import (
    VenueSubmitResult,
)

_READ_TIMEOUT_SECONDS = 10.0
_MAX_PRIVATE_HISTORY_PAGES = 1000
_DEFAULT_METADATA_CACHE_SIZE = 4096
_PRIVATE_CURSOR_TERMINALS = {"", "LTE=", "END"}
_EXPECTED_CHAIN_ID = 137
_SUPPORTED_MARKET_ORDER_TYPES = {"FAK", "FOK"}
_NETWORK_ERRORS = {
    "NetworkError",
    "RequestTimeout",
    "ExchangeNotAvailable",
    "OnMaintenance",
    "OperationFailed",
}
_RATE_LIMIT_ERRORS = {"RateLimitExceeded", "DDoSProtection"}
_AUTH_ERRORS = {"AuthenticationError", "PermissionDenied"}
_INVALID_ERRORS = {
    "ArgumentsRequired",
    "BadRequest",
    "BadSymbol",
    "InvalidOrder",
}
# Venue rejection detail kept for the operator. `utils.logger` now scrubs
# messages at emit time (registered secret values + credential
# URL/header/key patterns via `utils.log_redaction`), but the venue
# boundary keeps these local patterns as defence in depth for a venue body
# echoing a signed URL or key material into reason text before it ever
# reaches a sink.
_MAX_VENUE_REASON_CHARS = 300
_REDACTED = "***"
_BEARER_TOKEN_RE = re.compile(r"(?i)\bbearer\s+\S+")
_SECRET_ASSIGNMENT_RE = re.compile(
    r"(?i)([A-Za-z0-9_-]*(?:key|token|secret|password|passphrase|auth)[A-Za-z0-9_-]*)"
    r"([\"']?\s*[=:]\s*[\"']?)"
    r"([^\s&\"',}]+)"
)
# A 0x blob of 64+ hex digits is a private key, a signature or an order hash;
# 40-digit wallet addresses are public identity and stay readable.
_KEY_MATERIAL_RE = re.compile(r"0x[0-9a-fA-F]{64,}")

OrderSide = Literal["BUY", "SELL"]


def _exception_name(exc: BaseException) -> str:
    return type(exc).__name__


def _is_cancel(exc: BaseException) -> bool:
    return isinstance(exc, (KeyboardInterrupt, SystemExit, asyncio.CancelledError))


def _decimal(value: Any, *, field: str, default: Decimal | None = None) -> Decimal:
    if value is None:
        if default is not None:
            return default
        raise VenueInvalidOrderError(f"missing {field}")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise VenueInvalidOrderError(f"invalid {field}: {value!r}") from exc
    if not result.is_finite():
        raise VenueInvalidOrderError(f"invalid {field}: non-finite value")
    return result


def _maybe_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    dump = getattr(value, "model_dump", None)
    if callable(dump):
        raw = dump(mode="json", by_alias=True)
        return raw if isinstance(raw, dict) else {}
    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        raw = to_dict()
        return raw if isinstance(raw, dict) else {}
    return {}


def _nested_dict(mapping: dict[str, Any], key: str) -> dict[str, Any]:
    """Return one raw nested dictionary with a precise adapter-local type."""

    value = mapping.get(key)
    return _maybe_dict(value) if isinstance(value, dict) else {}


def _nested_dict_or_self(mapping: dict[str, Any], key: str) -> dict[str, Any]:
    """Return a present nested dictionary or the original mapping."""

    value = mapping.get(key)
    return _maybe_dict(value) if isinstance(value, dict) else mapping


def _first(mapping: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in mapping and mapping[key] is not None:
            return mapping[key]
    return None


def _nested(mapping: dict[str, Any], *path: str) -> Any:
    current: Any = mapping
    for key in path:
        if not isinstance(current, dict) or key not in current:
            return None
        current = current[key]
    return current


def _normalize_side(value: Any) -> OrderSide:
    side = str(value or "").upper()
    if side == "BUY":
        return "BUY"
    if side == "SELL":
        return "SELL"
    raise VenueInvalidOrderError(f"unsupported order side: {value!r}")


def _normalize_market_order_type(value: Any) -> str:
    order_type = str(value or "FOK").upper()
    if order_type not in _SUPPORTED_MARKET_ORDER_TYPES:
        raise VenueInvalidOrderError(f"unsupported market order type: {order_type}")
    return order_type


def _normalize_status(value: Any) -> str | None:
    status = str(value or "").strip().lower()
    if not status:
        return None
    aliases = {
        "live": "open",
        "order_status_live": "open",
        "placement": "open",
        "update": "open",
        "delayed": "open",
        "matched": "closed",
        "order_status_matched": "closed",
        "filled": "closed",
        "cancelled": "canceled",
        "order_status_cancelled": "canceled",
        "cancellation": "canceled",
        "cancelled_by_market": "canceled",
        "failed": "rejected",
        "order_status_failed": "rejected",
    }
    return aliases.get(status, status)


def _timestamp_ms(value: Any) -> int | None:
    if value is None:
        return None
    try:
        number = int(Decimal(str(value)))
    except (InvalidOperation, TypeError, ValueError):
        return None
    if number < 10_000_000_000:
        number *= 1000
    return number


def _datetime(value: Any, timestamp: Any = None) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str) and value:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    ts = _timestamp_ms(timestamp)
    if ts is None:
        return None
    return datetime.fromtimestamp(ts / 1000, tz=timezone.utc)


def _status_code(exc: BaseException) -> int | None:
    for name in ("status", "status_code", "http_status"):
        value = getattr(exc, name, None)
        if isinstance(value, int):
            return value
    return None


def _venue_reason(exc: BaseException) -> str:
    """Return the venue's own rejection text, scrubbed and bounded.

    A CCXT exception class alone is unactionable: ``InvalidOrder`` covers a bad
    price, a bad size and an unknown token alike. The vendor message carries
    the CLOB response body that distinguishes them, so it is kept — but a
    venue body can echo a signed request URL or key material, and no repo-wide
    message scrubber exists, so credentials are stripped here.

    Args:
        exc: The vendor exception raised by CCXT.

    Returns:
        A single-line, credential-free excerpt of ``str(exc)`` bounded to
        ``_MAX_VENUE_REASON_CHARS``, or ``"no venue detail"`` when the vendor
        supplied no message.
    """
    text = str(exc).strip()
    if not text:
        return "no venue detail"
    text = _BEARER_TOKEN_RE.sub(f"Bearer {_REDACTED}", text)
    text = _SECRET_ASSIGNMENT_RE.sub(rf"\1\2{_REDACTED}", text)
    text = _KEY_MATERIAL_RE.sub(_REDACTED, text)
    text = " ".join(text.split())
    if len(text) > _MAX_VENUE_REASON_CHARS:
        text = text[:_MAX_VENUE_REASON_CHARS] + "…"
    return text


def _raise_read_error(exc: BaseException, *, not_found: str) -> NoReturn:
    if _is_cancel(exc):
        raise exc
    name = _exception_name(exc)
    status = _status_code(exc)
    if name in _RATE_LIMIT_ERRORS or status == HTTPStatus.TOO_MANY_REQUESTS:
        raise VenueRateLimitError("Polymarket CCXT rate limit exceeded") from exc
    if name in _AUTH_ERRORS or status in (HTTPStatus.UNAUTHORIZED, HTTPStatus.FORBIDDEN):
        raise VenueAuthError("Polymarket CCXT authentication rejected") from exc
    if name == "OrderNotFound" or status == HTTPStatus.NOT_FOUND:
        error_type = (
            VenueOrderbookNotFoundError if not_found == "orderbook" else VenueOrderNotFoundError
        )
        raise error_type(f"{not_found}_not_found") from exc
    if name in _NETWORK_ERRORS or (status is not None and status >= 500):
        raise VenueUnavailableError(f"CCXT {name}") from exc
    if name in _INVALID_ERRORS:
        if not_found == "orderbook" and name == "BadSymbol":
            raise VenueOrderbookNotFoundError("orderbook_not_found") from exc
        raise VenueInvalidOrderError(f"CCXT {name}: {_venue_reason(exc)}") from exc
    if name == "InsufficientFunds":
        raise VenueInsufficientBalanceError("CCXT InsufficientFunds") from exc
    raise VenueUnavailableError(f"CCXT {name}") from exc


def _submission_exception(exc: BaseException) -> VenueSubmitResult:
    if _is_cancel(exc):
        raise exc
    name = _exception_name(exc)
    status = _status_code(exc)
    if name == "OrderNotFillable":
        # A killed FAK/FOK is a normal outcome, not an outage. Carry the venue's
        # wording: "no orders found to match" (nothing crossable at our limit)
        # and "could not be fully filled" (thin book, FOK) are different
        # diagnoses that this single token cannot tell apart.
        return VenueSubmitResult(
            success=False,
            status="canceled",
            error="order_not_fillable",
            non_retryable=True,
            venue_reason=_venue_reason(exc),
        )
    if name in _NETWORK_ERRORS or (status is None and name in {"TimeoutError", "ConnectionError"}):
        return VenueSubmitResult(
            success=False,
            error=f"ambiguous_{name.lower()}",
            ambiguous_post_failure=True,
        )
    if status is not None and status >= 500:
        return VenueSubmitResult(
            success=False,
            error=f"ambiguous_server_failure_{status}",
            ambiguous_post_failure=True,
        )
    if name in _RATE_LIMIT_ERRORS or status == HTTPStatus.TOO_MANY_REQUESTS:
        raise VenueRateLimitError("Polymarket CCXT rate limit exceeded") from exc
    if name in _AUTH_ERRORS or status in (HTTPStatus.UNAUTHORIZED, HTTPStatus.FORBIDDEN):
        raise VenueAuthError("Polymarket CCXT authentication rejected") from exc
    if name == "InsufficientFunds":
        raise VenueInsufficientBalanceError("Polymarket balance/allowance is insufficient") from exc
    if name in _INVALID_ERRORS or (status is not None and 400 <= status < 500):
        raise VenueInvalidOrderError(
            f"Polymarket CCXT order rejected ({name}): {_venue_reason(exc)}"
        ) from exc
    # Once create_order has been invoked, an unclassified exception is ambiguous:
    # the CLOB may have accepted the POST before the client failed to decode the
    # response.  Treating this as a retryable outage can duplicate a live order.
    return VenueSubmitResult(
        success=False,
        error=f"ambiguous_{name.lower()}",
        ambiguous_post_failure=True,
    )


async def _await_call(method: Any, *args: Any, **kwargs: Any) -> Any:
    result = method(*args, **kwargs)
    return await result if inspect.isawaitable(result) else result


# How long a metadata fetch stays usable for signing.
#
# Default 0 = refetch before every signed order, which is the money-path rule in
# CLAUDE.md ("refresh and validate tick size, minimum order size, and
# negative-risk metadata before signing"). Polymarket moves tick size while a
# market is live, notably near 0.96/0.04, and a stale tick signs a price off the
# grid, so this is deliberately opt-in rather than on by default.
#
# The cost of that safety is a duplicate CLOB round trip (~0.4s each for the
# market and neg-risk calls) on the critical path of a copy race whose window is
# 1-2 seconds, immediately after the pipeline already prefetched the same data.
# An operator who accepts a bounded staleness window can set
# ``execution.venue_metadata_ttl_seconds`` to reclaim it; a few seconds is
# enough to collapse the prefetch/submit pair.
_DEFAULT_METADATA_TTL_SECONDS = 0.0

