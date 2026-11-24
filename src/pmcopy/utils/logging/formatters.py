"""Provide secret-safe structured and operator-facing logging."""

from __future__ import annotations

import json
import logging
import sys
from logging import LogRecord
from pathlib import Path
from typing import Any

from rich.console import Console
from rich.markup import escape as _escape_markup
from rich.segment import Segment
from rich.theme import Theme

from pmcopy.utils.log_redaction import (
    REDACTED_PLACEHOLDER,
    is_secret_key,
    redact_object,
    redact_text,
)
from pmcopy.utils.packaged_resources import deployment_root

APP_NAME = "PolymarketCopyTradingBot"
DEFAULT_LOG_SUBDIR = "log"
DEFAULT_MAX_BYTES = 5 * 1024 * 1024
DEFAULT_BACKUP_COUNT = 3
MONITOR_HEARTBEAT_INTERVAL = 30

_THEME = Theme(
    {
        "info": "cyan",
        "warning": "yellow",
        "error": "bold red",
        "success": "bold green",
        "muted": "dim white",
        "header": "bold magenta",
        "money": "bold green",
        "address": "cyan",
        "label": "bold white",
        "trade.buy": "bold green",
        "trade.sell": "bold red",
        "trade.detected": "bold yellow",
        "trade.executed": "bold green",
        "heartbeat": "dim cyan",
    }
)

console = Console(theme=_THEME, stderr=False)

_LEVEL_STYLES = {
    "DEBUG": "muted",
    "WARNING": "warning",
    "ERROR": "error",
    "CRITICAL": "error",
}

_HTTP_ERROR_CATEGORIES = {
    "400": "bad_request",
    "401": "unauthorized",
    "403": "forbidden",
    "404": "not_found",
    "409": "conflict",
    "422": "unprocessable_entity",
    "429": "rate_limited",
    "500": "upstream_error",
    "502": "upstream_error",
    "503": "upstream_unavailable",
    "504": "upstream_timeout",
}


def _default_log_dir() -> Path:
    """Return the deployment-local default log directory.

    Never derived from the installed module location: in a wheel install
    that would create ``log/`` inside site-packages (or the venv's lib
    directory). Explicit ``log_dir``/``jsonl_file`` arguments and the
    ``logging.log_dir`` config setting take precedence over this default;
    the fallback is ``<deployment root>/log`` where the deployment root is
    ``PMCOPY_DEPLOYMENT_ROOT`` or the process CWD.
    """
    return deployment_root() / DEFAULT_LOG_SUBDIR


_STANDARD_RECORD_ATTRS = frozenset(vars(logging.LogRecord("", 0, "", 0, "", (), None)))


class JsonFormatter(logging.Formatter):
    """Render structured log records without exposing secrets.

    Defense in depth: even when a handler is attached without the global
    ``_SecretRedactionFilter`` installed by ``configure_logging``, this
    formatter redacts message text, serializable extras (nested values
    included), and the rendered exception traceback itself.
    """

    def format(self, record: LogRecord) -> str:
        """Format a record as bounded secret-safe structured JSON."""
        payload: dict[str, Any] = {
            "ts": self.formatTime(record, datefmt="%Y-%m-%d %H:%M:%S"),
            "level": record.levelname,
            "name": record.name,
            "message": redact_text(record.getMessage()),
            "module": record.module,
            "funcName": record.funcName,
            "lineno": record.lineno,
        }
        for key, value in vars(record).items():
            if key in _STANDARD_RECORD_ATTRS or key in payload:
                continue
            # Redact BEFORE the serializability probe: ``redact_object`` is
            # total (never raises; bounded depth; cycle-safe), so a cyclic or
            # over-deep extra becomes a safe placeholder here instead of
            # failing the probe on the raw structure and being silently
            # dropped (follow-up F3-2).
            redacted = redact_object(value)
            try:
                json.dumps(redacted)
            except (TypeError, ValueError):
                continue
            if is_secret_key(key):
                payload[key] = REDACTED_PLACEHOLDER
            else:
                payload[key] = redacted
        if record.exc_info:
            exc_type = record.exc_info[0].__name__ if record.exc_info[0] else "Exception"
            payload["exc_type"] = exc_type
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)

    def formatException(self, ei) -> str:
        """Render the exception chain with secret redaction applied."""
        return redact_text(super().formatException(ei))


_POSITION_DRIFT_PANEL: dict[str, tuple[str, str, str]] = {
    # status -> (title, border, icon)
    "clean": ("POSITION DRIFT CLEAN", "green", "✅"),
    "local_missing_venue_position": ("POSITION DRIFT DETECTED", "yellow", "⚠️"),
    "venue_missing_local_position": ("POSITION DRIFT DETECTED", "yellow", "⚠️"),
    "size_mismatch_positive": ("POSITION DRIFT DETECTED", "yellow", "⚠️"),
    "size_mismatch_negative": ("POSITION DRIFT DETECTED", "yellow", "⚠️"),
    "reservation_orphaned": ("POSITION DRIFT DETECTED", "yellow", "⚠️"),
    "unresolved": ("TRADING PAUSED FOR POSITION", "red", "🛑"),
}


_ORDER_LIFECYCLE_PANEL: dict[str, tuple[str, str, str]] = {
    # status -> (title, border_style, icon)
    "submitted_unconfirmed": ("ORDER UNCONFIRMED", "yellow", "⏳"),
    "delayed": ("ORDER DELAYED", "yellow", "⏳"),
    "submitted_delayed": ("ORDER DELAYED", "yellow", "⏳"),
    "live": ("ORDER LIVE", "cyan", "📘"),
    "order_live": ("ORDER LIVE", "cyan", "📘"),
    "matched": ("ORDER MATCHED", "cyan", "🔄"),
    "matched_pending_confirmation": ("ORDER MATCHED", "cyan", "🔄"),
    "mined": ("TRADE MINED", "cyan", "⛏️"),
    "trade_mined": ("TRADE MINED", "cyan", "⛏️"),
    "retrying": ("TRADE RETRYING", "yellow", "🔁"),
    "trade_retrying": ("TRADE RETRYING", "yellow", "🔁"),
    "confirmed": ("TRADE CONFIRMED", "green", "✅"),
    "trade_confirmed": ("TRADE CONFIRMED", "green", "✅"),
    "filled": ("TRADE CONFIRMED", "green", "✅"),
    "failed": ("ORDER FAILED", "red", "❌"),
    "rejected": ("ORDER FAILED", "red", "❌"),
    "expired": ("ORDER FAILED", "red", "❌"),
    "cancelled": ("ORDER CANCELLED", "red", "❌"),
    "canceled": ("ORDER CANCELLED", "red", "❌"),
    "ambiguous_timeout": ("ORDER AMBIGUOUS — TIMEOUT", "magenta", "⚠️"),
    "auto_released_ambiguous": ("ORDER AUTO-RELEASED", "magenta", "♻️"),
}


_ORDER_LIFECYCLE_LAST: dict[str, str] = {}
_ORDER_LIFECYCLE_LAST_MAX = 1024


def _lifecycle_dedup_key(
    order_id: str | None,
    signal_id: str | None,
    condition_id: str | None,
    account: str | None = None,
) -> str:
    """Build the lifecycle dedup key.

    A venue order id is globally unique and wins unchanged (legacy key).
    When no venue order id exists, an optional stable account identifier
    scopes the key so two copiers reacting to the same signal/condition do
    not collapse each other's lifecycle events. ``account=None`` reproduces
    the exact legacy single-copier key.
    """
    base = order_id or signal_id or condition_id or ""
    if not account or order_id:
        return base
    return f"{account}|{base}"


def _lifecycle_should_emit(key: str, status: str) -> bool:
    if not key:
        return True
    prev = _ORDER_LIFECYCLE_LAST.get(key)
    if prev == status:
        return False
    if len(_ORDER_LIFECYCLE_LAST) >= _ORDER_LIFECYCLE_LAST_MAX:
        # Drop oldest insertion. Python dicts preserve insertion order so this
        # popitem(last=False) equivalent works via next(iter(...)).
        try:
            oldest = next(iter(_ORDER_LIFECYCLE_LAST))
            _ORDER_LIFECYCLE_LAST.pop(oldest, None)
        except StopIteration:
            pass
    _ORDER_LIFECYCLE_LAST[key] = status
    return True


def _extra_value_text(value: Any) -> str:
    """Render one log-extra value as a safe, bounded console string."""
    if isinstance(value, str):
        return _escape_markup(redact_text(value))
    if isinstance(value, (bool, int, float, type(None))):
        return str(value)
    if isinstance(value, (list, tuple)):
        return _escape_markup("[" + ", ".join(_extra_value_text(item) for item in value) + "]")
    if isinstance(value, dict):
        return _escape_markup(
            "{"
            + ", ".join(
                f"{k}={REDACTED_PLACEHOLDER if is_secret_key(k) else _extra_value_text(v)}"
                for k, v in value.items()
            )
            + "}"
        )
    # Arbitrary object: never trust a raw repr/str in the console path —
    # redact any registered secret values or credential patterns it embeds.
    return _escape_markup(redact_text(str(value)))


def _record_extra_fields(record: LogRecord) -> dict[str, Any]:
    """Return non-standard log-record fields (the ``extra=...`` payload)."""
    return {
        str(key): value
        for key, value in vars(record).items()
        if key not in _STANDARD_RECORD_ATTRS and key != "message" and not str(key).startswith("_")
    }


def _append_extra_fields(msg: str, record: LogRecord) -> str:
    """Append console-safe key=value fields to a generic log line."""
    extras = _record_extra_fields(record)
    if not extras:
        return msg
    parts: list[str] = []
    for key, value in extras.items():
        if is_secret_key(key):
            parts.append(f"{key}={REDACTED_PLACEHOLDER}")
        else:
            parts.append(f"{key}={_extra_value_text(value)}")
    return f"{msg} | " + " | ".join(parts)


class _RedactedRenderable:
    """Wrap a Rich renderable and scrub secret material from rendered segments.

    Preserves segment styles (colors/panels) while replacing registered
    secret values and credential patterns in every text segment.
    """

    def __init__(self, inner: Any) -> None:
        self._inner = inner

    def __rich_console__(self, console: Any, options: Any):
        for segment in console.render(self._inner, options):
            if segment.text and segment.control is None:
                yield Segment(redact_text(segment.text), segment.style)
            else:
                yield segment



def _active_console() -> Console:
    mod = sys.modules.get("pmcopy.utils.logger")
    return getattr(mod, "console", console) if mod is not None else console


def shorten_addr(addr: str | None, prefix: int = 6, suffix: int = 4) -> str:
    """Return a compact display form of a wallet address."""
    if not addr:
        return "N/A"
    if len(addr) <= prefix + suffix + 3:
        return addr
    return f"{addr[:prefix]}...{addr[-suffix:]}"


def _build_panel_lines(**fields: Any) -> list[str]:
    """Format non-empty key/value panel lines."""
    lines: list[str] = []
    for label, value in fields.items():
        if value is not None:
            lines.append(f"[bold]{label}:[/bold]  {value}")
    return lines


class _ManagedHandlerMixin:
    """Mark handlers owned by this module for idempotent reconfiguration."""

    _pm_bot_managed = True


class _SecretRedactionFilter(logging.Filter):
    """Apply the single emit-time redaction pass for every sink.

    Installed on each managed HANDLER (not the logger): handler-level
    filters run for every record the handler processes, including records
    emitted through child loggers obtained via ``get_logger(name)`` whose
    own filter chain is never consulted. The mutation below is therefore
    visible to the Rich console renderer, the rotating JSONL sink, and the
    stderr fallback alike. The formatters add their own defense in depth
    for handlers attached without this filter.
    """

    def filter(self, record: LogRecord) -> bool:
        """Redact the record in place; never suppress and never raise."""
        try:
            _redact_record(record)
        except Exception:
            pass
        return True


def _redact_record(record: LogRecord) -> None:
    """Redact message text, args, extras, cached exception text, and stack text in place."""
    try:
        merged = record.getMessage()
    except Exception:
        merged = None
    if merged is not None:
        record.msg = redact_text(merged)
        record.args = ()
    else:
        if isinstance(record.msg, str):
            record.msg = redact_text(record.msg)
        if isinstance(record.args, tuple):
            record.args = tuple(
                redact_text(arg) if isinstance(arg, str) else arg for arg in record.args
            )
    for key, value in list(vars(record).items()):
        # Rendered-text standard attributes come FIRST: they are skipped by
        # the standard-attribute exclusion below (both ``exc_text`` and
        # ``stack_info`` are LogRecord defaults), yet they carry the exact
        # free text the stdlib ``Formatter.format`` appends verbatim to the
        # rendered message — the cached exception traceback and the caller
        # stack. Without this branch the raw text survives the emit-time
        # pass on the record object itself, so any handler attached without
        # this module's redacting formatters would print it unredacted.
        if key in ("exc_text", "stack_info"):
            if isinstance(value, str):
                setattr(record, key, redact_text(value))
            continue
        if key in _STANDARD_RECORD_ATTRS or key in ("message", "exc_info"):
            continue
        if key.startswith("_"):
            continue
        if is_secret_key(key):
            setattr(record, key, REDACTED_PLACEHOLDER)
            continue
        setattr(record, key, redact_object(value))

