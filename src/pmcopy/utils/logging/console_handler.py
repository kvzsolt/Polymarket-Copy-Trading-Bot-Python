"""Rich terminal LogHandler with structured card rendering and redaction."""

from __future__ import annotations

import logging
import sys
import time
from logging import Handler, LogRecord
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

from rich.markup import escape as _escape_markup
from rich.panel import Panel
from rich.text import Text
from rich.traceback import Traceback

from pmcopy.utils.log_redaction import (
    REDACTED_PLACEHOLDER,
    is_secret_key,
    redact_text,
)
from pmcopy.utils.logging.formatters import (
    APP_NAME,
    DEFAULT_BACKUP_COUNT,
    DEFAULT_MAX_BYTES,
    JsonFormatter,
    _LEVEL_STYLES,
    _ManagedHandlerMixin,
    _ORDER_LIFECYCLE_PANEL,
    _POSITION_DRIFT_PANEL,
    _RedactedRenderable,
    _SecretRedactionFilter,
    _active_console,
    _append_extra_fields,
    _build_panel_lines,
    _default_log_dir,
    _extra_value_text,
    _record_extra_fields,
    shorten_addr,
)


class RichConsoleHandler(logging.Handler):
    """Console handler with Rich-styled rendering for structured log types."""

    # Log types that should only go to JSONL file, not console
    _JSONL_ONLY_TYPES = frozenset(
        {
            "http_request_complete",  # high-frequency HTTP polling — JSONL only
            "monitor_heartbeat",
            "trade_detected",
            "trade_skip",
            "trade_decision",
            "trade_quality_decision",  # analytics granularity — JSONL only
            "trade_quality_execution",  # analytics granularity — JSONL only
            "tier_gate_watchlist",
            "tier_gate_paper",
            "trade_batch_summary",
            "sizing_audit",
            "sizing_final",
            "trade_copy_failed",
            "book_quality_gate_no_book",
            "book_quality_gate_wide_spread",
            "book_quality_gate_thin_book",
            "startup_health_check",
            "startup_summary",
        }
    )

    _JSONL_ONLY_MESSAGES = frozenset(
        {
            "orderbook_capture_failed",
        }
    )

    def emit(self, record: LogRecord) -> None:
        """Emit a record while isolating managed-handler failures."""
        try:
            log_type = getattr(record, "type", None)
            if (
                log_type in self._JSONL_ONLY_TYPES
                or record.getMessage() in self._JSONL_ONLY_MESSAGES
            ):
                return

            # Rich rendering for structured trade types
            if log_type and self._render_structured(record, log_type):
                return

            # Default: plain styled text with all structured extra fields
            # visible in the console, not only in the JSONL file.
            msg = _append_extra_fields(redact_text(self.format(record)), record)
            ts = time.strftime("%I:%M:%S %p")
            style = _LEVEL_STYLES.get(record.levelname, "info")
            prefix = Text(f"[{ts}] ", style="muted")
            body = Text(msg, style=style)
            _active_console().print(prefix + body, highlight=False)
            exc_info = record.exc_info
            if exc_info and exc_info[0] and exc_info[1] and not record.exc_text:
                _active_console().print(self._exception_renderable(exc_info))
        except Exception:
            self.handleError(record)

    def _exception_renderable(self, exc_info) -> Any:
        """Return a Traceback renderable, wrapped for redaction when needed.

        The common path (nothing redactable in the exception chain) passes
        the ``Traceback`` through untouched so panel styling is preserved.
        When the rendered exception text contains secret material, wrap it
        in ``_RedactedRenderable`` which scrubs each rendered segment.
        """
        traceback = Traceback.from_exception(
            exc_info[0],
            exc_info[1],
            exc_info[2],
            max_frames=12,
        )
        try:
            rendered = logging.Formatter().formatException(exc_info)
        except Exception:
            return traceback
        if redact_text(rendered) == rendered:
            return traceback
        return _RedactedRenderable(traceback)

    def _render_structured(self, record: LogRecord, log_type: str) -> bool:
        """Render structured log types as Rich panels/lines. Returns True if handled."""
        ts = time.strftime("%I:%M:%S %p")
        if log_type == "trade_decision":
            self._render_decision(record, ts)
            return True
        if log_type == "trade_execution":
            self._render_execution(record, ts)
            return True
        if log_type == "trade_submitted":
            self._render_submitted(record, ts)
            return True
        if log_type == "order_lifecycle":
            self._render_order_lifecycle(record, ts)
            return True
        if log_type == "position_drift":
            self._render_position_drift(record, ts)
            return True
        if log_type == "risk_exit":
            self._render_risk_exit(record, ts)
            return True
        if log_type == "api_error":
            self._render_api_error(record, ts)
            return True
        if log_type == "api_response":
            self._render_api_response(record, ts)
            return True
        if log_type == "clob_client_init":
            self._render_clob_client_init(record, ts)
            return True
        if log_type == "discovery_candidate_eval":
            self._render_discovery_candidate_eval(record, ts)
            return True
        if log_type:
            self._render_generic_event(record, ts, log_type)
            return True
        return False

    def _render_generic_event(self, record: LogRecord, ts: str, log_type: str) -> None:
        """Render any unrecognized structured log type as a clear Rich panel."""
        level = record.levelname
        if level == "WARNING":
            border, icon = "yellow", "⚠️"
        elif level in ("ERROR", "CRITICAL"):
            border, icon = "red", "❌"
        else:
            border, icon = "cyan", "ℹ️"

        title_label = str(log_type).replace("_", " ").upper()
        message = redact_text(self.format(record))
        extras = _record_extra_fields(record)
        lines: list[str] = []
        if message and message != title_label.lower():
            lines.append(f"[bold]Event:[/bold]  {_escape_markup(message)}")
        for key, value in extras.items():
            if key == "type":
                continue
            if is_secret_key(key):
                value_text = REDACTED_PLACEHOLDER
            else:
                value_text = _extra_value_text(value)
            label = " ".join(part.capitalize() for part in str(key).replace("_", " ").split())
            lines.append(f"[bold]{label}:[/bold]  {value_text}")
        if not lines:
            lines.append("[dim](no details)[/dim]")

        _active_console().print()
        _active_console().print(
            Panel(
                "\n".join(lines),
                title=f"{icon} [bold {border}]{title_label}[/bold {border}]  [muted][{ts}][/muted]",
                border_style=border,
                padding=(1, 2),
            )
        )

    def _render_discovery_candidate_eval(self, record: LogRecord, ts: str) -> None:
        event = str(getattr(record, "event", "") or "").lower()
        concurrency = getattr(record, "concurrency", None)
        duration = getattr(record, "duration_seconds", None)

        if event == "run_completed":
            total = getattr(record, "candidate_total", 0)
            evaluated = getattr(record, "evaluated_count", 0)
            errors = getattr(record, "error_count", 0)
            selected = getattr(record, "selected_count", 0)
            parts = [
                f"[muted][{ts}][/muted]",
                "[bold cyan]DISCOVERY CANDIDATE RUN[/bold cyan]",
                "[success]COMPLETED[/success]",
                f"total={total}",
                f"evaluated={evaluated}",
                f"errors={errors}",
                f"selected={selected}",
            ]
            if concurrency is not None:
                parts.append(f"concurrency={concurrency}")
            if duration is not None:
                parts.append(f"{float(duration):.1f}s")
            _active_console().print(" | ".join(parts), highlight=False)
            return

        idx = getattr(record, "candidate_index", 0)
        total = getattr(record, "candidate_total", 0)
        wallet = _escape_markup(shorten_addr(getattr(record, "wallet", None), 6, 4))
        status = str(getattr(record, "status", "") or "")
        score = getattr(record, "final_score", None)
        reason = getattr(record, "rejection_reason", None)
        error_type = getattr(record, "error_type", None)
        live = getattr(record, "eligible_live", None)
        selected = getattr(record, "selected", None)

        if event == "failed":
            event_style = "error"
            event_label = "FAILED"
        elif event == "completed" and status == "rejected":
            event_style = "warning"
            event_label = "REJECTED"
        elif event == "completed":
            event_style = "success" if selected else "info"
            event_label = "COMPLETED"
        else:
            event_style = "info"
            event_label = "STARTED"

        parts = [
            f"[muted][{ts}][/muted]",
            "[bold cyan]DISCOVERY CANDIDATE[/bold cyan]",
            f"{int(idx):03d}/{int(total):03d}" if total else str(idx),
            f"[address]{wallet}[/address]",
            f"[{event_style}]{event_label}[/{event_style}]",
        ]
        if concurrency is not None:
            parts.append(f"concurrency={concurrency}")
        if status:
            parts.append(f"status={_escape_markup(status)}")
        if score is not None:
            parts.append(f"score={float(score):.3f}")
        if live is not None:
            parts.append(f"live={str(bool(live)).lower()}")
        if reason:
            parts.append(f"reason={_escape_markup(str(reason))}")
        if error_type:
            parts.append(f"error={_escape_markup(str(error_type))}")
        if duration is not None:
            parts.append(f"{float(duration):.1f}s")
        _active_console().print(" | ".join(parts), highlight=False)

    def _render_api_error(self, record: LogRecord, ts: str) -> None:
        side = getattr(record, "side", "?")
        error_detail = getattr(record, "error_detail", None)
        error_raw = error_detail if error_detail is not None else getattr(record, "error", None)
        error = redact_text(str(error_raw)) if error_raw else error_raw
        cond = shorten_addr(getattr(record, "conditionId", None), 8, 6)
        asset = shorten_addr(getattr(record, "asset", None), 8, 6)
        retry = getattr(record, "retry", 0)
        max_retries = getattr(record, "maxRetries", 3)
        category = getattr(record, "error", "") if error_detail else None

        lines = _build_panel_lines(
            Side=side.upper(),
            Message=error,
            Category=category if category and category != error else None,
            Condition=cond if cond != "N/A" else None,
            Asset=asset if asset != "N/A" else None,
            Retries=f"{retry}/{max_retries}" if retry > 0 else None,
        )

        _active_console().print()
        _active_console().print(
            Panel(
                "\n".join(lines),
                title=f"❌ [bold red]ORDER REJECTED[/bold red]  [muted][{ts}][/muted]",
                border_style="red",
                padding=(1, 2),
            )
        )

    def _render_api_response(self, record: LogRecord, ts: str) -> None:
        method = getattr(record, "httpMethod", "?")
        url = redact_text(str(getattr(record, "url", "") or ""))
        status = getattr(record, "httpStatus", 0)
        data_summary = getattr(record, "data_summary", None)
        error = redact_text(str(getattr(record, "error", None) or "")) or None
        ok = 200 <= status < 400

        # Shorten URL for display
        short_url = url[:60] + "..." if len(url) > 60 else url
        border = "green" if ok else "red"
        icon = "✅" if ok else "❌"
        status_label = "OK" if ok else "ERR"

        lines = _build_panel_lines(
            Method=method,
            Status=str(status),
            URL=short_url or None,
        )
        if data_summary and isinstance(data_summary, dict):
            for k, v in data_summary.items():
                lines.append(f"  [dim]{k}:[/dim] {redact_text(str(v))}")
        if error:
            lines.append(f"  [dim]error:[/dim] {error}")

        _active_console().print()
        _active_console().print(
            Panel(
                "\n".join(lines),
                title=f"{icon} [bold {border}]API {status_label} {method}[/bold {border}]  [muted][{ts}][/muted]",
                border_style=border,
                padding=(1, 2),
            )
        )

    def _render_clob_client_init(self, record: LogRecord, ts: str) -> None:
        host = getattr(record, "host", None)
        chain_id = getattr(record, "chainId", None)
        signature_type = getattr(record, "signatureType", None)
        funder = shorten_addr(getattr(record, "funder", None), 8, 4)
        wallet_type = getattr(record, "walletType", None)
        lines = _build_panel_lines(
            Host=host,
            Chain=str(chain_id) if chain_id is not None else None,
            SignatureType=str(signature_type) if signature_type is not None else None,
            WalletType=wallet_type,
            Funder=funder if funder != "N/A" else None,
        )
        _active_console().print()
        _active_console().print(
            Panel(
                "\n".join(lines),
                title=f"🔐 [bold cyan]CLOB CLIENT READY[/bold cyan]  [muted][{ts}][/muted]",
                border_style="cyan",
                padding=(1, 2),
            )
        )

    def _render_decision(self, record: LogRecord, ts: str) -> None:
        allow = getattr(record, "allow", False)
        side = getattr(record, "side", "?")
        score = float(getattr(record, "decisionScore", 0.0) or 0.0)
        reason = getattr(record, "reason", "") or ""
        cond = shorten_addr(getattr(record, "conditionId", None), 8, 6)
        if allow:
            label_style = "trade.executed"
            icon = "✅"
            label = "ALLOW"
        else:
            label_style = "warning"
            icon = "⛔"
            label = "SKIP"
        side_style = "trade.buy" if side == "BUY" else "trade.sell" if side == "SELL" else "info"
        parts = [
            f"[muted][{ts}][/muted]",
            f"{icon} [{label_style}]DECISION {label}[/{label_style}]",
            f"[{side_style}]{side}[/{side_style}]",
            f"score={score:.3f}",
        ]
        if cond and cond != "N/A":
            parts.append(f"[muted]cond={cond}[/muted]")
        if not allow and reason:
            parts.append(f"[muted]{reason}[/muted]")
        _active_console().print(" | ".join(parts))

    def _render_execution(self, record: LogRecord, ts: str) -> None:
        side = getattr(record, "side", "?")
        size = float(getattr(record, "size", 0) or 0)
        price = float(getattr(record, "price", 0) or 0)
        usdc = float(getattr(record, "usdcValue", 0) or 0)
        cond = shorten_addr(getattr(record, "conditionId", None), 8, 6)
        title = getattr(record, "title", None) or ""
        submarket = getattr(record, "submarket", None) or ""
        outcome = getattr(record, "outcome", None) or ""
        order_id = shorten_addr(getattr(record, "copyOrderId", None), 8, 6)
        side_style = "trade.buy" if side == "BUY" else "trade.sell"
        lines = _build_panel_lines(
            Side=f"[{side_style}]{side}[/{side_style}]",
            Size=f"{size:.4f} tokens",
            Price=f"{price:.4f}",
            Value=f"[money]${usdc:.2f}[/money]",
            Market=title[:60] if title else None,
            Submarket=submarket or None,
            Outcome=outcome or None,
            Condition=cond if cond != "N/A" else None,
            OrderID=order_id if order_id != "N/A" else None,
        )
        _active_console().print()
        _active_console().print(
            Panel(
                "\n".join(lines),
                title=f"🎯 [bold green]TRADE EXECUTED — {side}[/bold green]  [muted][{ts}][/muted]",
                border_style="green",
                padding=(1, 2),
            )
        )

    def _render_submitted(self, record: LogRecord, ts: str) -> None:
        side = getattr(record, "side", "?")
        size = float(getattr(record, "size", 0) or 0)
        price = float(getattr(record, "price", 0) or 0)
        usdc = float(getattr(record, "usdcValue", 0) or 0)
        cond = shorten_addr(getattr(record, "conditionId", None), 8, 6)
        title = getattr(record, "title", None) or ""
        submarket = getattr(record, "submarket", None) or ""
        outcome = getattr(record, "outcome", None) or ""
        order_id = shorten_addr(getattr(record, "copyOrderId", None), 8, 6)
        submission_state = getattr(record, "submissionState", None)
        is_unconfirmed = submission_state == "submitted_unconfirmed"
        side_style = "trade.buy" if side == "BUY" else "trade.sell"
        lines = _build_panel_lines(
            Side=f"[{side_style}]{side}[/{side_style}]",
            Size=f"{size:.4f} tokens",
            Price=f"{price:.4f}",
            Value=f"[money]${usdc:.2f}[/money]",
            Market=title[:60] if title else None,
            Submarket=submarket or None,
            Outcome=outcome or None,
            Condition=cond if cond != "N/A" else None,
            OrderID=order_id if order_id != "N/A" else None,
            State=submission_state if is_unconfirmed else None,
        )
        _active_console().print()
        if is_unconfirmed:
            _active_console().print(
                Panel(
                    "\n".join(lines),
                    title=f"⏳ [bold yellow]ORDER UNCONFIRMED — {side}[/bold yellow]  [muted][{ts}][/muted]",
                    border_style="yellow",
                    padding=(1, 2),
                )
            )
        else:
            _active_console().print(
                Panel(
                    "\n".join(lines),
                    title=f"📤 [bold cyan]ORDER SUBMITTED — {side}[/bold cyan]  [muted][{ts}][/muted]",
                    border_style="cyan",
                    padding=(1, 2),
                )
            )

    def _render_order_lifecycle(self, record: LogRecord, ts: str) -> None:
        status = (getattr(record, "status", "") or "").lower()
        side = (getattr(record, "side", "") or "?").upper()
        venue_status = getattr(record, "venueStatus", None)
        submission_state = getattr(record, "submissionState", None)
        size_raw = getattr(record, "size", None)
        size_estimated = bool(getattr(record, "sizeEstimated", False))
        price_raw = getattr(record, "price", None)
        usdc_raw = getattr(record, "usdcValue", None)
        cond = shorten_addr(getattr(record, "conditionId", None), 8, 6)
        asset = shorten_addr(getattr(record, "asset", None), 8, 6)
        order_id = shorten_addr(getattr(record, "copyOrderId", None), 8, 6)
        tx = shorten_addr(getattr(record, "txHash", None), 8, 6)

        title_text, border, icon = _ORDER_LIFECYCLE_PANEL.get(
            status,
            ("ORDER LIFECYCLE", "white", "📋"),
        )
        side_style = "trade.buy" if side == "BUY" else "trade.sell" if side == "SELL" else "info"

        def _fmt_float(v: Any, fmt: str) -> str | None:
            try:
                return format(float(v), fmt) if v is not None else None
            except (TypeError, ValueError):
                return None

        size_str = _fmt_float(size_raw, ".4f")
        price_str = _fmt_float(price_raw, ".4f")
        usdc_str = _fmt_float(usdc_raw, ".2f")

        size_label = "Estimated Size" if size_estimated else "Size"
        size_field = {size_label: f"{size_str} tokens" if size_str else None}
        lines = _build_panel_lines(
            Side=f"[{side_style}]{side}[/{side_style}]",
            Status=status or None,
            VenueStatus=str(venue_status) if venue_status else None,
            State=str(submission_state) if submission_state else None,
            **size_field,
            Price=price_str,
            Value=f"[money]${usdc_str}[/money]" if usdc_str else None,
            Condition=cond if cond != "N/A" else None,
            Asset=asset if asset != "N/A" else None,
            OrderID=order_id if order_id != "N/A" else None,
            Tx=tx if tx != "N/A" else None,
        )
        _active_console().print()
        _active_console().print(
            Panel(
                "\n".join(lines),
                title=f"{icon} [bold {border}]{title_text} — {side}[/bold {border}]  [muted][{ts}][/muted]",
                border_style=border,
                padding=(1, 2),
            )
        )

    def _render_position_drift(self, record: LogRecord, ts: str) -> None:
        status = (getattr(record, "status", "") or "").lower()
        action = (getattr(record, "action", "") or "").lower()
        user_id = getattr(record, "userId", None)
        cond = shorten_addr(getattr(record, "conditionId", None), 8, 6)
        asset = shorten_addr(getattr(record, "asset", None), 8, 6)
        local = getattr(record, "localSize", None)
        venue = getattr(record, "venueSize", None)
        drift = getattr(record, "driftSize", None)
        reason = getattr(record, "reason", None)

        title, border, icon = _POSITION_DRIFT_PANEL.get(
            status,
            ("POSITION DRIFT DETECTED", "yellow", "⚠️"),
        )
        # Action overrides for clearer operator signal.
        if action == "prune_local_zero_position":
            title, border, icon = "LOCAL POSITION PRUNED", "cyan", "🧹"
        elif action in {"pause_scope", "require_operator_review"}:
            title, border, icon = "TRADING PAUSED FOR POSITION", "red", "🛑"

        def _fmt(v: Any) -> str | None:
            try:
                return f"{float(v):.4f}"
            except (TypeError, ValueError):
                return None

        lines = _build_panel_lines(
            User=user_id or None,
            Status=status or None,
            Action=action or None,
            Condition=cond if cond != "N/A" else None,
            Asset=asset if asset != "N/A" else None,
            LocalSize=_fmt(local),
            VenueSize=_fmt(venue),
            DriftSize=_fmt(drift),
            Reason=reason or None,
        )
        _active_console().print()
        _active_console().print(
            Panel(
                "\n".join(lines),
                title=f"{icon} [bold {border}]{title}[/bold {border}]  [muted][{ts}][/muted]",
                border_style=border,
                padding=(1, 2),
            )
        )

    def _render_risk_exit(self, record: LogRecord, ts: str) -> None:
        cond = shorten_addr(getattr(record, "conditionId", None), 8, 6)
        size = float(getattr(record, "size", 0) or 0)
        price = float(getattr(record, "price", 0) or 0)
        usdc = float(getattr(record, "usdcValue", 0) or 0)
        reason = getattr(record, "riskReason", "") or ""
        lines = _build_panel_lines(
            Trigger=reason,
            Size=f"{size:.4f} tokens",
            Price=f"{price:.4f}",
            Value=f"[money]${usdc:.2f}[/money]",
            Condition=cond if cond != "N/A" else None,
        )
        _active_console().print()
        _active_console().print(
            Panel(
                "\n".join(lines),
                title=f"🛡️ [bold red]RISK EXIT — SELL[/bold red]  [muted][{ts}][/muted]",
                border_style="red",
                padding=(1, 2),
            )
        )


class _ManagedRichConsoleHandler(_ManagedHandlerMixin, RichConsoleHandler):
    """Rich console handler owned by this logging configuration."""


class _ManagedRotatingFileHandler(_ManagedHandlerMixin, RotatingFileHandler):
    """Rotating JSONL handler owned by this logging configuration."""


class _ManagedStreamHandler(_ManagedHandlerMixin, logging.StreamHandler):
    """Fallback stderr handler owned by this logging configuration."""


def _remove_managed_handlers(logger: logging.Logger) -> None:
    keep: list[Handler] = []
    for h in logger.handlers:
        if getattr(h, "_pm_bot_managed", False):
            try:
                h.flush()
                h.close()
            except Exception:
                pass
            continue
        keep.append(h)
    logger.handlers = keep


def configure_logging(
    *,
    name: str = APP_NAME,
    level: str | int = "INFO",
    log_dir: Path | str | None = None,
    jsonl_file: Path | str | None = None,
    max_bytes: int = DEFAULT_MAX_BYTES,
    backup_count: int = DEFAULT_BACKUP_COUNT,
) -> logging.Logger:
    """Configure logging."""
    logging.raiseExceptions = False

    if isinstance(level, str):
        resolved_level = getattr(logging, level.upper(), logging.INFO)
    else:
        resolved_level = level

    log_dir_path = Path(log_dir) if log_dir else _default_log_dir()
    jsonl_path = Path(jsonl_file) if jsonl_file else (log_dir_path / f"{APP_NAME}.jsonl")

    logger = logging.getLogger(name)
    logger.setLevel(resolved_level)
    logger.propagate = False
    _remove_managed_handlers(logger)

    ch = _ManagedRichConsoleHandler()
    ch.setLevel(resolved_level)
    ch.setFormatter(logging.Formatter("%(message)s"))
    # Emit-time secret redaction, applied per handler so child loggers
    # (get_logger("Component")) are covered exactly like the app logger.
    ch.addFilter(_SecretRedactionFilter())
    logger.addHandler(ch)

    facade = sys.modules.get("pmcopy.utils.logger")
    rotating_cls = getattr(facade, "_ManagedRotatingFileHandler", _ManagedRotatingFileHandler)
    stream_cls = getattr(facade, "_ManagedStreamHandler", _ManagedStreamHandler)
    try:
        log_dir_path.mkdir(parents=True, exist_ok=True)
        file_handler: logging.Handler = rotating_cls(
            filename=str(jsonl_path),
            maxBytes=max(1, int(max_bytes)),
            backupCount=max(0, int(backup_count)),
            encoding="utf-8",
        )
    except Exception:
        file_handler = stream_cls(sys.stderr)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(JsonFormatter())
    file_handler.addFilter(_SecretRedactionFilter())
    logger.addHandler(file_handler)

    _CONFIGURED_LOGGERS[name] = logger
    return logger


_CONFIGURED_LOGGERS: dict[str, logging.Logger] = {}


def get_logger(name: str | None = None) -> logging.Logger:
    """Return the configured structured logger for a component."""
    if APP_NAME not in _CONFIGURED_LOGGERS:
        configure_logging()

    app_logger = logging.getLogger(APP_NAME)
    if name and name != APP_NAME:
        return app_logger.getChild(name)
    return app_logger


