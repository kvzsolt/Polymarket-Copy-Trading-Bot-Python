from __future__ import annotations

import json
import logging
import os
import re
import sys
import time
from logging import Handler, LogRecord
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

from rich.console import Console
from rich.panel import Panel
from rich.rule import Rule
from rich.table import Table
from rich.text import Text
from rich.traceback import Traceback
from rich.theme import Theme

APP_NAME = "PolymarketCopyTradingBot"
DEFAULT_LOG_SUBDIR = "log"
DEFAULT_MAX_BYTES = 5 * 1024 * 1024
DEFAULT_BACKUP_COUNT = 3
MONITOR_HEARTBEAT_INTERVAL = 30

_THEME = Theme({
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
})

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


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _default_log_dir() -> Path:
    configured = (os.getenv("LOG_DIR") or DEFAULT_LOG_SUBDIR).strip() or DEFAULT_LOG_SUBDIR
    return _project_root() / configured


_STANDARD_RECORD_ATTRS = frozenset(
    vars(logging.LogRecord("", 0, "", 0, "", (), None))
)


class JsonFormatter(logging.Formatter):
    def format(self, record: LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": self.formatTime(record, datefmt="%Y-%m-%d %H:%M:%S"),
            "level": record.levelname,
            "name": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "funcName": record.funcName,
            "lineno": record.lineno,
        }
        for key, value in vars(record).items():
            if key in _STANDARD_RECORD_ATTRS or key in payload:
                continue
            try:
                json.dumps(value)
                payload[key] = value
            except (TypeError, ValueError):
                pass
        if record.exc_info:
            exc_type = record.exc_info[0].__name__ if record.exc_info[0] else "Exception"
            payload["exc_type"] = exc_type
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


class RichConsoleHandler(logging.Handler):
    """Console handler with Rich-styled rendering for structured log types."""

    # Log types that should only go to JSONL file, not console
    _JSONL_ONLY_TYPES = frozenset({
        "http_request_complete",    # high-frequency HTTP polling — JSONL only
        "monitor_heartbeat",
        "trade_detected",
        "trade_skip",
        "trade_decision",
        "trade_quality_decision",   # analytics granularity — JSONL only
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
    })

    _JSONL_ONLY_MESSAGES = frozenset({
        "orderbook_capture_failed",
    })

    def emit(self, record: LogRecord) -> None:
        try:
            log_type = getattr(record, "type", None)
            if log_type in self._JSONL_ONLY_TYPES or record.getMessage() in self._JSONL_ONLY_MESSAGES:
                return

            # Rich rendering for structured trade types
            if log_type and self._render_structured(record, log_type):
                return

            # Default: plain styled text
            msg = self.format(record)
            ts = time.strftime("%I:%M:%S %p")
            style = _LEVEL_STYLES.get(record.levelname, "info")
            prefix = Text(f"[{ts}] ", style="muted")
            body = Text(msg, style=style)
            console.print(prefix + body, highlight=False)
            exc_info = record.exc_info
            if exc_info and exc_info[0] and exc_info[1] and not record.exc_text:
                console.print(
                    Traceback.from_exception(
                        exc_info[0],
                        exc_info[1],
                        exc_info[2],
                        max_frames=12,
                    )
                )
        except Exception:
            self.handleError(record)

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
        if log_type == "redemption_scan":
            self._render_redemption_scan(record, ts)
            return True
        if log_type == "redemption_result":
            self._render_redemption_result(record, ts)
            return True
        return False

    def _render_api_error(self, record: LogRecord, ts: str) -> None:
        side = getattr(record, "side", "?")
        error_detail = getattr(record, "error_detail", None)
        error = error_detail or getattr(record, "error", "")
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

        console.print()
        console.print(Panel(
            "\n".join(lines),
            title=f"❌ [bold red]ORDER REJECTED[/bold red]  [muted][{ts}][/muted]",
            border_style="red",
            padding=(1, 2),
        ))

    def _render_api_response(self, record: LogRecord, ts: str) -> None:
        method = getattr(record, "httpMethod", "?")
        url = getattr(record, "url", "")
        status = getattr(record, "httpStatus", 0)
        data_summary = getattr(record, "data_summary", None)
        error = getattr(record, "error", None)
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
                lines.append(f"  [dim]{k}:[/dim] {v}")
        if error:
            lines.append(f"  [dim]error:[/dim] {error}")

        console.print()
        console.print(Panel(
            "\n".join(lines),
            title=f"{icon} [bold {border}]API {status_label} {method}[/bold {border}]  [muted][{ts}][/muted]",
            border_style=border,
            padding=(1, 2),
        ))

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
        console.print()
        console.print(Panel(
            "\n".join(lines),
            title=f"🔐 [bold cyan]CLOB CLIENT READY[/bold cyan]  [muted][{ts}][/muted]",
            border_style="cyan",
            padding=(1, 2),
        ))

    def _render_redemption_scan(self, record: LogRecord, ts: str) -> None:
        lines = _build_panel_lines(
            Interval=f"{getattr(record, 'intervalSeconds', 0)}s",
            Redeemables=str(getattr(record, "redeemableCount", 0)),
            Conditions=str(getattr(record, "conditionCount", 0)),
            EstimatedValue=f"${float(getattr(record, 'estimatedValue', 0.0) or 0.0):.2f}",
        )
        console.print()
        console.print(Panel(
            "\n".join(lines),
            title=f"🏦 [bold green]REDEMPTION SCAN[/bold green]  [muted][{ts}][/muted]",
            border_style="green",
            padding=(1, 2),
        ))

    def _render_redemption_result(self, record: LogRecord, ts: str) -> None:
        status = str(getattr(record, "status", "unknown") or "unknown")
        ok = status == "confirmed"
        border = "green" if ok else "red"
        title = "REDEMPTION CONFIRMED" if ok else "REDEMPTION FAILED"
        lines = _build_panel_lines(
            Condition=shorten_addr(getattr(record, "conditionId", None), 10, 6),
            Positions=str(getattr(record, "positionCount", 0)),
            EstimatedValue=f"${float(getattr(record, 'estimatedValue', 0.0) or 0.0):.2f}",
            TxHash=shorten_addr(getattr(record, "txHash", None), 10, 6),
            Error=getattr(record, "error", None),
        )
        console.print()
        console.print(Panel(
            "\n".join(lines),
            title=f"{'✅' if ok else '❌'} [bold {border}]{title}[/bold {border}]  [muted][{ts}][/muted]",
            border_style=border,
            padding=(1, 2),
        ))

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
        console.print(" | ".join(parts))

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
        console.print()
        console.print(Panel(
            "\n".join(lines),
            title=f"🎯 [bold green]TRADE EXECUTED — {side}[/bold green]  [muted][{ts}][/muted]",
            border_style="green",
            padding=(1, 2),
        ))

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
        console.print()
        if is_unconfirmed:
            console.print(Panel(
                "\n".join(lines),
                title=f"⏳ [bold yellow]ORDER UNCONFIRMED — {side}[/bold yellow]  [muted][{ts}][/muted]",
                border_style="yellow",
                padding=(1, 2),
            ))
        else:
            console.print(Panel(
                "\n".join(lines),
                title=f"📤 [bold cyan]ORDER SUBMITTED — {side}[/bold cyan]  [muted][{ts}][/muted]",
                border_style="cyan",
                padding=(1, 2),
            ))

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
        console.print()
        console.print(Panel(
            "\n".join(lines),
            title=f"🛡️ [bold red]RISK EXIT — SELL[/bold red]  [muted][{ts}][/muted]",
            border_style="red",
            padding=(1, 2),
        ))


def _env_level(default: str) -> str:
    return os.getenv("POLYMARKET_LOG_LEVEL", default).upper()


def _env_int(name: str, default: int) -> int:
    try:
        return int(str(os.getenv(name, str(default))).strip())
    except (TypeError, ValueError):
        return default


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
) -> logging.Logger:
    logging.raiseExceptions = False

    if isinstance(level, str):
        resolved_level = getattr(logging, _env_level(level), logging.INFO)
    else:
        resolved_level = level

    log_dir_path = Path(log_dir) if log_dir else _default_log_dir()
    jsonl_override = (os.getenv("APP_LOG_JSONL_FILE") or "").strip()
    jsonl_path = Path(jsonl_override) if jsonl_override else (log_dir_path / f"{APP_NAME}.jsonl")
    max_bytes = _env_int("LOG_MAX_BYTES", DEFAULT_MAX_BYTES)
    backup_count = _env_int("LOG_BACKUP_COUNT", DEFAULT_BACKUP_COUNT)

    logger = logging.getLogger(name)
    logger.setLevel(resolved_level)
    logger.propagate = False
    _remove_managed_handlers(logger)

    ch = RichConsoleHandler()
    ch.setLevel(resolved_level)
    ch.setFormatter(logging.Formatter("%(message)s"))
    setattr(ch, "_pm_bot_managed", True)
    logger.addHandler(ch)

    try:
        log_dir_path.mkdir(parents=True, exist_ok=True)
        file_handler = RotatingFileHandler(
            filename=str(jsonl_path),
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8",
        )
    except Exception:
        file_handler = logging.StreamHandler(sys.stderr)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(JsonFormatter())
    setattr(file_handler, "_pm_bot_managed", True)
    logger.addHandler(file_handler)

    _CONFIGURED_LOGGERS[name] = logger
    return logger


_CONFIGURED_LOGGERS: dict[str, logging.Logger] = {}


def get_logger(name: str | None = None) -> logging.Logger:
    if APP_NAME not in _CONFIGURED_LOGGERS:
        configure_logging()

    app_logger = logging.getLogger(APP_NAME)
    if name and name != APP_NAME:
        return app_logger.getChild(name)
    return app_logger


def format_usdc(value: float | int | None) -> str:
    try:
        amount = float(value or 0.0)
    except (TypeError, ValueError):
        amount = 0.0
    return f"{amount:.2f} USDC"


def tx_link(tx_hash: str | None, chain: str | None = None) -> str:
    if not tx_hash:
        return ""
    chain = (chain or os.getenv("POLYMARKET_CHAIN") or "polygon").lower()
    explorers = {
        "polygon": "https://polygonscan.com/tx/",
        "mumbai": "https://mumbai.polygonscan.com/tx/",
        "ethereum": "https://etherscan.io/tx/",
        "sepolia": "https://sepolia.etherscan.io/tx/",
    }
    return f"{explorers.get(chain, explorers['polygon'])}{tx_hash}"


def event_link(event_slug: str | None) -> str:
    if not event_slug:
        return ""
    return f"https://polymarket.com/event/{event_slug}"


def shorten_addr(addr: str | None, prefix: int = 6, suffix: int = 4) -> str:
    if not addr:
        return "N/A"
    if len(addr) <= prefix + suffix + 3:
        return addr
    return f"{addr[:prefix]}...{addr[-suffix:]}"


def get_wallet_type(clob_client: Any) -> tuple[str, int | None]:
    """
    Safely detect wallet type from CLOB client's signature_type attribute.
    Handles both ClobClient and ClobClientWrapper (which has .client attribute).

    Returns:
        tuple: (wallet_type_name, signature_type_value)
            - wallet_type_name: "EOA", "POLY_PROXY", "GNOSIS_SAFE", or "UNKNOWN"
            - signature_type_value: The raw signature_type value or None if not found
    """
    # Check if this is a wrapper with an inner client
    inner_client = getattr(clob_client, "client", None)

    # Try to get signature_type from inner client first, then from the object itself
    signature_type = getattr(inner_client, "signature_type", None) if inner_client else None
    if signature_type is None:
        signature_type = getattr(clob_client, "signature_type", None)

    if signature_type is None:
        return ("UNKNOWN", None)

    if signature_type == 2:
        return ("GNOSIS_SAFE", signature_type)
    elif signature_type == 1:
        return ("POLY_PROXY", signature_type)
    elif signature_type == 0:
        return ("EOA", signature_type)
    return (f"UNKNOWN({signature_type})", signature_type)

def print_startup_banner() -> None:
    console.print()
    console.print(
        Panel(
            "[bold cyan]Polymarket Copy Trading Bot[/bold cyan]",
            border_style="cyan",
            padding=(0, 2),
        )
    )
    console.print()


def print_status_checks(
    *,
    rpc_ok: bool,
    balance: float,
    balance_warning_threshold: float = 5.0,
    api_ok: bool,
    database_ok: bool,
) -> None:
    rpc_icon = "✅" if rpc_ok else "❌"
    rpc_text = "RPC endpoint responding" if rpc_ok else "RPC endpoint NOT responding"
    console.print(f"ℹ️ RPC:  {rpc_icon}  {rpc_text}")

    if balance < 1:
        bal_icon, bal_label = "🔴", f"Very low balance: ${balance:.2f}"
    elif balance < balance_warning_threshold:
        bal_icon, bal_label = "⚠️", f"Low balance: ${balance:.2f}"
    else:
        bal_icon, bal_label = "✅", f"${balance:.2f}"
    console.print(f"ℹ️ Balance:  {bal_icon}  {bal_label}")

    api_icon = "✅" if api_ok else "❌"
    api_text = "API responding" if api_ok else "API NOT responding"
    console.print(f"ℹ️ Polymarket API:  {api_icon}  {api_text}")
    database_icon = "✅" if database_ok else "❌"
    database_text = "Database ready" if database_ok else "Unable to connect to database"
    console.print(f"ℹ️ Database:  {database_icon}  {database_text}")
    console.print()


def print_clob_client_status(clob_client) -> None:
    console.print("ℹ️ Initializing CLOB client...")
    wallet_type, _ = get_wallet_type(clob_client)
    console.print(f"ℹ️ Wallet type detected: {wallet_type}")
    console.print("✅ CLOB client ready")
    console.print()


def print_service_start(*, monitor: bool = True, executor: bool = True, trader_count: int = 1) -> None:
    if monitor:
        console.print("ℹ️ Starting trade monitor...")
    if executor:
        console.print("ℹ️ Starting trade executor...")
    console.print(f"✅ Trade executor ready for {trader_count} trader(s)")
    console.print()


def print_db_status(traders: dict[str, int] | None = None) -> None:
    console.print("🟠 [bold]Database Status:[/bold]")
    if traders:
        for addr, count in traders.items():
            console.print(f"   {shorten_addr(addr)}: [bold]{count}[/bold] trades")
    else:
        console.print("   No data")
    console.print()


def print_positions(
    *,
    wallet: str,
    available_cash: float,
    total_portfolio: float,
    positions: list[dict] | None = None,
) -> None:
    console.print(Rule("YOUR POSITIONS", style="yellow"))
    console.print(f"   Wallet: {shorten_addr(wallet)}")
    console.print(f"   💰 Available Cash:   [money]${available_cash:.2f}[/money]")
    console.print(f"   💰 Total Portfolio:  [money]${total_portfolio:.2f}[/money]")
    console.print()
    if positions:
        table = Table(show_header=True, header_style="bold", box=None, padding=(0, 2))
        table.add_column("Market")
        table.add_column("Side")
        table.add_column("Size", justify="right")
        table.add_column("Avg Price", justify="right")
        table.add_column("Value", justify="right")
        for p in positions:
            size = p.get("size") if p.get("size") is not None else p.get("size_tokens", 0)
            avg_price = p.get("avgPrice") if p.get("avgPrice") is not None else p.get("avg_entry_price", 0)
            value = p.get("currentValue") if p.get("currentValue") is not None else p.get("value", 0)
            market = p.get("title") or p.get("market") or p.get("condition_id") or "?"
            side = p.get("outcome") or p.get("side") or "?"
            table.add_row(
                str(market)[:40],
                str(side),
                f'{float(size or 0):.2f}',
                f'{float(avg_price or 0):.4f}',
                f'${float(value or 0):.2f}',
            )
        console.print(table)
    else:
        console.print("   No open positions")
    console.print()


def print_traders_copying(traders: list[dict] | None = None) -> None:
    console.print(Rule("TRADERS YOU'RE COPYING", style="blue"))
    if traders:
        for t in traders:
            console.print(f"   📋 {shorten_addr(t.get('address'))}: {t.get('positions', 0)} positions {t.get('leader_balance'):.2f} USDC")
    else:
        console.print("   No traders configured")
    console.print()


def print_monitor_start(trader_count: int, interval_s: int) -> None:
    console.print(f"✅ Monitoring {trader_count} trader(s) every {interval_s}s")
    console.print()


def print_first_run_init() -> None:
    console.print("ℹ️ First run: marking all historical trades as processed...")


def print_historical_done() -> None:
    console.print("Historical trades processed. Now monitoring for new trades only.")
    console.print()


class MonitorHeartbeat:
    def __init__(self, interval: int = MONITOR_HEARTBEAT_INTERVAL):
        self.interval = interval
        self._last_log: float = 0.0
        self._trader_count: int = 0

    def tick(
        self,
        *,
        trader_count: int = 1,
        checked_activities: int = 0,
        leader_wallet: str | None = None,
        monitored_leader_count: int | None = None,
        symbol: str = "🟢",
    ) -> None:
        self._trader_count = trader_count
        now = time.time()
        if now - self._last_log >= self.interval:
            self._last_log = now
            ts = time.strftime("%I:%M:%S %p")
            leader_label = shorten_addr(leader_wallet) if leader_wallet else "multi-leader"
            if monitored_leader_count and monitored_leader_count > 1:
                console.print(
                    f"[muted][{ts}][/muted] {symbol} [heartbeat]Trade Monitor is running and healthy | "
                    f"leaders {monitored_leader_count} total | current {leader_label} | "
                    f"watching {trader_count} trader(s)[/heartbeat]"
                )
            else:
                console.print(
                    f"[muted][{ts}][/muted] {symbol} [heartbeat]Trade Monitor is running and healthy | "
                    f"leader {leader_label} | watching {trader_count} trader(s)[/heartbeat]"
                )
            # Also write structured heartbeat to JSONL
            _logger = get_logger()
            log_monitor_heartbeat(
                _logger,
                interval_s=self.interval,
                checked_activities=checked_activities,
                leader_wallet=leader_wallet,
                monitored_leader_count=monitored_leader_count,
                symbol=symbol,
            )

    def reset(self) -> None:
        """Reset timer so next tick logs immediately after a trade."""
        self._last_log = 0.0


_heartbeat = MonitorHeartbeat()


def monitor_heartbeat_tick(
    *,
    trader_count: int = 1,
    checked_activities: int = 0,
    leader_wallet: str | None = None,
    monitored_leader_count: int | None = None,
    symbol: str = "🟢",
) -> None:
    _heartbeat.tick(
        trader_count=trader_count,
        checked_activities=checked_activities,
        leader_wallet=leader_wallet,
        monitored_leader_count=monitored_leader_count,
        symbol=symbol,
    )


def reset_heartbeat() -> None:
    _heartbeat.reset()


def print_new_trade_detected(trade: dict) -> None:
    addr = shorten_addr(trade.get("proxyWallet") or trade.get("user"))
    side = (trade.get("side") or "?").upper()
    title = trade.get("title") or "N/A"
    submarket = _first_present_text(
        trade,
        "submarket",
        "subMarket",
        "sub_market",
        "marketSubtype",
        "marketType",
        "lineType",
        "betType",
        "groupItemTitle",
        "groupTitle",
    )
    outcome = _first_present_text(
        trade,
        "outcome",
        "outcomeName",
        "outcome_name",
        "tokenOutcome",
        "selection",
        "selectionName",
        "choice",
    )
    usdc = float(trade.get("usdcSize") or 0)
    if usdc <= 0:
        usdc = float(trade.get("size", 0) or 0) * float(trade.get("price", 0) or 0)
    price = float(trade.get("price", 0) or 0)
    event_slug = trade.get("eventSlug") or trade.get("slug") or ""
    tx_hash = trade.get("transactionHash") or ""

    ts = time.strftime("%I:%M:%S %p")
    console.print()
    console.print(Rule(style="cyan"))
    console.print(f"  ⚡ [bold cyan]NEW TRADE DETECTED[/bold cyan]  [muted][{ts}][/muted]")
    console.print(Rule(style="cyan"))
    console.print()

    lines = [
        f"[bold]Trader:[/bold]  {addr}",
        f"[bold]Side:[/bold]    {side}",
        f"[bold]Title:[/bold]   {title}",
        f"[bold]Submarket:[/bold]  {submarket or 'N/A'}",
        f"[bold]Outcome:[/bold]  {outcome or 'N/A'}",
        f"[bold]Amount:[/bold]  [money]${usdc:.2f}[/money]",
        f"[bold]Price:[/bold]   {price:.4f}",
    ]
    agg_count = int(trade.get('_aggregatedCount') or 1)
    if agg_count > 1:
        lines.append(f"[bold]Merged fills:[/bold]  {agg_count}")
    if event_slug:
        url = event_link(event_slug)
        lines.append(f"[bold]Market:[/bold]  [link={url}]{url}[/link]")
    if tx_hash:
        url = tx_link(tx_hash)
        lines.append(f"[bold]TX:[/bold]     [link={url}]{url}[/link]")

    console.print(Panel(
        "\n".join(lines),
        title="📋 [bold yellow]NEW TRADE DETECTED[/bold yellow]",
        border_style="yellow",
        padding=(1, 2),
    ))
    console.print()


def print_trade_queued(trade: dict) -> None:
    addr = shorten_addr(trade.get("proxyWallet") or trade.get("user"))
    console.print(f"  ℹ️ New trade detected for {addr}")


def print_capital_info(
    *,
    my_balance: float,
    leader_balance: float,
    leader_addr: str,
) -> None:
    console.print("Capital (USDC + Positions):")
    console.print(f"   Your total capital:    [money]{format_usdc(my_balance)}[/money]")
    console.print(
        f"   Trader total capital:  [money]{format_usdc(leader_balance)}[/money] ({shorten_addr(leader_addr)})"
    )


def print_executing_strategy(side: str) -> None:
    console.print(f"ℹ️ Executing {side.upper()} strategy...")


def print_balance_update(balance: float) -> None:
    console.print(f"ℹ️ Your balance: [money]${balance:.2f}[/money]")


def print_trade_result(
    *,
    success: bool,
    side: str,
    leader_tx: str | None = None,
    copy_tx: str | None = None,
    amount: float = 0,
    price: float = 0,
    reason: str | None = None,
    title: str | None = None,
    submarket: str | None = None,
    outcome: str | None = None,
) -> None:
    side_style = "trade.buy" if side.upper() == "BUY" else "trade.sell"
    if success:
        lines = _build_panel_lines(
            Side=f"[{side_style}]{side.upper()}[/{side_style}]",
            Amount=f"[money]${amount:.2f}[/money]" if amount > 0 else None,
            Price=f"{price:.4f}" if price > 0 else None,
            Market=(title or "").strip()[:80] or None,
            Submarket=(submarket or "").strip() or None,
            Outcome=(outcome or "").strip() or None,
            LeaderTX=shorten_addr(leader_tx, 8, 6) if leader_tx else None,
            CopyTX=shorten_addr(copy_tx, 8, 6) if copy_tx else None,
        )
        console.print(Panel(
            "\n".join(lines),
            title="✅ [bold green]COPY EXECUTED[/bold green]",
            border_style="green",
            padding=(0, 2),
        ))
    else:
        lines = _build_panel_lines(
            Side=f"[{side_style}]{side.upper()}[/{side_style}]",
            Reason=reason or "Copy not executed",
            Market=(title or "").strip()[:80] or None,
            Submarket=(submarket or "").strip() or None,
            Outcome=(outcome or "").strip() or None,
            LeaderTX=shorten_addr(leader_tx, 8, 6) if leader_tx else None,
            CopyTX=shorten_addr(copy_tx, 8, 6) if copy_tx else None,
        )
        console.print(Panel(
            "\n".join(lines),
            title="⚠️ [bold yellow]COPY NOT EXECUTED[/bold yellow]",
            border_style="yellow",
            padding=(0, 2),
        ))


def print_skip_reason(reason: str, detail: str = "") -> None:
    lines = _build_panel_lines(
        Reason=reason,
        Detail=detail or None,
    )
    console.print(Panel(
        "\n".join(lines),
        title="⚠️ [bold yellow]COPY NOT EXECUTED[/bold yellow]",
        border_style="yellow",
        padding=(0, 2),
    ))


def print_error(msg: str) -> None:
    console.print(f"❌ [error]{msg}[/error]")


def print_warning(msg: str) -> None:
    console.print(f"⚠️ [warning]{msg}[/warning]")


def print_info(msg: str) -> None:
    console.print(f"ℹ️ {msg}")


def print_success(msg: str) -> None:
    console.print(f"✅ [success]{msg}[/success]")


def redact_error_detail(error: Any) -> str:
    if error is None:
        return "unknown_error"

    error_str = str(error).strip()
    if not error_str:
        return "unknown_error"

    parsed = _parse_api_error(error_str)
    status_code = parsed.get("status_code")
    if status_code:
        return _HTTP_ERROR_CATEGORIES.get(status_code, f"http_{status_code}")

    lowered = error_str.lower()
    if "timeout" in lowered:
        return "timeout"
    if "rate limit" in lowered or "too many requests" in lowered:
        return "rate_limited"
    if "connection" in lowered or "network" in lowered:
        return "network_error"
    if "signature" in lowered:
        return "signature_error"
    if "nonce" in lowered:
        return "nonce_error"
    if "insufficient" in lowered and ("fund" in lowered or "balance" in lowered):
        return "insufficient_funds"
    if isinstance(error, BaseException):
        return type(error).__name__
    return "request_failed"


def safe_error_detail(error: Any) -> str:
    """Return error class + message for diagnostics without leaking secrets.

    Unlike ``redact_error_detail`` (which strips DB/runtime messages to just
    the class name), this keeps the first 300 chars of ``str(error)`` so that
    constraint names, table names, and similar non-secret context survive into
    the structured log.

    When the error message is blank (e.g. IntegrityError with empty args),
    the traceback is appended so the originating line is still visible.
    """
    import traceback as _tb

    if error is None:
        return "unknown_error"
    cls = type(error).__name__ if isinstance(error, BaseException) else "Error"
    msg = str(error).strip()
    if not msg and isinstance(error, BaseException):
        # Blank message — include args repr and last 3 traceback frames
        frames = _tb.format_exception(type(error), error, error.__traceback__)
        tail = "".join(frames[-3:]) if len(frames) > 3 else "".join(frames)
        tail = tail.strip()
        args_repr = repr(error.args) if error.args else "()"
        detail = f"{cls} args={args_repr}"
        if tail:
            detail += f" | tb: {tail}"
        if len(detail) > 600:
            detail = detail[:600] + "…"
        return detail
    if not msg:
        return cls
    if len(msg) > 300:
        msg = msg[:300] + "…"
    return f"{cls}: {msg}"


def _response_shape(data: Any) -> str | None:
    if data is None:
        return None
    if isinstance(data, list):
        return f"list[{len(data)}]"
    if isinstance(data, dict):
        return f"dict[{len(data)}]"
    return type(data).__name__


def _parse_api_error(error_str: str) -> dict[str, str]:
    result: dict[str, str] = {"raw": str(error_str)}

    m = re.search(r"status_code=(\d+)", str(error_str))
    if m:
        result["status_code"] = m.group(1)

    m2 = re.search(r"error_message=(\{.*\})", str(error_str))
    if m2:
        try:
            inner = json.loads(m2.group(1).replace("'", '"'))
            result["error_message"] = inner.get("error", str(inner))
        except (json.JSONDecodeError, AttributeError):
            result["error_message"] = m2.group(1)
    elif "'error':" in str(error_str):
        m3 = re.search(r"'error':\s*'([^']+)'", str(error_str))
        if m3:
            result["error_message"] = m3.group(1)

    return result


def _build_panel_lines(**fields: Any) -> list[str]:
    lines: list[str] = []
    for label, value in fields.items():
        if value is not None:
            lines.append(f"[bold]{label}:[/bold]  {value}")
    return lines


def _first_present_text(payload: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = payload.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def print_api_error(
    *,
    side: str,
    error: str | dict | None = None,
    condition_id: str | None = None,
    asset: str | None = None,
    retry: int = 0,
    max_retries: int = 3,
) -> None:
    error_str = str(error) if error else "Unknown error"
    parsed = _parse_api_error(error_str)

    lines = _build_panel_lines(
        Side=side.upper(),
        Status=f"HTTP {parsed['status_code']}" if parsed.get("status_code") else None,
        Message=parsed.get("error_message") or error_str,
        Category=redact_error_detail(error),
        Condition=shorten_addr(condition_id, 8, 6) if condition_id else None,
        Asset=shorten_addr(asset, 8, 6) if asset else None,
        Retries=f"{retry}/{max_retries}" if retry > 0 else None,
    )

    console.print()
    console.print(Panel(
        "\n".join(lines),
        title="❌ [bold red]ORDER REJECTED[/bold red]",
        border_style="red",
        padding=(1, 2),
    ))


def print_api_response(
    *,
    side: str,
    response: dict | None = None,
    condition_id: str | None = None,
) -> None:
    if not response or response.get("success", False):
        return

    error_raw = response.get("error", "")
    parsed = _parse_api_error(str(error_raw))

    lines = _build_panel_lines(
        Side=side.upper(),
        Success="[bold red]False[/bold red]",
        Status=f"HTTP {parsed['status_code']}" if parsed.get("status_code") else None,
        Message=parsed.get("error_message") or (error_raw if error_raw else None),
        Category=redact_error_detail(error_raw) if error_raw else None,
        Condition=shorten_addr(condition_id, 8, 6) if condition_id else None,
    )

    console.print(Panel(
        "\n".join(lines),
        title=f"📡 [bold yellow]{side.upper()} API RESPONSE[/bold yellow]",
        border_style="yellow",
        padding=(1, 2),
    ))
    console.print()


def print_order_skip(
    *,
    side: str,
    reason: str,
    condition_id: str | None = None,
    asset: str | None = None,
    amount: float | None = None,
    price: float | None = None,
    detail: str = "",
) -> None:
    lines = _build_panel_lines(
        Side=side.upper(),
        Reason=reason,
        Detail=detail or None,
        Condition=shorten_addr(condition_id, 8, 6) if condition_id else None,
        Asset=shorten_addr(asset, 8, 6) if asset else None,
        Amount=f"${amount:.4f}" if amount is not None else None,
        Price=f"{price:.4f}" if price is not None else None,
    )

    console.print()
    console.print(Panel(
        "\n".join(lines),
        title="⚠️ [bold yellow]COPY NOT EXECUTED[/bold yellow]",
        border_style="yellow",
        padding=(1, 2),
    ))
    console.print()


def print_clob_error(
    *,
    operation: str,
    error: str | Exception,
    asset: str | None = None,
    side: str | None = None,
    condition_id: str | None = None,
) -> None:
    error_str = str(error)
    parsed = _parse_api_error(error_str)

    lines = _build_panel_lines(
        Operation=operation,
        Side=side.upper() if side else None,
        Status=f"HTTP {parsed['status_code']}" if parsed.get("status_code") else None,
        Message=parsed.get("error_message") or error_str,
        Category=redact_error_detail(error),
        Asset=shorten_addr(asset, 8, 6) if asset else None,
        Condition=shorten_addr(condition_id, 8, 6) if condition_id else None,
    )

    console.print(Panel(
        "\n".join(lines),
        title="❌ [bold red]CLOB ERROR[/bold red]",
        border_style="red",
        padding=(0, 2),
    ))


def _log_trade(
    logger: logging.Logger,
    *,
    label: str,
    trade_type: str,
    side: str,
    condition_id: str | None,
    event_slug: str | None,
    asset: str | None,
    size: float | None,
    price: float | None,
    usdc_value: float | None,
    tx_hash: str | None = None,
    meta: dict[str, Any] | None = None,
) -> None:
    _f = lambda v, fmt=".4f": format(float(v), fmt) if v is not None else "0"
    parts = [
        f"{label} {side.upper()}",
        f"cond={condition_id}",
        f"asset={asset}",
        f"size={_f(size)}",
        f"price={_f(price)}",
        format_usdc(usdc_value),
    ]
    tx_url = tx_link(tx_hash)
    ev_url = event_link(event_slug)
    if tx_hash:
        parts.append(f"tx={tx_hash}")
    if ev_url:
        parts.append(ev_url)

    extras: dict[str, Any] = {
        "type": trade_type,
        "side": side.upper(),
        "conditionId": condition_id,
        "asset": asset,
        "size": size,
        "price": price,
        "usdcValue": usdc_value,
    }
    if tx_hash:
        extras["txHash"] = tx_hash
        extras["txUrl"] = tx_url
    if ev_url:
        extras["eventUrl"] = ev_url
    if meta:
        extras.update(meta)

    logger.info(" | ".join(parts), extra=extras)


def log_api_error(
    logger: logging.Logger,
    *,
    side: str,
    error: str | dict | None = None,
    condition_id: str | None = None,
    asset: str | None = None,
    retry: int = 0,
    max_retries: int = 3,
    meta: dict[str, Any] | None = None,
) -> None:
    error_str = str(error) if error else "Unknown error"
    redacted_message = redact_error_detail(error)
    payload = {
        "type": "api_error",
        "side": side.upper(),
        "error": redacted_message,
        "error_detail": error_str,
        "conditionId": condition_id,
        "asset": asset,
        "retry": retry,
        "maxRetries": max_retries,
        **(meta or {}),
    }
    logger.error(
        f"API ERROR {side.upper()} | {redacted_message} | cond={condition_id}",
        extra=payload,
    )


def log_trade_execution(
    logger: logging.Logger,
    *,
    side: str,
    condition_id: str | None,
    asset: str | None,
    size: float | None,
    price: float | None,
    usdc_value: float | None,
    tx_hash: str | None = None,
    event_slug: str | None = None,
    meta: dict[str, Any] | None = None,
) -> None:
    _log_trade(
        logger, label="EXECUTED", trade_type="trade_execution",
        side=side, condition_id=condition_id, event_slug=event_slug,
        asset=asset, size=size, price=price, usdc_value=usdc_value,
        tx_hash=tx_hash, meta=meta,
    )


def log_trade_detected(
    logger: logging.Logger,
    *,
    side: str,
    condition_id: str | None,
    asset: str | None,
    size: float | None,
    price: float | None,
    usdc_value: float | None,
    tx_hash: str | None = None,
    event_slug: str | None = None,
    meta: dict[str, Any] | None = None,
) -> None:
    _log_trade(
        logger, label="DETECTED", trade_type="trade_detected",
        side=side, condition_id=condition_id, event_slug=event_slug,
        asset=asset, size=size, price=price, usdc_value=usdc_value,
        tx_hash=tx_hash, meta=meta,
    )


def log_trade_batch_summary(
    logger: logging.Logger,
    *,
    leader_wallet: str,
    trades: list[dict[str, Any]],
) -> None:
    buy_count = 0
    sell_count = 0
    gross_usdc = 0.0
    merged_fills = 0
    for trade in trades:
        side = str(trade.get("side") or "").upper()
        if side == "BUY":
            buy_count += 1
        elif side == "SELL":
            sell_count += 1
        gross_usdc += float(trade.get("usdcSize") or 0)
        merged_fills += max(0, int(trade.get("_aggregatedCount") or 1) - 1)

    logger.info(
        "trade_batch leader=%s trades=%d buy=%d sell=%d gross_usdc=%.2f merged_fills=%d",
        shorten_addr(leader_wallet),
        len(trades),
        buy_count,
        sell_count,
        gross_usdc,
        merged_fills,
        extra={
            "type": "trade_batch_summary",
            "leader": leader_wallet,
            "trades": len(trades),
            "buyCount": buy_count,
            "sellCount": sell_count,
            "grossUsdc": gross_usdc,
            "mergedFills": merged_fills,
        },
    )



def log_trade_submitted(
    logger: logging.Logger,
    *,
    side: str,
    condition_id: str | None,
    asset: str | None,
    size: float | None,
    price: float | None,
    usdc_value: float | None,
    event_slug: str | None = None,
    meta: dict[str, Any] | None = None,
) -> None:
    _log_trade(
        logger, label="SUBMIT", trade_type="trade_submitted",
        side=side, condition_id=condition_id, event_slug=event_slug,
        asset=asset, size=size, price=price, usdc_value=usdc_value,
        meta=meta,
    )


def log_trade_decision(
    logger: logging.Logger,
    *,
    side: str,
    condition_id: str | None,
    asset: str | None,
    decision_score: float,
    allow: bool,
    reason: str,
    event_slug: str | None = None,
    meta: dict[str, Any] | None = None,
) -> None:
    payload: dict[str, Any] = {
        "type": "trade_decision",
        "side": side.upper(),
        "conditionId": condition_id,
        "asset": asset,
        "decisionScore": decision_score,
        "allow": allow,
        "reason": reason,
    }
    ev_url = event_link(event_slug)
    if ev_url:
        payload["eventUrl"] = ev_url
    if meta:
        payload.update(meta)
    logger.info(
        "DECISION %s | side=%s | cond=%s | asset=%s | score=%.4f | reason=%s",
        "ALLOW" if allow else "SKIP",
        side.upper(),
        condition_id,
        asset,
        float(decision_score),
        reason,
        extra=payload,
    )


def log_trade_skip(
    logger: logging.Logger,
    *,
    side: str,
    condition_id: str | None,
    asset: str | None,
    size: float | None,
    price: float | None,
    usdc_value: float | None,
    skip_code: str,
    reason: str,
    status: str = "skipped",
    event_slug: str | None = None,
    meta: dict[str, Any] | None = None,
) -> None:
    _log_trade(
        logger,
        label=status.upper(),
        trade_type="trade_skip",
        side=side,
        condition_id=condition_id,
        event_slug=event_slug,
        asset=asset,
        size=size,
        price=price,
        usdc_value=usdc_value,
        meta={
            "skipCode": skip_code,
            "skipReason": reason,
            "status": status,
            **(meta or {}),
        },
    )


def log_risk_exit(
    logger: logging.Logger,
    *,
    condition_id: str | None,
    asset: str | None,
    size: float | None,
    price: float | None,
    usdc_value: float | None,
    reason: str,
    event_slug: str | None = None,
    meta: dict[str, Any] | None = None,
) -> None:
    _log_trade(
        logger,
        label="RISK_EXIT",
        trade_type="risk_exit",
        side="SELL",
        condition_id=condition_id,
        event_slug=event_slug,
        asset=asset,
        size=size,
        price=price,
        usdc_value=usdc_value,
        meta={"riskReason": reason, **(meta or {})},
    )


def log_monitor_heartbeat(
    logger: logging.Logger,
    *,
    interval_s: int,
    checked_activities: int,
    leader_wallet: str | None = None,
    monitored_leader_count: int | None = None,
    symbol: str = "🟢",
) -> None:
    leader_label = shorten_addr(leader_wallet) if leader_wallet else "multi-leader"
    leader_summary = (
        f"leaders={monitored_leader_count} total | current={leader_label}"
        if monitored_leader_count and monitored_leader_count > 1
        else f"leader={leader_label}"
    )
    logger.info(
        f"{symbol} Trade Monitor is running and healthy | {leader_summary} | "
        f"no new trades in last {interval_s}s | checked={checked_activities}",
        extra={
            "type": "monitor_heartbeat",
            "intervalSeconds": interval_s,
            "checkedActivities": checked_activities,
            "leaderWallet": leader_wallet,
            "monitoredLeaderCount": monitored_leader_count,
            "status": "healthy",
            "symbol": symbol,
        },
    )


def log_api_response(
    logger: logging.Logger,
    *,
    method: str,
    url: str,
    status: int,
    data: Any = None,
    error: str | None = None,
    meta: dict[str, Any] | None = None,
) -> None:
    """Write a structured JSONL entry for every external API response without raw payloads."""
    response_shape = _response_shape(data)

    ok = 200 <= status < 400
    msg = f"API {method} {status} {'OK' if ok else 'ERR'} {url}"

    extras: dict[str, Any] = {
        "type": "api_response",
        "httpMethod": method,
        "url": url,
        "httpStatus": status,
    }
    if response_shape is not None:
        extras["response_shape"] = response_shape

    # Restore rich data_summary for safe fields (v5 behavior)
    _SAFE_DATA_FIELDS = (
        "success", "orderID", "status", "error", "price", "size",
        "min_order_size", "transactionHash",
    )
    if isinstance(data, dict):
        summary = {k: data[k] for k in _SAFE_DATA_FIELDS if k in data}
        if summary:
            extras["data_summary"] = summary

    if error:
        extras["error"] = redact_error_detail(error)
        extras["error_detail"] = error
    if meta:
        extras.update(meta)

    if ok:
        logger.info(msg, extra=extras)
    else:
        logger.warning(msg, extra=extras)
