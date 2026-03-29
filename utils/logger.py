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
from rich.theme import Theme

APP_NAME = "PolymarketCopyTradingBot"
LOG_SUBDIR = "log"
MAX_BYTES = 5 * 1024 * 1024
BACKUP_COUNT = 3
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


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _default_log_dir() -> Path:
    return _project_root() / LOG_SUBDIR


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
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


class RichConsoleHandler(logging.Handler):
    """Console handler that suppresses JSONL-only log types (api_response, monitor_heartbeat)."""

    # Log types that should only go to JSONL file, not console
    _JSONL_ONLY_TYPES = frozenset({"api_response", "monitor_heartbeat"})

    def emit(self, record: LogRecord) -> None:
        try:
            # Skip JSONL-only records from console output
            if getattr(record, "type", None) in self._JSONL_ONLY_TYPES:
                return
            msg = self.format(record)
            ts = time.strftime("%I:%M:%S %p")
            style = _LEVEL_STYLES.get(record.levelname, "info")
            prefix = Text(f"[{ts}] ", style="muted")
            body = Text(msg, style=style)
            console.print(prefix + body, highlight=False)
            if record.exc_info and not record.exc_text:
                console.print_exception(show_locals=False)
        except Exception:
            self.handleError(record)


def _env_level(default: str) -> str:
    return os.getenv("POLYMARKET_LOG_LEVEL", default).upper()


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
    jsonl_path = log_dir_path / f"{APP_NAME}.jsonl"

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
            maxBytes=MAX_BYTES,
            backupCount=BACKUP_COUNT,
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
            table.add_row(
                p.get("title", "?")[:40],
                p.get("outcome", "?"),
                f'{float(p.get("size", 0)):.2f}',
                f'{float(p.get("avgPrice", 0)):.4f}',
                f'${float(p.get("currentValue", 0)):.2f}',
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

    def tick(self, *, trader_count: int = 1, checked_activities: int = 0) -> None:
        self._trader_count = trader_count
        now = time.time()
        if now - self._last_log >= self.interval:
            self._last_log = now
            ts = time.strftime("%I:%M:%S %p")
            console.print(
                f"[muted][{ts}][/muted] ✅ [heartbeat]Monitor running — watching "
                f"{trader_count} trader(s)[/heartbeat]"
            )
            # Also write structured heartbeat to JSONL
            _logger = get_logger()
            log_monitor_heartbeat(
                _logger,
                interval_s=self.interval,
                checked_activities=checked_activities,
            )

    def reset(self) -> None:
        """Reset timer so next tick logs immediately after a trade."""
        self._last_log = 0.0


_heartbeat = MonitorHeartbeat()


def monitor_heartbeat_tick(*, trader_count: int = 1, checked_activities: int = 0) -> None:
    _heartbeat.tick(trader_count=trader_count, checked_activities=checked_activities)


def reset_heartbeat() -> None:
    _heartbeat.reset()


def print_new_trade_detected(trade: dict) -> None:
    addr = shorten_addr(trade.get("proxyWallet") or trade.get("user"))
    side = (trade.get("side") or "?").upper()
    asset = trade.get("asset") or trade.get("tokenID") or "?"
    title = trade.get("title") or "N/A"
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
) -> None:
    if success:
        parts = [f"{side.upper()} OK"]
        if leader_tx:
            parts.append(f"leader: {shorten_addr(leader_tx, 8, 6)}")
        if copy_tx:
            parts.append(f"copy: {shorten_addr(copy_tx, 8, 6)}")
        if amount > 0:
            parts.append(f"${amount:.2f} @ {price:.4f}")
        console.print(f"✅ [trade.executed]{' | '.join(parts)}[/trade.executed]")
    else:
        msg = "Copy not executed"
        if reason:
            msg += f" — {reason}"
        console.print(f"❌ [error]{msg}[/error]")


def print_skip_reason(reason: str, detail: str = "") -> None:
    msg = f"⚠️ Copy not executed. Reason: {reason}"
    if detail:
        msg += f" | {detail}"
    console.print(f"[warning]{msg}[/warning]")


def print_error(msg: str) -> None:
    console.print(f"❌ [error]{msg}[/error]")


def print_warning(msg: str) -> None:
    console.print(f"⚠️ [warning]{msg}[/warning]")


def print_info(msg: str) -> None:
    console.print(f"ℹ️ {msg}")


def print_success(msg: str) -> None:
    console.print(f"✅ [success]{msg}[/success]")


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
) -> None:
    logger.info(
        f"Monitoring heartbeat: running (no new trades in last {interval_s}s) | "
        f"Checked: {checked_activities}",
        extra={
            "type": "monitor_heartbeat",
            "intervalSeconds": interval_s,
            "checkedActivities": checked_activities,
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
    """Write a structured JSONL entry for every external API response."""
    data_summary: Any = None
    if isinstance(data, list):
        data_summary = f"list[{len(data)}]"
    elif isinstance(data, dict):
        data_summary = {k: v for k, v in data.items() if k in (
            "success", "orderID", "status", "error", "price", "size",
            "min_order_size", "transactionHash",
        )}
    else:
        data_summary = str(data)[:200] if data is not None else None

    ok = 200 <= status < 400
    msg = f"API {method} {status} {'OK' if ok else 'ERR'} {url}"

    extras: dict[str, Any] = {
        "type": "api_response",
        "httpMethod": method,
        "url": url,
        "httpStatus": status,
    }
    if data_summary is not None:
        extras["response"] = data_summary
    if error:
        extras["error"] = error
    if meta:
        extras.update(meta)

    if ok:
        logger.info(msg, extra=extras)
    else:
        logger.warning(msg, extra=extras)

