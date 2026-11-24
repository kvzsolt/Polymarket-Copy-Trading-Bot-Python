"""Operator-facing Rich console printers, error redactors, and MonitorHeartbeat."""

from __future__ import annotations

import json
import logging
import re
import time
from typing import Any

from rich.markup import escape as _escape_markup
from rich.panel import Panel
from rich.rule import Rule
from rich.table import Table

from pmcopy.utils.log_redaction import (
    redact_text,
)
from pmcopy.utils.logging.console_handler import get_logger
from pmcopy.utils.logging.formatters import (
    MONITOR_HEARTBEAT_INTERVAL,
    _HTTP_ERROR_CATEGORIES,
    _active_console,
    _build_panel_lines,
    shorten_addr,
)


def format_usdc(value: float | int | None) -> str:
    """Format an exact USDC amount for operator output."""
    try:
        amount = float(value or 0.0)
    except (TypeError, ValueError):
        amount = 0.0
    return f"{amount:.2f} USDC"


def tx_link(tx_hash: str | None, chain: str | None = None) -> str:
    """Return the configured explorer link for a transaction."""
    if not tx_hash:
        return ""
    chain = (chain or "polygon").lower()
    explorers = {
        "polygon": "https://polygonscan.com/tx/",
        "mumbai": "https://mumbai.polygonscan.com/tx/",
        "ethereum": "https://etherscan.io/tx/",
        "sepolia": "https://sepolia.etherscan.io/tx/",
    }
    return f"{explorers.get(chain, explorers['polygon'])}{tx_hash}"


def event_link(event_slug: str | None) -> str:
    """Return the configured Polymarket event link."""
    if not event_slug:
        return ""
    return f"https://polymarket.com/event/{event_slug}"


def get_wallet_type(clob_client: Any) -> tuple[str, int | None]:
    """
    Safely detect wallet type from CLOB client's signature_type attribute.
    Handles both ClobClient and ClobClientWrapper (which has .client attribute).

    Returns:
        tuple: (wallet_type_name, signature_type_value)
            - wallet_type_name: "EOA", "POLY_PROXY", "GNOSIS_SAFE",
              "DEPOSIT_WALLET", or "UNKNOWN"
            - signature_type_value: The raw signature_type value or None if not found
    """
    # Check if this is a wrapper with an inner client
    inner_client = getattr(clob_client, "client", None)

    # The unified SDK's public wallet_type is authoritative over legacy numeric
    # configuration hints. Check the inner SDK client first, then a facade.
    wallet_type = getattr(inner_client, "wallet_type", None) if inner_client else None
    if wallet_type is None:
        wallet_type = getattr(clob_client, "wallet_type", None)
    if wallet_type is not None:
        normalized = str(wallet_type).strip().upper()
        signature_by_wallet = {
            "EOA": 0,
            "POLY_PROXY": 1,
            "GNOSIS_SAFE": 2,
            "DEPOSIT_WALLET": 3,
        }
        mapped = signature_by_wallet.get(normalized)
        if mapped is not None:
            return (normalized, mapped)
        return (f"UNKNOWN({normalized})", None)

    # Legacy fallback for clients that expose only a numeric signature type.
    signature_type = getattr(inner_client, "signature_type", None) if inner_client else None
    if signature_type is None:
        signature_type = getattr(clob_client, "signature_type", None)

    if signature_type is None:
        return ("UNKNOWN", None)

    if signature_type == 3:
        return ("DEPOSIT_WALLET", signature_type)
    elif signature_type == 2:
        return ("GNOSIS_SAFE", signature_type)
    elif signature_type == 1:
        return ("POLY_PROXY", signature_type)
    elif signature_type == 0:
        return ("EOA", signature_type)
    return (f"UNKNOWN({signature_type})", signature_type)


def print_startup_banner() -> None:
    """Render startup banner."""
    _active_console().print()
    _active_console().print(
        Panel(
            "[bold cyan]Polymarket Copy Trading Bot[/bold cyan]",
            border_style="cyan",
            padding=(0, 2),
        )
    )
    _active_console().print()


def print_status_checks(
    *,
    rpc_ok: bool,
    balance: float,
    balance_warning_threshold: float = 5.0,
    api_ok: bool,
    database_ok: bool,
) -> None:
    """Render status checks."""
    rpc_icon = "✅" if rpc_ok else "❌"
    rpc_text = "RPC endpoint responding" if rpc_ok else "RPC endpoint NOT responding"
    _active_console().print(f"ℹ️ RPC:  {rpc_icon}  {rpc_text}")

    if balance < 1:
        bal_icon, bal_label = "🔴", f"Very low balance: ${balance:.2f}"
    elif balance < balance_warning_threshold:
        bal_icon, bal_label = "⚠️", f"Low balance: ${balance:.2f}"
    else:
        bal_icon, bal_label = "✅", f"${balance:.2f}"
    _active_console().print(f"ℹ️ Balance:  {bal_icon}  {bal_label}")

    api_icon = "✅" if api_ok else "❌"
    api_text = "API responding" if api_ok else "API NOT responding"
    _active_console().print(f"ℹ️ Polymarket API:  {api_icon}  {api_text}")
    database_icon = "✅" if database_ok else "❌"
    database_text = "Database ready" if database_ok else "Unable to connect to database"
    _active_console().print(f"ℹ️ Database:  {database_icon}  {database_text}")
    _active_console().print()


def print_clob_client_status(clob_client) -> None:
    """Render clob client status."""
    _active_console().print("ℹ️ Initializing CLOB client...")
    wallet_type, _ = get_wallet_type(clob_client)
    _active_console().print(f"ℹ️ Wallet type detected: {wallet_type}")
    _active_console().print("✅ CLOB client ready")
    _active_console().print()


def print_service_start(
    *, monitor: bool = True, executor: bool = True, trader_count: int = 1
) -> None:
    """Render service start."""
    if monitor:
        _active_console().print("ℹ️ Starting trade monitor...")
    if executor:
        _active_console().print("ℹ️ Starting trade executor...")
    _active_console().print(f"✅ Trade executor ready for {trader_count} trader(s)")
    _active_console().print()


def print_db_status(traders: dict[str, int] | None = None) -> None:
    """Render db status."""
    _active_console().print("🟠 [bold]Database Status:[/bold]")
    if traders:
        for addr, count in traders.items():
            _active_console().print(f"   {shorten_addr(addr)}: [bold]{count}[/bold] trades")
    else:
        _active_console().print("   No data")
    _active_console().print()


def print_positions(
    *,
    wallet: str,
    available_cash: float,
    total_portfolio: float,
    positions: list[dict] | None = None,
) -> None:
    """Render positions."""
    _active_console().print(Rule("YOUR POSITIONS", style="yellow"))
    _active_console().print(f"   Wallet: {shorten_addr(wallet)}")
    _active_console().print(f"   💰 Available Cash:   [money]${available_cash:.2f}[/money]")
    _active_console().print(f"   💰 Total Portfolio:  [money]${total_portfolio:.2f}[/money]")
    _active_console().print()
    if positions:
        table = Table(show_header=True, header_style="bold", box=None, padding=(0, 2))
        table.add_column("Market")
        table.add_column("Side")
        table.add_column("Size", justify="right")
        table.add_column("Avg Price", justify="right")
        table.add_column("Value", justify="right")
        for p in positions:
            size = p.get("size") if p.get("size") is not None else p.get("size_tokens", 0)
            avg_price = (
                p.get("avgPrice") if p.get("avgPrice") is not None else p.get("avg_entry_price", 0)
            )
            value = (
                p.get("currentValue") if p.get("currentValue") is not None else p.get("value", 0)
            )
            market = p.get("title") or p.get("market") or p.get("condition_id") or "?"
            side = p.get("outcome") or p.get("side") or "?"
            table.add_row(
                str(market)[:40],
                str(side),
                f"{float(size or 0):.2f}",
                f"{float(avg_price or 0):.4f}",
                f"${float(value or 0):.2f}",
            )
        _active_console().print(table)
    else:
        _active_console().print("   No open positions")
    _active_console().print()


def print_traders_copying(traders: list[dict] | None = None) -> None:
    """Render traders copying."""
    _active_console().print(Rule("TRADERS YOU'RE COPYING", style="blue"))
    if traders:
        for t in traders:
            _active_console().print(
                f"   📋 {shorten_addr(t.get('address'))}: {t.get('positions', 0)} positions "
                f"{t.get('leader_balance'):.2f} USDC"
            )
    else:
        _active_console().print("   No traders configured")
    _active_console().print()


def print_monitor_start(trader_count: int, interval_s: int) -> None:
    """Render monitor start."""
    _active_console().print(f"✅ Monitoring {trader_count} trader(s) every {interval_s}s")
    _active_console().print()


def print_first_run_init() -> None:
    """Render first run init."""
    _active_console().print("ℹ️ First run: marking all historical trades as processed...")


def print_historical_done() -> None:
    """Render historical done."""
    _active_console().print("Historical trades processed. Now monitoring for new trades only.")
    _active_console().print()


class MonitorHeartbeat:
    """Track bounded operator heartbeat rendering state."""

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
        """Advance heartbeat state and report whether output is due."""
        self._trader_count = trader_count
        now = time.time()
        if now - self._last_log >= self.interval:
            self._last_log = now
            ts = time.strftime("%I:%M:%S %p")
            leader_label = shorten_addr(leader_wallet) if leader_wallet else "multi-leader"
            if monitored_leader_count and monitored_leader_count > 1:
                _active_console().print(
                    f"[muted][{ts}][/muted] {symbol} [heartbeat]Trade Monitor is running and healthy | "
                    f"leaders {monitored_leader_count} total | current {leader_label} | "
                    f"watching {trader_count} trader(s)[/heartbeat]"
                )
            else:
                _active_console().print(
                    f"[muted][{ts}][/muted] {symbol} [heartbeat]Trade Monitor is running and healthy | "
                    f"leader {leader_label} | watching {trader_count} trader(s)[/heartbeat]"
                )
            # Also write structured heartbeat to JSONL
            from pmcopy.utils.logging.trade_events import log_monitor_heartbeat

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
    """Advance the shared monitor heartbeat."""
    _heartbeat.tick(
        trader_count=trader_count,
        checked_activities=checked_activities,
        leader_wallet=leader_wallet,
        monitored_leader_count=monitored_leader_count,
        symbol=symbol,
    )


def reset_heartbeat() -> None:
    """Reset heartbeat."""
    _heartbeat.reset()


def print_new_trade_detected(trade: dict) -> None:
    """Render new trade detected."""
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
    _active_console().print()
    _active_console().print(Rule(style="cyan"))
    _active_console().print(f"  ⚡ [bold cyan]NEW TRADE DETECTED[/bold cyan]  [muted][{ts}][/muted]")
    _active_console().print(Rule(style="cyan"))
    _active_console().print()

    # P1-l: title / submarket / outcome / trader are leader-controlled; escape
    # Rich markup so an attacker-named market cannot inject console styling or
    # a clickable [link=…] phishing target into the operator terminal. The
    # JSONL file sink below is json.dumps-safe and keeps the raw values.
    lines = [
        f"[bold]Trader:[/bold]  {_escape_markup(str(addr))}",
        f"[bold]Side:[/bold]    {side}",
        f"[bold]Title:[/bold]   {_escape_markup(str(title))}",
        f"[bold]Submarket:[/bold]  {_escape_markup(str(submarket or 'N/A'))}",
        f"[bold]Outcome:[/bold]  {_escape_markup(str(outcome or 'N/A'))}",
        f"[bold]Amount:[/bold]  [money]${usdc:.2f}[/money]",
        f"[bold]Price:[/bold]   {price:.4f}",
    ]
    agg_count = int(trade.get("_aggregatedCount") or 1)
    if agg_count > 1:
        lines.append(f"[bold]Merged fills:[/bold]  {agg_count}")
    if event_slug:
        url = event_link(event_slug)
        lines.append(f"[bold]Market:[/bold]  [link={url}]{url}[/link]")
    if tx_hash:
        url = tx_link(tx_hash)
        lines.append(f"[bold]TX:[/bold]     [link={url}]{url}[/link]")

    _active_console().print(
        Panel(
            "\n".join(lines),
            title="📋 [bold yellow]NEW TRADE DETECTED[/bold yellow]",
            border_style="yellow",
            padding=(1, 2),
        )
    )
    _active_console().print()


def print_trade_queued(trade: dict) -> None:
    """Render trade queued."""
    addr = shorten_addr(trade.get("proxyWallet") or trade.get("user"))
    _active_console().print(f"  ℹ️ New trade detected for {addr}")


def print_capital_info(
    *,
    my_balance: float,
    leader_balance: float,
    leader_addr: str,
) -> None:
    """Render capital info."""
    _active_console().print("Capital (USDC + Positions):")
    _active_console().print(f"   Your total capital:    [money]{format_usdc(my_balance)}[/money]")
    _active_console().print(
        f"   Trader total capital:  [money]{format_usdc(leader_balance)}[/money] ({shorten_addr(leader_addr)})"
    )


def print_executing_strategy(side: str) -> None:
    """Render executing strategy."""
    _active_console().print(f"ℹ️ Executing {side.upper()} strategy...")


def print_balance_update(balance: float) -> None:
    """Render balance update."""
    _active_console().print(f"ℹ️ Your balance: [money]${balance:.2f}[/money]")


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
    """Render trade result."""
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
        _active_console().print(
            Panel(
                "\n".join(lines),
                title="✅ [bold green]COPY EXECUTED[/bold green]",
                border_style="green",
                padding=(0, 2),
            )
        )
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
        _active_console().print(
            Panel(
                "\n".join(lines),
                title="⚠️ [bold yellow]COPY NOT EXECUTED[/bold yellow]",
                border_style="yellow",
                padding=(0, 2),
            )
        )


def print_skip_reason(reason: str, detail: str = "") -> None:
    """Render skip reason."""
    lines = _build_panel_lines(
        Reason=reason,
        Detail=detail or None,
    )
    _active_console().print(
        Panel(
            "\n".join(lines),
            title="⚠️ [bold yellow]COPY NOT EXECUTED[/bold yellow]",
            border_style="yellow",
            padding=(0, 2),
        )
    )


def print_error(msg: str) -> None:
    """Render error."""
    _active_console().print(f"❌ [error]{msg}[/error]")


def print_warning(msg: str) -> None:
    """Render warning."""
    _active_console().print(f"⚠️ [warning]{msg}[/warning]")


def print_info(msg: str) -> None:
    """Render info."""
    _active_console().print(f"ℹ️ {msg}")


def print_success(msg: str) -> None:
    """Render success."""
    _active_console().print(f"✅ [success]{msg}[/success]")


def print_manual_trade_detected(trade: dict) -> None:
    """Rich-panel log for a detected manual (off-bot) trade on the funder wallet.

    Mirrors ``print_new_trade_detected`` shape but with BUY=green/🛒 and
    SELL=red/💵 to surface that the bot did NOT originate this trade.
    """
    side = str(trade.get("side") or "?").upper()
    size = float(trade.get("size") or 0)
    price = float(trade.get("price") or 0)
    usdc = float(trade.get("usdcSize") or 0) or (size * price)
    market = trade.get("title") or trade.get("market") or trade.get("slug") or "Unknown"
    outcome = trade.get("outcome") or ""
    tx_hash = trade.get("transactionHash") or trade.get("tx_hash") or ""
    event_slug = trade.get("eventSlug") or trade.get("slug") or ""

    is_buy = side == "BUY"
    icon = "🛒" if is_buy else "💵"
    border = "green" if is_buy else "red"

    ts = time.strftime("%I:%M:%S %p")
    _active_console().print()
    _active_console().print(Rule(style=border))
    _active_console().print(
        f"  {icon} [bold {border}]MANUAL TRADE DETECTED[/bold {border}] | {side}  [muted][{ts}][/muted]"
    )
    _active_console().print(Rule(style=border))
    _active_console().print()

    # P1-l: market / outcome are leader-controlled — escape Rich markup.
    lines = [
        f"[bold]Market:[/bold]      {_escape_markup(str(market))}",
    ]
    if outcome:
        lines.append(f"[bold]Outcome:[/bold]     {_escape_markup(str(outcome))}")
    lines.extend(
        [
            f"[bold]Side:[/bold]        {side}",
            f"[bold]Size:[/bold]        {size:.4f} tokens",
            f"[bold]Price:[/bold]       {price:.4f}",
            f"[bold]Value:[/bold]       [money]{format_usdc(usdc)}[/money]",
        ]
    )
    if event_slug:
        url = event_link(event_slug)
        lines.append(f"[bold]Event:[/bold]       [link={url}]{url}[/link]")
    if tx_hash:
        url = tx_link(tx_hash)
        lines.append(f"[bold]TX:[/bold]          [link={url}]{url}[/link]")

    _active_console().print(
        Panel(
            "\n".join(lines),
            title=f"{icon} [bold {border}]MANUAL TRADE DETECTED[/bold {border}]",
            border_style=border,
            padding=(1, 2),
        )
    )
    _active_console().print()

    # Structured grep-able line for JSONL sink — mirrors trade_pipeline patterns.
    _file_logger = logging.getLogger("ManualSync")
    _file_logger.info(
        "manual_trade_detected | side=%s size=%.4f price=%.4f usdc=%.2f market=%s outcome=%s tx=%s",
        side,
        size,
        price,
        usdc,
        market,
        outcome or "-",
        tx_hash or "-",
    )


def print_manual_sync_discrepancy(
    market: str, real_size: float, tracked_size: float, action: str
) -> None:
    """Yellow Panel surfacing a venue↔DB size mismatch detected during manual sync."""
    _active_console().print()
    _active_console().print(Rule(style="yellow"))
    _active_console().print("  ⚖️  [bold yellow]SYNCHRONIZER | PARITY DISCREPANCY[/bold yellow]")
    _active_console().print(Rule(style="yellow"))
    _active_console().print()

    lines = [
        f"[bold]Asset:[/bold]       {_escape_markup(str(market))}",  # P1-l: leader-controlled
        f"[bold]Real-World:[/bold]  [success]{real_size:.4f}[/success] shares (proxy wallet)",
        f"[bold]Database:[/bold]    [error]{tracked_size:.4f}[/error] shares (tracked sum)",
        f"[bold]Resolution:[/bold]  [bold cyan]{action}[/bold cyan]",
    ]
    _active_console().print(
        Panel(
            "\n".join(lines),
            title="⚖️ [bold yellow]SYNC DISCREPANCY[/bold yellow]",
            border_style="yellow",
            padding=(0, 2),
        )
    )
    _active_console().print()


def redact_error_detail(error: Any) -> str:
    """Redact error detail."""
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
    if "poly_api_transport" in lowered or "connectionterminated" in lowered:
        return "network_error"
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
    the structured log. The text is passed through the redaction engine
    (registered secret values plus credential URL/header patterns) before the
    truncation bounds are applied, honouring the no-secret-leak promise.

    When the error message is blank (e.g. IntegrityError with empty args),
    the traceback is appended so the originating line is still visible.
    """
    import traceback as _tb

    if error is None:
        return "unknown_error"
    cls = type(error).__name__ if isinstance(error, BaseException) else "Error"
    msg = redact_text(str(error).strip())
    if not msg and isinstance(error, BaseException):
        # Blank message — include args repr and last 3 traceback frames
        frames = _tb.format_exception(type(error), error, error.__traceback__)
        tail = redact_text("".join(frames[-3:]) if len(frames) > 3 else "".join(frames))
        tail = tail.strip()
        args_repr = redact_text(repr(error.args)) if error.args else "()"
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
    """Render api error."""
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

    _active_console().print()
    _active_console().print(
        Panel(
            "\n".join(lines),
            title="❌ [bold red]ORDER REJECTED[/bold red]",
            border_style="red",
            padding=(1, 2),
        )
    )


def print_api_response(
    *,
    side: str,
    response: dict | None = None,
    condition_id: str | None = None,
) -> None:
    """Render api response."""
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

    _active_console().print(
        Panel(
            "\n".join(lines),
            title=f"📡 [bold yellow]{side.upper()} API RESPONSE[/bold yellow]",
            border_style="yellow",
            padding=(1, 2),
        )
    )
    _active_console().print()


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
    """Render order skip."""
    lines = _build_panel_lines(
        Side=side.upper(),
        Reason=reason,
        Detail=detail or None,
        Condition=shorten_addr(condition_id, 8, 6) if condition_id else None,
        Asset=shorten_addr(asset, 8, 6) if asset else None,
        Amount=f"${amount:.4f}" if amount is not None else None,
        Price=f"{price:.4f}" if price is not None else None,
    )

    _active_console().print()
    _active_console().print(
        Panel(
            "\n".join(lines),
            title="⚠️ [bold yellow]COPY NOT EXECUTED[/bold yellow]",
            border_style="yellow",
            padding=(1, 2),
        )
    )
    _active_console().print()


def print_clob_error(
    *,
    operation: str,
    error: str | Exception,
    asset: str | None = None,
    side: str | None = None,
    condition_id: str | None = None,
) -> None:
    """Render clob error."""
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

    _active_console().print(
        Panel(
            "\n".join(lines),
            title="❌ [bold red]CLOB ERROR[/bold red]",
            border_style="red",
            padding=(0, 2),
        )
    )

