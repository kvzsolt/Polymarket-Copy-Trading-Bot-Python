from __future__ import annotations

from dataclasses import asdict
from typing import Any, Iterable
from urllib.parse import urlparse

from rich.panel import Panel
from rich.rule import Rule

from orchestrators.startup_health import StartupHealthReport
from utils.logger import console, get_logger, shorten_addr

logger = get_logger("RuntimeSummary")


def _status_icon(status: str) -> str:
    return {
        "ok": "✅",
        "warning": "⚠️",
        "error": "❌",
    }.get(str(status).lower(), "ℹ️")


def _format_number(value: float) -> str:
    return f"{value:.4f}".rstrip("0").rstrip(".")


def _mask_url(url: str) -> str:
    value = str(url or "").strip()
    if not value:
        return "disabled"
    parsed = urlparse(value)
    host = parsed.netloc or parsed.path or "private-endpoint"
    return f"{parsed.scheme or 'https'}://{host}/..."


def _looks_sensitive(key: str) -> bool:
    lowered = str(key or "").lower()
    return any(token in lowered for token in ("private", "secret", "token", "api_key", "password"))


def _render_value(section: str, key: str, value: Any) -> str:
    if value is None or value == "":
        return "disabled"
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, float):
        return _format_number(value)
    if isinstance(value, list):
        if key == "wallets":
            return f"{len(value)} configured (hidden)"
        rendered = [str(item) for item in value]
        if len(rendered) > 6:
            rendered = [*rendered[:6], f"+{len(value) - 6} more"]
        return ", ".join(rendered)
    if _looks_sensitive(key):
        return "<redacted>"
    if key == "rpc_url":
        return _mask_url(str(value))
    if key.endswith("_address") or key in {"wallet", "funder", "user_id"}:
        return shorten_addr(str(value), 8, 4)
    if section == "leader" and key == "wallets":
        return "hidden"
    if key == "multi_user_config_file":
        return str(value) if str(value).strip() else "disabled"
    return str(value)


def _format_mapping(section: str, mapping: dict[str, Any]) -> str:
    lines: list[str] = []
    for key, value in mapping.items():
        rendered = _render_value(section, key, value)
        lines.append(f"• {key}: {rendered}")
    return "\n".join(lines)


def _position_symbol(position: dict[str, Any]) -> str:
    symbol = (
        position.get("symbol")
        or position.get("ticker")
        or position.get("slug")
        or position.get("outcome")
        or position.get("asset")
        or position.get("tokenID")
        or position.get("token_id")
        or "?"
    )
    return str(symbol)


def _sample(values: Iterable[str], *, limit: int = 6, shorten: bool = False) -> str:
    rendered: list[str] = []
    for value in values:
        text = str(value)
        rendered.append(shorten_addr(text, 10, 6) if shorten else text)
        if len(rendered) >= limit:
            break
    return ", ".join(rendered)


def print_startup_health_check(report: StartupHealthReport) -> None:
    lines = []
    for name, item in report.checks.items():
        icon = _status_icon(item.status)
        lines.append(f"{icon} {name}: {item.message}")

    lines.append(
        "📦 position sync: "
        f"live={report.position_sync.live_count} | "
        f"persisted_before={report.position_sync.persisted_before} | "
        f"persisted_after={report.position_sync.persisted_after} | "
        f"pruned={report.position_sync.pruned_positions}"
    )
    lines.append(
        "📬 orders: "
        f"successful={report.order_summary.get('successful', 0)} | "
        f"failed={report.order_summary.get('failed', 0)}"
    )
    if report.order_cleanup.normalized_total > 0:
        lines.append(
            "🧯 startup order cleanup: "
            f"pending={report.order_cleanup.normalized_pending} | "
            f"delayed={report.order_cleanup.normalized_delayed} | "
            f"retry_pending={report.order_cleanup.normalized_retry_pending}"
        )
    if report.position_sync.pruned_condition_ids:
        lines.append(
            "🧹 pruned conditions: "
            f"{_sample(report.position_sync.pruned_condition_ids, shorten=True)}"
        )
    if report.position_sync.pruned_token_ids:
        lines.append(
            "🏷️ pruned symbols: "
            f"{_sample(report.position_sync.pruned_token_ids, shorten=True)}"
        )

    console.print(
        Panel(
            "\n".join(lines),
            title=f"🏥 [bold]STARTUP HEALTH CHECK[/bold] [{report.status}]",
            border_style="green" if report.status == "ok" else ("yellow" if report.status == "warning" else "red"),
            padding=(1, 2),
        )
    )
    console.print()


def print_startup_summary(
    *,
    wallet: str,
    available_cash: float,
    total_portfolio: float,
    positions: list[dict],
    active_leaders: list[str],
    config,
    health_report: StartupHealthReport,
) -> None:
    print_startup_health_check(health_report)

    console.print(Rule("STARTUP SUMMARY", style="cyan"))
    console.print(
        Panel(
            "\n".join(
                [
                    f"💼 wallet: {shorten_addr(wallet, 8, 4)}",
                    f"💵 cash: ${available_cash:.2f}",
                    f"🧾 total: ${total_portfolio:.2f}",
                    f"📂 open_positions: {len(positions)}",
                ]
            ),
            title="📊 [bold]PORTFOLIO[/bold]",
            border_style="cyan",
            padding=(1, 2),
        )
    )

    if positions:
        position_lines: list[str] = []
        for position in positions[:8]:
            market = str(position.get("title") or position.get("market") or position.get("conditionId") or position.get("condition_id") or "?")
            symbol = _position_symbol(position)
            side = str(position.get("outcome") or position.get("side") or "?")
            size = float(position.get("size") or position.get("size_tokens") or 0.0)
            value = float(position.get("currentValue") or position.get("value") or 0.0)
            position_lines.append(
                f"• {market[:72]}\n"
                f"  ↳ symbol={symbol} | side={side} | size={size:.2f} | value=${value:.2f}"
            )
        remaining = len(positions) - 8
        if remaining > 0:
            position_lines.append(f"• ... and {remaining} more position(s)")
    else:
        position_lines = ["• No open positions"]

    console.print(
        Panel(
            "\n".join(position_lines),
            title="📦 [bold]POSITIONS[/bold]",
            border_style="blue",
            padding=(1, 2),
        )
    )

    console.print(
        Panel(
            "\n".join(
                [
                    f"👥 active leaders: {len(active_leaders)}",
                    "🕶️ wallets: hidden",
                ]
            ),
            title="🧠 [bold]LEADER ENGINE[/bold]",
            border_style="magenta",
            padding=(1, 2),
        )
    )
    console.print()

    engine_sections = [
        ("logging", asdict(config.logging)),
        ("polling", asdict(config.polling)),
        ("sizing", asdict(config.sizing)),
        ("risk", asdict(config.risk)),
        ("execution", asdict(config.execution)),
        (
            "leader",
            {
                **asdict(config.leader),
                "wallets": f"{len(config.leader.wallets)} configured (hidden)",
            },
        ),
        ("discovery", asdict(config.discovery)),
        (
            "redemption",
            {
                **asdict(config.redemption),
                "redeem_check_interval": config.polling.redeem_check_interval,
            },
        ),
        ("features", asdict(config.features)),
        ("network", asdict(config.network)),
    ]

    for section_name, section_data in engine_sections:
        console.print(
            Panel(
                _format_mapping(section_name, section_data),
                title=f"⚙️ [bold]{section_name.upper()}[/bold]",
                border_style="blue",
                padding=(0, 1),
            )
        )

    sanitized_engine_config = {
        name: {
            key: _render_value(name, key, value)
            for key, value in data.items()
        }
        for name, data in engine_sections
    }
    sanitized_positions = [
        {
            "title": str(position.get("title") or position.get("market") or ""),
            "symbol": _position_symbol(position),
            "outcome": str(position.get("outcome") or position.get("side") or ""),
            "size": float(position.get("size") or position.get("size_tokens") or 0.0),
            "value": float(position.get("currentValue") or position.get("value") or 0.0),
        }
        for position in positions[:12]
    ]

    logger.info(
        (
            f"STARTUP SUMMARY | wallet={shorten_addr(wallet, 8, 4)} | leaders={len(active_leaders)} | "
            f"cash={available_cash:.2f} | total={total_portfolio:.2f} | open_positions={len(positions)}"
        ),
        extra={
            "type": "startup_summary",
            "wallet": shorten_addr(wallet, 8, 4),
            "activeLeadersCount": len(active_leaders),
            "availableCash": available_cash,
            "totalPortfolio": total_portfolio,
            "openPositionCount": len(positions),
            "livePositions": sanitized_positions,
            "engineConfig": sanitized_engine_config,
            "health": health_report.as_log_payload(),
        },
    )
    console.print()
