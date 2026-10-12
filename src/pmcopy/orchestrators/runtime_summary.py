"""Render sanitized application runtime configuration summaries."""

from __future__ import annotations

from dataclasses import asdict
from decimal import Decimal
from typing import Any, Iterable
from urllib.parse import urlparse

from rich.panel import Panel

from pmcopy.domain.position_lifecycle import PositionLifecycle
from pmcopy.orchestrators.startup_health import StartupHealthReport
from pmcopy.utils.logger import console, get_logger, shorten_addr


def _safe_dsn_host(dsn: str) -> str:
    try:
        parsed = urlparse(dsn)
        host = parsed.hostname or ""
        port = parsed.port or ""
        return f"{host}:{port}" if port else host
    except Exception:
        return "<unparseable>"


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
    if isinstance(value, (list, tuple)):
        rendered = [
            shorten_addr(str(item), 8, 4)
            if key in {"wallets", "active_leaders", "copied_wallets"}
            else str(item)
            for item in value
        ]
        if len(rendered) > 6:
            rendered = [*rendered[:6], f"+{len(rendered) - 6} more"]
        return ", ".join(rendered)
    if _looks_sensitive(key):
        return "<redacted>"
    if key == "rpc_url":
        return _mask_url(str(value))
    if key.endswith("_address") or key in {"wallet", "funder", "user_id"}:
        return shorten_addr(str(value), 8, 4)
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
    """Render a sanitized startup-health panel for the operator."""
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
    if report.position_sync.pruned_condition_ids:
        lines.append(
            "🧹 pruned conditions: "
            f"{_sample(report.position_sync.pruned_condition_ids, shorten=True)}"
        )
    if report.position_sync.pruned_token_ids:
        lines.append(
            f"🏷️ pruned symbols: {_sample(report.position_sync.pruned_token_ids, shorten=True)}"
        )

    console.print(
        Panel(
            "\n".join(lines),
            title=f"🏥 [bold]STARTUP HEALTH CHECK[/bold] [{report.status}]",
            border_style="green"
            if report.status == "ok"
            else ("yellow" if report.status == "warning" else "red"),
            padding=(1, 2),
        )
    )
    console.print()


def _safe_pct(numer: Decimal, denom: Decimal) -> Decimal:
    """Return ``numer/denom * 100`` as Decimal — Decimal(0) on zero
    denominator so the banner never raises mid-render."""
    if denom is None or denom == 0:
        return Decimal("0")
    return (numer / denom) * Decimal("100")


def _color_pct(pct: Decimal) -> str:
    """Wrap a percentage in a rich color tag based on sign."""
    if pct > 0:
        return f"[green]+{pct:.2f}%[/green]"
    if pct < 0:
        return f"[red]{pct:.2f}%[/red]"
    return f"[white]{pct:.2f}%[/white]"


def _money(value: Decimal | float | int) -> str:
    """Format a money value as ``$NNN.NN`` with thousands separator."""
    d = value if isinstance(value, Decimal) else Decimal(str(value))
    return f"${d:,.2f}"


def _cents(value: Decimal) -> str:
    """Render a Polymarket price (0–1) as cents (e.g. 0.55 → ``55¢``).
    Banner real estate is tight; cents is easier to scan than ``$0.55``."""
    return f"{(value * Decimal('100')):.1f}¢"


def _leader_aggregate_gain(snapshots: list) -> tuple[Decimal, Decimal, Decimal]:
    """Sum invested / current / realized across a leader's snapshots.

    Returns ``(invested_cost_basis, current_value, total_gain_pct)``.
    Invested = size × avg_price. Total gain = (current_value + realized_pnl
    − invested) / invested × 100, defaulting to 0% when invested is 0.
    """
    invested = Decimal("0")
    current = Decimal("0")
    realized = Decimal("0")
    for s in snapshots:
        size = s.size if isinstance(s.size, Decimal) else Decimal(str(s.size or 0))
        avg = s.avg_price if isinstance(s.avg_price, Decimal) else Decimal(str(s.avg_price or 0))
        cv = (
            s.current_value
            if isinstance(s.current_value, Decimal)
            else Decimal(str(s.current_value or 0))
        )
        rp = (
            s.realized_pnl
            if isinstance(s.realized_pnl, Decimal)
            else Decimal(str(s.realized_pnl or 0))
        )
        invested += size * avg
        current += cv
        realized += rp
    gain_pct = _safe_pct(current + realized - invested, invested)
    return invested, current, gain_pct


def print_startup_summary(
    *,
    wallet: str,
    available_cash: "Decimal | float",
    total_portfolio: "Decimal | float",
    positions: list[dict],
    active_leaders: list[str],
    config,
    health_report: StartupHealthReport,
) -> None:
    """Render the sanitized startup portfolio and configuration summary."""
    print_startup_health_check(health_report)
    active_positions = [
        position for position in positions if position.get("lifecycle") != PositionLifecycle.SETTLED
    ]
    settled_position_count = len(positions) - len(active_positions)

    if active_positions:
        position_lines: list[str] = []
        for position in active_positions[:8]:
            market = str(
                position.get("title")
                or position.get("market")
                or position.get("conditionId")
                or position.get("condition_id")
                or "?"
            )
            symbol = _position_symbol(position)
            side = str(position.get("outcome") or position.get("side") or "?")
            size = float(position.get("size") or position.get("size_tokens") or 0.0)
            value = float(position.get("currentValue") or position.get("value") or 0.0)
            position_lines.append(
                f"• {market[:72]}\n"
                f"  ↳ symbol={symbol} | side={side} | size={size:.2f} | value=${value:.2f}"
            )
        remaining = len(active_positions) - 8
        if remaining > 0:
            position_lines.append(f"• ... and {remaining} more position(s)")
    else:
        position_lines = ["• No open positions"]

    header_lines = [
        f"💼 wallet: {shorten_addr(wallet, 8, 4)}",
        f"💵 cash: ${available_cash:.2f}",
        f"🧾 total: ${total_portfolio:.2f}",
        f"📂 open_positions: {len(active_positions)}",
        f"📁 settled_holdings: {settled_position_count}",
    ]
    console.print(
        Panel(
            "\n".join(header_lines) + "\n\n" + "\n".join(position_lines),
            title="📊 [bold]PORTFOLIO[/bold]",
            border_style="cyan",
            padding=(1, 2),
        )
    )
    console.print()

    copied_wallets = config.copy_trading.enabled_wallets()
    copied_wallets_set = {wallet.lower() for wallet in copied_wallets}
    active_leaders_set = {
        leader.lower()
        for leader in (active_leaders or [])
        if isinstance(leader, str) and leader.strip()
    }

    if copied_wallets_set == active_leaders_set:
        leader_lines = [
            f"👥 monitored leaders: {len(active_leaders)}",
            f"🪪 wallets: {_render_value('leader', 'active_leaders', active_leaders) if active_leaders else 'none'}",
        ]
    else:
        copied_rendered = (
            _render_value("copy_trading", "copied_wallets", copied_wallets)
            if copied_wallets
            else "none"
        )
        leader_lines = [
            f"👥 copied wallets: {len(copied_wallets)}",
            f"🪪 wallets: {copied_rendered}",
            f"📡 active leaders: {len(active_leaders)}",
            f"🛰️ monitored: {_render_value('leader', 'active_leaders', active_leaders) if active_leaders else 'none'}",
        ]
    console.print(
        Panel(
            "\n".join(leader_lines),
            title="🧠 [bold]LEADER ENGINE[/bold]",
            border_style="magenta",
            padding=(1, 2),
        )
    )
    console.print()

    engine_sections = [
        (
            "process_and_logging",
            "PROCESS & LOGGING",
            {
                **asdict(config.logging),
            },
        ),
        (
            "polymarket_connectivity_and_signing",
            "POLYMARKET CONNECTIVITY & SIGNING",
            {
                "chain_id": config.polymarket.chain_id,
                "rpc_url": config.polymarket.rpc_url,
                "usdc_contract_address": config.polymarket.usdc_contract_address,
            },
        ),
        (
            "leader_discovery_and_polling",
            "LEADER DISCOVERY & POLLING",
            {
                "detection_mode": config.detection.mode,
                "fetch_interval": config.detection.fetch_interval,
                "aggregate_signal_window_seconds": config.detection.aggregate_signal_window_seconds,
                "configured_wallets": copied_wallets,
                "disabled_wallet_count": sum(
                    not entry.enabled for entry in config.copy_trading.wallets
                ),
                "discovery_enabled": config.discovery.scheduler.enabled,
                "discovery_interval_seconds": config.discovery.scheduler.rediscover_seconds,
                "rescore_interval_seconds": config.discovery.scheduler.rescore_seconds,
                "max_active_leaders": config.discovery.scheduler.max_active_leaders,
                "falcon_token_configured": config.discovery.api.has_token,
                "promote_score": config.discovery.hysteresis.promote_score,
                "demote_score": config.discovery.hysteresis.demote_score,
            },
        ),
        (
            "strategy_engine",
            "STRATEGY ENGINE",
            {
                **asdict(config.sizing),
            },
        ),
        (
            "polymarket_base_execution_rules",
            "POLYMARKET BASE EXECUTION RULES",
            {
                "min_order_size": "venue_metadata",
                "sizing_source": "venue_metadata",
            },
        ),
        (
            "bot_throughput_and_rate_limiting",
            "BOT THROUGHPUT & RATE LIMITING",
            {
                "trade_analysis_concurrency": config.execution.trade_analysis_concurrency,
                "reconcile_concurrency": config.execution.reconcile_concurrency,
                "execution_submit_concurrency": config.execution.execution_submit_concurrency,
                "leader_activity_min_fetch_spacing_ms": config.detection.leader_activity_min_fetch_spacing_ms,
                "leader_positions_min_fetch_spacing_ms": config.detection.leader_positions_min_fetch_spacing_ms,
            },
        ),
        (
            "copy_decision_engine",
            "COPY DECISION ENGINE",
            {
                "enable_copy_decision_gate": config.execution.enable_copy_decision_gate,
                "copy_decision_min_score": config.copy_trading.copy_decision_min_score,
                "copy_max_price_drift_pct": config.copy_trading.copy_max_price_drift_pct,
                "copy_max_edge_decay_pct": config.copy_trading.copy_max_edge_decay_pct,
                "copy_max_signal_age_seconds": config.copy_trading.copy_max_signal_age_seconds,
                "copy_liquidity_target_usdc": config.copy_trading.copy_liquidity_target_usdc,
            },
        ),
        (
            "leader_scoring_engine",
            "LEADER SCORING ENGINE",
            {
                "runtime_weights": config.discovery.scoring.runtime_weights.model_dump(),
                "runtime_pnl_target_usdc": config.discovery.scoring.runtime_pnl_target_usdc,
                "runtime_activity_target": config.discovery.scoring.runtime_activity_target,
                "runtime_score_floor": config.discovery.scoring.runtime_score_floor,
            },
        ),
        (
            "execution_quality_engine",
            "EXECUTION QUALITY ENGINE",
            {
                **asdict(config.execution),
            },
        ),
        (
            "risk_management_engine",
            "RISK MANAGEMENT ENGINE",
            {
                "enable_risk_mgmt": config.risk.enable_risk_mgmt,
                "kill_switch": config.risk.kill_switch,
                "risk_check_interval": config.risk.risk_check_interval,
                "max_open_positions": config.risk.max_open_positions,
                "max_market_exposure_usdc": config.risk.max_market_exposure_usdc,
                "max_category_exposure_usdc": config.risk.max_category_exposure_usdc,
                "max_leader_exposure_usdc": config.risk.max_leader_exposure_usdc,
                "max_daily_drawdown_pct": config.risk.max_daily_drawdown_pct,
            },
        ),
        (
            "risk_management_sub_engines",
            "RISK MANAGEMENT SUB-ENGINES",
            {
                "enable_stop_loss": config.protective_exits.enable_stop_loss,
                "stop_loss_threshold": config.protective_exits.stop_loss_threshold,
                "stop_loss_grace_period_seconds": config.protective_exits.stop_loss_grace_period_seconds,
                "enable_profit_lock": config.protective_exits.enable_profit_lock,
                "profit_lock_trigger_pct": config.protective_exits.profit_lock_trigger_pct,
                "profit_lock_sell_fraction": config.protective_exits.profit_lock_sell_fraction,
                "enable_near_max_exit": config.protective_exits.enable_near_max_exit,
                "near_max_price_warn": config.protective_exits.near_max_price_warn,
                "near_max_price_warn_trim_ratio": config.protective_exits.near_max_price_warn_trim_ratio,
                "near_max_price_exit": config.protective_exits.near_max_price_exit,
                "enable_trailing_stop": config.protective_exits.enable_trailing_stop,
                "trailing_activation_pct": (
                    "auto"
                    if config.protective_exits.trailing_activation_pct is None
                    else config.protective_exits.trailing_activation_pct
                ),
                "trailing_distance": config.protective_exits.trailing_distance,
                "enable_staged_tp": config.protective_exits.enable_staged_tp,
                "staged_tp_levels": config.protective_exits.staged_tp_levels,
                "staged_tp_fractions": config.protective_exits.staged_tp_fractions,
                "enable_leader_divergence_exit": config.protective_exits.enable_leader_divergence_exit,
            },
        ),
        (
            "balance_policy_engine",
            "BALANCE POLICY ENGINE",
            {
                "enable_balance_tiered_policy": config.risk.enable_balance_tiered_policy,
                "day_start_equity_usdc": config.risk.day_start_equity_usdc,
                "balance_policy_reserve_pct": config.risk.balance_policy_reserve_pct,
                "balance_policy_reserve_floor": config.risk.balance_policy_reserve_floor,
                "balance_policy_hard_cap_pct": config.risk.balance_policy_hard_cap_pct,
                "balance_policy_hard_cap_ceiling": config.risk.balance_policy_hard_cap_ceiling,
                "balance_policy_max_open_positions": config.risk.balance_policy_max_open_positions,
                "balance_policy_allow_partial_exits": config.risk.balance_policy_allow_partial_exits,
                "balance_policy_allow_pyramiding": config.risk.balance_policy_allow_pyramiding,
                "balance_policy_max_adds_per_position": config.risk.balance_policy_max_adds_per_position,
                "balance_policy_base_min_score": config.risk.balance_policy_base_min_score,
                "balance_policy_min_notional_usdc": config.risk.balance_policy_min_notional_usdc,
                "balance_policy_dynamic_threshold_raise": config.risk.balance_policy_dynamic_threshold_raise,
            },
        ),
        (
            "discovery_and_tiering_engines",
            "DISCOVERY & TIERING ENGINES",
            {
                "enable_tier_gate_watchlist": config.copy_trading.enable_tier_gate_watchlist,
                "enable_tier_gate_watchlist_log": config.copy_trading.enable_tier_gate_watchlist_log,
                "enabled": config.discovery.scheduler.enabled,
                "candidate_pool_size": config.discovery.shortlist.candidate_pool_size,
                "promote_score": config.discovery.hysteresis.promote_score,
                "demote_score": config.discovery.hysteresis.demote_score,
                "min_tenure_cycles": config.discovery.hysteresis.min_tenure_cycles,
                "min_dwell_seconds": config.discovery.hysteresis.min_dwell_seconds,
                "max_active_leaders": config.discovery.scheduler.max_active_leaders,
                "longitudinal_enabled": config.discovery.longitudinal.enabled,
            },
        ),
        (
            "paper_live_safety_and_geographic_controls",
            "PAPER/LIVE SAFETY & GEOGRAPHIC CONTROLS",
            {
                "paper_trade_only": config.execution.paper_trade_only,
                "enforce_geoblock": config.polymarket.enforce_geoblock,
            },
        ),
        (
            "storage_and_persistence",
            "STORAGE & PERSISTENCE",
            {
                "postgres_dsn_host": _safe_dsn_host(config.database.dsn),
            },
        ),
        (
            "metrics_and_reconciliation_engines",
            "METRICS & RECONCILIATION ENGINES",
            {
                "enable_metrics_server": config.app.enable_metrics_server,
                "metrics_port": config.app.metrics_port,
                "metrics_bind_host": config.app.metrics_bind_host,
                "reconcile_stale_seconds": config.reconciliation.stale_threshold_seconds,
                "reconcile_auto_cancel": config.reconciliation.auto_cancel_stale,
                "reconcile_interval_seconds": config.reconciliation.reconcile_interval_seconds,
            },
        ),
    ]

    hidden_console_sections = {
        "process_and_logging",
        "polymarket_connectivity_and_signing",
        "storage_and_persistence",
        "metrics_and_reconciliation_engines",
    }

    for section_name, section_title, section_data in engine_sections:
        if section_name in hidden_console_sections:
            continue
        console.print(
            Panel(
                _format_mapping(section_name, section_data),
                title=f"⚙️ [bold]{section_title}[/bold]",
                border_style="blue",
                padding=(0, 1),
            )
        )

    sanitized_engine_config = {
        name: {key: _render_value(name, key, value) for key, value in data.items()}
        for name, _title, data in engine_sections
    }
    sanitized_positions = [
        {
            "title": str(position.get("title") or position.get("market") or ""),
            "symbol": _position_symbol(position),
            "outcome": str(position.get("outcome") or position.get("side") or ""),
            "size": float(position.get("size") or position.get("size_tokens") or 0.0),
            "value": float(position.get("currentValue") or position.get("value") or 0.0),
        }
        for position in active_positions[:12]
    ]

    logger.info(
        (
            f"STARTUP SUMMARY | wallet={shorten_addr(wallet, 8, 4)} | leaders={len(active_leaders)} | "
            f"cash={available_cash:.2f} | total={total_portfolio:.2f} | "
            f"open_positions={len(active_positions)} | settled_holdings={settled_position_count}"
        ),
        extra={
            "type": "startup_summary",
            "wallet": shorten_addr(wallet, 8, 4),
            "activeLeadersCount": len(active_leaders),
            # JSON-log boundary: cast Decimal → float for the structured
            # extra payload so the formatter never raises on a Decimal.
            "availableCash": float(available_cash),
            "totalPortfolio": float(total_portfolio),
            "openPositionCount": len(active_positions),
            "settledPositionCount": settled_position_count,
            "livePositions": sanitized_positions,
            "engineConfig": sanitized_engine_config,
            "health": health_report.as_log_payload(),
        },
    )
    console.print()
