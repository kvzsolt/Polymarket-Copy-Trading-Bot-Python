from __future__ import annotations

from dataclasses import asdict
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Iterable
from urllib.parse import urlparse

from rich.panel import Panel
from rich.rule import Rule

from orchestrators.startup_health import StartupHealthReport
from utils.logger import console, get_logger, shorten_addr


def _safe_dsn_host(dsn: str) -> str:
    try:
        parsed = urlparse(dsn)
        host = parsed.hostname or ""
        port = parsed.port or ""
        return f"{host}:{port}" if port else host
    except Exception:
        return "<unparseable>"

if TYPE_CHECKING:
    from services.discovery.position_snapshot_service import SnapshotResult

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
            shorten_addr(str(item), 8, 4) if key in {"wallets", "active_leaders", "copied_wallets"} else str(item)
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
        avg = (
            s.avg_price
            if isinstance(s.avg_price, Decimal)
            else Decimal(str(s.avg_price or 0))
        )
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


def print_traders_youre_copying(
    snapshot_result: "SnapshotResult | None",
    *,
    max_positions_per_leader: int = 5,
) -> None:
    """Render the TRADERS YOU'RE COPYING panel from a SnapshotResult.

    Per-leader: header with shortened wallet + open position count
    + total gain%. Then up to ``max_positions_per_leader`` per-position
    lines showing market title, current value, PnL%, bought-at price,
    current price.

    A leader with NO snapshots (fetch failed or empty position list)
    still renders a one-line header so the operator sees the leader
    is being monitored and what failed.

    Renders nothing when ``snapshot_result`` is None (legacy callers
    that have not been migrated to the snapshot pipeline).
    """
    if snapshot_result is None or not snapshot_result.leader_snapshots:
        return

    lines: list[str] = []
    for leader_wallet, snapshots in snapshot_result.leader_snapshots.items():
        failed = leader_wallet in snapshot_result.leaders_failed
        wallet_short = shorten_addr(leader_wallet, 8, 4)
        if not snapshots:
            if failed:
                lines.append(
                    f"\n[bold yellow]{wallet_short}[/bold yellow]  "
                    f"[dim](fetch failed — no positions captured)[/dim]"
                )
            else:
                lines.append(
                    f"\n[bold]{wallet_short}[/bold]  [dim](no open positions)[/dim]"
                )
            continue

        _, _, gain_pct = _leader_aggregate_gain(snapshots)
        lines.append(
            f"\n[bold]{wallet_short}[/bold]  "
            f"positions={len(snapshots)}  "
            f"total_gain={_color_pct(gain_pct)}"
        )

        # Sort by current_value DESC so the largest position renders first.
        ordered = sorted(
            snapshots,
            key=lambda s: (
                s.current_value
                if isinstance(s.current_value, Decimal)
                else Decimal(str(s.current_value or 0))
            ),
            reverse=True,
        )
        for snap in ordered[:max_positions_per_leader]:
            # Pull market title from raw_json if present (the data-api
            # payload carries ``title``); fall back to condition_id.
            title = snap.condition_id
            try:
                import json as _json
                payload = (
                    _json.loads(snap.raw_json)
                    if isinstance(snap.raw_json, str)
                    else snap.raw_json
                )
                title = str(
                    payload.get("title")
                    or payload.get("market")
                    or snap.condition_id
                )
            except Exception:
                pass

            invested = (snap.size or Decimal("0")) * (snap.avg_price or Decimal("0"))
            pnl = (snap.current_value or Decimal("0")) - invested
            pnl_pct = _safe_pct(pnl, invested)
            lines.append(
                f"  • {title[:42]:<42}  "
                f"val={_money(snap.current_value or 0):>8}  "
                f"pnl={_color_pct(pnl_pct)}  "
                f"@{_cents(snap.avg_price or Decimal('0'))}→{_cents(snap.current_price or Decimal('0'))}"
            )
        remaining = len(snapshots) - max_positions_per_leader
        if remaining > 0:
            lines.append(f"  • [dim]... +{remaining} more[/dim]")

    console.print(
        Panel(
            "\n".join(lines).lstrip(),
            title="👥 [bold]TRADERS YOU'RE COPYING[/bold]",
            border_style="magenta",
            padding=(1, 2),
        )
    )
    console.print()


def print_your_positions(
    snapshot_result: "SnapshotResult | None",
    *,
    own_wallet: str,
    available_cash: "Decimal | float",
    total_portfolio: "Decimal | float",
    max_positions: int = 8,
) -> None:
    """Render the YOUR POSITIONS panel from own-wallet snapshots.

    Mirrors the TS-bot screenshot: own wallet, available cash, portfolio
    total, open position count, invested, current value, P/L%, top
    positions list. Falls back to a header-only render when the
    snapshot pipeline supplied no own snapshots (data-api degraded).
    """
    if snapshot_result is None:
        return

    # cubic P2: in multi-user runtime ``snapshot_result.own_snapshots``
    # contains rows from every configured own_wallet. Without this
    # filter, positions from all users render under the single
    # ``own_wallet`` label passed in — visually misleading. Match on
    # ``trader_wallet`` (case-insensitive) so the panel only shows
    # positions actually belonging to the wallet whose address it
    # advertises. Multi-wallet rendering (one panel per own wallet)
    # is a separate follow-up.
    _own_wallet_lc = (own_wallet or "").lower().strip()
    own = [
        s for s in snapshot_result.own_snapshots
        if (s.trader_wallet or "").lower().strip() == _own_wallet_lc
    ] if _own_wallet_lc else list(snapshot_result.own_snapshots)
    invested = sum(
        ((s.size or Decimal("0")) * (s.avg_price or Decimal("0")) for s in own),
        Decimal("0"),
    )
    current = sum(
        (s.current_value or Decimal("0") for s in own), Decimal("0")
    )
    realized = sum(
        (s.realized_pnl or Decimal("0") for s in own), Decimal("0")
    )
    pnl_pct = _safe_pct(current + realized - invested, invested)

    header_lines = [
        f"💼 wallet:           {shorten_addr(own_wallet, 8, 4)}",
        f"💵 available_cash:   {_money(available_cash)}",
        f"🧾 portfolio_total:  {_money(total_portfolio)}",
        f"📂 open_positions:   {len(own)}",
        f"📥 invested:         {_money(invested)}",
        f"📈 current_value:    {_money(current)}",
        f"💰 pnl:              {_color_pct(pnl_pct)}",
    ]
    if snapshot_result.own_failed:
        header_lines.append(
            "[dim yellow]⚠ own snapshot fetch failed — banner uses cached / partial data[/dim yellow]"
        )

    position_lines: list[str] = []
    if own:
        ordered = sorted(
            own,
            key=lambda s: (
                s.current_value
                if isinstance(s.current_value, Decimal)
                else Decimal(str(s.current_value or 0))
            ),
            reverse=True,
        )
        for snap in ordered[:max_positions]:
            title = snap.condition_id
            try:
                import json as _json
                payload = (
                    _json.loads(snap.raw_json)
                    if isinstance(snap.raw_json, str)
                    else snap.raw_json
                )
                title = str(
                    payload.get("title")
                    or payload.get("market")
                    or snap.condition_id
                )
            except Exception:
                pass
            inv_i = (snap.size or Decimal("0")) * (snap.avg_price or Decimal("0"))
            pnl_i_pct = _safe_pct((snap.current_value or Decimal("0")) - inv_i, inv_i)
            position_lines.append(
                f"  • {title[:42]:<42}  "
                f"val={_money(snap.current_value or 0):>8}  "
                f"pnl={_color_pct(pnl_i_pct)}  "
                f"@{_cents(snap.current_price or Decimal('0'))}"
            )
        remaining = len(own) - max_positions
        if remaining > 0:
            position_lines.append(f"  • [dim]... +{remaining} more[/dim]")
    else:
        position_lines = ["  • [dim]No open positions[/dim]"]

    body = "\n".join(header_lines) + "\n\n" + "\n".join(position_lines)
    console.print(
        Panel(
            body,
            title="📊 [bold]PORTFOLIO[/bold]",
            border_style="cyan",
            padding=(1, 2),
        )
    )
    console.print()


def print_startup_summary(
    *,
    wallet: str,
    available_cash: "Decimal | float",
    total_portfolio: "Decimal | float",
    positions: list[dict],
    active_leaders: list[str],
    config,
    health_report: StartupHealthReport,
    snapshot_result: "SnapshotResult | None" = None,
) -> None:
    print_startup_health_check(health_report)

    # Boot-time snapshot supplied — render TRADERS YOU'RE COPYING + PORTFOLIO
    # panels (PORTFOLIO is the single per-wallet panel previously named
    # YOUR POSITIONS; it already carries wallet/cash/total/open_positions/
    # invested/current_value/pnl plus the per-position list). Legacy fallback
    # below renders a minimal text-only PORTFOLIO when the snapshot pipeline
    # was unavailable.
    if snapshot_result is not None:
        print_traders_youre_copying(snapshot_result)
        print_your_positions(
            snapshot_result,
            own_wallet=wallet,
            available_cash=available_cash,
            total_portfolio=total_portfolio,
        )
    else:
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

        header_lines = [
            f"💼 wallet: {shorten_addr(wallet, 8, 4)}",
            f"💵 cash: ${available_cash:.2f}",
            f"🧾 total: ${total_portfolio:.2f}",
            f"📂 open_positions: {len(positions)}",
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

    copied_wallets_set = {
        w.lower()
        for w in (config.leader.wallets or [])
        if isinstance(w, str) and w.strip()
    }
    active_leaders_set = {
        l.lower()
        for l in (active_leaders or [])
        if isinstance(l, str) and l.strip()
    }

    if copied_wallets_set == active_leaders_set:
        leader_lines = [
            f"👥 monitored leaders: {len(active_leaders)}",
            f"🪪 wallets: {_render_value('leader', 'active_leaders', active_leaders) if active_leaders else 'none'}",
        ]
    else:
        leader_lines = [
            f"👥 copied wallets: {len(config.leader.wallets)}",
            f"🪪 wallets: {_render_value('leader', 'copied_wallets', config.leader.wallets) if config.leader.wallets else 'none'}",
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
                "pythonunbuffered": config.raw.get('PYTHONUNBUFFERED', 'disabled'),
                **asdict(config.logging),
            },
        ),
        (
            "polymarket_connectivity_and_signing",
            "POLYMARKET CONNECTIVITY & SIGNING",
            {
                "chain_id": config.network.chain_id,
                "clob_http_url": config.clob.http_url,
                "rpc_url": config.network.rpc_url,
                "usdc_contract_address": config.network.usdc_contract_address,
            },
        ),
        (
            "leader_discovery_and_polling",
            "LEADER DISCOVERY & POLLING",
            {
                "fetch_interval": config.polling.fetch_interval,
                "aggregate_signal_window_seconds": config.polling.aggregate_signal_window_seconds,
                "leader_auto": config.leader.leader_auto,
                "leader_auto_count": config.leader.leader_auto_count,
                "copied_wallets": config.leader.wallets,
                "auto_discover_leaders": config.leader.auto_discover_leaders,
                "auto_discover_max_leaders": config.leader.auto_discover_max_leaders,
                "auto_discover_top_n": config.leader.auto_discover_top_n,
                "auto_discover_min_score": config.leader.auto_discover_min_score,
                "discovery_interval_seconds": config.discovery.discovery_interval_seconds,
                "discovery_promote_to_active": config.discovery.discovery_promote_to_active,
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
                "enable_executable_order_guard": config.raw.get(
                    "ENABLE_EXECUTABLE_ORDER_GUARD", "disabled"
                ),
                "copy_size_decimals": config.raw.get(
                    "COPY_SIZE_DECIMALS", "disabled"
                ),
                "executable_order_buffer_notional_usdc": config.raw.get(
                    "EXECUTABLE_ORDER_BUFFER_NOTIONAL_USDC", "disabled"
                ),
                "min_order_size": config.raw.get("MIN_ORDER_SIZE", "venue_metadata"),
                "sizing_source": "venue_metadata",
            },
        ),
        (
            "bot_throughput_and_rate_limiting",
            "BOT THROUGHPUT & RATE LIMITING",
            {
                "trade_analysis_concurrency": config.raw.get('TRADE_ANALYSIS_CONCURRENCY', 'disabled'),
                "reconcile_concurrency": config.raw.get('RECONCILE_CONCURRENCY', 'disabled'),
                "execution_submit_concurrency": config.raw.get('EXECUTION_SUBMIT_CONCURRENCY', 'disabled'),
                "leader_activity_min_fetch_spacing_ms": config.raw.get('LEADER_ACTIVITY_MIN_FETCH_SPACING_MS', 'disabled'),
                "leader_positions_min_fetch_spacing_ms": config.raw.get('LEADER_POSITIONS_MIN_FETCH_SPACING_MS', 'disabled'),
            },
        ),
        (
            "copy_decision_engine",
            "COPY DECISION ENGINE",
            {
                "enable_copy_decision_gate": config.execution.enable_copy_decision_gate,
                "copy_decision_min_score": config.leader.copy_decision_min_score,
                "copy_max_price_drift_pct": config.leader.copy_max_price_drift_pct,
                "copy_max_edge_decay_pct": config.leader.copy_max_edge_decay_pct,
                "copy_max_signal_age_seconds": config.leader.copy_max_signal_age_seconds,
                "copy_liquidity_target_usdc": config.leader.copy_liquidity_target_usdc,
                "tiny_leader_high_confidence_score": config.leader.tiny_leader_high_confidence_score,
            },
        ),
        (
            "leader_scoring_engine",
            "LEADER SCORING ENGINE",
            {
                "min_leader_trade_notional_usdc": config.leader.min_leader_trade_notional_usdc,
                "leader_score_balance_target_usdc": config.leader.leader_score_balance_target_usdc,
                "leader_score_pnl_bonus_cap": config.leader.leader_score_pnl_bonus_cap,
                "leader_score_activity_target": config.leader.leader_score_activity_target,
                "leader_score_weight_balance": config.leader.leader_score_weight_balance,
                "leader_score_weight_analytics": config.leader.leader_score_weight_analytics,
                "leader_score_weight_activity": config.leader.leader_score_weight_activity,
                "leader_score_floor": config.leader.leader_score_floor,
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
                "risk_check_interval": config.polling.risk_check_interval,
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
                "enable_stop_loss": config.features.enable_stop_loss,
                "stop_loss_threshold": config.risk.stop_loss_threshold,
                "stop_loss_grace_period_seconds": config.risk.stop_loss_grace_period_seconds,
                "enable_profit_lock": config.features.enable_profit_lock,
                "profit_lock_trigger_pct": config.risk.profit_lock_trigger_pct,
                "profit_lock_sell_fraction": config.risk.profit_lock_sell_fraction,
                "enable_near_max_exit": config.features.enable_near_max_exit,
                "near_max_price_warn": config.risk.near_max_price_warn,
                "near_max_price_warn_trim_ratio": config.risk.near_max_price_warn_trim_ratio,
                "near_max_price_exit": config.risk.near_max_price_exit,
                "enable_trailing_stop": config.features.enable_trailing_stop,
                "trailing_activation_pct": config.risk.trailing_activation_pct,
                "trailing_distance": config.risk.trailing_distance,
                "enable_staged_tp": config.features.enable_staged_tp,
                "staged_tp_levels": config.risk.staged_tp_levels,
                "staged_tp_fractions": config.risk.staged_tp_fractions,
                "enable_leader_divergence_exit": config.features.enable_leader_divergence_exit,
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
                "enable_tier_gate_watchlist": config.features.enable_tier_gate_watchlist,
                "enable_tier_gate_watchlist_log": config.features.enable_tier_gate_watchlist_log,
                "enable_trader_discovery": config.discovery.enable_trader_discovery,
                "min_leader_pnl": config.discovery.min_leader_pnl,
                "min_leader_win_rate": config.discovery.min_leader_win_rate,
                "min_leader_positions": config.discovery.min_leader_positions,
                "discovery_max_results": config.discovery.discovery_max_results,
                "tier_promote_to_paper": config.leader.tier_promote_to_paper,
                "tier_promote_to_live": config.leader.tier_promote_to_live,
                "tier_min_paper_seconds": config.leader.tier_min_paper_seconds,
                "tier_demote_to_paper": config.leader.tier_demote_to_paper,
                "tier_demote_to_watchlist": config.leader.tier_demote_to_watchlist,
            },
        ),
        (
            "paper_live_safety_and_geographic_controls",
            "PAPER/LIVE SAFETY & GEOGRAPHIC CONTROLS",
            {
                "paper_trade_only": config.execution.paper_trade_only,
                "enforce_geoblock": config.network.enforce_geoblock,
            },
        ),
        (
            "storage_and_persistence",
            "STORAGE & PERSISTENCE",
            {
                "postgres_dsn_host": _safe_dsn_host(config.database.dsn),
                "pnl_log_file": config.logging.pnl_log_file,
                "multi_user_config_file": config.discovery.multi_user_config_file,
            },
        ),
        (
            "metrics_and_reconciliation_engines",
            "METRICS & RECONCILIATION ENGINES",
            {
                "enable_metrics_server": config.network.enable_metrics_server,
                "metrics_port": config.network.metrics_port,
                "metrics_bind_host": config.network.metrics_bind_host,
                "reconcile_stale_seconds": config.raw.get('RECONCILE_STALE_SECONDS', 'disabled'),
                "reconcile_auto_cancel": config.raw.get('RECONCILE_AUTO_CANCEL', 'disabled'),
                "reconcile_interval_seconds": config.raw.get('RECONCILE_INTERVAL_SECONDS', 'disabled'),
            },
        ),
        (
            "near_one_exit_engine",
            "NEAR-ONE EXIT ENGINE",
            {
                "enable_near_one_exit": config.near_one_exit.enable_near_one_exit,
                "threshold": str(config.near_one_exit.threshold),
                "dwell_seconds": config.near_one_exit.dwell_seconds,
                "check_interval": config.near_one_exit.check_interval,
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
        name: {
            key: _render_value(name, key, value)
            for key, value in data.items()
        }
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
            # JSON-log boundary: cast Decimal → float for the structured
            # extra payload so the formatter never raises on a Decimal.
            "availableCash": float(available_cash),
            "totalPortfolio": float(total_portfolio),
            "openPositionCount": len(positions),
            "livePositions": sanitized_positions,
            "engineConfig": sanitized_engine_config,
            "health": health_report.as_log_payload(),
        },
    )
    console.print()
