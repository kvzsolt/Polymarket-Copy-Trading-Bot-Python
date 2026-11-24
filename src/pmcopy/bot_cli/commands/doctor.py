"""PR8 — operator-facing live readiness gate.

``run_doctor_checks(mode)`` returns a structured dict::

    {
      "status": "ok" | "warning" | "blocked",
      "mode": "live" | "paper",
      "checks": [
        {"name": str, "status": str, "detail": str, "fix": str},
        ...
      ],
    }

Live mode requires every blocker to be clear. Paper mode only runs the
subset of checks that apply outside live submission.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import re

from typing import Any
from collections.abc import Awaitable, Callable

from pmcopy.config.app_config import (
    AppConfig,
    EnvValidationError,
    load_config,
)
from pmcopy.config.env import secret_value
from pmcopy.orchestrators.lifecycle.trader_startup_policy import TraderStartupPolicy
from pmcopy.orchestrators.bootstrap.provider_runtime import ProviderRuntime



from pmcopy.bot_cli.commands.doctor_checks import (
    _GATE_INSTALLED_PACKAGES,  # noqa: F401
    _STATUS_BLOCKED,
    _STATUS_OK,
    _STATUS_WARNING,
    _DoctorContext,
    _GateScanUnavailable,
    _auto_trader_db_candidate_count,
    _automatic_rosters_available,
    _check_gateway_ccxt,
    _check_wss_endpoint,
    _code_gate_coordinator_alias,
    _code_gate_scan,
    _missing_db_tables,
    _redact_dsn,
    _repo_root,
    _resolve_jsonl_path,
    _resolve_log_dir,
    _try_port_bind,
)


# ---------------------------------------------------------------------------
def register_doctor_command(subparsers: argparse._SubParsersAction) -> None:
    """Register the diagnostic command with the CLI parser."""
    parser = subparsers.add_parser("doctor", help="Run diagnostic checks")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--live", action="store_true", help="Check readiness for live trading")
    group.add_argument("--paper", action="store_true", help="Check readiness for paper trading")


async def run_doctor_checks(
    mode: str,
    *,
    config: AppConfig | None = None,
    auto_trader: bool = False,
    provider_runtime: Any = None,
) -> dict[str, Any]:
    """Run the structured readiness gate.

    Args:
        mode: Requested ``paper`` or ``live`` diagnostic mode.
        config: Optional immutable config snapshot shared with startup.
        auto_trader: Ignore manual leaders and validate discovery startup.

    Returns:
        Structured diagnostic result documented by this module.
    """
    ctx = _DoctorContext(mode=mode)

    # ----- config_load --------------------------------------------------
    if config is None:
        try:
            config = load_config()
        except EnvValidationError as exc:
            ctx.blocked("config_load", "validation_error", str(exc))
            return ctx.to_dict()
        except Exception as exc:
            ctx.blocked("config_load", f"unexpected error: {exc!r}", "review config")
            return ctx.to_dict()
    ctx.ok("config_load", "AppConfig loaded")

    live = mode == "live"

    # ----- mode flags ---------------------------------------------------
    if live:
        # P0-2: live trading is gated by the LIVE_TRADING master switch (default
        # paper). If the doctor is asked for "live" but the config resolved to
        # paper, the operator must set the master switch — there is no silent
        # fallback to paper, the bot refuses to start.
        if config.execution.paper_trade_only:
            ctx.blocked(
                "paper_trade_only_false",
                "live mode requested but config resolves to PAPER",
                "set trading_mode: live in config/environments/live/execution.yml "
                "(PMCOPY_ENVIRONMENT=live) and restart",
            )
        else:
            ctx.ok("paper_trade_only_false", "live trading enabled (paper_trade_only=false)")
        if config.dry_run:
            ctx.blocked(
                "dry_run_false",
                "dry_run=true (config resolves to paper)",
                "set trading_mode: live in config/environments/live/execution.yml "
                "(PMCOPY_ENVIRONMENT=live) and restart",
            )
        else:
            ctx.ok("dry_run_false", "dry_run=false")
    # ----- kill switch --------------------------------------------------
    # C1 — three kill switches are now enforced at act-time in
    # ExecutionService.execute: global (master, observe-only), execution
    # (refuses BUY entries, allows protective SELL exits), and the legacy
    # risk.kill_switch (kept for backward compatibility). Any one engaged
    # blocks a live restart.
    global_kill = bool(getattr(config, "app", None) and config.app.kill_switch)
    exec_kill = bool(getattr(config.execution, "kill_switch", False))
    risk_kill = bool(config.risk.kill_switch)
    if global_kill:
        ctx.blocked(
            "kill_switch_off",
            "global.kill_switch=true (master: observe only, submits nothing)",
            "set kill_switch: false in config/app.yml",
        )
    elif exec_kill:
        ctx.blocked(
            "kill_switch_off",
            "execution.kill_switch=true (refuses BUY entries)",
            "set kill_switch: false in config/execution.yml",
        )
    elif risk_kill:
        ctx.blocked(
            "kill_switch_off",
            "risk.kill_switch=true (legacy)",
            "set kill_switch: false in config/risk.yml",
        )
    else:
        ctx.ok("kill_switch_off", "global/execution/risk kill_switch=false")

    # ----- wallet basics ------------------------------------------------
    if live:
        # Task 22 — with a schema v2 copiers roster the per-copier env
        # references are the wallet credential source, so the GLOBAL
        # single-account wallet checks are replaced by an end-to-end roster
        # load (structure, identities, modes, and live secret presence).
        # Loader errors are already secret-safe: they name variables, not
        # values.
        app_settings = getattr(config, "app", None)
        copiers_file = str(getattr(app_settings, "copiers_config_file", "") or "").strip()
        if copiers_file:
            try:
                from pmcopy.services.accounts.multi_user_config import load_runtime_users

                roster_users = load_runtime_users([], config)
                ctx.ok(
                    "copiers_roster",
                    f"{len(roster_users)} copier account(s) loaded from the "
                    "schema v2 roster (wallet credentials resolve per copier)",
                )
            except ValueError as exc:
                ctx.blocked(
                    "copiers_roster",
                    str(exc),
                    "fix app.copiers_config_file (per-copier *_env references) and retry",
                )
        else:
            if not secret_value(config.wallet.private_key):
                ctx.blocked(
                    "wallet_private_key",
                    "wallet.private_key is missing",
                    "set PMCOPY__WALLET__PRIVATE_KEY in secrets.env",
                )
            else:
                ctx.ok("wallet_private_key", "wallet.private_key present")
            funder = config.wallet.funder_address or ""
            if not funder:
                ctx.blocked(
                    "wallet_funder_resolution",
                    "verified Deposit Wallet funder_address is missing",
                    "set PMCOPY__WALLET__FUNDER_ADDRESS in secrets.env",
                )
            elif re.match(r"^0x[0-9a-fA-F]{40}$", funder):
                ctx.ok("wallet_funder_valid", f"configured Deposit Wallet: {funder}")
            else:
                ctx.blocked(
                    "wallet_funder_valid",
                    f"invalid wallet.funder_address: {funder!r}",
                    "remove wallet.funder_address or set a 42-char Deposit Wallet address",
                )
            ctx.ok(
                "wallet_signature_type",
                "DEPOSIT_WALLET / POLY_1271 / signature_type=3 enforced by CCXT composition",
            )

    # ----- database -----------------------------------------------------
    from pmcopy.storage.postgres.connection import init_pool, shutdown_pool

    pool = None
    db_label = _redact_dsn(config.database.dsn)
    try:
        pool = await init_pool(
            config.database.dsn,
            min_size=1,
            max_size=2,
            command_timeout_seconds=15.0,
        )
        async with pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        ctx.ok("db_writable", db_label)
    except Exception as exc:
        ctx.blocked(
            "db_writable",
            f"db not writable: {exc}",
            f"ensure Postgres at {db_label} is reachable",
        )

    try:
        if pool is None:
            raise RuntimeError("pool not initialised")
        missing = await _missing_db_tables(pool)
        if missing:
            ctx.blocked(
                "db_schema_ok",
                f"missing tables: {missing}",
                "run apply_migrations",
            )
        else:
            ctx.ok("db_schema_ok", "all required tables present")
    except Exception as exc:
        ctx.blocked("db_schema_ok", f"schema probe failed: {exc}", "investigate DB")

    # ----- pending orders / stale idempotency ---------------------------
    try:
        if pool is None:
            raise RuntimeError("pool not initialised")
        # ``orders`` table existence is implied by db_schema_ok above;
        # the long-running bot owns schema lifecycle. Skip the explicit
        # table-presence probe (was sqlite_master-specific).
        ttl = config.execution.pending_submission_ttl_seconds
        from pmcopy.storage.postgres.orders_repo import OrdersRepo

        stale_rows = await OrdersRepo(pool).list_stale_submission_locks(ttl_seconds=int(ttl))
        count = len(stale_rows)
        if count:
            ctx.warning(
                "stale_idempotency",
                f"{count} stale pending orders",
                "run stale-lock resolve and reconciliation before live",
            )
        else:
            ctx.ok("stale_idempotency", "no stale pending orders")
    except Exception as exc:
        # Pre-fix: ctx.warning() — a DB corruption / permissions error
        # silently degraded to WARNING and the doctor's live gate
        # (which blocks only on status='blocked') silently passed it.
        # A DB probe failure that prevents us from counting stale
        # orders is a fail-closed condition: we cannot prove the
        # idempotency state is safe, therefore live is blocked.
        ctx.blocked(
            "stale_idempotency",
            f"probe failed: {type(exc).__name__}",
            "investigate DB connectivity + schema before live",
        )
    finally:
        if pool is not None:
            try:
                await shutdown_pool()
            except Exception:
                pass

    # ----- RPC ----------------------------------------------------------
    from pmcopy.orchestrators.lifecycle.startup_health import _check_rpc

    rpc = (
        await _check_rpc(config.polymarket.rpc_url, provider_runtime=provider_runtime)
        if provider_runtime is not None
        else await _check_rpc(config.polymarket.rpc_url)
    )
    if getattr(rpc, "status", "") == "error":
        ctx.blocked(
            "network_rpc",
            f"rpc unreachable: {getattr(rpc, 'details', '')}",
            "verify network.rpc_url",
        )
    else:
        ctx.ok("network_rpc", "RPC reachable")

    # ----- network probes ----------------------------------------------
    if live:
        ok, detail = await asyncio.to_thread(
            _check_wss_endpoint, "ws-subscriptions-clob.polymarket.com"
        )
        (ctx.ok if ok else ctx.blocked)(
            "network_market_wss", detail, "" if ok else "check egress / DNS"
        )
        ok2, detail2 = await asyncio.to_thread(_check_wss_endpoint, "clob.polymarket.com")
        (ctx.ok if ok2 else ctx.blocked)(
            "network_rest_clob", detail2, "" if ok2 else "check egress / DNS"
        )

    # ----- geoblock -----------------------------------------------------
    if config.polymarket.enforce_geoblock:
        from pmcopy.services.compliance import check_geoblock

        decision = await check_geoblock()
        if decision.allowed:
            ctx.ok("geoblock_check", "allowed")
        else:
            ctx.blocked(
                "geoblock_check",
                f"blocked: {decision.reason}",
                "geoblock policy denies live trading",
            )
    else:
        ctx.skipped("geoblock_check", "not enforced")

    # ----- CCXT prediction gateway --------------------------------------
    if live:
        ok, detail = _check_gateway_ccxt()
        (ctx.ok if ok else ctx.blocked)(
            "gateway_ccxt", detail, "" if ok else "install the pinned CCXT runtime dependency"
        )

    # ----- subsystem wiring (dry import) --------------------------------
    if live:
        for name, module_path in (
            ("reconciliation_service_wired", "pmcopy.services.execution.reconciliation_service"),
            ("market_data_service_wired", "pmcopy.services.market_data.market_data_service"),
            ("position_drift_service_wired", "pmcopy.services.positions.position_drift_service"),
            ("position_action_coordinator_wired", "pmcopy.services.positions.position_action_coordinator"),
            ("coordinator_wired", "pmcopy.services.execution.coordinator"),
            ("reservation_service_wired", "pmcopy.services.execution.reservation_service"),
        ):
            try:
                __import__(module_path)
                ctx.ok(name, f"{module_path} importable")
            except Exception as exc:
                ctx.blocked(name, f"import failed: {exc}", f"fix {module_path}")

    # ----- readiness keys known ----------------------------------------
    try:
        from pmcopy.services.observability.health_server import _STRICT_REQUIRED_KEYS, ReadinessTracker

        snap = ReadinessTracker().snapshot()
        missing = [k for k in _STRICT_REQUIRED_KEYS if k not in snap]
        if missing:
            ctx.blocked(
                "readiness_keys_known",
                f"missing keys: {missing}",
                "wire all strict readiness keys",
            )
        else:
            ctx.ok(
                "readiness_keys_known",
                f"{len(_STRICT_REQUIRED_KEYS)} strict keys defined",
            )
    except Exception as exc:
        ctx.blocked("readiness_keys_known", f"probe failed: {exc}", "")

    # ----- drift wiring -------------------------------------------------
    if live:
        if config.reconciliation.position_drift.check_on_startup:
            try:
                from pmcopy.orchestrators.bootstrap.runtime_bootstrap import startup_drift_check  # noqa: F401

                ctx.ok("drift_check_runs", "startup_drift_check available")
            except Exception as exc:
                ctx.blocked(
                    "drift_check_runs",
                    f"startup_drift_check import failed: {exc}",
                    "verify bootstrap module",
                )
        else:
            ctx.warning(
                "drift_check_runs",
                "POSITION_DRIFT_CHECK_ON_STARTUP=false",
                "enable for live: POSITION_DRIFT_CHECK_ON_STARTUP=true",
            )

    # ----- code gates ---------------------------------------------------
    # Concatenate forbidden substrings at runtime so the doctor source
    # itself does not contain the literals it is scanning for. The PR3+
    # forbidden tokens are split across two halves below. Each gate either
    # scans real files (source tree, or the installed package when the
    # source tree is absent) and says so in its detail, or fails closed —
    # never again a clean pass for zero files scanned.
    repo_root = _repo_root()
    _PRICE_CACHE = "services." + "price_cache"
    _POSITION_PRICE_CACHE = "PositionPrice" + "Cache"
    _MARKET_META_SERVICE = "MarketMeta" + "Service"
    try:
        forbidden_legacy, gate_provenance = _code_gate_scan(
            repo_root=repo_root,
            forbidden_substrings=(_PRICE_CACHE, _POSITION_PRICE_CACHE),
        )
    except _GateScanUnavailable as exc:
        ctx.blocked(
            "code_gate_price_cache",
            f"scan unavailable: {exc}",
            "reinstall the pmcopy package or run doctor from the source checkout",
        )
    else:
        if forbidden_legacy:
            ctx.blocked(
                "code_gate_price_cache",
                f"references found: {forbidden_legacy[:3]}",
                "remove legacy price cache references",
            )
        else:
            ctx.ok("code_gate_price_cache", f"no legacy references ({gate_provenance})")

    try:
        forbidden_meta, gate_provenance = _code_gate_scan(
            repo_root=repo_root,
            forbidden_substrings=(_MARKET_META_SERVICE,),
        )
    except _GateScanUnavailable as exc:
        ctx.blocked(
            "code_gate_market_meta_service",
            f"scan unavailable: {exc}",
            "reinstall the pmcopy package or run doctor from the source checkout",
        )
    else:
        if forbidden_meta:
            ctx.blocked(
                "code_gate_market_meta_service",
                f"references found: {forbidden_meta[:3]}",
                "remove legacy meta-service references",
            )
        else:
            ctx.ok(
                "code_gate_market_meta_service",
                f"no legacy meta-service references ({gate_provenance})",
            )

    try:
        alias_violations, gate_provenance = _code_gate_coordinator_alias(repo_root)
    except _GateScanUnavailable as exc:
        ctx.blocked(
            "code_gate_coordinator_alias",
            f"scan unavailable: {exc}",
            "reinstall the pmcopy package or run doctor from the source checkout",
        )
    else:
        if alias_violations:
            ctx.blocked(
                "code_gate_coordinator_alias",
                f"unguarded aliases: {alias_violations[:3]}",
                "ensure each fallback co-located with execution_coordinator_missing guard",
            )
        else:
            ctx.ok("code_gate_coordinator_alias", f"all aliases guarded ({gate_provenance})")

    # ----- log paths ----------------------------------------------------
    try:
        log_dir = _resolve_log_dir(config)
        log_dir.mkdir(parents=True, exist_ok=True)
        probe = log_dir / ".doctor_probe"
        probe.write_text("ok", encoding="utf-8")
        probe.unlink()
        ctx.ok("log_directory_writable", str(log_dir))
    except Exception as exc:
        ctx.blocked(
            "log_directory_writable",
            f"log directory not writable: {exc}",
            "set log_dir in config/logging.yml",
        )
    try:
        jsonl = _resolve_jsonl_path(config)
        jsonl.parent.mkdir(parents=True, exist_ok=True)
        with jsonl.open("a", encoding="utf-8"):
            pass
        ctx.ok("jsonl_log_writable", str(jsonl))
    except Exception as exc:
        ctx.blocked(
            "jsonl_log_writable",
            f"jsonl path not writable: {exc}",
            "set app_log_jsonl_file in config/logging.yml",
        )

    # ----- log type registration --------------------------------------
    try:
        from pmcopy.utils.logger import RichConsoleHandler

        if "order_lifecycle" in RichConsoleHandler._JSONL_ONLY_TYPES:
            ctx.blocked(
                "order_lifecycle_logs_enabled",
                "order_lifecycle is JSONL-only",
                "remove order_lifecycle from _JSONL_ONLY_TYPES",
            )
        else:
            ctx.ok("order_lifecycle_logs_enabled", "console rendering enabled")
        if "position_drift" in RichConsoleHandler._JSONL_ONLY_TYPES:
            ctx.blocked(
                "position_drift_logs_enabled",
                "position_drift is JSONL-only",
                "remove from _JSONL_ONLY_TYPES",
            )
        else:
            ctx.ok("position_drift_logs_enabled", "console rendering enabled")
    except Exception as exc:
        ctx.warning("logger_inspection", f"could not introspect: {exc}", "")

    # ----- metrics port -----------------------------------------------
    if config.app.enable_metrics_server:
        ok, detail = _try_port_bind(config.app.metrics_bind_host, int(config.app.metrics_port))
        if ok:
            ctx.ok("metrics_port_bindable", detail)
        else:
            ctx.warning(
                "metrics_port_bindable",
                detail,
                "free the port or change network.metrics_port",
            )
    else:
        ctx.warning(
            "metrics_port_bindable",
            "metrics server disabled",
            "set network.enable_metrics_server: true",
        )

    # ----- leaders -----------------------------------------------------
    startup_policy = TraderStartupPolicy(auto_trader=auto_trader)
    if auto_trader:
        try:
            restorable = await _automatic_rosters_available(config)
        except Exception as exc:
            ctx.blocked(
                "leaders",
                f"automatic roster database probe failed: {type(exc).__name__}",
                "restore database access before starting automatic traders",
            )
        else:
            if restorable:
                ctx.ok(
                    "leaders", "automatic roster is saved; startup restores it without discovery"
                )
            elif config.discovery.api.has_token:
                ctx.ok("leaders", "initial automatic discovery is configured")
            else:
                ctx.blocked(
                    "leaders",
                    "automatic roster is uninitialized and Falcon credentials are missing",
                    "configure discovery.api.token for the first automatic initialization",
                )
    elif startup_policy.configured_leaders(config):
        ctx.ok("leaders", "leader wallets configured")
    elif not config.discovery.scheduler.start_from_roster:
        ctx.blocked(
            "leaders",
            "no manual leader wallets and roster startup is disabled",
            "configure wallets in config/copy_trading.yml (or environments/live) "
            "or enable discovery.scheduler.start_from_roster",
        )
    else:
        try:
            db_candidate_count = await _auto_trader_db_candidate_count(config)
        except Exception as exc:
            ctx.warning(
                "leaders",
                f"auto detection enabled but DB candidate probe failed: {type(exc).__name__}",
                "investigate discovery DB access; startup bootstrap will re-check candidates before CLOB init",
            )
        else:
            if db_candidate_count > 0:
                ctx.ok(
                    "leaders",
                    f"startup auto detection enabled; {db_candidate_count} eligible DB candidates available",
                )
            elif config.discovery.api.has_token and startup_policy.discovery_loop_enabled(config):
                ctx.ok(
                    "leaders",
                    "no active roster entry yet; startup discovery is configured",
                )
            else:
                ctx.blocked(
                    "leaders",
                    "no manual leaders, no active roster entry, or discovery credentials are missing",
                    "define the leader in config/traders/<name>.yml, or set "
                    "discovery.api.token in secrets.env and enable the discovery scheduler",
                )

    return ctx.to_dict()


# ---------------------------------------------------------------------------
# CLI rendering
# ---------------------------------------------------------------------------
_ICON = {
    _STATUS_OK: "✅",
    _STATUS_WARNING: "⚠️",
    _STATUS_BLOCKED: "❌",
    "skipped": "▫️",
}


def _render_console_summary(result: dict[str, Any]) -> str:
    lines: list[str] = []
    overall = result.get("status", "ok")
    mode = result.get("mode", "?")
    lines.append(f"DOCTOR  {mode}  status={overall}")
    for check in result.get("checks", []):
        icon = _ICON.get(check["status"], "?")
        lines.append(f"{icon} {check['name']:<32} {check.get('detail', '')}")
        fix = check.get("fix")
        if fix:
            lines.append(f"   fix: {fix}")
    return "\n".join(lines)


class AdmittedDoctorCommand:
    """Compose diagnostic reads behind admission before any provider I/O."""

    def __init__(
        self,
        config_loader: Callable[[], AppConfig],
        provider_factory: Callable[[AppConfig], ProviderRuntime],
        checks: Callable[..., Awaitable[dict[str, Any]]],
    ) -> None:
        """Inject configuration, provider lifetime, and existing diagnostic checks."""
        self._config_loader = config_loader
        self._provider_factory = provider_factory
        self._checks = checks

    async def run(self, mode: str) -> dict[str, Any]:
        """Return safe blocked evidence when admission or configuration fails."""
        try:
            config = self._config_loader()
            runtime = self._provider_factory(config)
        except Exception:
            return {
                "status": "blocked",
                "mode": mode,
                "checks": [
                    {
                        "name": "provider_admission",
                        "status": "blocked",
                        "detail": "provider configuration or free-budget admission failed",
                        "fix": "verify provider settings and budget baseline",
                    }
                ],
            }
        try:
            return await self._checks(mode, config=config, provider_runtime=runtime)
        finally:
            await runtime.aclose()


async def _run_doctor_async(args: argparse.Namespace) -> int:
    try:
        output = await AdmittedDoctorCommand(
            load_config, ProviderRuntime.from_config, run_doctor_checks
        ).run("live" if args.live else "paper")
    finally:
        from pmcopy.utils.fetch_data import close_session

        await close_session()
    summary = _render_console_summary(output)
    print(summary)
    print(json.dumps(output, indent=2))
    return 1 if output.get("status") == _STATUS_BLOCKED else 0


def run_doctor(args: argparse.Namespace) -> int:
    """Run deterministic startup diagnostics and return an exit code."""
    return asyncio.run(_run_doctor_async(args))
