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
import socket

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from config.app_config import (
    AppConfig,
    EnvValidationError,
    load_config,
)
from config.env import secret_value


# ---------------------------------------------------------------------------
# Structured check primitives
# ---------------------------------------------------------------------------
_STATUS_OK = "ok"
_STATUS_WARNING = "warning"
_STATUS_BLOCKED = "blocked"

# Severity ordering for "worst-status wins" aggregation.
_STATUS_ORDER = {_STATUS_OK: 0, _STATUS_WARNING: 1, _STATUS_BLOCKED: 2}


@dataclass
class _DoctorContext:
    mode: str
    checks: list[dict[str, Any]] = field(default_factory=list)
    overall: str = _STATUS_OK

    def ok(self, name: str, detail: str = "", fix: str = "") -> None:
        # ``fix`` is accepted for symmetry with warning/blocked at call sites;
        # for ok records the fix string is always blank.
        self._append(name, _STATUS_OK, detail, "")

    def warning(self, name: str, detail: str, fix: str = "") -> None:
        self._append(name, _STATUS_WARNING, detail, fix)

    def blocked(self, name: str, detail: str, fix: str = "") -> None:
        self._append(name, _STATUS_BLOCKED, detail, fix)

    def skipped(self, name: str, detail: str = "") -> None:
        # Skipped checks do not affect overall status.
        self.checks.append({"name": name, "status": "skipped", "detail": detail, "fix": ""})

    def _append(self, name: str, status: str, detail: str, fix: str) -> None:
        self.checks.append({"name": name, "status": status, "detail": detail, "fix": fix})
        if _STATUS_ORDER.get(status, 0) > _STATUS_ORDER.get(self.overall, 0):
            self.overall = status

    def to_dict(self) -> dict[str, Any]:
        return {"status": self.overall, "mode": self.mode, "checks": list(self.checks)}


# ---------------------------------------------------------------------------
# Probes
# ---------------------------------------------------------------------------
def _check_wss_endpoint(host: str, port: int = 443, timeout: float = 3.0) -> tuple[bool, str]:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True, f"{host}:{port} reachable"
    except OSError as exc:
        return False, f"{host}:{port} unreachable: {exc}"


def _check_gateway_ccxt() -> tuple[bool, str]:
    """Return whether the installed CCXT prediction adapter is importable."""

    try:
        import ccxt
        from ccxt.prediction import polymarket as prediction_factory  # noqa: F401
    except Exception as exc:
        return False, f"ccxt_prediction_import_failed: {type(exc).__name__}"
    return True, f"ccxt.prediction.polymarket importable (ccxt {ccxt.__version__})"


def _wallet_signature_detail(
    *,
    detected_signature_type: Any,
    detected_wallet_type: str,
    detected_signer: str | None,
    funder_address: str,
    configured_signature_type: Any,
) -> str:
    """Render preflight diagnostics without treating the config hint as authority."""
    shortened_signer = (detected_signer or "")[:10] + "…" if detected_signer else "unknown"
    shortened_funder = funder_address[:10] + "…" if funder_address else "unknown"
    detail = (
        f"walletType={detected_wallet_type} "
        f"signature_type={int(detected_signature_type)} "
        f"signer={shortened_signer} "
        f"funder={shortened_funder} "
        "ccxt_explicit_funder_authoritative=true"
    )
    if configured_signature_type is not None and int(configured_signature_type) != int(
        detected_signature_type
    ):
        detail += f" configured_hint={int(configured_signature_type)} (diagnostic-only)"
    return detail


_REQUIRED_DB_TABLES = (
    "orders",
    "fills",
    "positions",
    "risk_blocks",
    "order_reservations",
    "processed_trades",
)


async def _missing_db_tables(pool) -> list[str]:
    """Return the list of required tables that are absent from PG.

    Uses ``information_schema.tables`` against the active asyncpg pool
    (long-running bot owns schema lifecycle via ``connect_db()``; the
    doctor is a read-only probe).
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public'
            """
        )
    present = {r["table_name"] for r in rows}
    return [t for t in _REQUIRED_DB_TABLES if t not in present]


def _code_gate_scan(
    *,
    repo_root: Path,
    forbidden_substrings: tuple[str, ...],
    roots: tuple[str, ...] = ("orchestrators", "services", "clients", "domain", "cache", "utils"),
) -> list[tuple[str, str]]:
    """Search *.py under given roots for forbidden substrings. Return [(path, substr)]."""
    hits: list[tuple[str, str]] = []
    for root in roots:
        base = repo_root / root
        if not base.exists():
            continue
        for path in base.rglob("*.py"):
            try:
                text = path.read_text(encoding="utf-8")
            except OSError:
                continue
            for sub in forbidden_substrings:
                if sub in text:
                    hits.append((str(path.relative_to(repo_root)), sub))
    return hits


_ALIAS_ALLOWED = {"services/execution/coordinator.py"}
_ALIAS_PATTERNS = (
    "execution_service.execute",
    'getattr(execution_service, "execute"',
    "getattr(execution_service, 'execute'",
)


def _code_gate_coordinator_alias(repo_root: Path) -> list[str]:
    """Mirror PR6.1 stricter scan. Returns relative paths of unguarded aliases."""
    violations: list[str] = []
    for root in ("orchestrators", "services"):
        base = repo_root / root
        if not base.exists():
            continue
        for path in base.rglob("*.py"):
            rel = str(path.relative_to(repo_root))
            if rel in _ALIAS_ALLOWED:
                continue
            try:
                text = path.read_text(encoding="utf-8")
            except OSError:
                continue
            for pat in _ALIAS_PATTERNS:
                if pat in text and "execution_coordinator_missing" not in text:
                    violations.append(f"{rel}: {pat}")
                    break
    return violations


def _try_port_bind(host: str, port: int) -> tuple[bool, str]:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, port))
        return True, f"bind {host}:{port} ok"
    except OSError as exc:
        return False, f"bind {host}:{port} failed: {exc}"
    finally:
        try:
            sock.close()
        except OSError:
            pass


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _resolve_log_dir(config=None) -> Path:
    if config is not None:
        return Path(config.logging.log_dir)
    from utils.logger import _default_log_dir  # noqa: WPS437

    return _default_log_dir()


def _resolve_jsonl_path(config=None) -> Path:
    from utils.logger import APP_NAME

    if config is not None and str(config.logging.app_log_jsonl_file).strip():
        return Path(config.logging.app_log_jsonl_file)
    return _resolve_log_dir(config) / f"{APP_NAME}.jsonl"


async def _auto_trader_db_candidate_count(config) -> int:
    """Count hysteresis-qualified active roster entries."""
    from storage.postgres.connection import init_pool, shutdown_pool
    from storage.postgres.leaders_repo import LeadersRepo
    from services.discovery.models import discovery_config_hash

    pool = None
    try:
        pool = await init_pool(
            config.database.dsn,
            min_size=1,
            max_size=2,
            command_timeout_seconds=15.0,
        )
        return len(
            await LeadersRepo(pool).list_active(
                limit=config.discovery.scheduler.max_active_leaders,
                config_hash=discovery_config_hash(config.discovery),
            )
        )
    finally:
        if pool is not None:
            try:
                await shutdown_pool()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Main entry
# ---------------------------------------------------------------------------
def register_doctor_command(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("doctor", help="Run diagnostic checks")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--live", action="store_true", help="Check readiness for live trading")
    group.add_argument("--paper", action="store_true", help="Check readiness for paper trading")


async def run_doctor_checks(
    mode: str,
    *,
    config: AppConfig | None = None,
) -> dict[str, Any]:
    """Structured readiness gate. Returns dict shape documented above."""
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
    from storage.postgres.connection import init_pool, shutdown_pool

    pool = None
    db_label = config.database.dsn or "<database.dsn is empty>"
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
        from storage.postgres.orders_repo import OrdersRepo

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
    from orchestrators.startup_health import _check_rpc

    rpc = await _check_rpc(config.polymarket.rpc_url)
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
        from services.compliance import check_geoblock

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
            ("reconciliation_service_wired", "services.execution.reconciliation_service"),
            ("market_data_service_wired", "services.market_data_service"),
            ("position_drift_service_wired", "services.position_drift_service"),
            ("position_action_coordinator_wired", "services.positionActionCoordinator"),
            ("coordinator_wired", "services.execution.coordinator"),
            ("reservation_service_wired", "services.execution.reservation_service"),
        ):
            try:
                __import__(module_path)
                ctx.ok(name, f"{module_path} importable")
            except Exception as exc:
                ctx.blocked(name, f"import failed: {exc}", f"fix {module_path}")

    # ----- readiness keys known ----------------------------------------
    try:
        from services.healthServer import _STRICT_REQUIRED_KEYS, ReadinessTracker

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
                from orchestrators.runtime_bootstrap import startup_drift_check  # noqa: F401

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
    # forbidden tokens are split across two halves below.
    repo_root = _repo_root()
    _PRICE_CACHE = "services." + "price_cache"
    _POSITION_PRICE_CACHE = "PositionPrice" + "Cache"
    _MARKET_META_SERVICE = "MarketMeta" + "Service"
    forbidden_legacy = _code_gate_scan(
        repo_root=repo_root,
        forbidden_substrings=(_PRICE_CACHE, _POSITION_PRICE_CACHE),
    )
    if forbidden_legacy:
        ctx.blocked(
            "code_gate_price_cache",
            f"references found: {forbidden_legacy[:3]}",
            "remove legacy price cache references",
        )
    else:
        ctx.ok("code_gate_price_cache", "no legacy references")

    forbidden_meta = _code_gate_scan(
        repo_root=repo_root,
        forbidden_substrings=(_MARKET_META_SERVICE,),
    )
    if forbidden_meta:
        ctx.blocked(
            "code_gate_market_meta_service",
            f"references found: {forbidden_meta[:3]}",
            "remove legacy meta-service references",
        )
    else:
        ctx.ok("code_gate_market_meta_service", "no legacy meta-service references")

    alias_violations = _code_gate_coordinator_alias(repo_root)
    if alias_violations:
        ctx.blocked(
            "code_gate_coordinator_alias",
            f"unguarded aliases: {alias_violations[:3]}",
            "ensure each fallback co-located with execution_coordinator_missing guard",
        )
    else:
        ctx.ok("code_gate_coordinator_alias", "all aliases guarded")

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
        from utils.logger import RichConsoleHandler

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
    if config.copy_trading.wallets:
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
            elif config.discovery.api.has_token and config.discovery.scheduler.enabled:
                ctx.ok(
                    "leaders",
                    "no active roster entry yet; startup discovery is configured",
                )
            else:
                ctx.blocked(
                    "leaders",
                    "no manual leaders, no active roster entry, or discovery credentials are missing",
                    "set leader.wallets or discovery.api.token in secrets.env and enable the discovery scheduler",
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


async def _run_doctor_async(args: argparse.Namespace) -> int:
    mode = "live" if args.live else "paper"
    output = await run_doctor_checks(mode)
    summary = _render_console_summary(output)
    print(summary)
    print(json.dumps(output, indent=2))
    return 1 if output.get("status") == _STATUS_BLOCKED else 0


def run_doctor(args: argparse.Namespace) -> int:
    return asyncio.run(_run_doctor_async(args))
