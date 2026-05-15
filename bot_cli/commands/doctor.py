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
from services.wallet_signature_detector import detect_wallet_signature_type

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from config.app_config import (
    EnvValidationError,
    load_config,
)
from config.env import ENV, rehydrate_env
from config.runtime_env import load_runtime_env
from storage.sqlite.connection import create_connection


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
        self.checks.append(
            {"name": name, "status": "skipped", "detail": detail, "fix": ""}
        )

    def _append(self, name: str, status: str, detail: str, fix: str) -> None:
        self.checks.append(
            {"name": name, "status": status, "detail": detail, "fix": fix}
        )
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


def _check_gateway_sdk() -> tuple[bool, str]:
    try:
        from utils.createClobClient import ClobClient  # noqa: F401
    except Exception as exc:
        return False, f"clob_sdk_import_failed: {exc}"
    return True, "CLOB SDK importable"


_REQUIRED_DB_TABLES = (
    "orders",
    "fills",
    "positions",
    "risk_blocks",
    "order_reservations",
    "processed_trades",
)


def _missing_db_tables(db_path: str) -> list[str]:
    with create_connection(db_path) as conn:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        present = {r["name"] for r in rows}
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


def _resolve_log_dir() -> Path:
    from utils.logger import _default_log_dir  # noqa: WPS437

    return _default_log_dir()


def _resolve_jsonl_path() -> Path:
    import os

    from utils.logger import APP_NAME

    override = (os.getenv("APP_LOG_JSONL_FILE") or "").strip()
    return Path(override) if override else (_resolve_log_dir() / f"{APP_NAME}.jsonl")


# ---------------------------------------------------------------------------
# Main entry
# ---------------------------------------------------------------------------
def register_doctor_command(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("doctor", help="Run diagnostic checks")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--live", action="store_true", help="Check readiness for live trading")
    group.add_argument("--paper", action="store_true", help="Check readiness for paper trading")


async def run_doctor_checks(mode: str) -> dict[str, Any]:
    """Structured readiness gate. Returns dict shape documented above."""
    ctx = _DoctorContext(mode=mode)

    # ----- config_load --------------------------------------------------
    try:
        load_runtime_env()
        rehydrate_env()
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
        if config.execution.paper_trade_only:
            ctx.blocked(
                "paper_trade_only_false",
                "PAPER_TRADE_ONLY=true in live mode",
                "set PAPER_TRADE_ONLY=false",
            )
        else:
            ctx.ok("paper_trade_only_false", "PAPER_TRADE_ONLY=false")
        if config.dry_run:
            ctx.blocked(
                "dry_run_false",
                "dry_run=true",
                "disable PAPER_TRADE_ONLY env",
            )
        else:
            ctx.ok("dry_run_false", "dry_run=false")
        # Prefer config.raw (the parsed env mapping the doctor's load_config
        # actually consumed) over the global ENV dict so test injections take
        # effect without monkeypatching os.environ.
        raw_clob = config.raw.get("CLOB_V2_READY") if hasattr(config, "raw") else None
        if raw_clob is None:
            raw_clob = ENV.get("CLOB_V2_READY", False)
        clob_ready = (
            bool(raw_clob)
            if not isinstance(raw_clob, str)
            else str(raw_clob).strip().lower() in {"1", "true", "yes"}
        )
        if not clob_ready:
            ctx.blocked(
                "clob_v2_ready",
                "CLOB_V2_READY is not true",
                "set CLOB_V2_READY=true after manually confirming readiness",
            )
        else:
            ctx.ok("clob_v2_ready", "CLOB_V2_READY=true")

    # ----- kill switch --------------------------------------------------
    if config.risk.kill_switch:
        ctx.blocked(
            "kill_switch_off",
            "KILL_SWITCH=true",
            "set KILL_SWITCH=false to enable trading",
        )
    else:
        ctx.ok("kill_switch_off", "KILL_SWITCH=false")

    # ----- wallet basics ------------------------------------------------
    if live:
        if not config.wallet.private_key:
            ctx.blocked(
                "wallet_private_key",
                "missing PRIVATE_KEY",
                "set PRIVATE_KEY or PRIVATE_KEY_FILE",
            )
        else:
            ctx.ok("wallet_private_key", "PRIVATE_KEY present")
        funder = config.wallet.funder_address or ""
        if re.match(r"^0x[0-9a-fA-F]{40}$", funder):
            ctx.ok("wallet_funder_valid", funder)
        else:
            ctx.blocked(
                "wallet_funder_valid",
                f"invalid FUNDER_ADDRESS: {funder!r}",
                "set FUNDER_ADDRESS to a 42-char hex address",
            )
        try:
            detected_sig, detected_wallet_type, detected_signer = await detect_wallet_signature_type(
                config.wallet, fail_closed_on_rpc_error=True
            )
        except RuntimeError as exc:
            ctx.blocked(
                "wallet_signature_type",
                f"RPC probe failed: {exc}",
                "verify RPC_URL connectivity or set SIGNATURE_TYPE explicitly",
            )
        else:
            configured_sig = getattr(config.wallet, "signature_type", None)
            shortened_signer = (detected_signer or "")[:10] + "…" if detected_signer else "unknown"
            shortened_funder = (config.wallet.funder_address or "")[:10] + "…" if config.wallet.funder_address else "unknown"
            detail = (
                f"walletType={detected_wallet_type} "
                f"signature_type={int(detected_sig)} "
                f"signer={shortened_signer} "
                f"funder={shortened_funder}"
            )
            if configured_sig is not None and int(configured_sig) != int(detected_sig):
                detail += f" runtime_autodetect=override (configured={int(configured_sig)})"
            ctx.ok("wallet_signature_type", detail)

    # ----- database -----------------------------------------------------
    try:
        with create_connection(config.database.db_path) as conn:
            conn.execute("PRAGMA user_version;")
        ctx.ok("db_writable", config.database.db_path)
    except Exception as exc:
        ctx.blocked(
            "db_writable",
            f"db not writable: {exc}",
            f"ensure {config.database.db_path} is writable",
        )
    try:
        missing = _missing_db_tables(config.database.db_path)
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
        with create_connection(config.database.db_path) as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS c FROM sqlite_master WHERE type='table' AND name='orders'"
            ).fetchone()
            if row and row["c"] > 0:
                ttl = config.execution.pending_submission_ttl_seconds
                stale = conn.execute(
                    "SELECT COUNT(*) AS c FROM orders "
                    "WHERE status IN ('submitted_unconfirmed','delayed','pending','pending_lock','submitting') "
                    "AND (strftime('%s','now') - strftime('%s', created_at)) > ?",
                    (ttl,),
                ).fetchone()
                count = stale["c"] if stale else 0
                if count:
                    ctx.warning(
                        "stale_idempotency",
                        f"{count} stale pending orders",
                        "run stale-lock cleanup before live",
                    )
                else:
                    ctx.ok("stale_idempotency", "no stale pending orders")
            else:
                ctx.skipped("stale_idempotency", "orders table missing")
    except Exception as exc:
        ctx.warning("stale_idempotency", f"probe failed: {exc}", "")

    # ----- RPC ----------------------------------------------------------
    from orchestrators.startup_health import _check_rpc

    rpc = await _check_rpc(config.network.rpc_url)
    if getattr(rpc, "status", "") == "error":
        ctx.blocked(
            "network_rpc",
            f"rpc unreachable: {getattr(rpc, 'details', '')}",
            "verify RPC_URL",
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
    if config.network.enforce_geoblock:
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

    # ----- gateway SDK --------------------------------------------------
    if live:
        ok, detail = _check_gateway_sdk()
        (ctx.ok if ok else ctx.blocked)(
            "gateway_sdk", detail, "" if ok else "install py-clob-client-v2"
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
        if config.position_drift.check_on_startup:
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
        log_dir = _resolve_log_dir()
        log_dir.mkdir(parents=True, exist_ok=True)
        probe = log_dir / ".doctor_probe"
        probe.write_text("ok", encoding="utf-8")
        probe.unlink()
        ctx.ok("log_directory_writable", str(log_dir))
    except Exception as exc:
        ctx.blocked(
            "log_directory_writable",
            f"log directory not writable: {exc}",
            "set APP_LOG_DIR to a writable path",
        )
    try:
        jsonl = _resolve_jsonl_path()
        jsonl.parent.mkdir(parents=True, exist_ok=True)
        with jsonl.open("a", encoding="utf-8"):
            pass
        ctx.ok("jsonl_log_writable", str(jsonl))
    except Exception as exc:
        ctx.blocked(
            "jsonl_log_writable",
            f"jsonl path not writable: {exc}",
            "set APP_LOG_JSONL_FILE to a writable path",
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
    if config.network.enable_metrics_server:
        ok, detail = _try_port_bind(
            config.network.metrics_bind_host, int(config.network.metrics_port)
        )
        if ok:
            ctx.ok("metrics_port_bindable", detail)
        else:
            ctx.warning(
                "metrics_port_bindable",
                detail,
                "free the port or change METRICS_PORT",
            )
    else:
        ctx.warning(
            "metrics_port_bindable",
            "metrics server disabled",
            "enable ENABLE_METRICS_SERVER for live observability",
        )

    # ----- leaders -----------------------------------------------------
    if not config.leader.wallets and not config.leader.auto_discover_leaders:
        ctx.blocked(
            "leaders",
            "no leader wallets and discovery disabled",
            "set LEADER_WALLETS or LEADER_AUTO=true",
        )
    else:
        ctx.ok("leaders", "leaders configured or auto-discovery enabled")

    if config.near_one_exit.enable_near_one_exit:
        threshold = float(config.near_one_exit.threshold)
        if not (0.5 < threshold < 1.0):
            ctx.blocked(
                "near_one_exit",
                f"threshold out of range: {threshold}",
                "0.5 < NEAR_ONE_EXIT_THRESHOLD < 1.0",
            )
        elif config.near_one_exit.dwell_seconds <= 0:
            ctx.blocked(
                "near_one_exit",
                f"dwell_seconds invalid: {config.near_one_exit.dwell_seconds}",
                "set NEAR_ONE_EXIT_DWELL_SECONDS > 0",
            )
        else:
            ctx.ok(
                "near_one_exit",
                f"threshold={threshold} dwell={config.near_one_exit.dwell_seconds}",
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
        lines.append(
            f"{icon} {check['name']:<32} {check.get('detail', '')}"
        )
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
