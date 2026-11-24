"""Doctor readiness check primitives, code-gate scanners, and DB/network probes."""

from __future__ import annotations

import re
import socket
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


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
        """Return a successful diagnostic result."""
        self._append(name, _STATUS_OK, detail, "")

    def warning(self, name: str, detail: str, fix: str = "") -> None:
        """Return a warning diagnostic result."""
        self._append(name, _STATUS_WARNING, detail, fix)

    def blocked(self, name: str, detail: str, fix: str = "") -> None:
        """Return a blocked diagnostic result."""
        self._append(name, _STATUS_BLOCKED, detail, fix)

    def skipped(self, name: str, detail: str = "") -> None:
        # Skipped checks do not affect overall status.
        """Return a skipped diagnostic result."""
        self.checks.append({"name": name, "status": "skipped", "detail": detail, "fix": ""})

    def _append(self, name: str, status: str, detail: str, fix: str) -> None:
        self.checks.append({"name": name, "status": status, "detail": detail, "fix": fix})
        if _STATUS_ORDER.get(status, 0) > _STATUS_ORDER.get(self.overall, 0):
            self.overall = status

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable representation."""
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


# Roots the code gates scan in a source checkout, and the matching
# installed ``pmcopy`` subpackages for wheel deployments.
_GATE_SOURCE_ROOTS = (
    "src/pmcopy/orchestrators",
    "src/pmcopy/services",
    "src/pmcopy/clients",
    "src/pmcopy/domain",
    "src/pmcopy/cache",
    "src/pmcopy/utils",
)
_GATE_INSTALLED_PACKAGES = tuple(
    root[len("src/") :].replace("/", ".") for root in _GATE_SOURCE_ROOTS
)
# The coordinator-alias gate keeps its historical (narrower) scope.
_ALIAS_ROOTS = ("src/pmcopy/orchestrators", "src/pmcopy/services")


class _GateScanUnavailable(RuntimeError):
    """Neither the source tree nor the installed package could be scanned."""


def _gate_source_files(repo_root: Path, roots: tuple[str, ...]) -> list[tuple[str, Path]] | None:
    """Return gated ``(display path, absolute path)`` pairs from the source tree.

    ``None`` means none of the gated roots exist here — an installed wheel
    has no source tree, which is exactly where the old scanner silently
    scanned zero files and still reported a clean pass.
    """
    if not any((repo_root / root).exists() for root in roots):
        return None
    files: list[tuple[str, Path]] = []
    for root in roots:
        base = repo_root / root
        if not base.exists():
            continue
        for path in base.rglob("*.py"):
            files.append((str(path.relative_to(repo_root)), path))
    return files


def _gate_installed_files(roots: tuple[str, ...]) -> tuple[list[tuple[str, Path]], str]:
    """Return gated ``(display path, absolute path)`` pairs from the install.

    Display paths are relative to the package parent (site-packages or the
    editable source root), so an installed ``services/execution/x.py``
    reports as ``pmcopy/services/execution/x.py``.
    """
    import importlib.util

    files: list[tuple[str, Path]] = []
    provenance_root: Path | None = None
    for root in roots:
        module_name = root[len("src/") :].replace("/", ".")
        spec = importlib.util.find_spec(module_name)
        if spec is None or not spec.submodule_search_locations:
            # Mirror the source scanner's tolerance for individually
            # absent roots; the all-missing case fails closed below.
            continue
        base = Path(list(spec.submodule_search_locations)[0])
        package_parent = base.parent.parent
        if provenance_root is None:
            provenance_root = package_parent
        for path in sorted(base.rglob("*.py")):
            files.append((str(path.relative_to(package_parent)), path))
    if provenance_root is None or not files:
        raise _GateScanUnavailable(
            "the gated pmcopy subpackages are not importable from the running interpreter"
        )
    return files, str(provenance_root)


def _gate_files(
    repo_root: Path, roots: tuple[str, ...] = _GATE_SOURCE_ROOTS
) -> tuple[list[tuple[str, Path]], str]:
    """Locate the modules a code gate scans, with scan provenance.

    A source checkout scans the repository roots. An installed wheel has
    no source tree, so the same gate scans the installed ``pmcopy``
    subpackages — the code that actually runs in that deployment — and the
    ok detail carries the site-packages provenance plus the file count.
    Fails closed (``_GateScanUnavailable``) when neither can be located: a
    gate that scanned nothing must never report a clean result.
    """
    source_files = _gate_source_files(repo_root, roots)
    if source_files is not None:
        if not source_files:
            raise _GateScanUnavailable(
                f"gated source roots exist under {repo_root} but contain no Python files"
            )
        return source_files, f"source tree scan, {len(source_files)} files under {repo_root}"
    installed_files, provenance_root = _gate_installed_files(roots)
    return installed_files, (
        f"installed package scan, {len(installed_files)} files under "
        f"{provenance_root} (source tree absent)"
    )


def _code_gate_scan(
    *,
    repo_root: Path,
    forbidden_substrings: tuple[str, ...],
    roots: tuple[str, ...] = _GATE_SOURCE_ROOTS,
) -> tuple[list[tuple[str, str]], str]:
    """Search gated modules for forbidden substrings.

    Returns ``(hits, provenance)`` — each hit is ``(display path,
    substring)`` and provenance names what was actually scanned. Raises
    ``_GateScanUnavailable`` when nothing could be scanned, so the gate can
    never pass vacuously.
    """
    files, provenance = _gate_files(repo_root, roots)
    hits: list[tuple[str, str]] = []
    for display_path, path in files:
        try:
            text = path.read_text(encoding="utf-8")
        except OSError:
            continue
        for sub in forbidden_substrings:
            if sub in text:
                hits.append((display_path, sub))
    return hits, provenance


_ALIAS_ALLOWED = {
    "src/pmcopy/services/execution/coordinator.py",
    # The same guarded file under the installed-package path spelling.
    "pmcopy/services/execution/coordinator.py",
}
_ALIAS_PATTERNS = (
    "execution_service.execute",
    'getattr(execution_service, "execute"',
    "getattr(execution_service, 'execute'",
)


def _code_gate_coordinator_alias(
    repo_root: Path, roots: tuple[str, ...] = _ALIAS_ROOTS
) -> tuple[list[str], str]:
    """Mirror PR6.1 stricter scan. Returns unguarded aliases plus provenance."""
    files, provenance = _gate_files(repo_root, roots)
    violations: list[str] = []
    for display_path, path in files:
        if display_path in _ALIAS_ALLOWED:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except OSError:
            continue
        for pat in _ALIAS_PATTERNS:
            if pat in text and "execution_coordinator_missing" not in text:
                violations.append(f"{display_path}: {pat}")
                break
    return violations, provenance


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


def _redact_dsn(dsn: str) -> str:
    """Return ``dsn`` with any ``user:password@`` credential masked.

    Doctor output is printed to the console and pasted into tickets, so the
    Postgres password must never survive into the ``db_writable`` detail.
    """
    if not dsn:
        return "<database.dsn is empty>"
    return re.sub(r"(://[^:/@\s]+):[^@\s]*@", r"\1:***@", dsn)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[4]


def _resolve_log_dir(config=None) -> Path:
    if config is not None:
        return Path(config.logging.log_dir)
    from pmcopy.utils.logger import _default_log_dir  # noqa: WPS437

    return _default_log_dir()


def _resolve_jsonl_path(config=None) -> Path:
    from pmcopy.utils.logger import APP_NAME

    if config is not None and str(config.logging.app_log_jsonl_file).strip():
        return Path(config.logging.app_log_jsonl_file)
    return _resolve_log_dir(config) / f"{APP_NAME}.jsonl"


async def _automatic_rosters_available(config) -> bool:
    """Compose a local durable startup probe without contacting Falcon."""
    from pmcopy.domain.auto_trader import AutoTraderScope
    from pmcopy.orchestrators.discovery.auto_trader_startup import AutoTraderStartupProbe
    from pmcopy.services.accounts.multi_user_config import load_runtime_users
    from pmcopy.storage.postgres.auto_trader_repository import PostgresAutoTraderRepository
    from pmcopy.storage.postgres.connection import init_pool, shutdown_pool
    from pmcopy.storage.postgres.leaders_repo import LeadersRepo

    pool = await init_pool(
        config.database.dsn, min_size=1, max_size=2, command_timeout_seconds=15.0
    )
    try:
        repository = PostgresAutoTraderRepository(pool)
        if not await repository.schema_available():
            return bool(await LeadersRepo(pool).list_active())
        return await AutoTraderStartupProbe(
            repository=repository,
            scopes=tuple(
                AutoTraderScope(user.user_id, config.execution.trading_mode)
                for user in load_runtime_users([], config)
            ),
            legacy_wallets=LeadersRepo(pool).list_active,
        ).restorable()
    finally:
        await shutdown_pool()


async def _auto_trader_db_candidate_count(config) -> int:
    """Count hysteresis-qualified active roster entries."""
    from pmcopy.storage.postgres.connection import init_pool, shutdown_pool
    from pmcopy.storage.postgres.leaders_repo import LeadersRepo
    from pmcopy.services.discovery.models import discovery_config_hash

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
