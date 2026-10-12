"""Relocated trading-core primitives and composition roots need no control plane.

Task 11 of the standalone refactor moved the shared primitives the trading
runtime needs (monotonic clock, durable full-stop order-cancel apply, and the
local ``bot trading`` command surface) out of the control-plane namespaces.
Task 12 deletes the control-plane packages entirely. These guards simulate the
post-deletion world: every removed control-plane module is blocked from import
(or absent from disk), and the composition roots must still import cleanly and
register none of the removed modules.

The subprocess tests run in a child process so the import blocker cannot poison
the shared module cache of the main test process.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]

_REMOVED_MODULE_PREFIXES = (
    "pmcopy.integrations.control_plane",
    "pmcopy.services.control_plane",
)

_REMOVED_MODULE_NAMES = (
    "pmcopy.orchestrators.control_plane_auto_traders",
    "pmcopy.orchestrators.control_plane_bootstrap",
    "pmcopy.orchestrators.control_plane_discovery",
    "pmcopy.orchestrators.control_plane_portfolio",
    "pmcopy.orchestrators.control_plane_reads",
    "pmcopy.orchestrators.control_plane_runtime",
    "pmcopy.orchestrators.control_plane_settings",
    "pmcopy.orchestrators.control_plane_tasks",
    "pmcopy.storage.postgres.control_plane_audit_repository",
    "pmcopy.storage.postgres.control_plane_entry_control_repository",
    "pmcopy.storage.postgres.control_plane_query_repository",
    "pmcopy.storage.postgres.control_plane_reentry_lock_repository",
    "pmcopy.storage.postgres.control_plane_task_bulk_repository",
)

_BLOCKER = """
import builtins
import importlib.abc
import importlib.machinery
import sys


class _ControlPlaneBlocker(importlib.abc.MetaPathFinder, importlib.abc.Loader):
    BLOCKED_PREFIXES = %r
    BLOCKED_NAMES = %r

    def _blocked(self, fullname):
        for prefix in self.BLOCKED_PREFIXES:
            if fullname == prefix or fullname.startswith(prefix + "."):
                return True
        return fullname in self.BLOCKED_NAMES

    def find_spec(self, fullname, path=None, target=None):
        if self._blocked(fullname):
            return importlib.machinery.ModuleSpec(fullname, self)
        return None

    def create_module(self, spec):
        return None

    def exec_module(self, module):
        raise ImportError(
            "control-plane import blocked: " + module.__name__
        )


sys.meta_path.insert(0, _ControlPlaneBlocker())
""" % (
    _REMOVED_MODULE_PREFIXES,
    _REMOVED_MODULE_NAMES,
)

_VERIFY = """
from pmcopy.adapters.clock import MonotonicClock, SystemMonotonicClock

clock = SystemMonotonicClock()
assert isinstance(clock.monotonic(), float)
assert clock.monotonic() <= clock.monotonic()
assert hasattr(MonotonicClock, "monotonic")

from pmcopy.services.trading_control import (
    NullRuntimeTelemetry,
    OpenOrderControlGateway,
    RuntimeSafetyApplyError,
    RuntimeTelemetry,
    TradingControlRuntimeSignal,
)

import pmcopy.bot_cli.commands.trading as trading_command

assert hasattr(trading_command, "register_trading_command")
assert hasattr(trading_command, "run_trading")

print("cp-free-ok")
"""

_VERIFY_COMPOSITION_ROOTS = """
import sys

import pmcopy.app  # noqa: F401  (composition root: must import with CP blocked)
import pmcopy.bootstrap  # noqa: F401
import pmcopy.bot_cli.main as cli_main  # noqa: F401

blocked_prefixes = %r
blocked_names = %r

registered = set(sys.modules)
for name in registered:
    for prefix in blocked_prefixes:
        assert not (name == prefix or name.startswith(prefix + ".")), (
            "composition root registered removed module: " + name
        )
    assert name not in blocked_names, (
        "composition root registered removed module: " + name
    )

print("composition-roots-cp-free-ok")
""" % (
    _REMOVED_MODULE_PREFIXES,
    _REMOVED_MODULE_NAMES,
)


def _run_subprocess(script: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=300,
        cwd=REPOSITORY_ROOT,
        env={**os.environ, "PYTHONPATH": str(REPOSITORY_ROOT / "src")},
    )


def test_relocated_primitives_import_with_control_plane_blocked() -> None:
    """Clock, full-stop apply, and the trading CLI need no control-plane import."""

    completed = _run_subprocess(_BLOCKER + _VERIFY)

    assert completed.returncode == 0, (
        f"control-plane-free import failed:\\nstdout:\\n{completed.stdout}\\n"
        f"stderr:\\n{completed.stderr}"
    )
    assert "cp-free-ok" in completed.stdout


def test_composition_roots_import_and_register_no_control_plane() -> None:
    """Standalone startup imports register none of the removed CP modules."""

    completed = _run_subprocess(_BLOCKER + _VERIFY_COMPOSITION_ROOTS)

    assert completed.returncode == 0, (
        f"control-plane-free composition-root import failed:\\n"
        f"stdout:\\n{completed.stdout}\\nstderr:\\n{completed.stderr}"
    )
    assert "composition-roots-cp-free-ok" in completed.stdout


def test_relocated_modules_declare_no_control_plane_imports() -> None:
    """Static guard: the relocated sources never mention the control plane."""

    relocated = (
        REPOSITORY_ROOT / "src" / "pmcopy" / "adapters" / "clock.py",
        REPOSITORY_ROOT / "src" / "pmcopy" / "services" / "trading_control.py",
        REPOSITORY_ROOT / "src" / "pmcopy" / "bot_cli" / "commands" / "trading.py",
    )
    for path in relocated:
        source = path.read_text(encoding="utf-8")
        assert "control_plane" not in source, (
            f"{path} must not reference control_plane after the relocation"
        )


def test_removed_control_plane_surfaces_are_absent_from_disk() -> None:
    """Deletion guard: the removed packages and orchestrators stay deleted."""

    removed_paths = (
        REPOSITORY_ROOT / "src" / "pmcopy" / "integrations" / "control_plane",
        REPOSITORY_ROOT / "src" / "pmcopy" / "services" / "control_plane",
        REPOSITORY_ROOT / "tests" / "control_plane",
        REPOSITORY_ROOT / "tests" / "control_plane_http",
        REPOSITORY_ROOT / "run_ccxt.py",
        REPOSITORY_ROOT / "docs" / "ui",
        REPOSITORY_ROOT / "docs" / "frontend-control-plane.md",
        *(
            REPOSITORY_ROOT / "src" / "pmcopy" / "orchestrators" / f"control_plane_{stem}.py"
            for stem in (
                "auto_traders",
                "bootstrap",
                "discovery",
                "portfolio",
                "reads",
                "runtime",
                "settings",
                "tasks",
            )
        ),
        *(
            REPOSITORY_ROOT / "src" / "pmcopy" / "storage" / "postgres" / f"control_plane_{stem}_repository.py"
            for stem in ("audit", "entry_control", "query", "reentry_lock", "task_bulk")
        ),
        *(
            REPOSITORY_ROOT / "scripts" / f"{stem}.py"
            for stem in (
                "export_api_contract",
                "api_contract_introspection",
                "api_contract_tables",
                "api_contract_wire",
            )
        ),
    )
    for path in removed_paths:
        assert not path.exists(), f"{path} must stay deleted after Task 12"


def test_kept_trading_survivors_never_import_control_plane() -> None:
    """Static guard: kept surfaces are free of control-plane imports."""

    kept = (
        REPOSITORY_ROOT / "src" / "pmcopy" / "services" / "healthServer.py",
        REPOSITORY_ROOT / "src" / "pmcopy" / "orchestrators" / "liveness_heartbeat.py",
        REPOSITORY_ROOT / "src" / "pmcopy" / "services" / "trading_permission.py",
        REPOSITORY_ROOT / "src" / "pmcopy" / "storage" / "postgres" / "trading_control_repo.py",
        REPOSITORY_ROOT / "src" / "pmcopy" / "storage" / "postgres" / "market_reentry_lock_repository.py",
        REPOSITORY_ROOT / "src" / "pmcopy" / "services" / "telemetry",
        REPOSITORY_ROOT / "src" / "pmcopy" / "orchestrators" / "telemetry_bootstrap.py",
        REPOSITORY_ROOT / "src" / "pmcopy" / "services" / "trading_control.py",
        REPOSITORY_ROOT / "src" / "pmcopy" / "adapters" / "clock.py",
        REPOSITORY_ROOT / "src" / "pmcopy" / "bot_cli",
    )
    for path in kept:
        sources = [path] if path.is_file() else sorted(path.rglob("*.py"))
        assert sources, f"missing kept surface: {path}"
        for source in sources:
            text = source.read_text(encoding="utf-8")
            assert "control_plane" not in text, (
                f"{source} must not reference control_plane after Task 12"
            )
