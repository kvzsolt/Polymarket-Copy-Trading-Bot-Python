"""Relocated trading-core primitives must import without any control plane.

Task 11 of the standalone refactor moves the shared primitives the trading
runtime needs (monotonic clock, durable full-stop order-cancel apply, and the
local ``bot trading`` command surface) out of the control-plane namespaces.
This guard simulates the post-Task-12 world: the ``integrations.control_plane``
and ``services.control_plane`` packages are blocked from import entirely, and
the relocated modules must still import and work.

The test runs in a subprocess so the import blocker cannot poison the shared
module cache of the main test process.
"""

from __future__ import annotations

import subprocess
import sys

_BLOCKER = """
import builtins
import importlib.abc
import importlib.machinery
import sys


class _ControlPlaneBlocker(importlib.abc.MetaPathFinder, importlib.abc.Loader):
    BLOCKED_PREFIXES = (
        "integrations.control_plane",
        "services.control_plane",
    )

    def find_spec(self, fullname, path=None, target=None):
        for prefix in self.BLOCKED_PREFIXES:
            if fullname == prefix or fullname.startswith(prefix + "."):
                return importlib.machinery.ModuleSpec(fullname, self)
        return None

    def create_module(self, spec):
        return None

    def exec_module(self, module):
        raise ImportError(
            "control-plane import blocked: " + module.__name__
        )


sys.meta_path.insert(0, _ControlPlaneBlocker())
"""

_VERIFY = """
from adapters.clock import MonotonicClock, SystemMonotonicClock

clock = SystemMonotonicClock()
assert isinstance(clock.monotonic(), float)
assert clock.monotonic() <= clock.monotonic()
assert hasattr(MonotonicClock, "monotonic")

from services.trading_control import (
    NullRuntimeTelemetry,
    OpenOrderControlGateway,
    RuntimeSafetyApplyError,
    RuntimeTelemetry,
    TradingControlRuntimeSignal,
)

import bot_cli.commands.trading as trading_command

assert hasattr(trading_command, "register_trading_command")
assert hasattr(trading_command, "run_trading")

print("cp-free-ok")
"""


def test_relocated_primitives_import_with_control_plane_blocked() -> None:
    """Clock, full-stop apply, and the trading CLI need no control-plane import."""

    completed = subprocess.run(
        [sys.executable, "-c", _BLOCKER + _VERIFY],
        capture_output=True,
        text=True,
        timeout=120,
    )

    assert completed.returncode == 0, (
        f"control-plane-free import failed:\nstdout:\n{completed.stdout}\n"
        f"stderr:\n{completed.stderr}"
    )
    assert "cp-free-ok" in completed.stdout


def test_relocated_modules_declare_no_control_plane_imports() -> None:
    """Static guard: the relocated sources never mention the control plane."""

    from pathlib import Path

    repository_root = Path(__file__).resolve().parents[1]
    relocated = (
        repository_root / "adapters" / "clock.py",
        repository_root / "services" / "trading_control.py",
        repository_root / "bot_cli" / "commands" / "trading.py",
    )
    for path in relocated:
        source = path.read_text(encoding="utf-8")
        assert "control_plane" not in source, (
            f"{path} must not reference control_plane after the relocation"
        )
