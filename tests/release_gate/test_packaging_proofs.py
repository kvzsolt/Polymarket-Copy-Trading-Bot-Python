"""Release-gate: wheel + sdist packaging proof battery (Task 19).

Runs the repeatable ``scripts/verify_package.py`` battery end to end:
builds the wheel and sdist, installs each into a FRESH throwaway virtual
environment (empty CWD, ``PYTHONPATH`` unset, checkout not importable), and
proves entry points, ``python -m pmcopy``, config validation against an
explicit external deployment root, ``importlib.resources`` lookups
(config YAML tree + SQL migrations), a deterministic paper-mode workflow
smoke, deployment-root-honoring writable log paths, and a clean
``pip check``.

Skips gracefully when no wheel build tool is available (neither the
``build`` package nor a ``uv`` binary). Everything else in this file is
covered by the default suite (``tests/test_packaged_resource_resolution.py``
and ``tests/test_completion_package_contents.py``).
"""

from __future__ import annotations

import importlib.util
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]


def _build_tool_available() -> bool:
    if shutil.which("uv"):
        return True
    return importlib.util.find_spec("build") is not None


def test_wheel_sdist_proof_battery_green() -> None:
    if not _build_tool_available():
        pytest.skip("no wheel build tool available (install 'build' or 'uv')")

    result = subprocess.run(
        [sys.executable, str(REPO_ROOT / "scripts" / "verify_package.py")],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=600,
    )

    assert result.returncode == 0, (
        "packaging proof battery failed:\n"
        f"stdout:\n{result.stdout[-8000:]}\nstderr:\n{result.stderr[-4000:]}"
    )
    assert "ALL PACKAGING PROOFS GREEN" in result.stdout
    for kind in ("wheel", "sdist"):
        assert f"PASS [{kind}] entry-point-help" in result.stdout
        assert f"PASS [{kind}] module-help" in result.stdout
        assert f"PASS [{kind}] paper-workflow-smoke" in result.stdout
        assert f"PASS [{kind}] importlib-resources+writable-paths" in result.stdout
