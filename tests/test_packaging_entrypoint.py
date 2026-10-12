"""Regression checks for the installed ``pmcopy run`` entry point."""

from __future__ import annotations

import tomllib
from pathlib import Path


def test_top_level_runtime_modules_are_packaged() -> None:
    root = Path(__file__).resolve().parents[1]
    project = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))

    find_cfg = project["tool"]["setuptools"]["packages"]["find"]
    packages = set(find_cfg["include"])

    assert find_cfg["where"] == ["src"]
    assert packages == {"pmcopy*"}
    assert "py-modules" not in project["tool"]["setuptools"]


def test_docker_import_check_runs_outside_source_tree() -> None:
    root = Path(__file__).resolve().parents[1]
    dockerfile = (root / "Dockerfile").read_text(encoding="utf-8")

    assert any(
        line.strip().startswith('RUN cd /tmp && python -c "') for line in dockerfile.splitlines()
    ), "Docker import verification must run outside the /app source tree"
