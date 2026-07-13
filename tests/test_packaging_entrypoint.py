"""Regression checks for the installed ``pmcopy run`` entry point."""

from __future__ import annotations

import tomllib
from pathlib import Path


def test_top_level_runtime_modules_are_packaged() -> None:
    root = Path(__file__).resolve().parents[1]
    project = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))

    modules = set(project["tool"]["setuptools"]["py-modules"])
    packages = set(project["tool"]["setuptools"]["packages"]["find"]["include"])

    assert {"app", "bootstrap", "bot"} <= modules
    assert "cache*" in packages


def test_docker_import_check_runs_outside_source_tree() -> None:
    root = Path(__file__).resolve().parents[1]
    dockerfile = (root / "Dockerfile").read_text(encoding="utf-8")

    assert 'RUN cd /tmp && python -c "import app;' in dockerfile
