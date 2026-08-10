"""Dependency-drift guard tests (runtime-hardening).

``scripts/check_requirements_drift.py`` fails CI when a ``[project].dependencies``
entry in pyproject.toml has no matching distribution in requirements.txt — the
exact gap that broke the Docker / documented install path for ``asyncpg``.
"""

from __future__ import annotations

import pathlib

from scripts.check_requirements_drift import find_missing


def test_reports_missing_runtime_dep():
    pyproject = """
[project]
dependencies = ["aiohttp==3.13.5", "asyncpg>=0.30.0"]
"""
    requirements = "aiohttp==3.13.5\n"

    missing = find_missing(pyproject, requirements)

    assert missing == ["asyncpg>=0.30.0"]


def test_clean_when_all_present():
    pyproject = """
[project]
dependencies = ["aiohttp==3.13.5", "asyncpg>=0.30.0"]
"""
    requirements = "aiohttp==3.13.5\nasyncpg>=0.30.0\n"

    assert find_missing(pyproject, requirements) == []


def test_name_match_ignores_version_and_case_and_underscores():
    pyproject = """
[project]
dependencies = ["Python-Dotenv==1.2.2"]
"""
    # requirements pins a different version + uses different casing/separator.
    requirements = "python_dotenv==1.0.0\n"

    assert find_missing(pyproject, requirements) == []


def test_dev_extras_are_not_required_in_requirements():
    pyproject = """
[project]
dependencies = ["aiohttp==3.13.5"]
[project.optional-dependencies]
dev = ["testcontainers>=4.0.0"]
"""
    requirements = "aiohttp==3.13.5\n"

    assert find_missing(pyproject, requirements) == []


def test_repo_is_drift_free():
    root = pathlib.Path(__file__).resolve().parents[1]
    pyproject = (root / "pyproject.toml").read_text()
    requirements = (root / "requirements.txt").read_text()

    assert find_missing(pyproject, requirements) == []
