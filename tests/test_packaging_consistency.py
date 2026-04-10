from __future__ import annotations

import fnmatch
import importlib.util
import tomllib
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
CORE_PACKAGES = {
    "config",
    "interfaces",
    "models",
    "services",
    "utils",
    "domain",
    "network",
    "orchestrators",
    "storage",
}


def _read_pyproject() -> dict:
    return tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))


def _read_requirements() -> list[str]:
    requirements_path = REPO_ROOT / "requirements.txt"
    lines = requirements_path.read_text(encoding="utf-8").splitlines()
    return [line.strip() for line in lines if line.strip() and not line.strip().startswith("#")]


def test_pytest_dependency_versions_match_between_pyproject_and_requirements():
    data = _read_pyproject()
    dev_deps = data["project"]["optional-dependencies"]["dev"]
    req_deps = _read_requirements()

    dev_pytest = {dep for dep in dev_deps if dep.startswith("pytest")}
    req_pytest = {dep for dep in req_deps if dep.startswith("pytest")}
    assert dev_pytest == req_pytest


def test_setuptools_includes_core_runtime_packages():
    data = _read_pyproject()
    setuptools_cfg = data["tool"]["setuptools"]
    package_cfg = setuptools_cfg.get("packages")

    if isinstance(package_cfg, list):
        assert CORE_PACKAGES.issubset(set(package_cfg))
        return

    find_cfg = package_cfg.get("find", {}) if isinstance(package_cfg, dict) else {}
    include_patterns = find_cfg.get("include", ["*"])
    exclude_patterns = find_cfg.get("exclude", [])

    for package in CORE_PACKAGES:
        assert any(fnmatch.fnmatch(package, pattern) for pattern in include_patterns)
        assert not any(fnmatch.fnmatch(package, pattern) for pattern in exclude_patterns)


def test_core_packages_resolve_via_import_system():
    for package in CORE_PACKAGES:
        assert importlib.util.find_spec(package) is not None
