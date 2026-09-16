"""Installed code must include its repository ports and ordered SQL migrations."""

import fnmatch
from pathlib import Path
import tomllib


def test_package_discovery_includes_runtime_repository_ports():
    """OrdersRepo imports ports.repositories in an installed application."""
    project = tomllib.loads((Path(__file__).parents[1] / "pyproject.toml").read_text())
    patterns = project["tool"]["setuptools"]["packages"]["find"]["include"]
    assert any(fnmatch.fnmatchcase("ports", pattern) for pattern in patterns)


def test_package_data_includes_runtime_sql_migrations():
    """V1MigrationCatalog must find the same files outside a source checkout."""
    project = tomllib.loads((Path(__file__).parents[1] / "pyproject.toml").read_text())
    data = project["tool"]["setuptools"]["package-data"]
    assert "migrations/*.sql" in data.get("storage.postgres", [])
