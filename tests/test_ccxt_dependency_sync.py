"""Pinned CCXT dependency synchronization contract."""

from __future__ import annotations

from pathlib import Path
import tomllib


def test_venue_dependencies_are_pinned_and_synchronized() -> None:
    """Keep the CCXT-only venue pin in project and export files."""

    root = Path(__file__).resolve().parents[1]
    dependencies = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))["project"][
        "dependencies"
    ]
    requirements = [
        line.strip()
        for line in (root / "requirements.txt").read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]
    runtime_requirements = [
        line.strip()
        for line in (root / "requirements.runtime.txt").read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]

    project_ccxt = [dependency for dependency in dependencies if dependency.startswith("ccxt")]
    exported_ccxt = [dependency for dependency in requirements if dependency.startswith("ccxt")]
    runtime_ccxt = [
        dependency for dependency in runtime_requirements if dependency.startswith("ccxt")
    ]
    assert project_ccxt == ["ccxt==4.5.92"]
    assert exported_ccxt == ["ccxt==4.5.92"]
    assert runtime_ccxt == ["ccxt==4.5.92"]

    # The official SDK must not reappear in any dependency surface.
    for banned in ("polymarket-client", "py-clob-client"):
        assert not any(banned in dep for dep in dependencies), f"{banned} in pyproject"
        assert not any(banned in dep for dep in requirements), f"{banned} in requirements.txt"
        assert not any(banned in dep for dep in runtime_requirements), (
            f"{banned} in requirements.runtime.txt"
        )


def test_standalone_constraint_uses_the_verified_exact_release() -> None:
    """Prevent targeted installs from selecting an unverified patch release."""

    root = Path(__file__).resolve().parents[1]
    constraints = [
        line.strip()
        for line in (root / "requirements.ccxt.txt").read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]

    assert constraints == ["ccxt==4.5.92"]


def test_frozen_installs_resolve_the_verified_ccxt_release() -> None:
    """Keep the lockfile package and root requirement aligned with the manifest."""

    root = Path(__file__).resolve().parents[1]
    packages = tomllib.loads((root / "uv.lock").read_text(encoding="utf-8"))["package"]
    locked_versions = [package["version"] for package in packages if package["name"] == "ccxt"]
    certifi_versions = [package["version"] for package in packages if package["name"] == "certifi"]
    project = next(
        package for package in packages if package["name"] == "polymarket-copy-trading-bot"
    )
    ccxt_requirements = [
        dependency["specifier"]
        for dependency in project["metadata"]["requires-dist"]
        if dependency["name"] == "ccxt"
    ]

    assert locked_versions == ["4.5.92"]
    assert ccxt_requirements == ["==4.5.92"]
    assert certifi_versions and tuple(int(p) for p in certifi_versions[0].split(".")) >= (
        2026,
        10,
        18,
    )
    assert not {"polymarket-client", "py-clob-client"}.intersection(
        package["name"] for package in packages
    )
