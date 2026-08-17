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
    assert project_ccxt == ["ccxt==4.5.73"]
    assert exported_ccxt == ["ccxt==4.5.73"]
    assert runtime_ccxt == ["ccxt==4.5.73"]

    # The official SDK must not reappear in any dependency surface.
    for banned in ("polymarket-client", "py-clob-client"):
        assert not any(banned in dep for dep in dependencies), f"{banned} in pyproject"
        assert not any(banned in dep for dep in requirements), f"{banned} in requirements.txt"
        assert not any(banned in dep for dep in runtime_requirements), (
            f"{banned} in requirements.runtime.txt"
        )
