"""Pinned CCXT dependency synchronization contract."""

from __future__ import annotations

from pathlib import Path
import tomllib


def test_venue_dependencies_are_pinned_and_synchronized() -> None:
    """Keep official SDK and optional CCXT pins in project and export files."""

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
    assert project_ccxt == ["ccxt==4.5.70"]
    assert exported_ccxt == ["ccxt==4.5.70"]
    assert "polymarket-client==0.1.0b18" in dependencies
    assert "polymarket-client==0.1.0b18" in requirements
    assert "polymarket-client==0.1.0b18" in runtime_requirements
