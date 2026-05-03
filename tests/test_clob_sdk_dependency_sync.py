from __future__ import annotations

from pathlib import Path
import tomllib


def test_clob_sdk_dependency_is_synced_between_pyproject_and_requirements() -> None:
    repo_root = Path(__file__).resolve().parent.parent
    pyproject = tomllib.loads((repo_root / "pyproject.toml").read_text())
    project_deps = pyproject["project"]["dependencies"]
    requirements = [
        line.strip()
        for line in (repo_root / "requirements.txt").read_text().splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]

    pyproject_clob = [dep for dep in project_deps if dep.startswith("py-clob-client")]
    requirements_clob = [dep for dep in requirements if dep.startswith("py-clob-client")]

    assert pyproject_clob == ["py-clob-client-v2==1.0.0"]
    assert requirements_clob == ["py-clob-client-v2==1.0.0"]
    assert pyproject_clob == requirements_clob
