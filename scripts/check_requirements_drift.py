#!/usr/bin/env python3
"""Fail-closed guard against pyproject.toml / requirements.txt dependency drift.

``pyproject.toml`` is the canonical dependency source: CI installs via
``pip install -e ".[dev]"`` (reading ``[project].dependencies``), but Docker and
the documented install path install from the hand-maintained
``requirements.txt``. If a runtime dependency is added to pyproject but not to
requirements.txt, the package builds green in CI yet the Docker image / README
install is broken at runtime (this is exactly how ``asyncpg`` slipped through).

This script checks that every ``[project].dependencies`` distribution NAME is
present in requirements.txt and exits non-zero listing any that are missing.
Comparison is name-only (case-insensitive, normalized) because requirements.txt
pins exact versions while pyproject uses ranges; only the dev extras may legally
diverge (they are intentionally not shipped in requirements.txt).

Run standalone (``python scripts/check_requirements_drift.py``) or import
``find_missing`` for tests.
"""

from __future__ import annotations

from dotenv import load_dotenv

load_dotenv()

import pathlib
import re
import sys
import tomllib

# Strip everything after the first specifier / extra / marker char to leave the
# bare distribution name.
_NAME_RE = re.compile(r"^[A-Za-z0-9._-]+")


def _normalize(name: str) -> str:
    """PEP 503 name normalization: lowercase, runs of [-_.] collapse to '-'."""
    return re.sub(r"[-_.]+", "-", name.strip().lower())


def _dep_name(spec: str) -> str:
    """Extract the bare, normalized distribution name from a dep/req line."""
    match = _NAME_RE.match(spec.strip())
    return _normalize(match.group(0)) if match else ""


def find_missing(pyproject_text: str, requirements_text: str) -> list[str]:
    """Return runtime dep names declared in pyproject but absent from requirements.

    Compares ``[project].dependencies`` against requirements.txt by normalized
    distribution name. Returns the original pyproject spec strings for any
    missing dependency, in declaration order.
    """
    deps = tomllib.loads(pyproject_text).get("project", {}).get("dependencies", [])

    req_names: set[str] = set()
    for line in requirements_text.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or line.startswith("-"):
            continue
        name = _dep_name(line)
        if name:
            req_names.add(name)

    missing: list[str] = []
    for dep in deps:
        if _dep_name(dep) and _dep_name(dep) not in req_names:
            missing.append(dep)
    return missing


def main() -> int:
    root = pathlib.Path(__file__).resolve().parent.parent
    pyproject = (root / "pyproject.toml").read_text()
    requirements = (root / "requirements.txt").read_text()

    missing = find_missing(pyproject, requirements)
    if missing:
        print(
            "requirements.txt drift — runtime deps in pyproject.toml but not "
            "requirements.txt (Docker / documented installs would break):"
        )
        for dep in missing:
            print(f"  - {dep}")
        print("Add the missing dependency(ies) to requirements.txt.")
        return 1

    print("OK: requirements.txt covers all [project].dependencies.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
