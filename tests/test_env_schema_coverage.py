"""Env-schema coverage + duplicate-key lint (deep-research P1: AppConfig drift).

Every key shipped in ``config/runtime.env`` / ``config/runtime.local.env`` must
be either:

  1. referenced in the typed config surface (``config/app_config.py``), or the
     legacy ``config/env.py`` snapshot / ``config/addresses.py`` constants, or
  2. listed in ``OPERATIONAL_ALLOWLIST`` below with a justification.

This fails loudly when a new runtime key is added without a typed home, which is
exactly the "partial config migration / some paths read raw env" smell the audit
flagged. It also forbids duplicate keys within a single env file.

Note: the repo keeps tests flat under ``tests/`` (no ``tests/config/`` package),
so this lives at ``tests/test_env_schema_coverage.py`` rather than the
``tests/config/`` path named in the original brief.
"""

from __future__ import annotations

import re
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
# runtime.env and runtime.local.env are gitignored (operator-supplied), so they
# are absent in a fresh CI checkout; runtime.local.example is the only tracked
# template. Check whichever exist — the full 195-key sweep runs locally, the
# tracked template is always covered in CI.
_CANDIDATE_ENV_FILES = (
    _ROOT / "config" / "runtime.env",
    _ROOT / "config" / "runtime.local.env",
    _ROOT / "config" / "runtime.local.example",
)
_DECLARING_SOURCES = (
    _ROOT / "config" / "app_config.py",
    _ROOT / "config" / "env.py",
    _ROOT / "config" / "addresses.py",
)


def _present_env_files() -> list[Path]:
    return [p for p in _CANDIDATE_ENV_FILES if p.exists()]


# Keys intentionally consumed OUTSIDE the typed AppConfig surface. Each entry
# must carry a reason — this is the documented escape hatch, not a dumping
# ground.
OPERATIONAL_ALLOWLIST: dict[str, str] = {
    # Python interpreter flag (unbuffered stdout). Not bot behaviour; only
    # surfaced in the runtime summary for diagnostics.
    "PYTHONUNBUFFERED": "Python interpreter flag, diagnostics-only surface",
}

_KEY_LINE = re.compile(r"^([A-Z][A-Z0-9_]*)\s*=")
_UPPER_LITERAL = re.compile(r"""["']([A-Z][A-Z0-9_]{2,})["']""")


def _env_keys_in_order(path: Path) -> list[str]:
    keys: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        match = _KEY_LINE.match(stripped)
        if match:
            keys.append(match.group(1))
    return keys


def _declared_keys() -> set[str]:
    declared: set[str] = set()
    for source in _DECLARING_SOURCES:
        declared |= set(_UPPER_LITERAL.findall(source.read_text(encoding="utf-8")))
    return declared


def test_runtime_env_keys_are_declared_or_allowlisted() -> None:
    declared = _declared_keys()
    runtime_keys: set[str] = set()
    for env_file in _present_env_files():
        runtime_keys |= set(_env_keys_in_order(env_file))
    undeclared = sorted(
        key
        for key in runtime_keys
        if key not in declared and key not in OPERATIONAL_ALLOWLIST
    )
    assert not undeclared, (
        "Runtime env keys are neither typed in AppConfig nor allowlisted "
        f"(add a typed field or an OPERATIONAL_ALLOWLIST entry): {undeclared}"
    )


def test_no_duplicate_keys_in_runtime_env_files() -> None:
    for path in _present_env_files():
        keys = _env_keys_in_order(path)
        duplicates = sorted({key for key in keys if keys.count(key) > 1})
        assert not duplicates, f"Duplicate keys in {path.name}: {duplicates}"
