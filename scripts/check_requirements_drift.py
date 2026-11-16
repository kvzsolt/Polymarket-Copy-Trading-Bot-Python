#!/usr/bin/env python3
"""Fail-closed guard keeping uv.lock, its exports, and pyproject.toml in sync.

Since Task 16 the dependency contract is:

- ``uv.lock`` is the canonical resolution (single source of truth);
- ``requirements.txt`` (dev export) and ``requirements.runtime.txt``
  (production export) are *generated* by ``scripts/generate_requirements.py``
  from the lock and must never be hand-edited;
- ``pyproject.toml`` declares the direct dependency set; every runtime
  dependency must be present in the lock.

This script fails non-zero with actionable diffs when any layer drifts:

1. Pure checks (stdlib only, mirrored by ``tests/test_check_requirements_
   drift.py``):
   - every ``[project].dependencies`` name is present in the lock;
   - every export line (name + version) resolves to a locked package — in
     both exports;
   - the dev export (universal, all extras) covers every locked package;
   - shared packages carry identical version + marker in both exports;
   - the removed official SDKs (``polymarket-client`` / ``py-clob-client``)
     never reappear.
2. uv-backed checks (require the ``uv`` binary — CI installs it pinned):
   - ``uv lock --check`` — the lock is current for ``pyproject.toml``;
   - both exports byte-match a fresh render via ``generate_requirements``
     (exact name + version + marker equality against the lock).

   The byte-exact comparison is uv-version-sensitive: uv rendering changes
   (e.g. marker formatting) between releases can make a correct, committed
   export differ from a fresh render. CI therefore installs a pinned uv
   (see the drift step in ``.github/workflows/ci.yml``); when bumping that
   pin, regenerate both exports with the same version via
   ``python scripts/generate_requirements.py``.

Run standalone (``python scripts/check_requirements_drift.py``) or import
the ``find_*`` helpers for tests. Pure helpers never invoke uv, so the unit
suite runs on machines without it.
"""

from __future__ import annotations

import pathlib
import re
import shutil
import subprocess
import sys
import tomllib
from typing import Any

from dotenv import load_dotenv

load_dotenv()

BANNED_PACKAGES = ("polymarket-client", "py-clob-client")

# Bare distribution name at the start of a requirement line.
_NAME_RE = re.compile(r"^[A-Za-z0-9._-]+")
# ``name==version`` with an optional ``; marker`` suffix (uv export format).
_EXPORT_RE = re.compile(r"^([A-Za-z0-9._-]+)==([^;\s]+)\s*(?:;\s*(.+))?\s*$")


def _normalize(name: str) -> str:
    """PEP 503 name normalization: lowercase, runs of [-_.] collapse to '-'."""
    return re.sub(r"[-_.]+", "-", name.strip().lower())


def _dep_name(spec: str) -> str:
    """Extract the bare, normalized distribution name from a dep/req line."""
    match = _NAME_RE.match(spec.strip())
    return _normalize(match.group(0)) if match else ""


def parse_lock_packages(lock_text: str) -> dict[str, set[str]]:
    """Return ``{normalized name: {locked versions}}`` from uv.lock text."""

    data: dict[str, Any] = tomllib.loads(lock_text)
    packages: dict[str, set[str]] = {}
    for package in data.get("package", []):
        name = _normalize(str(package.get("name", "")))
        version = package.get("version")
        if name and version is not None:
            packages.setdefault(name, set()).add(str(version))
    return packages


def parse_export(export_text: str) -> dict[str, list[tuple[str, str]]]:
    """Parse an export file into ``{name: [(version, marker), ...]}``.

    Continuation/annotation lines (``# via``) and the generated header are
    ignored. A name may appear multiple times with disjoint markers.
    """

    parsed: dict[str, list[tuple[str, str]]] = {}
    for raw_line in export_text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or line.startswith("-"):
            continue
        match = _EXPORT_RE.match(line)
        if match is None:
            continue
        name = _normalize(match.group(1))
        version = match.group(2)
        marker = (match.group(3) or "").strip()
        parsed.setdefault(name, []).append((version, marker))
    return parsed


def find_pyproject_names_missing_from_lock(pyproject_text: str, lock_text: str) -> list[str]:
    """Return runtime dep specs declared in pyproject but absent from the lock.

    Compares ``[project].dependencies`` names against locked package names
    (normalized). Version-specifier freshness is enforced by ``uv lock
    --check`` in ``main``; this check catches name-level drift (a dep added
    to pyproject without re-locking).
    """

    deps = tomllib.loads(pyproject_text).get("project", {}).get("dependencies", [])
    locked = parse_lock_packages(lock_text)

    missing: list[str] = []
    for dep in deps:
        if _dep_name(dep) and _dep_name(dep) not in locked:
            missing.append(dep)
    return missing


def find_export_entries_absent_from_lock(export_text: str, lock_text: str) -> list[str]:
    """Return export lines whose name==version is not a locked package."""

    locked = parse_lock_packages(lock_text)
    problems: list[str] = []
    for name, entries in parse_export(export_text).items():
        for version, _marker in entries:
            if version not in locked.get(name, set()):
                problems.append(f"{name}=={version}")
    return problems


def find_lock_packages_absent_from_dev_export(lock_text: str, dev_export_text: str) -> list[str]:
    """Return locked packages missing from the universal all-extras export.

    The dev export is generated with ``--all-extras --no-strip-markers``-
    equivalent semantics, so every locked distribution (the root project
    itself is not emitted and is skipped) must appear in it.
    """

    exported = parse_export(dev_export_text)
    problems: list[str] = []
    for name in parse_lock_packages(lock_text):
        if name == "polymarket-copy-trading-bot":
            continue
        if name not in exported:
            problems.append(name)
    return sorted(problems)


def find_runtime_dev_inconsistencies(dev_export_text: str, runtime_export_text: str) -> list[str]:
    """Return shared packages whose version/marker differ between exports."""

    dev = parse_export(dev_export_text)
    runtime = parse_export(runtime_export_text)
    problems: list[str] = []
    for name in sorted(runtime):
        if name in dev and sorted(runtime[name]) != sorted(dev[name]):
            problems.append(
                f"{name}: version/marker differs between exports "
                f"(runtime: {sorted(runtime[name])}, dev: {sorted(dev[name])})"
            )
    return problems


def _pure_problems(root: pathlib.Path) -> list[str]:
    """Run every stdlib-only consistency check; return failure messages."""

    pyproject = (root / "pyproject.toml").read_text(encoding="utf-8")
    lock = (root / "uv.lock").read_text(encoding="utf-8")
    dev = (root / "requirements.txt").read_text(encoding="utf-8")
    runtime = (root / "requirements.runtime.txt").read_text(encoding="utf-8")

    problems: list[str] = []
    for dep in find_pyproject_names_missing_from_lock(pyproject, lock):
        problems.append(f"pyproject dependency not present in uv.lock: {dep} (run `uv lock`)")
    for surface, text in (("requirements.txt", dev), ("requirements.runtime.txt", runtime)):
        for entry in find_export_entries_absent_from_lock(text, lock):
            problems.append(
                f"{surface} entry does not match uv.lock: {entry} "
                f"(regenerate: python scripts/generate_requirements.py)"
            )
    for name in find_lock_packages_absent_from_dev_export(lock, dev):
        problems.append(
            f"locked package missing from requirements.txt: {name} "
            f"(regenerate: python scripts/generate_requirements.py)"
        )
    problems.extend(find_runtime_dev_inconsistencies(dev, runtime))
    problems.extend(find_banned_sdk_violations(lock, dev, runtime))
    return problems


def find_banned_sdk_violations(
    lock_text: str, dev_export_text: str, runtime_export_text: str
) -> list[str]:
    """Return messages for re-introduced official SDK distributions.

    Matching anchors on parsed package NAME positions (lock package names and
    export requirement names), never on raw substring occurrences — a token
    like ``polymarket-client`` appearing inside a marker, annotation, or a
    longer package name must not trigger (or mask) the guard.
    """

    problems: list[str] = []
    locked_names = set(parse_lock_packages(lock_text))
    export_names = set(parse_export(dev_export_text)) | set(parse_export(runtime_export_text))
    for banned in BANNED_PACKAGES:
        if banned in locked_names:
            problems.append(f"banned official SDK reappeared in uv.lock: {banned}")
        if banned in export_names:
            problems.append(f"banned official SDK reappeared in an export: {banned}")
    return problems


def _run(command: list[str], cwd: pathlib.Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, cwd=cwd, capture_output=True, text=True, check=False)


def _uv_backed_problems(root: pathlib.Path) -> tuple[list[str], str | None]:
    """Run ``uv lock --check`` and byte-compare both exports.

    Returns ``(problems, uv_version)``. When the ``uv`` binary is missing the
    caller should fail closed with an actionable message (the script degrades
    to pure checks only when explicitly allowed via ``--no-uv``).
    """

    problems: list[str] = []

    lock_check = _run(["uv", "lock", "--check"], root)
    if lock_check.returncode != 0:
        problems.append(
            "uv.lock is not up to date with pyproject.toml "
            f"(uv lock --check failed: {lock_check.stderr.strip() or lock_check.stdout.strip()})"
        )

    # Running this file directly puts scripts/ (not the repo root) on
    # sys.path; make the sibling module importable either way.
    repo_root = str(root)
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)
    from scripts.generate_requirements import (
        DEV_EXPORT_FLAGS,
        DEV_OUTPUT,
        RUNTIME_EXPORT_FLAGS,
        RUNTIME_OUTPUT,
        render_export,
    )

    for flags, name in (
        (DEV_EXPORT_FLAGS, DEV_OUTPUT),
        (RUNTIME_EXPORT_FLAGS, RUNTIME_OUTPUT),
    ):
        committed = (root / name).read_text(encoding="utf-8")
        try:
            expected = render_export(flags, name)
        except RuntimeError as exc:
            problems.append(f"could not re-render {name} from uv.lock: {exc}")
            continue
        if committed != expected:
            committed_lines = committed.splitlines()
            expected_lines = expected.splitlines()
            # strict=False is deliberate: compare the common prefix, then
            # report the shorter length as the first differing line.
            first_diff = next(
                (
                    i
                    for i, (a, b) in enumerate(zip(committed_lines, expected_lines, strict=False))
                    if a != b
                ),
                min(len(committed_lines), len(expected_lines)),
            )
            problems.append(
                f"{name} does not byte-match a fresh export from uv.lock "
                f"(first differing line {first_diff + 1}; "
                f"regenerate: python scripts/generate_requirements.py)"
            )

    version = _run(["uv", "--version"], root)
    return problems, (version.stdout.strip() or None)


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    allow_missing_uv = "--no-uv" in args

    root = pathlib.Path(__file__).resolve().parent.parent

    problems = _pure_problems(root)
    if problems:
        print("Dependency drift detected (lock/exports/pyproject disagree):")
        for problem in problems:
            print(f"  - {problem}")

    if shutil.which("uv") is None:
        if allow_missing_uv:
            print("note: uv not found; byte-exact export check skipped (--no-uv)")
        else:
            problems.append(
                "uv binary not found; cannot verify exports byte-match the lock "
                "(install uv, e.g. `pip install uv`, or pass --no-uv to allow)"
            )
    else:
        uv_problems, uv_version = _uv_backed_problems(root)
        for problem in uv_problems:
            print(f"  - {problem}")
        problems.extend(uv_problems)
        if uv_version:
            print(f"uv: {uv_version}")

    if problems:
        print(
            "\nFix: update pyproject.toml, run `uv lock`, then "
            "`python scripts/generate_requirements.py`."
        )
        return 1

    print("OK: uv.lock, requirements.txt, requirements.runtime.txt, and pyproject.toml agree.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
