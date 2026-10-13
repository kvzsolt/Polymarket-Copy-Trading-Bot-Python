#!/usr/bin/env python3
"""Per-file mypy type-debt ratchet (composition-pinned).

The previous ratchet pinned only the TOTAL error count for
services/orchestrators/domain. A lateral regression — mypy errors deleted
from one file while equal errors appear in another — keeps the total flat
and passed the gate. This script pins EVERY file's error count exactly in
``scripts/mypy_baseline.json``:

* any count change (growth, shrinkage, a new erroring file, a file cleaned
  to zero errors) fails the check — shrinkage fails too, so the manifest is
  ratcheted deliberately via ``--update`` instead of drifting silently;
* the TOTAL is derived (the sum of the manifest) and only reported, never
  a separately pinned number;
* the mypy invocation is flag-identical to the historical CI ratchet
  (``--install-types --non-interactive --ignore-missing-imports`` over the
  same three targets) so counts are comparable across the switch.

Usage:
    python scripts/check_mypy_ratchet.py            # verify (CI-blocking)
    python scripts/check_mypy_ratchet.py --update   # re-record the manifest

Run from the repository root (paths in the manifest and in mypy's output
are repository-relative).
"""

from __future__ import annotations

import argparse
import json
import pathlib
import re
import subprocess
import sys

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
MANIFEST_PATH = pathlib.Path(__file__).with_name("mypy_baseline.json")
MYPY_TARGETS = (
    "src/pmcopy/services",
    "src/pmcopy/orchestrators",
    "src/pmcopy/domain",
)

# mypy error lines: "<path>:<line>: error: ..." (column only with --show-column-numbers)
_ERROR_LINE = re.compile(r"^(?P<path>.+?):\d+(?::\d+)?: error:")


def parse_error_counts(mypy_output: str) -> dict[str, int]:
    """Per-file ``error:`` line counts from mypy's combined output text.

    Notes, the ``Found N errors in M files`` summary, and install-stubs
    chatter contain no ``error:`` marker and are ignored.
    """
    counts: dict[str, int] = {}
    for line in mypy_output.splitlines():
        match = _ERROR_LINE.match(line)
        if match is None:
            continue
        path = match.group("path")
        counts[path] = counts.get(path, 0) + 1
    return counts


def diff_manifest(manifest: dict[str, int], actual: dict[str, int]) -> list[str]:
    """Human-readable per-file mismatches; an empty list is an exact match."""
    problems: list[str] = []
    for path in sorted(set(manifest) | set(actual)):
        expected = manifest.get(path)
        got = actual.get(path)
        if expected == got:
            continue
        if expected is None:
            problems.append(f"{path}: new erroring file with {got} errors (manifest has no entry)")
        elif got is None:
            problems.append(
                f"{path}: manifest pins {expected} errors but the file now reports none"
            )
        else:
            problems.append(f"{path}: manifest pins {expected} errors, mypy reports {got}")
    return problems


def run_mypy() -> str:
    """Run the ratchet-scoped mypy exactly as the historical CI step did."""
    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "mypy",
            "--install-types",
            "--non-interactive",
            "--ignore-missing-imports",
            *MYPY_TARGETS,
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    return proc.stdout + proc.stderr


def load_manifest() -> tuple[dict[str, int], str]:
    data = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    files = data["files"]
    if not isinstance(files, dict) or not files:
        raise SystemExit(f"{MANIFEST_PATH.name}: 'files' must be a non-empty object")
    for path, count in files.items():
        if not isinstance(count, int) or isinstance(count, bool) or count < 1:
            raise SystemExit(f"{MANIFEST_PATH.name}: {path} must map to a positive int")
    return dict(files), str(data.get("mypy_version", "unknown"))


def write_manifest(actual: dict[str, int]) -> None:
    version = subprocess.run(
        [sys.executable, "-m", "mypy", "--version"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    version_match = re.search(r"\d+\.\d+\.\d+", version)
    payload = {
        "files": {path: actual[path] for path in sorted(actual)},
        "mypy_version": version_match.group(0) if version_match else "unknown",
    }
    MANIFEST_PATH.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--update",
        action="store_true",
        help="re-record scripts/mypy_baseline.json from the current mypy run",
    )
    args = parser.parse_args(argv)

    actual = parse_error_counts(run_mypy())
    if args.update:
        write_manifest(actual)
        print(
            f"mypy baseline updated: {len(actual)} erroring files, "
            f"{sum(actual.values())} errors total (derived)"
        )
        return 0

    manifest, recorded_version = load_manifest()
    total = sum(manifest.values())
    print(
        f"mypy per-file ratchet: manifest pins {len(manifest)} erroring files "
        f"/ {total} errors total (derived); recorded with mypy {recorded_version}"
    )
    problems = diff_manifest(manifest, actual)
    if problems:
        print(
            "per-file composition drifted — fix the drift, or ratchet the "
            "manifest deliberately with `python scripts/check_mypy_ratchet.py --update`:"
        )
        for line in problems:
            print(f"  {line}")
        return 1
    print("mypy per-file ratchet: OK (exact per-file match)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
