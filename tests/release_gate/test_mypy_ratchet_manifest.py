"""Release gate: the mypy type-debt ratchet is composition-pinned (F5-3).

Motivating regression: the old ratchet pinned only the TOTAL error count
(``BASELINE=34`` inline in ci.yml). Lateral moves — mypy errors deleted
from one file while equal errors appear in another — kept the total flat
and the gate green. The new gate pins EVERY file's error count exactly in
``scripts/mypy_baseline.json``; the total is derived (sum of the manifest),
never a separately pinned number, and the check stays ONE blocking CI step.

The pure parsing/comparison helpers are exercised here with synthetic
fixtures (same posture as ``tests/test_check_requirements_drift.py``); the
real mypy run lives in ``scripts/check_mypy_ratchet.py``'s main and in CI.
"""

from __future__ import annotations

import json
import pathlib

from scripts.check_mypy_ratchet import (
    MANIFEST_PATH,
    MYPY_TARGETS,
    diff_manifest,
    parse_error_counts,
)

ROOT = pathlib.Path(__file__).resolve().parents[2]

SAMPLE_MYPY_OUTPUT = """\
src/pmcopy/services/auto_trader.py:12: error: Incompatible types in assignment  [assignment]
src/pmcopy/services/auto_trader.py:40: error: "None" is not a valid type  [valid-type]
src/pmcopy/services/auto_trader.py:40: note: See https://mypy.readthedocs.io/
src/pmcopy/orchestrators/trade_pipeline.py:7:1: error: Cannot find implementation  [import]
src/pmcopy/services/auto_trader.py:12: error: duplicate line for the same file
Found 3 errors in 2 files (checked 211 source files)
"""


def test_parse_error_counts_groups_by_file_and_ignores_notes_and_summaries() -> None:
    counts = parse_error_counts(SAMPLE_MYPY_OUTPUT)
    assert counts == {
        "src/pmcopy/services/auto_trader.py": 3,
        "src/pmcopy/orchestrators/trade_pipeline.py": 1,
    }


def test_parse_error_counts_empty_output_is_empty() -> None:
    assert parse_error_counts("Success: no issues found in 5 source files\n") == {}


def test_lateral_error_move_passes_the_old_total_only_ratchet_but_fails_per_file() -> None:
    """The motivating case, pinned forever.

    ``manifest`` and ``moved`` have IDENTICAL totals, so the historical
    count-only gate (``test "$ERRORS" -le "$BASELINE"``) stayed green; the
    per-file composition check must catch both sides of the move.
    """
    manifest = {"src/pmcopy/services/a.py": 2, "src/pmcopy/services/b.py": 1}
    # a.py no longer erroring → absent from the per-file map entirely
    # (parse_error_counts only lists files that reported at least one error).
    moved = {"src/pmcopy/services/b.py": 3}

    # Old gate semantics: total did not grow → passed.
    assert sum(moved.values()) == sum(manifest.values())

    # New gate semantics: both files drifted → blocked.
    assert diff_manifest(manifest, moved) == [
        "src/pmcopy/services/a.py: manifest pins 2 errors but the file now reports none",
        "src/pmcopy/services/b.py: manifest pins 1 errors, mypy reports 3",
    ]


def test_exact_per_file_match_passes() -> None:
    manifest = {"src/a.py": 2, "src/b.py": 1}
    assert diff_manifest(manifest, {"src/b.py": 1, "src/a.py": 2}) == []


def test_any_single_file_growth_fails_even_when_total_shrinks() -> None:
    manifest = {"src/a.py": 5, "src/b.py": 5}
    actual = {"src/a.py": 3, "src/b.py": 6}  # total 10 -> 9, still blocked
    assert diff_manifest(manifest, actual) == [
        "src/a.py: manifest pins 5 errors, mypy reports 3",
        "src/b.py: manifest pins 5 errors, mypy reports 6",
    ]


def test_new_erroring_file_fails() -> None:
    problems = diff_manifest({"src/a.py": 1}, {"src/a.py": 1, "src/new.py": 1})
    assert problems == ["src/new.py: new erroring file with 1 errors (manifest has no entry)"]


def test_cleaned_file_fails_until_the_manifest_is_ratcheted() -> None:
    problems = diff_manifest({"src/a.py": 1, "src/b.py": 2}, {"src/b.py": 2})
    assert problems == ["src/a.py: manifest pins 1 errors but the file now reports none"]


def test_committed_manifest_is_a_valid_nonempty_per_file_map() -> None:
    data = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    assert isinstance(data.get("mypy_version"), str) and data["mypy_version"]
    files = data["files"]
    assert isinstance(files, dict) and files, "manifest must list at least one erroring file"
    for path, count in files.items():
        assert path.startswith("src/pmcopy/"), f"manifest path must be repo-relative: {path}"
        assert isinstance(count, int) and not isinstance(count, bool) and count > 0, (
            f"manifest count for {path} must be a positive integer, got {count!r}"
        )
    # The total is DERIVED from the per-file map — never a second pinned number.
    assert "total" not in data and "TOTAL" not in data and "baseline" not in data


def test_ratchet_scope_covers_the_documented_three_targets() -> None:
    assert MYPY_TARGETS == (
        "src/pmcopy/services",
        "src/pmcopy/orchestrators",
        "src/pmcopy/domain",
    )


def test_ci_ratchet_step_runs_the_per_file_script_as_one_blocking_step() -> None:
    workflow = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    # Bound the step text at the next job definition so later jobs' cleanup
    # steps (which legitimately keep `|| true` under `if: always()`) don't
    # leak into this step's contract.
    step = workflow.split("- name: mypy (type-debt ratchet", maxsplit=1)[1].split(
        "\n  docker-smoke:", maxsplit=1
    )[0]
    assert "python scripts/check_mypy_ratchet.py" in step
    # The count-only ratchet is gone: no inline numeric baseline remains.
    assert "BASELINE=" not in step
    # Still one BLOCKING step — no result concealment.
    assert "continue-on-error" not in step
    assert "|| true" not in step
