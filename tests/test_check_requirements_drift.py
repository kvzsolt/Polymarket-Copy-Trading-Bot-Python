"""Lock-canonical dependency-drift guard tests.

``scripts/check_requirements_drift.py`` enforces the Task 16 dependency
contract:

- ``uv.lock`` is the single resolution source of truth;
- ``requirements.txt`` / ``requirements.runtime.txt`` are generated exports
  that must match the lock exactly (name + version + marker);
- every ``[project].dependencies`` entry must be present in the lock.

The pure parsing/comparison helpers are exercised here with synthetic
fixtures; the repository-state test runs them against the real files. The
uv-driven byte-exact re-render (and ``uv lock --check``) run in the script's
``main`` and in CI, not in the unit suite.
"""

from __future__ import annotations

import pathlib

from scripts.check_requirements_drift import (
    find_banned_sdk_violations,
    find_export_entries_absent_from_lock,
    find_lock_packages_absent_from_dev_export,
    find_pyproject_names_missing_from_lock,
    find_runtime_dev_inconsistencies,
    parse_export,
)

ROOT = pathlib.Path(__file__).resolve().parents[1]

LOCK_FIXTURE = """
version = 1
requires-python = ">=3.11,<3.13"

[[package]]
name = "aiohttp"
version = "3.14.5"
source = { registry = "https://pypi.org/simple" }

[[package]]
name = "uvloop"
version = "0.22.1"
source = { registry = "https://pypi.org/simple" }

[[package]]
name = "pytest"
version = "9.1.1"
source = { registry = "https://pypi.org/simple" }

[[package]]
name = "polymarket-copy-trading-bot"
version = "0.1.0"
source = { editable = "." }
"""


def _pyproject(runtime: list[str]) -> str:
    return f"""
[project]
dependencies = [
{chr(10).join(f'  "{dep}",' for dep in runtime)}
]
"""


def test_parse_export_handles_versions_and_markers() -> None:
    parsed = parse_export(
        "# header comment\n"
        "aiohttp==3.14.5\n"
        "    # via\n"
        "    #   pyproject\n"
        "uvloop==0.22.1 ; sys_platform != 'win32'\n"
    )

    assert parsed == {
        "aiohttp": [("3.14.5", "")],
        "uvloop": [("0.22.1", "sys_platform != 'win32'")],
    }


def test_parse_export_accumulates_repeated_names() -> None:
    parsed = parse_export("orjson==3.11.9\norjson==3.13.0 ; platform_machine == 's390x'\n")

    assert parsed["orjson"] == [("3.11.9", ""), ("3.13.0", "platform_machine == 's390x'")]


def test_pyproject_dep_missing_from_lock_is_actionable() -> None:
    pyproject = _pyproject(["aiohttp==3.14.5", "asyncpg>=0.30.0"])

    missing = find_pyproject_names_missing_from_lock(pyproject, LOCK_FIXTURE)

    assert missing == ["asyncpg>=0.30.0"]


def test_pyproject_dep_present_under_different_case_is_matched() -> None:
    pyproject = _pyproject(["PyYAML>=6"])

    assert find_pyproject_names_missing_from_lock(pyproject, LOCK_FIXTURE) == ["PyYAML>=6"]
    lock_with_yaml = (
        LOCK_FIXTURE
        + """
[[package]]
name = "pyyaml"
version = "6.0.3"
source = { registry = "https://pypi.org/simple" }
"""
    )
    assert find_pyproject_names_missing_from_lock(pyproject, lock_with_yaml) == []


def test_export_entry_absent_from_lock_is_reported() -> None:
    export = "aiohttp==3.14.5\nh2==4.4.1\n"

    problems = find_export_entries_absent_from_lock(export, LOCK_FIXTURE)

    assert problems == ["h2==4.4.1"]


def test_export_version_mismatch_against_lock_is_reported() -> None:
    export = "aiohttp==3.14.4\n"

    problems = find_export_entries_absent_from_lock(export, LOCK_FIXTURE)

    assert problems == ["aiohttp==3.14.4"]


def test_lock_package_missing_from_dev_export_is_reported() -> None:
    dev_export = "aiohttp==3.14.5\nuvloop==0.22.1 ; sys_platform != 'win32'\n"
    # pytest is locked but absent from the (all-extras) dev export.

    problems = find_lock_packages_absent_from_dev_export(LOCK_FIXTURE, dev_export)

    assert problems == ["pytest"]
    # The root project package is never exported (--no-emit-project).
    assert "polymarket-copy-trading-bot" not in problems


def test_runtime_dev_inconsistencies_are_reported() -> None:
    dev = "aiohttp==3.14.5\nuvloop==0.22.1 ; sys_platform != 'win32'\n"

    same = "aiohttp==3.14.5\nuvloop==0.22.1 ; sys_platform != 'win32'\n"
    assert find_runtime_dev_inconsistencies(dev, same) == []

    version_drift = "aiohttp==3.14.4\nuvloop==0.22.1 ; sys_platform != 'win32'\n"
    assert find_runtime_dev_inconsistencies(dev, version_drift) == [
        "aiohttp: version/marker differs between exports "
        "(runtime: [('3.14.4', '')], dev: [('3.14.5', '')])"
    ]

    marker_drift = "aiohttp==3.14.5\nuvloop==0.22.1\n"
    assert find_runtime_dev_inconsistencies(dev, marker_drift) == [
        "uvloop: version/marker differs between exports "
        "(runtime: [('0.22.1', '')], dev: [('0.22.1', \"sys_platform != 'win32'\")])"
    ]


def test_banned_sdk_violation_matches_package_names_not_substrings() -> None:
    """The banned-SDK guard anchors on requirement NAME positions.

    A substring match would false-positive on unrelated text (markers,
    annotations, packages whose name merely contains the banned token) and
    could also miss a re-introduced SDK hidden mid-line. Matching must use
    the parsed name positions of the lock and both exports.
    """

    # A package whose name merely CONTAINS the banned token: not a violation.
    lookalike_lock = (
        LOCK_FIXTURE
        + """
[[package]]
name = "not-polymarket-client-adapter"
version = "1.0.0"
source = { registry = "https://pypi.org/simple" }
"""
    )
    lookalike_export = "aiohttp==3.14.5\nnot-polymarket-client-adapter==1.0.0\n"
    assert find_banned_sdk_violations(lookalike_lock, lookalike_export, "") == []

    # The real banned distribution name in the lock: violation.
    banned_lock = (
        LOCK_FIXTURE
        + """
[[package]]
name = "polymarket-client"
version = "0.1.0"
source = { registry = "https://pypi.org/simple" }
"""
    )
    problems = find_banned_sdk_violations(banned_lock, "aiohttp==3.14.5\n", "")
    assert problems == ["banned official SDK reappeared in uv.lock: polymarket-client"]

    # The banned distribution name as an export line (either export): violation.
    problems = find_banned_sdk_violations(
        LOCK_FIXTURE, "aiohttp==3.14.5\n", "py-clob-client==1.2.3\n"
    )
    assert problems == ["banned official SDK reappeared in an export: py-clob-client"]

    # Substring occurrences inside a marker or annotation are not violations.
    marker_only = "aiohttp==3.14.5 ; extra != 'polymarket-client'\n"
    assert find_banned_sdk_violations(LOCK_FIXTURE, marker_only, "") == []


def test_repo_exports_match_lock_and_pyproject() -> None:
    """Repository state: exports and lock must agree under every pure check."""

    pyproject = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    lock = (ROOT / "uv.lock").read_text(encoding="utf-8")
    dev = (ROOT / "requirements.txt").read_text(encoding="utf-8")
    runtime = (ROOT / "requirements.runtime.txt").read_text(encoding="utf-8")

    assert find_pyproject_names_missing_from_lock(pyproject, lock) == []
    assert find_export_entries_absent_from_lock(dev, lock) == []
    assert find_export_entries_absent_from_lock(runtime, lock) == []
    assert find_lock_packages_absent_from_dev_export(lock, dev) == []
    assert find_runtime_dev_inconsistencies(dev, runtime) == []
    assert find_banned_sdk_violations(lock, dev, runtime) == []
