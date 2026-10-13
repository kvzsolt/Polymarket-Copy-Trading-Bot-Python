"""Release-gate invariant — the source tree parses under the running interpreter.

The project is Python 3.13-only. A handful of constructs are legal on 3.14 but
are ``SyntaxError`` on 3.13 — most notably PEP 758, the parenthesis-free
``except A, B:`` form. Such a construct is invisible to a 3.14-only test run
(tests pass, the interpreter accepts the file) yet makes the module
un-importable on the 3.13 matrix interpreter. This gate is the automated shape
of that failure mode: it ``ast.parse``-es every source file with the RUNNING
interpreter and fails with ``file:line: message`` for each offender.

Value by interpreter:

* on the 3.13 target (today's venv) this is the real guard: the migration
  parenthesized all 16 PEP 758 sites (an ``ast.parse`` sweep reports only the
  first error per file, which is why an earlier count said 4), so any failure
  here means a parens-free clause has re-entered;
* on any interpreter that accepts both forms it is the shape check only — the
  tree is already PEP-758-free, so the scan stays green.

``tests/`` is intentionally out of scope here: this gate protects shipped
source (``src/``) and operator scripts (``scripts/``). Test modules are parsed
by pytest itself when the matrix runs 3.13.
"""

from __future__ import annotations

import ast
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

SOURCE_ROOTS: tuple[str, ...] = ("src", "scripts")

# A floor, not an exact count: the point is that a glob which silently stops
# matching (layout move, packaging change) cannot turn this gate vacuously
# green. Today's tree has 402 files across the two roots (~391 src + 11
# scripts), so the floor sits far below the real count and above any single
# accidental subdirectory.
MINIMUM_FILES_SCANNED = 100


def _iter_source_files() -> list[Path]:
    """Every ``.py`` file under the scanned roots, bytecode caches excluded."""
    files: list[Path] = []
    for entry in SOURCE_ROOTS:
        root = REPO_ROOT / entry
        if root.is_file() and root.suffix == ".py":
            files.append(root)
            continue
        for path in sorted(root.rglob("*.py")):
            if "__pycache__" in path.parts:
                continue
            files.append(path)
    return files


def _parse_failures(files: list[Path]) -> list[str]:
    """``file:line: message`` for every file the running interpreter cannot parse."""
    failures: list[str] = []
    for path in files:
        rel = str(path.relative_to(REPO_ROOT))
        try:
            ast.parse(path.read_text(encoding="utf-8"), filename=rel)
        except SyntaxError as exc:
            location = f"{rel}:{exc.lineno}" if exc.lineno is not None else rel
            failures.append(f"{location}: {exc.msg}")
    return failures


def test_source_tree_parses_under_running_interpreter() -> None:
    files = _iter_source_files()
    assert len(files) >= MINIMUM_FILES_SCANNED, (
        f"source scan matched only {len(files)} files; expected at least "
        f"{MINIMUM_FILES_SCANNED} under {SOURCE_ROOTS} — the glob is broken, so a "
        "pass would be vacuous"
    )
    failures = _parse_failures(files)
    assert not failures, (
        f"{len(failures)} source file(s) do not parse on this interpreter "
        f"(this gate exists because PEP 758 parenthesis-free 'except' is legal "
        f"on 3.14 but a SyntaxError on 3.13):\n  - " + "\n  - ".join(failures)
    )
