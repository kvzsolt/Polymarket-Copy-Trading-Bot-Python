#!/usr/bin/env python3
"""One-shot namespace migration: flat layout -> ``src/pmcopy`` src-layout (Task 18).

DETERMINISTIC, REVIEWED codemod. Run exactly once from the repository root:

    python scripts/migrate_namespace.py

Pipeline (each step asserts its own preconditions; any failure aborts before
the next step so the tree is never half-migrated at a step boundary):

  0. Idempotency guard: refuses to run if ``src/pmcopy`` already exists and
     the flat roots are gone (prints "already migrated", exit 0).
  1. Inventory verification against ``git ls-files`` — the 16 flat packages
     and the ``app``/``bootstrap``/``bot`` modules must exist exactly; any
     unexpected 17th package or missing root is a hard error.
  2. ``git mv`` each flat root under ``src/pmcopy/``; create
     ``src/pmcopy/__init__.py`` and ``src/pmcopy/__main__.py``.
  3. SPECIAL_EDITS: exact literal replacements (asserted occurrence counts)
     for everything not safely generalizable — repo-root-relative path joins
     in vetted test files, embedded subprocess scripts, packaging-contract
     tests, sys.path bootstraps, pyproject/pytest.ini, Dockerfile, compose,
     ignore files, CI workflow, rc1_check.sh, greptile.json, docs commands.
  4. AST import rewrite in every tracked ``.py`` file: ``import X`` /
     ``import X.Y [as z]`` / ``from X[.Y] import ...`` statements whose head
     is a moved name become ``pmcopy.``-prefixed. Statements only — strings,
     comments, and docstrings are untouched by this pass.
  5. AST dotted-module string rewrite: string constants whose entire value is
     ``<moved>.<dotted.tail>`` (mock.patch targets, lazy-import maps, doctor
     wiring probes, __import__ names) get the ``pmcopy.`` prefix. Values whose
     final segment looks like a file extension (``app.yml``, ``bot.py``) are
     excluded. f-string internals are skipped.
  6. Vetted path-text pass over an explicitly enumerated set of test files:
     repo-root-relative joins (``ROOT / "config"`` ->
     ``ROOT / "src" / "pmcopy" / "config"``), ``Path("...")`` literals, and
     standalone slashed literals — each file only with the rule families it
     was reviewed for. Operator-deployment layouts (``tmp_path / "config"``,
     ``deployment / "config"``) are never matched.
  7. Verification re-scan: zero moved-head imports, zero unprefixed dotted
     module strings, inventory present under ``src/pmcopy``, flat roots gone.
  8. ``git add -A`` (never commits).

Review ``git diff --cached`` before committing. ZERO behavior change is the
goal: import rewriting and path fixes only.
"""

from __future__ import annotations

import ast
import re
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
NAMESPACE = "pmcopy"
SRC_DIR = "src"
NS_ROOT = REPO / SRC_DIR / NAMESPACE

# ---------------------------------------------------------------------------
# Inventory (Task 18 contract: 16 packages + 3 modules, nothing else moves).
# ---------------------------------------------------------------------------

PACKAGES: tuple[str, ...] = (
    "adapters",
    "application",
    "bot_cli",
    "cache",
    "clients",
    "config",
    "domain",
    "integrations",
    "interfaces",
    "models",
    "orchestration",
    "orchestrators",
    "ports",
    "services",
    "storage",
    "utils",
)
MODULES: tuple[str, ...] = ("app", "bootstrap", "bot")
MOVED: frozenset[str] = frozenset((*PACKAGES, *MODULES))

INIT_PY = '''"""pmcopy — Polymarket copy-trading bot application namespace.

Single top-level package introduced by the standalone refactor (Task 18).
Every former flat top-level package and module lives under here:
``pmcopy.app``, ``pmcopy.bootstrap``, ``pmcopy.bot``, ``pmcopy.bot_cli``,
``pmcopy.services``, ... No forwarding shims remain at the old flat paths.

Entry points:

* console script: ``pmcopy`` -> ``pmcopy.bot_cli.main:main``
* ``python -m pmcopy`` -> :mod:`pmcopy.__main__` -> :mod:`pmcopy.bot_cli.main`
"""
'''

MAIN_PY = '''"""``python -m pmcopy`` entry (mirrors ``pmcopy.bot``)."""

import sys

from pmcopy.bot_cli.main import main

if __name__ == "__main__":
    sys.exit(main())
'''

# ---------------------------------------------------------------------------
# Small helpers.
# ---------------------------------------------------------------------------


def _git(*args: str) -> str:
    proc = subprocess.run(["git", *args], cwd=REPO, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"git {' '.join(args)} failed:\n{proc.stderr}")
    return proc.stdout


def _tracked(*patterns: str) -> list[str]:
    return [p for p in _git("ls-files", "--", *patterns).splitlines() if p]


def _require_clean_worktree() -> None:
    status = _git("status", "--porcelain")
    if status.strip():
        raise RuntimeError(f"worktree must be clean before migration; got:\n{status}")


def _line_starts(data: bytes) -> list[int]:
    starts = [0]
    for m in re.finditer(b"\n", data):
        starts.append(m.end())
    return starts


def _span(starts: list[int], node: ast.AST) -> tuple[int, int]:
    # ast col_offset/end_col_offset are UTF-8 BYTE offsets within the line,
    # so every span computation happens on the raw bytes (312 tracked files
    # contain non-ASCII text).
    a = starts[node.lineno - 1] + node.col_offset
    b = starts[node.end_lineno - 1] + node.end_col_offset
    return a, b


class _Rewriter:
    """Collect non-overlapping (start, end, replacement) byte edits, apply in reverse."""

    def __init__(self, data: bytes) -> None:
        self.data = data
        self.edits: list[tuple[int, int, bytes]] = []

    def add(self, start: int, end: int, replacement: bytes) -> None:
        self.edits.append((start, end, replacement))

    def apply(self) -> tuple[bytes, int]:
        self.edits.sort(key=lambda e: (e[0], e[1]))
        last_end = -1
        for start, end, _ in self.edits:
            if start < last_end:
                raise RuntimeError(f"overlapping edits at {start} (last end {last_end})")
            last_end = end
        out = self.data
        for start, end, replacement in sorted(self.edits, key=lambda e: -e[0]):
            out = out[:start] + replacement + out[end:]
        return out, len(self.edits)


# ---------------------------------------------------------------------------
# Step 2 helpers: the git mv move set.
# ---------------------------------------------------------------------------


def _verify_inventory() -> None:
    py_files = _tracked("*.py")
    top_dirs = {p.split("/", 1)[0] for p in py_files if "/" in p}
    pkg_roots = {d for d in top_dirs if (REPO / d / "__init__.py").is_file()}
    unexpected = pkg_roots - set(PACKAGES) - {"tests"}
    if unexpected:
        raise RuntimeError(
            f"unexpected tracked python package roots beyond the expected 16: {sorted(unexpected)}"
        )
    missing_pkgs = [p for p in PACKAGES if not (REPO / p / "__init__.py").is_file()]
    if missing_pkgs:
        raise RuntimeError(f"expected packages missing on disk: {missing_pkgs}")
    missing_mods = [m for m in MODULES if not (REPO / f"{m}.py").is_file()]
    if missing_mods:
        raise RuntimeError(f"expected modules missing on disk: {missing_mods}")
    untracked_leftovers = [
        p
        for p in (*PACKAGES, *(f"{m}.py" for m in MODULES))
        if _tracked(p) and not (REPO / p).exists()
    ]
    if untracked_leftovers:
        raise RuntimeError(f"tracked-but-deleted roots present in index: {untracked_leftovers}")


def _already_migrated() -> bool:
    return NS_ROOT.is_dir() and all(not (REPO / p).exists() for p in (*PACKAGES, *(f"{m}.py" for m in MODULES)))


def _do_moves() -> None:
    NS_ROOT.mkdir(parents=True, exist_ok=False)
    for name in (*PACKAGES, *(f"{m}.py" for m in MODULES)):
        _git("mv", name, f"{SRC_DIR}/{NAMESPACE}/{name}")
    (NS_ROOT / "__init__.py").write_text(INIT_PY, encoding="utf-8")
    (NS_ROOT / "__main__.py").write_text(MAIN_PY, encoding="utf-8")
    print(f"[moves] {len(PACKAGES)} packages + {len(MODULES)} modules -> {SRC_DIR}/{NAMESPACE}/")


# ---------------------------------------------------------------------------
# Step 3: exact literal special edits (applied BEFORE the AST passes so the
# `old` texts match the pristine post-move content).
# ---------------------------------------------------------------------------

SYS_PATH_LINE = 'sys.path.insert(0, str(Path(__file__).resolve().parents[1]))'
SYS_PATH_LINE_NEW = 'sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))'

_CORE_PACKAGES_OLD = """CORE_PACKAGES = {
    "config",
    "interfaces",
    "models",
    "services",
    "utils",
    "domain",
    "clients.network",
    "orchestrators",
    "storage",
}"""
_CORE_PACKAGES_NEW = """CORE_PACKAGES = {
    "pmcopy.config",
    "pmcopy.interfaces",
    "pmcopy.models",
    "pmcopy.services",
    "pmcopy.utils",
    "pmcopy.domain",
    "pmcopy.clients.network",
    "pmcopy.orchestrators",
    "pmcopy.storage",
}"""

_PRODUCTION_ROOTS_OLD = """PRODUCTION_ROOTS: tuple[str, ...] = (
    "bootstrap.py",
    "app.py",
    "bot.py",
    "bot_cli",
    "clients",
    "config",
    "domain",
    "integrations",
    "interfaces",
    "orchestrators",
    "scripts",
    "services",
    "storage",
    "utils",
)"""
_PRODUCTION_ROOTS_NEW = """PRODUCTION_ROOTS: tuple[str, ...] = (
    "src/pmcopy/bootstrap.py",
    "src/pmcopy/app.py",
    "src/pmcopy/bot.py",
    "src/pmcopy/bot_cli",
    "src/pmcopy/clients",
    "src/pmcopy/config",
    "src/pmcopy/domain",
    "src/pmcopy/integrations",
    "src/pmcopy/interfaces",
    "src/pmcopy/orchestrators",
    "scripts",
    "src/pmcopy/services",
    "src/pmcopy/storage",
    "src/pmcopy/utils",
)"""

_WHEEL_CONTRACT_OLD = '''def test_console_entrypoint_modules_are_in_the_installed_wheel() -> None:
    project = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text())
    modules = set(project["tool"]["setuptools"]["py-modules"])
    packages = set(project["tool"]["setuptools"]["packages"]["find"]["include"])
    assert {"app", "bootstrap"} <= modules, (
        "pmcopy run imports top-level app/bootstrap modules, so both must be "
        "packaged for non-editable Docker installs"
    )
    assert "cache*" in packages, (
        "app imports the cache package, so it must be included in the wheel"
    )'''
_WHEEL_CONTRACT_NEW = '''def test_console_entrypoint_modules_are_in_the_installed_wheel() -> None:
    project = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text())
    setuptools_cfg = project["tool"]["setuptools"]
    find_cfg = setuptools_cfg["packages"]["find"]
    assert find_cfg["where"] == ["src"], "package discovery must target src/ (src-layout)"
    assert set(find_cfg["include"]) == {"pmcopy*"}, (
        "the single installable namespace must be pmcopy under src/ "
        "(pmcopy.app/pmcopy.bootstrap ship as modules of that package for "
        "non-editable Docker installs)"
    )
    assert "py-modules" not in setuptools_cfg, (
        "flat py-modules must not reappear beside the src/pmcopy namespace"
    )'''

_COMPOSE_MOUNT_OLD = '''    volumes:
      # Keep this tree from the same checkout as the image: config includes
      # Python modules as well as YAML. Fail instead of creating a missing path.
      - type: bind
        source: ./config
        target: /app/config'''
_COMPOSE_MOUNT_NEW = '''    volumes:
      # Keep this tree from the same checkout as the image: config includes
      # Python modules as well as YAML. Fail instead of creating a missing path.
      - type: bind
        source: ./src/pmcopy/config
        target: /app/src/pmcopy/config'''

_COMPOSE_TEST_OLD = '''    mounts = artifacts.compose["services"]["bot"]["volumes"]
    mount = next(
        (item for item in mounts if isinstance(item, dict) and item["target"] == "/app/config"),
        None,
    )
    assert mount is not None, "Compose must mount the canonical config tree"
    assert mount["type"] == "bind"
    assert mount["source"] == "./config"
    assert mount["read_only"] is True
    assert mount["bind"]["create_host_path"] is False
    assert artifacts.environment().get("PYTHONPATH") == "/app", (
        "The installed pmcopy command must import config from the mounted tree"
    )'''
_COMPOSE_TEST_NEW = '''    mounts = artifacts.compose["services"]["bot"]["volumes"]
    mount = next(
        (item for item in mounts if isinstance(item, dict) and item["target"] == "/app/src/pmcopy/config"),
        None,
    )
    assert mount is not None, "Compose must mount the canonical config tree"
    assert mount["type"] == "bind"
    assert mount["source"] == "./src/pmcopy/config"
    assert mount["read_only"] is True
    assert mount["bind"]["create_host_path"] is False
    assert artifacts.environment().get("PYTHONPATH") == "/app/src", (
        "The installed pmcopy command must import config from the mounted tree"
    )'''

_DOCKERFILE_CHECK_OLD = '''# Run this outside /app so the check proves the installed console entry point
# can import the top-level application modules from site-packages. Checking
# from /app masks missing ``py-modules`` packaging because cwd is importable.
RUN cd /tmp && python -c "import app, bootstrap; from config.app_config import load_config; import storage.postgres.connection; print('OK')"'''
_DOCKERFILE_CHECK_NEW = '''# Run this outside /app so the check proves the installed console entry point
# can import the namespaced application modules from site-packages. Checking
# from /app masks missing ``pmcopy`` packaging because cwd is importable.
RUN cd /tmp && python -c "import pmcopy.app, pmcopy.bootstrap; from pmcopy.config.app_config import load_config; import pmcopy.storage.postgres.connection; print('OK')"'''

_DOCKERFILE_PYTHONPATH_OLD = '''# The console script otherwise imports the installed config package and
# silently ignores service YAML mounted at /app/config. Set this only after
# the installed-wheel checks above so those checks cannot use the source tree.
ENV PYTHONPATH=/app'''
_DOCKERFILE_PYTHONPATH_NEW = '''# The console script otherwise imports the installed config package and
# silently ignores service YAML mounted at /app/src/pmcopy/config. Set this
# only after the installed-wheel checks above so those checks cannot use the
# source tree.
ENV PYTHONPATH=/app/src'''

_PYPROJECT_SETUPTOOLS_OLD = '''[tool.setuptools]
py-modules = ["app", "bootstrap", "bot"]

[tool.setuptools.package-data]
"storage.postgres" = ["migrations/*.sql"]
config = [
  "*.yml",
  "*.yaml",
  "traders/*.yml",
  "environments/*/*.yml",
]

[tool.setuptools.packages.find]
include = [
  "cache*",
  "clients*",
  "config*",
  "interfaces*",
  "ports*",
  "models*",
  "services*",
  "utils*",
  "domain*",
  "orchestrators*",
  "storage*",
  "integrations*",
  "bot_cli*",
  "adapters*",
  "application*",
  "orchestration*",
]
exclude = ["tests*", "legacy*"]'''
_PYPROJECT_SETUPTOOLS_NEW = '''[tool.setuptools]

[tool.setuptools.package-data]
"pmcopy.storage.postgres" = ["migrations/*.sql"]
"pmcopy.config" = [
  "*.yml",
  "*.yaml",
  "traders/*.yml",
  "environments/*/*.yml",
]

[tool.setuptools.packages.find]
where = ["src"]
include = ["pmcopy*"]
exclude = ["tests*", "legacy*"]
namespaces = false'''

_CI_BANDIT_OLD = "          bandit -r services orchestrators clients config utils cache storage interfaces domain integrations bot_cli app.py bootstrap.py -ll --skip B101,B311,B608,B108,B310"
_CI_BANDIT_NEW = "          bandit -r src/pmcopy/services src/pmcopy/orchestrators src/pmcopy/clients src/pmcopy/config src/pmcopy/utils src/pmcopy/cache src/pmcopy/storage src/pmcopy/interfaces src/pmcopy/domain src/pmcopy/integrations src/pmcopy/bot_cli src/pmcopy/app.py src/pmcopy/bootstrap.py -ll --skip B101,B311,B608,B108,B310"

_CI_MYPY_OLD = '''          mypy --install-types --non-interactive --explicit-package-bases \\
            --ignore-missing-imports --follow-imports=silent \\
            integrations/polymarket/ccxt_prediction_client.py \\
            integrations/polymarket/ccxt_prediction_gateway.py \\
            integrations/polymarket/venue_runtime.py \\
            clients/network/http_client.py'''
_CI_MYPY_NEW = '''          mypy --install-types --non-interactive --explicit-package-bases \\
            --ignore-missing-imports --follow-imports=silent \\
            src/pmcopy/integrations/polymarket/ccxt_prediction_client.py \\
            src/pmcopy/integrations/polymarket/ccxt_prediction_gateway.py \\
            src/pmcopy/integrations/polymarket/venue_runtime.py \\
            src/pmcopy/clients/network/http_client.py'''

_CI_MYPY_SOFT_OLD = "          mypy --install-types --non-interactive --ignore-missing-imports services orchestrators domain || true"
_CI_MYPY_SOFT_NEW = "          mypy --install-types --non-interactive --ignore-missing-imports src/pmcopy/services src/pmcopy/orchestrators src/pmcopy/domain || true"

_RC1_COMPILEALL_OLD = "python -m compileall -q app.py bot_cli clients orchestrators services cache domain integrations interfaces storage utils"
_RC1_COMPILEALL_NEW = "python -m compileall -q src/pmcopy/app.py src/pmcopy/bot_cli src/pmcopy/clients src/pmcopy/orchestrators src/pmcopy/services src/pmcopy/cache src/pmcopy/domain src/pmcopy/integrations src/pmcopy/interfaces src/pmcopy/storage src/pmcopy/utils"

_RC1_FORBID_OLD = '_FORBID_ROOTS=("app.py" "bot_cli" "clients" "orchestrators" "services" "cache" "domain" "integrations" "interfaces" "storage" "utils" "config")'
_RC1_FORBID_NEW = '_FORBID_ROOTS=("src/pmcopy/app.py" "src/pmcopy/bot_cli" "src/pmcopy/clients" "src/pmcopy/orchestrators" "src/pmcopy/services" "src/pmcopy/cache" "src/pmcopy/domain" "src/pmcopy/integrations" "src/pmcopy/interfaces" "src/pmcopy/storage" "src/pmcopy/utils" "src/pmcopy/config")'

_RC1_CD_OLD = '''cd "$(dirname "$0")/.."

echo "[1/4] compileall..."'''
_RC1_CD_NEW = '''cd "$(dirname "$0")/.."

# Flat layout retired (Task 18): the pmcopy namespace lives under src/.
export PYTHONPATH="src${PYTHONPATH:+:$PYTHONPATH}"

echo "[1/4] compileall..."'''

_RC1_ALLOWED_OLD = 'allowed = {"services/execution/coordinator.py"}'
_RC1_ALLOWED_NEW = 'allowed = {"src/pmcopy/services/execution/coordinator.py"}'

_RC1_ROOTS_OLD = 'for root in ["orchestrators", "services"]:'
_RC1_ROOTS_NEW = 'for root in ["src/pmcopy/orchestrators", "src/pmcopy/services"]:'

_RC1_DOCTOR_OLD = "python -m bot_cli.main doctor --live"
_RC1_DOCTOR_NEW = "python -m pmcopy.bot_cli.main doctor --live"

_GREPTILE_OLD = '''      "config/**",
      "services/**",
      "orchestrators/**",
      "integrations/**",
      "clients/**",
      "domain/**",
      "models/**",
      "utils/**",
      "bot_cli/**",
      "app.py",
      "bot.py",
      "bootstrap.py",'''
_GREPTILE_NEW = '''      "src/pmcopy/config/**",
      "src/pmcopy/services/**",
      "src/pmcopy/orchestrators/**",
      "src/pmcopy/integrations/**",
      "src/pmcopy/clients/**",
      "src/pmcopy/domain/**",
      "src/pmcopy/models/**",
      "src/pmcopy/utils/**",
      "src/pmcopy/bot_cli/**",
      "src/pmcopy/app.py",
      "src/pmcopy/bot.py",
      "src/pmcopy/bootstrap.py",'''

_DOCKERMD_OLD = """Compose mounts the checkout's `config/` read-only, including service YAML,
`config/environments/`, and `config/traders/`. Keep that directory and the image"""
_DOCKERMD_NEW = """Compose mounts the checkout's `src/pmcopy/config/` read-only, including service YAML,
`src/pmcopy/config/environments/`, and `src/pmcopy/config/traders/`. Keep that directory and the image"""


def _special_edits() -> list[tuple[str, str, str, int]]:
    """Return (path, old, new, expected_count) rows. Paths are POST-move."""
    ed: list[tuple[str, str, str, int]] = []

    # --- sys.path bootstraps: tests + golden fixture regenerator + scripts ---
    for rel in (
        "tests/conftest.py",
        "tests/test_decision_engine.py",
        "tests/test_execution_atomic_lock.py",
        "tests/test_execution_idempotency_extended.py",
        "tests/test_idempotency.py",
        "tests/test_issue5_position_accounting.py",
        "tests/test_logger.py",
        "tests/test_logger_secret_redaction.py",
        "tests/test_phase03_atomic_idempotency.py",
        "tests/test_production_services.py",
    ):
        ed.append((rel, SYS_PATH_LINE, SYS_PATH_LINE_NEW, 1))
    ed.append(
        (
            "tests/test_v31_features.py",
            "sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))",
            "sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), \"src\"))",
            1,
        )
    )
    for rel in ("tests/fixtures/golden_logs/regenerate.py",):
        ed.append(
            (
                rel,
                "REPO_ROOT = Path(__file__).resolve().parents[2]\nif str(REPO_ROOT) not in sys.path:\n    sys.path.insert(0, str(REPO_ROOT))",
                "REPO_ROOT = Path(__file__).resolve().parents[2]\nSRC_ROOT = REPO_ROOT / \"src\"\nif str(SRC_ROOT) not in sys.path:\n    sys.path.insert(0, str(SRC_ROOT))",
                1,
            )
        )
    for rel in ("scripts/bench_telemetry_emit.py", "scripts/run_discovery_only.py"):
        ed.append(
            (
                rel,
                "PROJECT_ROOT = Path(__file__).resolve().parents[1]\nif str(PROJECT_ROOT) not in sys.path:\n    sys.path.insert(0, str(PROJECT_ROOT))",
                "PROJECT_ROOT = Path(__file__).resolve().parents[1]\nSRC_ROOT = PROJECT_ROOT / \"src\"\nif str(SRC_ROOT) not in sys.path:\n    sys.path.insert(0, str(SRC_ROOT))",
                1,
            )
        )

    # --- repo-root heuristics inside moved code (depth changed by src/pmcopy) ---
    ed.append(
        (
            "src/pmcopy/bot_cli/commands/doctor.py",
            "def _repo_root() -> Path:\n    return Path(__file__).resolve().parents[2]",
            "def _repo_root() -> Path:\n    return Path(__file__).resolve().parents[4]",
            1,
        )
    )
    ed.append(
        (
            "src/pmcopy/bot_cli/commands/profile_leader.py",
            "_REPO_ROOT = Path(__file__).resolve().parents[2]",
            "_REPO_ROOT = Path(__file__).resolve().parents[4]",
            1,
        )
    )

    # --- doctor code-gate scans walk repo-root-relative source roots ---
    ed.append(
        (
            "src/pmcopy/bot_cli/commands/doctor.py",
            '    roots: tuple[str, ...] = ("orchestrators", "services", "clients", "domain", "cache", "utils"),',
            '    roots: tuple[str, ...] = (\n        "src/pmcopy/orchestrators",\n        "src/pmcopy/services",\n        "src/pmcopy/clients",\n        "src/pmcopy/domain",\n        "src/pmcopy/cache",\n        "src/pmcopy/utils",\n    ),',
            1,
        )
    )
    ed.append(
        (
            "src/pmcopy/bot_cli/commands/doctor.py",
            '    for root in ("orchestrators", "services"):',
            '    for root in ("src/pmcopy/orchestrators", "src/pmcopy/services"):',
            1,
        )
    )
    ed.append(
        (
            "src/pmcopy/bot_cli/commands/doctor.py",
            '_ALIAS_ALLOWED = {"services/execution/coordinator.py"}',
            '_ALIAS_ALLOWED = {"src/pmcopy/services/execution/coordinator.py"}',
            1,
        )
    )

    # --- default log dir stays at the project root, not src/pmcopy ---
    ed.append(
        (
            "src/pmcopy/utils/logger.py",
            "def _project_root() -> Path:\n    return Path(__file__).resolve().parents[1]",
            "def _project_root() -> Path:\n    return Path(__file__).resolve().parents[3]",
            1,
        )
    )

    # --- v1 migration-runner sys.path bootstrap (parents[2] variant) ---
    ed.append(
        (
            "tests/v1/test_v1_migration_runner.py",
            "sys.path.insert(0, str(Path(__file__).resolve().parents[2]))",
            "sys.path.insert(0, str(Path(__file__).resolve().parents[2] / \"src\"))",
            1,
        )
    )

    # --- lazy-load error message names the module it guards ---
    ed.append(
        (
            "src/pmcopy/clients/__init__.py",
            "raise AttributeError(f\"module 'clients' has no attribute {name!r}\")",
            "raise AttributeError(f\"module 'pmcopy.clients' has no attribute {name!r}\")",
            1,
        )
    )

    # --- embedded subprocess probe: config importable without venue clients ---
    ed.append(
        (
            "tests/test_service_config_loader.py",
            "from config.app_config import load_config",
            "from pmcopy.config.app_config import load_config",
            1,
        )
    )
    ed.append(
        (
            "tests/test_service_config_loader.py",
            "import json\nimport subprocess\nimport sys\nfrom pathlib import Path",
            "import json\nimport os\nimport subprocess\nimport sys\nfrom pathlib import Path",
            1,
        )
    )
    ed.append(
        (
            "tests/test_service_config_loader.py",
            "    result = subprocess.run(\n        [sys.executable, \"-c\", probe, str(config_dir), json.dumps(_overrides())],\n        cwd=Path(__file__).resolve().parents[1],",
            "    result = subprocess.run(\n        [sys.executable, \"-c\", probe, str(config_dir), json.dumps(_overrides())],\n        cwd=Path(__file__).resolve().parents[1],\n        env={**os.environ, \"PYTHONPATH\": str(Path(__file__).resolve().parents[1] / \"src\")},",
            1,
        )
    )

    # --- packaging-contract and layout-contract tests ---
    ed.append(("tests/test_packaging_consistency.py", _CORE_PACKAGES_OLD, _CORE_PACKAGES_NEW, 1))
    ed.append(("tests/release_gate/test_no_legacy_imports.py", _PRODUCTION_ROOTS_OLD, _PRODUCTION_ROOTS_NEW, 1))
    ed.append(("tests/release_gate/test_dockerfile_safety_defaults.py", _WHEEL_CONTRACT_OLD, _WHEEL_CONTRACT_NEW, 1))
    ed.append(("tests/test_docker_ccxt_runtime.py", _COMPOSE_TEST_OLD, _COMPOSE_TEST_NEW, 1))

    # --- embedded subprocess scripts (control-plane-free composition roots) ---
    ed.append(("tests/test_control_plane_free_primitives.py", "import subprocess\nimport sys\nfrom pathlib import Path", "import os\nimport subprocess\nimport sys\nfrom pathlib import Path", 1))
    ed.append(("tests/test_control_plane_free_primitives.py", "from adapters.clock import MonotonicClock, SystemMonotonicClock", "from pmcopy.adapters.clock import MonotonicClock, SystemMonotonicClock", 1))
    ed.append(("tests/test_control_plane_free_primitives.py", "from services.trading_control import (", "from pmcopy.services.trading_control import (", 1))
    ed.append(("tests/test_control_plane_free_primitives.py", "import bot_cli.commands.trading as trading_command", "import pmcopy.bot_cli.commands.trading as trading_command", 1))
    ed.append(("tests/test_control_plane_free_primitives.py", "import app  # noqa: F401  (composition root: must import with CP blocked)", "import pmcopy.app  # noqa: F401  (composition root: must import with CP blocked)", 1))
    ed.append(("tests/test_control_plane_free_primitives.py", "import bootstrap  # noqa: F401", "import pmcopy.bootstrap  # noqa: F401", 1))
    ed.append(("tests/test_control_plane_free_primitives.py", "import bot_cli.main as cli_main  # noqa: F401", "import pmcopy.bot_cli.main as cli_main  # noqa: F401", 1))
    ed.append(
        (
            "tests/test_control_plane_free_primitives.py",
            "    return subprocess.run(\n        [sys.executable, \"-c\", script],\n        capture_output=True,\n        text=True,\n        timeout=300,\n        cwd=REPOSITORY_ROOT,\n    )",
            "    return subprocess.run(\n        [sys.executable, \"-c\", script],\n        capture_output=True,\n        text=True,\n        timeout=300,\n        cwd=REPOSITORY_ROOT,\n        env={**os.environ, \"PYTHONPATH\": str(REPOSITORY_ROOT / \"src\")},\n    )",
            1,
        )
    )

    # --- subprocess entry-point smoke tests ---
    ed.append(
        (
            "tests/test_standalone_config_rejects_control_plane.py",
            "from __future__ import annotations\n\nimport subprocess\nimport sys\nfrom pathlib import Path",
            "from __future__ import annotations\n\nimport os\nimport subprocess\nimport sys\nfrom pathlib import Path",
            1,
        )
    )

    # --- packaging-contract tests reading the old flat discovery config ---
    ed.append(
        (
            "tests/test_completion_package_contents.py",
            '    assert any(fnmatch.fnmatchcase("ports", pattern) for pattern in patterns)',
            '    assert any(fnmatch.fnmatchcase("pmcopy.ports", pattern) for pattern in patterns)',
            1,
        )
    )
    ed.append(
        (
            "tests/test_packaging_entrypoint.py",
            'def test_top_level_runtime_modules_are_packaged() -> None:\n    root = Path(__file__).resolve().parents[1]\n    project = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))\n\n    modules = set(project["tool"]["setuptools"]["py-modules"])\n    packages = set(project["tool"]["setuptools"]["packages"]["find"]["include"])\n\n    assert {"app", "bootstrap", "bot"} <= modules\n    assert "cache*" in packages',
            'def test_top_level_runtime_modules_are_packaged() -> None:\n    root = Path(__file__).resolve().parents[1]\n    project = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))\n\n    find_cfg = project["tool"]["setuptools"]["packages"]["find"]\n    packages = set(find_cfg["include"])\n\n    assert find_cfg["where"] == ["src"]\n    assert packages == {"pmcopy*"}\n    assert "py-modules" not in project["tool"]["setuptools"]',
            1,
        )
    )
    ed.append(
        (
            "tests/test_rc1_release_artifacts.py",
            "    result = subprocess.run(\n        [sys.executable, \"-m\", \"bot_cli.main\", \"--help\"],\n        cwd=repo_root,\n        capture_output=True,\n        check=False,\n        text=True,\n    )",
            "    result = subprocess.run(\n        [sys.executable, \"-m\", \"pmcopy.bot_cli.main\", \"--help\"],\n        cwd=repo_root,\n        env={**os.environ, \"PYTHONPATH\": str(repo_root / \"src\")},\n        capture_output=True,\n        check=False,\n        text=True,\n    )",
            1,
        )
    )
    ed.append(("tests/test_rc1_release_artifacts.py", '    assert "python -m bot_cli.main doctor --live" in text', '    assert "python -m pmcopy.bot_cli.main doctor --live" in text', 1))
    ed.append(
        (
            "tests/test_rc1_release_artifacts.py",
            "    missing = {\n        rel: sorted(set(where))\n        for rel, where in referenced.items()\n        if not (repo_root / rel).is_file()\n    }",
            "    missing = {\n        rel: sorted(set(where))\n        for rel, where in referenced.items()\n        if not (repo_root / \"src\" / \"pmcopy\" / rel).is_file()\n    }",
            1,
        )
    )
    ed.append(
        (
            "tests/test_standalone_config_rejects_control_plane.py",
            "    completed = subprocess.run(\n        [sys.executable, \"-m\", \"bot_cli.main\", *command],\n        capture_output=True,\n        text=True,\n        timeout=120,\n        cwd=REPOSITORY_ROOT,\n    )",
            "    completed = subprocess.run(\n        [sys.executable, \"-m\", \"pmcopy.bot_cli.main\", *command],\n        capture_output=True,\n        text=True,\n        timeout=120,\n        cwd=REPOSITORY_ROOT,\n        env={**os.environ, \"PYTHONPATH\": str(REPOSITORY_ROOT / \"src\")},\n    )",
            1,
        )
    )

    # --- one-off dynamic/string references ---
    ed.append(("tests/test_app_onchain_wiring.py", 'assert not hasattr(__import__("app"), "_build_confirmed_onchain_runtime")', 'assert not hasattr(__import__("pmcopy.app"), "_build_confirmed_onchain_runtime")', 1))
    ed.append(
        (
            "tests/test_app_leader_config_attrs.py",
            '_APP_PY = pathlib.Path(__file__).resolve().parent.parent / "app.py"',
            '_APP_PY = pathlib.Path(__file__).resolve().parent.parent / "src" / "pmcopy" / "app.py"',
            1,
        )
    )

    # --- packaging / tooling configuration ---
    ed.append(("pyproject.toml", 'pmcopy = "bot_cli.main:main"', 'pmcopy = "pmcopy.bot_cli.main:main"', 1))
    ed.append(("pyproject.toml", _PYPROJECT_SETUPTOOLS_OLD, _PYPROJECT_SETUPTOOLS_NEW, 1))
    ed.append(("pytest.ini", "testpaths = tests", "testpaths = tests\npythonpath = src", 1))
    ed.append(("Dockerfile", _DOCKERFILE_CHECK_OLD, _DOCKERFILE_CHECK_NEW, 1))
    ed.append(("Dockerfile", "from integrations.polymarket.ccxt_compat.version_guard import", "from pmcopy.integrations.polymarket.ccxt_compat.version_guard import", 1))
    ed.append(("Dockerfile", _DOCKERFILE_PYTHONPATH_OLD, _DOCKERFILE_PYTHONPATH_NEW, 1))
    ed.append(("docker-compose.yml", _COMPOSE_MOUNT_OLD, _COMPOSE_MOUNT_NEW, 1))
    ed.append(
        (
            ".dockerignore",
            "config/bot.toml\n",
            "config/bot.toml\nsrc/pmcopy/config/runtime.local.yml\nsrc/pmcopy/config/runtime.local.*\nsrc/pmcopy/config/bot.toml\n",
            1,
        )
    )
    ed.append(
        (
            ".gitignore",
            "config/runtime.local.yml\nconfig/bot.toml\n",
            "config/runtime.local.yml\nconfig/bot.toml\nsrc/pmcopy/config/runtime.local.yml\nsrc/pmcopy/config/bot.toml\nsrc/pmcopy/config/data/\nsrc/pmcopy/config/logs/\n",
            1,
        )
    )
    ed.append((".github/workflows/ci.yml", _CI_BANDIT_OLD, _CI_BANDIT_NEW, 1))
    ed.append((".github/workflows/ci.yml", _CI_MYPY_OLD, _CI_MYPY_NEW, 1))
    ed.append((".github/workflows/ci.yml", _CI_MYPY_SOFT_OLD, _CI_MYPY_SOFT_NEW, 1))

    # --- operator scripts + tool configs ---
    ed.append(("scripts/rc1_check.sh", _RC1_CD_OLD, _RC1_CD_NEW, 1))
    ed.append(("scripts/rc1_check.sh", _RC1_COMPILEALL_OLD, _RC1_COMPILEALL_NEW, 1))
    ed.append(("scripts/rc1_check.sh", _RC1_FORBID_OLD, _RC1_FORBID_NEW, 1))
    ed.append(("scripts/rc1_check.sh", _RC1_ALLOWED_OLD, _RC1_ALLOWED_NEW, 1))
    ed.append(("scripts/rc1_check.sh", _RC1_ROOTS_OLD, _RC1_ROOTS_NEW, 1))
    ed.append(("scripts/rc1_check.sh", _RC1_DOCTOR_OLD, _RC1_DOCTOR_NEW, 1))
    ed.append(("greptile.json", _GREPTILE_OLD, _GREPTILE_NEW, 1))
    ed.append(("scripts/data/followable_traders.json", '"rotation_command": "python bot.py rotate-traders"', '"rotation_command": "pmcopy rotate-traders"', 1))

    # --- docs: load-bearing invocation/path references only (full pass = Task 26) ---
    ed.append(("README.md", "- **Composition root:** `bootstrap.py::build_app` (pure, no I/O) wires the shared graph; `app.py::main` adds async / per-user wiring.", "- **Composition root:** `src/pmcopy/bootstrap.py::build_app` (pure, no I/O) wires the shared graph; `src/pmcopy/app.py::main` adds async / per-user wiring.", 1))
    ed.append(("README.md", "- **Operator CLI:** `bot.py` → `bot_cli.main` → `doctor`, `config`, `reservations`, `stale-locks`, `drift`.", "- **Operator CLI:** `pmcopy` (console script, or `python -m pmcopy`) → `pmcopy.bot_cli.main` → `doctor`, `config`, `reservations`, `stale-locks`, `drift`.", 1))
    ed.append(("README.md", "python bot.py redeem", "pmcopy redeem", 2))
    ed.append(("docs/03-paper-trading.md", "python3 bot.py rotate-traders", "pmcopy rotate-traders", 1))
    ed.append(("docs/auto-follow.md", "python bot.py run", "pmcopy run", 2))
    ed.append(("docs/handoffs/logs_only_confirmed_source_prompt.md", "python bot.py run", "pmcopy run", 1))
    ed.append(("docs/historical-evidence-import.md", "PYTHONPATH=. .venv/bin/python -m bot_cli.main", "PYTHONPATH=src .venv/bin/python -m pmcopy.bot_cli.main", 2))
    ed.append(("docs/known_limitations.md", "python bot.py resolve", "pmcopy resolve", 1))
    ed.append(("docs/rc1_validation.md", "python -m bot_cli.main doctor --live", "python -m pmcopy.bot_cli.main doctor --live", 1))
    ed.append(("docs/recovery_playbook.md", "python -m bot_cli.main doctor --live", "python -m pmcopy.bot_cli.main doctor --live", 1))
    ed.append(("docs/recovery_playbook.md", "python bot.py resolve", "pmcopy resolve", 2))
    ed.append(("docs/redemption.md", "python bot.py redeem", "pmcopy redeem", 5))
    ed.append(("docs/runbooks/00-index.md", "python3 bot.py ", "pmcopy ", 2))
    ed.append(("docs/trader_discovery.md", "python bot.py run --auto-trader", "pmcopy run --auto-trader", 1))
    ed.append(("docs/docker.md", _DOCKERMD_OLD, _DOCKERMD_NEW, 1))

    return ed


def _apply_special_edits() -> int:
    applied = 0
    by_path: dict[str, list[tuple[str, str, int]]] = {}
    for path, old, new, count in _special_edits():
        by_path.setdefault(path, []).append((old, new, count))
    for rel, rows in sorted(by_path.items()):
        target = REPO / rel
        if not target.is_file():
            raise RuntimeError(f"special edit target missing: {rel}")
        text = target.read_text(encoding="utf-8")
        for old, new, count in rows:
            found = text.count(old)
            if found != count:
                raise RuntimeError(
                    f"special edit mismatch in {rel}: expected {count} occurrence(s) of:\n{old[:120]!r}\nfound {found}"
                )
            text = text.replace(old, new)
            applied += count
        target.write_text(text, encoding="utf-8")
    print(f"[special-edits] {applied} exact replacements across {len(by_path)} files")
    return applied


# ---------------------------------------------------------------------------
# Steps 4+5: AST-based import and dotted-module-string rewrites.
# ---------------------------------------------------------------------------

_DOTTED_RE = re.compile(r"^(?:" + "|".join(sorted(MOVED)) + r")(?:\.[A-Za-z_]\w*)+$")
# Tail segments that make a dotted string a FILE NAME (e.g. "app.yml") rather
# than a module path. Attribute-plausible tails (log, db, env, sh) are
# deliberately NOT excluded — real patch targets like
# "services.execution.reconciliation_service._log.log" end in them.
_FILE_EXT_RE = re.compile(
    r"\.(?:py|pyi|yml|yaml|toml|json|sql|md|txt|cfg|ini|lock|example)$"
)


def _rewrite_module_source(data: bytes, rel: str) -> tuple[bytes, dict[str, int]]:
    try:
        tree = ast.parse(data, filename=rel)
    except SyntaxError as exc:  # pragma: no cover - suite is green pre-migration
        raise RuntimeError(f"SyntaxError parsing {rel}: {exc}") from exc
    starts = _line_starts(data)
    rw = _Rewriter(data)
    stats = {"imports": 0, "strings": 0}
    prefix = f"{NAMESPACE}.".encode()

    parents: dict[int, ast.AST] = {}
    for parent in ast.walk(tree):
        for child in ast.iter_child_nodes(parent):
            parents[id(child)] = parent

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.split(".")[0] in MOVED:
                    a, b = _span(starts, alias)
                    seg = data[a:b]
                    if not seg.startswith(alias.name.encode()):  # pragma: no cover - defensive
                        raise RuntimeError(f"alias span mismatch in {rel}: {seg!r}")
                    if alias.asname is None:
                        # ``import X`` binds the name ``X``; a plain
                        # ``import pmcopy.X`` would bind ``pmcopy`` instead and
                        # break every later bare ``X.`` reference. Keep the
                        # original binding with an explicit alias.
                        top = alias.name.split(".")[0].encode()
                        rw.add(
                            a,
                            a + len(alias.name.encode()),
                            prefix + alias.name.encode() + b" as " + top,
                        )
                    else:
                        rw.add(a, a + len(alias.name.encode()), prefix + alias.name.encode())
                    stats["imports"] += 1
        elif isinstance(node, ast.ImportFrom):
            if node.level or not node.module:
                continue
            if node.module.split(".")[0] not in MOVED:
                continue
            a, b = _span(starts, node)
            seg = data[a:b]
            module_b = node.module.encode()
            new_seg, n = re.subn(
                b"(\\bfrom\\s+)" + re.escape(module_b) + b"(?=\\s+import\\b)",
                lambda m: m.group(1) + prefix + module_b,
                seg,
                count=1,
            )
            if n != 1:  # pragma: no cover - defensive
                raise RuntimeError(f"ImportFrom rewrite failed in {rel}: {seg[:80]!r}")
            rw.add(a, b, new_seg)
            stats["imports"] += 1
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            value = node.value
            if not _DOTTED_RE.match(value) or _FILE_EXT_RE.search(value):
                continue
            parent = parents.get(id(node))
            if isinstance(parent, ast.JoinedStr):
                continue  # f-string internals: skip (conservative)
            a, b = _span(starts, node)
            seg = data[a:b]
            value_b = value.encode()
            if seg.count(value_b) == 1:
                rw.add(a, b, seg.replace(value_b, prefix + value_b, 1))
            else:
                # Implicit string concatenation ("a." "b"): the Constant span
                # covers several adjacent literals whose concatenation is the
                # value. Insert the prefix right after the first opening
                # quote — always the start of the concatenated value.
                quote_b = seg[-1:] if seg.endswith((b'"', b"'")) else None
                if quote_b is None:  # pragma: no cover - defensive
                    raise RuntimeError(f"dotted string not verbatim in {rel}: {value!r}")
                inner_start = seg.find(quote_b)
                first_chunk_end = seg.find(quote_b, inner_start + 1)
                first_chunk = seg[inner_start + 1 : first_chunk_end]
                if inner_start < 0 or not first_chunk or not value_b.startswith(first_chunk):
                    raise RuntimeError(f"dotted string not verbatim in {rel}: {value!r}")
                insert_at = a + inner_start + 1
                rw.add(insert_at, insert_at, prefix)
            stats["strings"] += 1

    out, _ = rw.apply()
    return out, stats


def _run_ast_pass() -> dict[str, int]:
    totals = {"files": 0, "imports": 0, "strings": 0}
    for rel in _tracked("*.py"):
        path = REPO / rel
        data = path.read_bytes()
        new_data, stats = _rewrite_module_source(data, rel)
        if new_data != data:
            path.write_bytes(new_data)
            totals["files"] += 1
        totals["imports"] += stats["imports"]
        totals["strings"] += stats["strings"]
    print(
        f"[ast] import statements rewritten: {totals['imports']}; "
        f"dotted module strings rewritten: {totals['strings']}; files touched: {totals['files']}"
    )
    return totals


# Corrections applied AFTER the AST passes: two dotted literals that the
# general module-path rule rewrote but which are NOT module references — they
# are expected substrings matched against production SOURCE TEXT or unchanged
# operator documentation, so they must keep their original spelling.
_POST_AST_EDITS: tuple[tuple[str, str, str, int], ...] = (
    # app.py's ``application.config.copy_trading.copy_tasks`` is attribute
    # access in source, not an import path.
    (
        "tests/test_app_v1_runtime_wiring.py",
        'assert "pmcopy.config.copy_trading.copy_tasks" in source',
        'assert "config.copy_trading.copy_tasks" in source',
        1,
    ),
    # docs/rc1_validation.md and rc1_check.sh deliberately carry the raw
    # forbidden-token spelling; the scanning substring still matches
    # pmcopy-prefixed source via substring containment.
    (
        "tests/test_rc1_release_artifacts.py",
        '"pmcopy.services.price_cache",',
        '"services.price_cache",',
        2,
    ),
)


def _apply_post_ast_edits() -> int:
    applied = 0
    for rel, old, new, count in _POST_AST_EDITS:
        target = REPO / rel
        text = target.read_text(encoding="utf-8")
        found = text.count(old)
        if found != count:
            raise RuntimeError(f"post-AST edit mismatch in {rel}: expected {count}, found {found}")
        target.write_text(text.replace(old, new), encoding="utf-8")
        applied += count
    print(f"[post-ast] {applied} correction(s) applied")
    return applied


# ---------------------------------------------------------------------------
# Step 6: vetted path-text pass (repo-root joins / Path(...) / slashed literals).
# ---------------------------------------------------------------------------

_JOIN_LEFT = r"(?:\bROOT\b|\broot\b|\brepo_root\b|\bREPOSITORY_ROOT\b|parents\[\d\])"
_BARE = "|".join(PACKAGES)
_SLASHED = rf"(?:{_BARE})/[\w./-]+"
_PYFILE = r"(?:app|bootstrap|bot)\.py"

# Per-file rule families: which of (join, path, bare) are reviewed-true.
VETTED_PATH_FILES: dict[str, tuple[bool, bool, bool]] = {
    "tests/config_helpers.py": (True, False, False),
    "tests/test_app_onchain_wiring.py": (True, False, False),
    "tests/test_ccxt_only_runtime_cutover.py": (True, False, True),
    "tests/test_config.py": (True, False, False),
    "tests/test_config_layer_precedence.py": (True, False, False),
    "tests/test_control_plane_free_primitives.py": (True, False, False),
    "tests/test_discovery_policy_config.py": (True, False, False),
    "tests/test_falcon_permission_context.py": (True, False, False),
    "tests/test_falcon_premium_startup.py": (True, False, False),
    "tests/test_leader_wallet_config.py": (True, False, False),
    "tests/test_live_environment_overrides.py": (True, False, False),
    "tests/test_python_311_compatibility.py": (True, False, False),
    "tests/test_rc1_release_artifacts.py": (True, False, False),
    "tests/test_reconcile_cli_sdk_default.py": (False, False, True),
    "tests/test_standalone_config_rejects_control_plane.py": (True, False, False),
    "tests/test_config_yaml_documentation.py": (True, False, False),
    "tests/test_retry_backoff_jitter.py": (True, False, False),
    "tests/v1/test_v1_migration.py": (True, False, False),
    "tests/release_gate/test_no_legacy_imports.py": (True, False, False),
    "tests/release_gate/test_startup_recovery.py": (True, False, False),
    "tests/release_gate/test_wss_after_reconcile_ordering.py": (True, False, False),
    "tests/test_doctor_app_startup.py": (True, False, False),
    "tests/v1/test_v1_runtime_config.py": (True, False, False),
    "tests/v1/test_v1_runtime_bootstrap.py": (False, True, False),
    "tests/v1/test_v1_app_event_wiring.py": (False, True, False),
    "tests/test_aethertrade_native_runtime.py": (False, True, False),
}


def _vetted_path_pass() -> int:
    total = 0
    for rel, (join, pathform, bare) in sorted(VETTED_PATH_FILES.items()):
        target = REPO / rel
        if not target.is_file():
            raise RuntimeError(f"vetted path file missing: {rel}")
        text = target.read_text(encoding="utf-8")
        alts = []
        if join:
            alts.append(rf"(?P<jleft>{_JOIN_LEFT}\s*/\s*)(?P<jq>[\"'])(?P<jseg>{_SLASHED}|{_PYFILE}|{_BARE})(?P=jq)")
        if pathform:
            alts.append(rf"Path\(\s*(?P<pq>[\"'])(?P<pseg>{_SLASHED}|{_PYFILE})(?P=pq)\s*\)")
        if bare:
            alts.append(rf"(?<![\w/.])(?P<bq>[\"'])(?P<bseg>{_SLASHED})(?P=bq)")
        if not alts:
            continue
        pattern = re.compile("|".join(alts))

        def _sub(m: re.Match[str]) -> str:
            groups = m.re.groupindex
            if "jleft" in groups and m.group("jleft") is not None:
                return f'{m.group("jleft")}"src" / "{NAMESPACE}" / {m.group("jq")}{m.group("jseg")}{m.group("jq")}'
            if "pq" in groups and m.group("pq") is not None:
                return f'Path({m.group("pq")}src/{NAMESPACE}/{m.group("pseg")}{m.group("pq")})'
            return f'{m.group("bq")}src/{NAMESPACE}/{m.group("bseg")}{m.group("bq")}'

        new_text, n = pattern.subn(_sub, text)
        if n:
            target.write_text(new_text, encoding="utf-8")
            total += n
            print(f"[paths] {rel}: {n} rewrite(s)")
    return total


# ---------------------------------------------------------------------------
# Step 7: verification re-scan.
# ---------------------------------------------------------------------------


def _verify() -> None:
    problems: list[str] = []
    # The two _POST_AST_EDITS corrections legitimately leave these unprefixed
    # dotted literals in place (they match source text / operator docs, not
    # module paths).
    allowlist = {(rel, new.split('"')[1]) for rel, _old, new, _c in _POST_AST_EDITS}
    for rel in _tracked("*.py"):
        text = (REPO / rel).read_text(encoding="utf-8")
        try:
            tree = ast.parse(text, filename=rel)
        except SyntaxError as exc:
            problems.append(f"{rel}: SyntaxError after rewrite: {exc}")
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name.split(".")[0] in MOVED:
                        problems.append(f"{rel}: flat import remains: import {alias.name}")
            elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                if node.module.split(".")[0] in MOVED:
                    problems.append(f"{rel}: flat import remains: from {node.module} import ...")
            elif isinstance(node, ast.Constant) and isinstance(node.value, str):
                if _DOTTED_RE.match(node.value) and not _FILE_EXT_RE.search(node.value):
                    if (rel, node.value) in allowlist:
                        continue
                    problems.append(f"{rel}: unprefixed dotted module string remains: {node.value!r}")
    for name in (*PACKAGES, *(f"{m}.py" for m in MODULES)):
        if (REPO / name).exists():
            problems.append(f"flat root still exists: {name}")
    for name in (*PACKAGES, *(f"{m}.py" for m in MODULES)):
        if not (NS_ROOT / name).exists():
            problems.append(f"missing under src/{NAMESPACE}: {name}")
    if not (NS_ROOT / "__init__.py").is_file() or not (NS_ROOT / "__main__.py").is_file():
        problems.append("src/pmcopy/__init__.py or __main__.py missing")
    if problems:
        raise RuntimeError("verification failed:\n  " + "\n  ".join(problems))
    py_count = len(_tracked("*.py"))
    print(f"[verify] clean: {py_count} tracked .py files, no flat imports/strings remain")


def main() -> int:
    if _already_migrated():
        print("already migrated (src/pmcopy present, flat roots gone) — nothing to do")
        return 0
    if NS_ROOT.exists():
        raise RuntimeError("src/pmcopy exists but flat roots remain — tree is half-migrated; restore and retry")
    _require_clean_worktree()
    _verify_inventory()
    _do_moves()
    _apply_special_edits()
    _run_ast_pass()
    _apply_post_ast_edits()
    _vetted_path_pass()
    _verify()
    _git("add", "-A")
    stat = _git("diff", "--cached", "--stat").splitlines()[-1]
    print(f"[staged] {stat}")
    print("migration complete — review `git diff --cached`, then commit")
    return 0


if __name__ == "__main__":
    sys.exit(main())
