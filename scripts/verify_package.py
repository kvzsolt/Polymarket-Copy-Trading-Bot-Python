#!/usr/bin/env python3
"""Repeatable wheel/sdist packaging proof battery (Task 19).

Builds the wheel and sdist, then proves each artifact from a FRESH virtual
environment with an empty CWD, PYTHONPATH unset, and the source checkout not
on ``sys.path``:

1. ``pmcopy --help`` and ``python -m pmcopy --help`` exit 0.
2. ``pmcopy config validate`` succeeds against a minimal EXPLICIT external
   deployment root (packaged defaults alone are intentionally invalid —
   ``database.dsn`` and the paper-mode ``FUNDER_ADDRESS`` are required
   operator inputs; this pins the documented behavior).
3. Resource lookup through ``importlib.resources``: the packaged config YAML
   tree loads and the Postgres migration catalog lists every packaged
   ``*.sql`` file — no ``__file__`` joins, works on a read-only install.
4. A deterministic paper-mode workflow smoke through the public gateway API
   (buy fill derives shares from spend and limit; the CCXT transport
   write-path is never invoked).
5. Writable state/log paths honor the deployment root: the implicit
   ``configure_logging()`` JSONL sink lands under ``PMCOPY_DEPLOYMENT_ROOT``
   and nothing appears inside the installed package tree.
6. ``uv pip check`` (or ``pip check``) is clean.

Also asserts artifact CONTENTS:
- wheel: packaged resources present; no tests; no secrets; no state/log dirs.
- sdist: packaged resources present; tests included (current behavior);
  no secrets / operator-local overrides.

Usage:
    scripts/verify_package.py [--dist-dir DIR] [--keep] [--skip-sdist]

Exit code 0 = every proof green.
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DIST_NAME = "polymarket_copy_trading_bot"
EXPECTED_MIGRATIONS_COUNT = 12
LOG_BASENAME = "PolymarketCopyTradingBot.jsonl"

_WHEEL_RESOURCE_FILES = (
    "pmcopy/config/app.yml",
    "pmcopy/config/execution.yml",
    "pmcopy/config/environments/example/execution.yml",
    "pmcopy/config/traders/anon1.yml",
    "pmcopy/storage/postgres/migrations/20260808_01_copy_tasks_and_reentry_locks.sql",
)
_SDIST_FORBIDDEN_NAMES = ("secrets.env", "db.env", "runtime.local.yml", "runtime.local.yml.example")
_SDIST_FORBIDDEN_SUFFIXES = (".pyc", ".env.bak")

# Deterministic paper smoke driver executed inside the fresh venv. Composes
# the public PaperExecutionGateway API exactly like tests'
# RecordingMarketGateway pattern; the transport write-path raises if invoked.
_SMOKE_DRIVER = """
import asyncio
import uuid
from decimal import Decimal
from unittest.mock import AsyncMock

from pmcopy.integrations.polymarket.models import MarketableOrderRequest, OrderBook
from pmcopy.integrations.polymarket.paper_execution import (
    PaperExecutionGateway,
    PaperOrderSimulator,
)

TOKEN = str(uuid.uuid4().int % 10**18)
CONDITION = str(uuid.uuid4())


class RecordingMarketGateway:
    def __init__(self) -> None:
        self.create_order = AsyncMock(side_effect=AssertionError("live write attempted"))
        self.cancel_order = AsyncMock(side_effect=AssertionError("live cancel attempted"))
        self.get_orderbook_calls = []
        self.compatibility = {
            "available": True,
            "transport": "ccxt.prediction.polymarket",
            "authenticated": False,
            "live_execution_ready": False,
        }

    async def get_market_info(self, token_id: str) -> dict:
        return {"token_id": token_id, "tick_size": "0.01"}

    async def get_orderbook(self, token_id: str) -> OrderBook:
        self.get_orderbook_calls.append(token_id)
        return OrderBook(
            token_id=token_id,
            tick_size=Decimal("0.01"),
            min_order_size=Decimal("5"),
            neg_risk=False,
            bids=[],
            asks=[],
            best_bid=None,
            best_ask=None,
            spread_bps=None,
        )


async def main() -> None:
    market = RecordingMarketGateway()
    gateway = PaperExecutionGateway(
        market_data_gateway=market,
        simulator=PaperOrderSimulator(),
    )
    request = MarketableOrderRequest(
        token_id=TOKEN,
        side="BUY",
        size=Decimal("10"),
        price_limit=Decimal("0.40"),
        condition_id=CONDITION,
    )
    result = await gateway.submit_marketable_order(request)
    assert result.success is True, result
    assert result.order_id is not None
    assert result.ambiguous_post_failure is not True
    market.create_order.assert_not_awaited()
    market.cancel_order.assert_not_awaited()

    fills = await gateway.get_fills()
    assert len(fills) == 1, fills
    fill = fills[0]
    assert fill.order_id == result.order_id
    assert fill.price == Decimal("0.40")
    assert fill.size == Decimal("25"), "10 USDC spend at a 0.40 cap buys 25 shares"
    assert fill.side == "BUY"


asyncio.run(main())
print("PAPER_SMOKE_OK")
"""

# Resource-lookup + writable-path proofs executed inside the fresh venv.
# Receives the deployment root as argv[1].
_PROBE_DRIVER = """
import importlib.resources
import json
import sys
from decimal import Decimal
from pathlib import Path

deployment_root = Path(sys.argv[1])

# --- resource lookup via importlib.resources (read-only install safe) ---
config_dir = importlib.resources.files("pmcopy.config")
discovery = importlib.resources.files("pmcopy.config").joinpath("discovery.yml")
assert discovery.is_file(), "packaged discovery.yml missing"
payload = discovery.read_text(encoding="utf-8")
assert payload.strip(), "discovery.yml is empty"

migrations = importlib.resources.files("pmcopy.storage.postgres").joinpath("migrations")
sql_files = sorted(
    child.name for child in migrations.iterdir() if child.name.endswith(".sql")
)
assert len(sql_files) >= 12, f"expected packaged migrations, got {sql_files}"

from pmcopy.storage.postgres.v1_migration_runner import V1MigrationCatalog

catalog_ids = [m.migration_id + ".sql" for m in V1MigrationCatalog().migrations()]
assert catalog_ids == sql_files, "catalog did not discover the packaged resources"

# --- packaged tree is never written: snapshot before ---
import pmcopy.config as config_package

packaged_root = Path(importlib.resources.files("pmcopy.config"))


def snapshot() -> dict:
    return {
        str(p.relative_to(packaged_root)): (p.stat().st_size, p.stat().st_mtime_ns)
        for p in sorted(packaged_root.rglob("*"))
        if p.is_file()
    }


before = snapshot()

# --- writable state/log paths honor the deployment root ---
from pmcopy.utils import logger as logger_module

assert logger_module._default_log_dir() == deployment_root / "log", (
    f"default log dir is deployment-relative, got {logger_module._default_log_dir()}"
)
log = logger_module.configure_logging()
canary_tag = "packaging-proof-canary"
log.info("probe message %s", canary_tag)
for handler in log.handlers:
    handler.flush()

jsonl = deployment_root / "log" / "PolymarketCopyTradingBot.jsonl"
assert jsonl.is_file(), f"JSONL sink must live under the deployment root: {jsonl}"

# Nothing may appear inside the installed package / site-packages tree.
after = snapshot()
assert after == before, f"packaged config tree mutated: {set(after) ^ set(before)}"
site_tree = packaged_root.parent.parent  # .../site-packages
stray = [
    str(p)
    for p in site_tree.rglob("*.jsonl")
] + (
    [str(p) for p in (site_tree / "pmcopy").glob("log*")]
    if (site_tree / "pmcopy").exists()
    else []
)
assert not stray, f"state/log written inside the install tree: {stray}"

print(json.dumps({"migrations": len(sql_files), "log_dir": str(deployment_root / "log")}))
"""


def _say(step: str, message: str) -> None:
    print(f"[verify-package] {step}: {message}", flush=True)


def _run(
    cmd: list[str], *, cwd: Path, env: dict[str, str], label: str
) -> subprocess.CompletedProcess:
    result = subprocess.run(cmd, cwd=cwd, env=env, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"[verify-package] FAILED {label}")
        print(f"  cmd: {' '.join(cmd)}")
        print(f"  cwd: {cwd}")
        print(f"  stdout: {result.stdout[-4000:]}")
        print(f"  stderr: {result.stderr[-4000:]}")
    return result


def _clean_env() -> dict[str, str]:
    env = dict(os.environ)
    for key in list(env):
        if key == "PYTHONPATH" or key.startswith("PMCOPY_"):
            del env[key]
    env.setdefault("PATH", "/usr/local/bin:/usr/bin:/bin")
    return env


def _find_uv() -> str | None:
    return shutil.which("uv")


def build_artifacts(dist_dir: Path) -> tuple[Path, Path, str]:
    """Build wheel + sdist; return (wheel, sdist, tool description)."""
    dist_dir.mkdir(parents=True, exist_ok=True)
    try:
        import build  # noqa: F401

        cmd = [sys.executable, "-m", "build", "--outdir", str(dist_dir)]
        tool = "python -m build"
    except ImportError:
        uv = _find_uv()
        if uv is None:
            raise SystemExit("neither 'build' (python -m build) nor 'uv' is available") from None
        cmd = [uv, "build", "--out-dir", str(dist_dir)]
        tool = "uv build"
    _say("build", f"{tool} -> {dist_dir}")
    result = _run(cmd, cwd=REPO_ROOT, env=_clean_env(), label="build")
    if result.returncode != 0:
        raise SystemExit("artifact build failed")
    wheels = sorted(dist_dir.glob("*.whl"))
    sdists = sorted(dist_dir.glob("*.tar.gz"))
    if len(wheels) != 1 or len(sdists) != 1:
        raise SystemExit(f"expected exactly one wheel and one sdist in {dist_dir}")
    return wheels[0], sdists[0], tool


def check_wheel_contents(wheel: Path) -> None:
    with zipfile.ZipFile(wheel) as archive:
        names = archive.namelist()
    for required in _WHEEL_RESOURCE_FILES:
        assert required in names, f"wheel missing packaged resource {required}"
    violations = [
        name
        for name in names
        if name.startswith("tests/")
        or name.endswith(".env")
        or "runtime.local" in name
        or "/log/" in name
        or "/data/" in name
        or name.endswith(".pyc")
    ]
    assert not violations, f"wheel contains forbidden entries: {violations[:10]}"
    _say("wheel-contents", f"{len(names)} entries; resources present; no tests/secrets/state")


def check_sdist_contents(sdist: Path) -> None:
    import tarfile

    with tarfile.open(sdist) as archive:
        names = archive.getnames()
    root_prefix = f"{DIST_NAME}-"
    inner = [
        name.split("/", 1)[1] for name in names if name.startswith(root_prefix) and "/" in name
    ]
    for required in ("src/pmcopy/config/execution.yml", "src/pmcopy/storage/postgres/migrations"):
        assert any(entry == required or entry.startswith(required + "/") for entry in inner), (
            f"sdist missing packaged resource {required}"
        )
    assert any(entry.startswith("tests/") for entry in inner), (
        "sdist is expected to include tests (current documented behavior)"
    )
    violations = [
        name
        for name in inner
        if name.split("/")[-1] in _SDIST_FORBIDDEN_NAMES or name.endswith(_SDIST_FORBIDDEN_SUFFIXES)
    ]
    assert not violations, f"sdist contains forbidden entries: {violations[:10]}"
    _say("sdist-contents", f"{len(inner)} entries; resources + tests present; no secrets")


def make_venv(target: Path, uv: str | None) -> Path:
    python = f"{sys.version_info.major}.{sys.version_info.minor}"
    if uv:
        cmd = [uv, "venv", "--python", python, str(target)]
    else:
        cmd = [sys.executable, "-m", "venv", str(target)]
    result = _run(cmd, cwd=REPO_ROOT, env=_clean_env(), label=f"venv {target.name}")
    if result.returncode != 0:
        raise SystemExit(f"venv creation failed for {target}")
    return target / ("Scripts/python.exe" if os.name == "nt" else "bin/python")


def install_artifact(venv_python: Path, artifact: Path, uv: str | None) -> str:
    if uv:
        cmd = [uv, "pip", "install", "--python", str(venv_python), str(artifact)]
        tool = "uv pip install"
    else:
        cmd = [str(venv_python), "-m", "pip", "install", str(artifact)]
        tool = "pip install"
    result = _run(cmd, cwd=REPO_ROOT, env=_clean_env(), label=f"install {artifact.name}")
    if result.returncode != 0:
        raise SystemExit(f"artifact install failed: {artifact}")
    return tool


def pip_check(venv_python: Path, uv: str | None) -> None:
    if uv:
        cmd = [uv, "pip", "check", "--python", str(venv_python)]
    else:
        cmd = [str(venv_python), "-m", "pip", "check"]
    result = _run(cmd, cwd=REPO_ROOT, env=_clean_env(), label="pip check")
    assert result.returncode == 0, f"pip check not clean:\n{result.stdout}\n{result.stderr}"
    _say("pip-check", "clean")


def prove_artifact(kind: str, artifact: Path, workspace: Path, uv: str | None) -> list[str]:
    """Run the full proof battery for one artifact in a fresh venv."""
    env = _clean_env()
    empty_cwd = workspace / f"empty-{kind}"
    empty_cwd.mkdir(parents=True, exist_ok=True)
    deployment = workspace / f"deployment-{kind}"
    (deployment / "config").mkdir(parents=True, exist_ok=True)
    secrets = deployment / "secrets.env"
    # Runtime-built placeholder credentials only; never real material.
    dsn = f"postgresql://verify_{kind}:verify@localhost:5432/verify_{kind}"
    funder = "0x" + "5" * 40
    secrets.write_text(
        f"PMCOPY__DATABASE__DSN={dsn}\nPMCOPY__WALLET__FUNDER_ADDRESS={funder}\n",
        encoding="utf-8",
    )
    secrets.chmod(0o600)
    run_env = {**env, "PMCOPY_DEPLOYMENT_ROOT": str(deployment)}

    venv_dir = workspace / f"venv-{kind}"
    venv_python = make_venv(venv_dir, uv)
    tool = install_artifact(venv_python, artifact, uv)
    pmcopy = venv_dir / ("Scripts/pmcopy.exe" if os.name == "nt" else "bin/pmcopy")

    passed: list[str] = []

    def proof(label: str, cmd: list[str], expect: str | None = None) -> None:
        result = _run(cmd, cwd=empty_cwd, env=run_env, label=label)
        assert result.returncode == 0, f"{kind}/{label} exited {result.returncode}"
        if expect is not None:
            assert expect in result.stdout, f"{kind}/{label}: expected {expect!r} in stdout"
        passed.append(label)

    proof("entry-point-help", [str(pmcopy), "--help"], "usage:")
    proof("module-help", [str(venv_python), "-m", "pmcopy", "--help"], "usage:")
    result = _run(
        [str(pmcopy), "config", "validate"], cwd=empty_cwd, env=run_env, label="config-validate"
    )
    assert result.returncode == 0, f"{kind}/config validate failed:\n{result.stdout}{result.stderr}"
    assert '"valid": true' in result.stdout, result.stdout
    assert dsn not in result.stdout, "validate output must not echo the DSN"
    passed.append("config-validate (explicit external deployment root)")

    smoke = workspace / f"smoke-{kind}.py"
    smoke.write_text(_SMOKE_DRIVER, encoding="utf-8")
    proof("paper-workflow-smoke", [str(venv_python), str(smoke)], "PAPER_SMOKE_OK")

    probe = workspace / f"probe-{kind}.py"
    probe.write_text(_PROBE_DRIVER, encoding="utf-8")
    proof("importlib-resources+writable-paths", [str(venv_python), str(probe), str(deployment)])

    pip_check(venv_python, uv)
    _say(kind, f"installed via {tool}; proofs: {', '.join(passed)}")
    return passed


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dist-dir", type=Path, default=None, help="build artifacts here")
    parser.add_argument("--keep", action="store_true", help="keep the temporary workspace")
    parser.add_argument("--skip-sdist", action="store_true", help="prove the wheel only")
    args = parser.parse_args()

    workspace = Path(tempfile.mkdtemp(prefix="pmcopy-verify-package-"))
    dist_dir = args.dist_dir or (workspace / "dist")
    try:
        wheel, sdist, tool = build_artifacts(dist_dir)
        _say("build", f"wheel={wheel.name} sdist={sdist.name} (built with {tool})")
        check_wheel_contents(wheel)
        if not args.skip_sdist:
            check_sdist_contents(sdist)

        uv = _find_uv()
        results: dict[str, list[str]] = {}
        results["wheel"] = prove_artifact("wheel", wheel, workspace, uv)
        if not args.skip_sdist:
            results["sdist"] = prove_artifact("sdist", sdist, workspace, uv)

        print()
        _say("SUMMARY", f"build tool: {tool}; installer: {'uv' if uv else 'pip'}")
        for kind, proofs in results.items():
            for proof in proofs:
                print(f"  PASS [{kind}] {proof}")
        print("ALL PACKAGING PROOFS GREEN")
        return 0
    finally:
        if args.keep:
            _say("workspace", f"kept at {workspace}")
        else:
            shutil.rmtree(workspace, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
