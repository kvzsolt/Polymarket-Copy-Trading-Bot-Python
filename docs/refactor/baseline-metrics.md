# Baseline metrics — standalone refactor

Captured 2026-10-11 in the refactor worktree
(`refactor/standalone-pmcopy`, HEAD `925ad0d111443ee9cb032aafa5032c70eb061ba8`
"docs: add standalone-refactor implementation plan (SDD task source)").
Environment: Linux 6.12.111+deb13-amd64 x64. Every number below comes from a
command that is quoted next to it; nothing is estimated.

## Toolchain versions

| Component | Version | Command |
|---|---|---|
| Python | 3.12.13 | `.venv/bin/python --version` |
| pip | not installed in venv (uv-managed venv) | `.venv/bin/python -m pip --version` → `No module named pip` |
| uv | 0.11.31 | `uv --version` |
| ccxt | 4.5.78 (pinned `==4.5.78` in pyproject) | `.venv/bin/python -c "import ccxt; print(ccxt.__version__)"` |
| pytest | 9.0.3 | `.venv/bin/python -m pytest --version` |
| pytest-asyncio | 1.4.0 | `importlib.metadata.version("pytest-asyncio")` |
| rich | 14.3.3 | `importlib.metadata.version("rich")` |
| asyncpg | 0.31.0 | `importlib.metadata.version("asyncpg")` |
| web3 | 7.15.0 | `importlib.metadata.version("web3")` |
| aiohttp | 3.14.3 | `importlib.metadata.version("aiohttp")` |
| httpx | 0.28.1 | `importlib.metadata.version("httpx")` |
| pydantic | 2.13.4 | `importlib.metadata.version("pydantic")` |

## Test-suite baseline

### Default suite (integration + release_gate deselected by `pytest.ini`)

Command: `.venv/bin/python -m pytest -q`
Exit code: **1**

```
2 failed, 4904 passed, 636 deselected in 100.00s (0:01:40)
```

The 2 failures are the known pre-existing baseline failures targeted by a
later task (do not treat as new regressions):

1. `tests/test_discovery_policy_config.py::test_packaged_discovery_yml_matches_policy_defaults`
2. `tests/test_live_environment_overrides.py::test_live_environment_has_only_the_explicit_operator_policy_differences`

### Release-gate suite

Command: `.venv/bin/python -m pytest -m release_gate -q`
Exit code: **0**

```
171 passed, 5371 deselected in 143.63s (0:02:23)
```

### Collected test count

Command: `.venv/bin/python -m pytest --collect-only -q`

```
4906/5542 tests collected (636 deselected) in 4.01s
```

## Size metrics

All counts use git-tracked `.py` files ("production" = tracked `.py` not
under `tests/`).

| Metric | Value | Command |
|---|---|---|
| Tracked `.py` files (total) | 974 | `git ls-files '*.py' \| wc -l` |
| Production `.py` files | 425 | `git ls-files '*.py' \| grep -v '^tests/' \| wc -l` |
| Test `.py` files | 549 | `git ls-files 'tests/**/*.py' \| wc -l` |
| Production LOC | 119,668 | `git ls-files '*.py' \| grep -v '^tests/' \| xargs wc -l \| tail -1` |
| Test LOC | 24,223 | `git ls-files 'tests/**/*.py' \| xargs wc -l \| tail -1` |
| Production : test LOC ratio | ~4.94 : 1 | derived from the two numbers above |
| Module count (distinct directories holding production `.py`) | 39 | `git ls-files '*.py' \| grep -v '^tests/' \| xargs -n1 dirname \| sort -u \| wc -l` |
| Direct dependencies (`[project].dependencies`) | 14 | `tomllib.load("pyproject.toml")["project"]["dependencies"]` count |
| Pinned entries in `requirements.txt` | 74 | `grep -c "==" requirements.txt` |
| Pinned entries in `requirements.runtime.txt` | 65 | `grep -c "==" requirements.runtime.txt` |

### Public import surface

Top-level importable packages (directories with `__init__.py`, excluding
`tests/`): **16**

Command: `for d in adapters application bot_cli cache clients config domain integrations interfaces models orchestration orchestrators ports scripts services storage utils tests; do [ -f "$d/__init__.py" ] && echo "$d"; done`

```
adapters application bot_cli cache clients config domain integrations
interfaces models orchestration orchestrators ports services storage utils
```

(`scripts/` contains tracked `.py` but has no `__init__.py` — scripts, not an
importable package.)

Root-level modules (importable as top-level modules): **4** — `app.py`,
`bootstrap.py`, `bot.py`, `run_ccxt.py`
(`git ls-files '*.py' | grep -v '^tests/' | grep -v '/'`).

### Largest modules

Command: `git ls-files '*.py' | grep -v '^tests/' | xargs wc -c | sort -rn | head -11`

| Module | Bytes |
|---|---|
| `orchestrators/trade_pipeline.py` | 167,906 |
| `services/execution/reconciliation_service.py` | 157,006 |
| `app.py` | 120,362 |
| `config/app_config.py` | 116,415 |
| `services/executionService.py` | 100,071 |
| `services/risk/risk_monitor.py` | 92,290 |
| `utils/logger.py` | 75,702 |
| `storage/postgres/orders_repo.py` | 72,503 |
| `orchestrators/runtime_bootstrap.py` | 62,377 |
| `integrations/polymarket/ccxt_prediction_gateway.py` | 62,227 |

## Companion artifacts

- `docs/refactor/ownership-map.md` — lifecycle stage → owning module map
  (verified by import inspection).
- `tests/fixtures/golden_logs/` — golden console/JSONL fixtures for the
  twelve order-lifecycle logging moments, plus `regenerate.py` harness and
  README. Regeneration check: `python tests/fixtures/golden_logs/regenerate.py --check`.
