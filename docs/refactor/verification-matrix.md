# Verification matrix — standalone refactor (Task 25)

Captured 2026-10-13 in the refactor worktree (`refactor/standalone-pmcopy`),
recorded through commits `ccbff5f` (shared integration Postgres) and `4aab97e`
(CI hardening + lint-debt clearance). Environment: Linux 6.12.111+deb13-amd64
x64, Python 3.14.6 (`.venv`, uv-managed), uv 0.11.31, Docker with a local
`postgres:16-alpine` image (digest `sha256:721873c34ceb…`, matches the Task 25
brief). Every row quotes the exact command, its exit code, counts, and
duration; nothing is estimated. Rows that could not be executed are marked
UNVERIFIED with the reason — no check is recorded as clean without a run.

## (a) Default full suite

Command: `.venv/bin/python -m pytest -q`
Exit code: **0** — 5162 passed, 681 deselected, 2 warnings, 138.05s (wall 2m20s).

`pytest.ini` `addopts = -m "not integration and not release_gate"` performs the
deselection. Exact set arithmetic from the measured tier sizes: 681 deselected
= |integration ∪ release_gate|, with |integration| = 539 (row d) and
|release_gate| = 187 (row c), i.e. 45 items carry both marks (release-gate
Postgres tests auto-classified as integration by the conftest source scan).
Baseline (2026-10-11, pre-refactor): 2 failed / 4904 passed / 636 deselected —
both known baseline failures were fixed during the refactor and the suite is
now fully green.

## (b) Adapter contract group (CLAUDE.md 10-file command)

Command: `.venv/bin/python -m pytest -q tests/test_ccxt_precision.py
tests/test_ccxt_compat_tick_size.py tests/test_ccxt_compat_credentials.py
tests/test_ccxt_compat_version_and_health.py tests/test_ccxt_prediction_client.py
tests/test_ccxt_prediction_gateway_contract.py tests/test_ccxt_prediction_gateway_errors.py
tests/test_ccxt_prediction_gateway_pagination.py tests/test_paper_execution_gateway.py
tests/test_venue_runtime.py`
Exit code: **0** — 228 passed in 2.70s (3.4s wall).

The CI `ccxt-runtime` job runs a 12-file superset (adds
`tests/test_ccxt_prediction_market_data.py` and
`tests/test_ccxt_optional_dependency.py`); those two files were also run
directly: 8 passed in 0.16s (both are integration-classified, so their
green run is the row-d tier).

## (c) Release-gate tier

Command: `.venv/bin/python -m pytest -m release_gate -q`
Exit code: **0** — 187 passed, 5656 deselected in 49.62s (50.7s wall).
Baseline was 171 passed. Every test under `tests/release_gate/` is auto-tagged
by `tests/release_gate/conftest.py`.

## (d) Integration tier — shared-Postgres fixture, zero container churn

Command: `.venv/bin/python -m pytest -m integration -q`

| Run | Context | Result | Duration |
|---|---|---|---|
| 1 | first ever run of the fixture (bootstrapped the named container once via `docker run -d`) | **0** — 539 passed, 5304 deselected | 275.45s |
| 2 | after `docker stop pmcopy-integration-pg` (proves the `docker start` recovery path) | **0** — 539 passed, 5304 deselected | 265.53s |

Churn proof (the user constraint this fixture exists for):

- `docker ps -aq | sort` before run 1: 1 container (an unrelated exited
  `postgres:17` from another project). After run 1: exactly 2 — the only
  addition is `pmcopy-integration-pg` (`f8c51b7a105b…`), created once, never
  removed.
- Run 2 executed with the shared container **stopped**; the fixture
  `docker start`-ed the identical id
  (`f8c51b7a105bfc3e89619f102d42060ba56ba78a68a1822a3cab8cbad2731ca3`) and the
  container-id set was byte-identical before/after (`diff` empty → zero
  containers created, removed, or re-pulled across 539 tests).
- Zero image pulls after the one-time bootstrap pull (the image was absent
  locally — digest verified against the brief — and the fixture's
  `_verify_image` refuses to pull outside CI).
- Zero leaked test databases after both runs
  (`SELECT count(*) FROM pg_database WHERE datname LIKE 'pmcopy_it_%'` → 0;
  per-facade `stop()` drops its database `WITH (FORCE)`).

The old design would have created/destroyed hundreds of disposable
testcontainers across this tier. The contract is pinned by
`tests/test_integration_shared_postgres.py` (container-id set identical
before/during/after, isolated database per facade, drop-on-stop,
testcontainers-shaped DSN).

## (e) Replay suite

Command: `.venv/bin/python -m pytest tests/replay -q`
Exit code: **5** — `no tests ran in 0.08s` (collection exit code for an empty
selection).

Selection mechanics (recorded, not a failure): `tests/replay/` contains only a
`conftest.py` (fixture loader helpers) and five JSON scenario fixtures
(`delayed_buy_then_fill`, `duplicate_sell_delayed`, `partial_fill_then_cancel`,
`rejected_sell_after_reserved`, `restart_with_pending_orders`,
`venue_timeout_then_reconciled_fill`); there is no `test_*.py` module there, so
nothing is collected. No production or test code imports
`tests.replay.conftest.load_replay_fixture`; the replay-style scenario tests
(e.g. `tests/test_execution_fak_repost_defer.py::test_partial_fill_then_cancel_with_order_id_is_not_reposted`,
which embeds the partial-fill-then-cancel scenario inline) live in the default
tier and are covered by row (a). Verdict: replay directory = scaffolding;
replay-equivalent coverage green via row (a).

## (f) Package verification battery

Command: `.venv/bin/python scripts/verify_package.py`
Exit code: **0** — `ALL PACKAGING PROOFS GREEN` (wheel + sdist batteries from
fresh venvs: entry-point help, module help, config-validate, paper
workflow smoke, importlib-resources + writable paths), 15.9s.

## (g) Docker build + smokes

| Step | Command (abridged) | Exit | Evidence |
|---|---|---|---|
| compose config | `install -m 600 secrets.env.example secrets.env` … ; `env -u POSTGRES_PASSWORD docker compose --env-file secrets.env config` | **0** | modes 600 asserted; local `secrets.env`/`db.env` backed up and restored around the run |
| build | `docker build -t polymarket-copy-trader:ci .` | **0** | image `7189ea560b4b`, 485MB (multi-stage wheel-only, Task 20 design) |
| offline CLI smoke | `docker run --rm --network none …:ci --help` / `run --help` / `doctor --help` | **0 / 0 / 0** | all three printed usage |
| paper smoke | CI `docker-smoke` job reproduced end-to-end (short-lived `pmct-postgres` + `pmct-rpc` + `pmct-smoke` on `pmct-smoke-net`, per-run CA, fake approved-hostname Polygon provider, health probe on `/health`) | **0** | `container healthy (attempt 2)`, 20.7s total |

Postgres need note: the paper smoke uses the CI-faithful short-lived
`pmct-postgres` container rather than the shared fixture, because
`pmcopy-integration-pg` publishes its port on `127.0.0.1` only (unreachable
from a docker bridge network without recreating it — which would itself be
churn). These smoke containers are one-off, named `pmct-*`, and removed by the
script's cleanup trap; `docker ps -a` afterwards shows zero `pmct-*` residue.

## (h) Runtime dependency consistency (pip check)

The venv is uv-managed and has no pip (`.venv/bin/python -m pip --version` →
`No module named pip` — same as baseline). Equivalent command:
`~/.local/bin/uv pip check`
Exit code: **0** — `Checked 71 packages … All installed packages are
compatible`. (CI runs literal `pip check` after installing from
`requirements.txt`; same consistency property.)

## (i) Requirements drift check

Command: `.venv/bin/python scripts/check_requirements_drift.py`
Exit code: **0** — `OK: uv.lock, requirements.txt, requirements.runtime.txt,
and pyproject.toml agree.` (uv 0.11.31, the pinned generator version), 0.4s.

## (j) Golden log fixtures

Command: `.venv/bin/python tests/fixtures/golden_logs/regenerate.py --check`
Exit code: **0** — `All 25 golden log fixtures match.` (12 lifecycle moments:
console + JSONL captures).

## (k) mypy — money-path spine (BLOCKING in CI)

Command: `mypy --install-types --non-interactive --explicit-package-bases
--ignore-missing-imports --follow-imports=silent
src/pmcopy/integrations/polymarket/ccxt_prediction_client.py
src/pmcopy/integrations/polymarket/ccxt_prediction_gateway.py
src/pmcopy/integrations/polymarket/venue_runtime.py
src/pmcopy/clients/network/http_client.py`
Exit code: **0** — `Success: no issues found in 4 source files`.

Host-binary caveat: the workstation `mypy` (1.x on Python 3.13.5) cannot parse
the four PEP 758 (`except A, B:`) production files, so the spine command was
also confirmed under Python 3.14 via `uvx --python 3.14 mypy …` — identical
clean result. CI installs mypy on 3.14 and is unaffected.

## (l) bandit (CI command)

Command (CI): `bandit -r src/pmcopy/services src/pmcopy/orchestrators
src/pmcopy/clients src/pmcopy/config src/pmcopy/utils src/pmcopy/cache
src/pmcopy/storage src/pmcopy/interfaces src/pmcopy/domain
src/pmcopy/integrations src/pmcopy/bot_cli src/pmcopy/app.py
src/pmcopy/bootstrap.py -ll --skip B101,B311,B608,B108,B310`
Exit code: **0** — `No issues identified.` at medium+ severity.

Host-binary caveat (recorded honestly): the same binary on the local Python
3.13.5 host silently **skipped 4 production files** with `syntax error while
parsing AST` (PEP 758 syntax in `runtime_bootstrap.py`, `trade_pipeline.py`,
`multiUserConfig.py`, `logger.py`). Re-run under Python 3.14 via
`uvx --python 3.14 bandit …`: exit 0, `No issues identified.`, **0 files
skipped**, ~88k LOC scanned. CI (py3.14) gets full coverage natively.

## (m) ruff — FULL scan (now BLOCKING in CI, zero CLI ignores)

Command: `ruff check --exclude=legacy,test.py,bootstrap.py .`
(rules/limits come from `pyproject.toml [tool.ruff]`: select E,F,W,B,ASYNC,PLE;
line-length 100 with E501 backstop at 120; tests exempt E501, scripts exempt
E402/ASYNC240).

- Before the Task 25 debt clearance (at HEAD `c497bf5`): exit **1** —
  **66 findings** (44 E501, 8 F541, 7 F401, 4 F811 — including the known
  pre-existing `tests/test_issue74_position_drift_reconciliation.py`
  `datetime` redefinition — 1 B905, 1 B023, 1 B017), concentrated in
  `scripts/migrate_namespace.py` (44) and five test files + two production
  files.
- After the fixes (commit `4aab97e`): exit **0** — `All checks passed!`.
  No noqa comments were added; every finding was fixed for real (implicit
  string concatenation for long literals, `f""`→`""` without placeholders,
  unused imports deleted, duplicate imports deleted, the B017
  `pytest.raises(Exception)` narrowed to the actual `DuplicateIntentError`,
  B905 `zip(..., strict=False)` made explicit, B023 loop-variable lambda
  bound via default arg).

## (n) compileall

Command: `.venv/bin/python -m compileall -q .`
Exit code: **0** — 4.3s, no output (quiet success), matches the CI step.

## Audits

### Dependency vulnerability audit

Command: `pip-audit -r requirements.txt --no-deps` (advisory DB reachable —
NOT recorded as unverified).

**2026-10-13 re-run (after the ccxt 4.5.87 bump): exit 0 — "No known
vulnerabilities found". RESOLVED.** Cross-checked with `uv audit`
(experimental, preview): "Found no known vulnerabilities and no adverse
project statuses in 73 packages". Provenance: ccxt 4.5.87's requires_dist
re-pins `multidict==6.9.1`; the lock and both requirements exports carry
`multidict==6.9.1` (> affected range 6.7.0–6.9.0).

Original finding at the 2026-10-12 audit (now fixed): exit **1 — 1 known
vulnerability found** (recorded as an OPEN finding, not clean, not
unverified):

| Package | Version | Advisory | Fix versions |
|---|---|---|---|
| multidict | 6.7.1 | CVE-2026-104874 | 6.9.1 |

- Nature: reference leak in `CIMultiDict`/`MultiDict` items-view union and
  subtraction (`d.items() | x`, `d.items() - x`) — memory-exhaustion class,
  not RCE; requires those specific operations over attacker-influenced
  sequences.
- Remediation **was blocked upstream**: `ccxt==4.5.85` (the mandated,
  exact-pinned trading transport) itself pinned `multidict==6.7.1` exactly
  (`uv lock --upgrade-package "multidict==6.9.1"` → `No solution found`).
  Resolved 2026-10-13 by the bump to `ccxt==4.5.87` (polymarket module
  byte-identical to 4.5.85 — zero adapter-behavior change), which carries
  `multidict==6.9.1`; lock delta verified to move ONLY ccxt and multidict.
- Exposure in the interim was low — the bot's aiohttp usage is
  market-data/account reads; no code path performs items-view
  union/subtraction on remote data.

### Secret scan over tracked files

gitleaks is not installed; documented grep patterns over `git ls-files`
(1109 tracked paths):

- `BEGIN (RSA |EC |OPENSSH |DSA )?PRIVATE KEY` → **0 hits**.
- `AKIA[0-9A-Z]{16}` (AWS access keys) → **0 hits**.
- `0x[0-9a-fA-F]{64}` outside `tests/` → only the public OrderFilled event
  `topic0` constant `0xd543adfd…84d8ee` (well-known signature hash, appears in
  `grouped_detection.py`, `order_filled_detector.py`, two docs).
- secret-shaped assignments
  (`(api[_-]?key|secret|bearer|password|passwd|token)…=…` with 16+ char
  literals) outside tests/docs/examples → **0 hits**; all example-file values
  are documented placeholders.
- `secrets.env` and `db.env` (real) are untracked; only `*.example` variants
  are committed.

Verdict: **clean**.

### Mimosa LOW findings — insecure randomness triage

All six flagged sites audited; none uses `random` for anything
security-sensitive (no tokens, keys, nonces, or identifiers — only timing
jitter and test RNG). Verdict per site: intentional, keep.

| Site | Use | Verdict |
|---|---|---|
| `tests/test_retry_backoff_jitter.py:22,32` | reproduces the production jitter expression to assert its bounds | test RNG by design |
| `src/pmcopy/services/executionService.py:1822` | `min(0.15*attempt, 0.30) + random.uniform(0, 0.1)` retry-sleep jitter | intentional thundering-herd decorrelation |
| `src/pmcopy/clients/onchain_ws.py:204` | reconnect delay `+ random.uniform(0, 0.5)` | intentional reconnect-storm decorrelation |
| `src/pmcopy/clients/network/http_client.py:127` | full-jitter backoff `delay += uniform(0, delay*0.2)` | intentional retry decorrelation |
| `src/pmcopy/services/execution/reconciliation_service.py:840` | poll backoff multiplier `uniform(0.8, 1.2)` | intentional poll decorrelation |

Documented centrally in the ci.yml bandit B311 skip comment (updated in
`4aab97e`).

### CI gate hardening (A12) summary

`ci.yml`:

- ruff: the hard/soft pair (narrowed hard step + `continue-on-error` full
  scan) collapsed into ONE blocking full scan with zero CLI ignores.
- mypy: money-spine step unchanged (BLOCKING, no `|| true`); the concealed
  soft step (`continue-on-error: true` **and** `|| true`) replaced by a
  **blocking ratchet**: the services/orchestrators/domain error count must
  stay ≤ 34 (the recorded baseline below). The only `|| true` retained in the
  new step tolerates `grep -c`'s exit-1-on-zero-matches (absent-match
  tolerance, not result concealment).
- bandit B311 skip comment now cites the audited random-use sites.
- Paper-smoke step: `"$RPC_IP"` / `"$CERTIFI_PATH"` / `"$FAKE_RPC_TOKEN"`
  quoted (actionlint/shellcheck SC2086, pre-existing). Both workflows are now
  **actionlint-clean** (exit 0).
- No-xfail grep enforcement kept blocking (`grep -rnE
  'pytest\.mark\.xfail|pytest\.xfail' tests/` → clean).
- Remaining `|| true` uses are exclusively in `if: always()` cleanup steps
  (absent-resource tolerance — semantically correct).

`release-gate.yml`: verified — no `continue-on-error`, no `|| true`, gates on
`pytest -m release_gate --maxfail=1`; nothing to change.

mypy expansion beyond the spine (optional per the task brief): **not
expanded**; the 34-error debt is ratcheted instead. Baseline list (by file):
`storage/postgres/v1_migration_runner.py` ×8 (+2 alias-validity +1 attr =
11), `services/auto_trader.py` ×5, `services/reconciliation.py` ×3,
`services/executionService.py` ×3, `services/risk/risk_monitor.py` ×2,
`orchestrators/trade_pipeline.py` ×2, `orchestrators/native_detection_runtime.py`
×2, and 1 each in `storage/postgres/risk_lot_repository.py`,
`services/multiUserConfig.py` (`_is_descendant` redefinition — worth a real
fix first), `services/market_data_quality.py`,
`services/execution/reconciliation_service.py`, `services/copy_tasks/budget.py`,
`orchestrators/startup_health.py` (invalid `type: ignore` comment),
`orchestrators/auto_trader_paper_runtime.py`, `integrations/falcon/client.py`,
`cache/market_state_store.py`.

## Metrics after (same method as `baseline-metrics.md`)

Baseline was captured at `925ad0d` (2026-10-11, flat layout, Python 3.12);
"after" is post-Task-25 (`4aab97e`, `src/pmcopy`, Python 3.14). One baseline
correction: `baseline-metrics.md`'s "Test LOC 24,223" was an `xargs wc -l |
tail -1` last-batch artifact — the exact baseline test LOC (recomputed from
`git ls-tree 925ad0d`) is **117,103**; both columns below use exact sums.

| Metric | Baseline | After | Δ / explanation |
|---|---|---|---|
| Tracked `.py` files | 974 | 960 | −14 (deletion manifest) |
| Production `.py` files | 425 | 401 | −24 (dead/orphan modules removed) |
| Test `.py` files | 549 | 559 | +10 (new contract/gate tests, incl. the shared-fixture contract suite) |
| Production LOC | 119,668 | 110,662 | −9,006 (−7.5%): deletions outpaced additions |
| Test LOC (exact) | 117,103 | 123,288 | +6,185 (+5.3%): regression coverage added across tasks |
| Prod : test LOC ratio | ≈1.02 : 1 | ≈0.90 : 1 | test coverage grew relative to production |
| Distinct production dirs | 39 | 37 | −2 (consolidations) |
| Direct `[project].dependencies` | 14 | 14 | unchanged |
| `requirements.txt` pins | 74 | 73 | −1 (`coincurve` removed during the refactor; Task 25 changed none) |
| `requirements.runtime.txt` pins | 65 | 64 | same `coincurve` removal |
| Top-level importable packages | 16 (+4 root modules) | **1** (`pmcopy` under `src/`) + 0 root modules | the namespace migration itself |
| Collected tests | 4906/5542 (636 deselected) | 5162/5843 (681 deselected) | +256 default-tier tests, +45 tier-marked |
| Default suite | 2 failed / 4904 passed (exit 1) | 5162 passed (exit 0) | baseline failures fixed |
| Release-gate tier | 171 passed | 187 passed | +16 gate tests |
| Integration tier | not run at baseline (per-test testcontainers) | 539 passed, ONE shared container | Task 25 anti-churn fixture |

Largest modules (bytes, `wc -c`): `trade_pipeline.py` 174,078 (was 167,906),
`reconciliation_service.py` 157,249 (was 157,006), `app_config.py` 115,815
(was 116,415), `app.py` 113,856 (was 120,362), `executionService.py` 101,710
(was 100,071), `risk_monitor.py` 94,310 (was 92,290), `orders_repo.py` 88,533
(was 72,503), `logger.py` 81,759 (was 75,702), `runtime_bootstrap.py` 75,634
(was 62,377), `ccxt_prediction_gateway.py` 65,637 (was 62,227) — growth tracks
feature tasks (orders/leases/evidence), not the refactor.

Toolchain now: Python 3.14.6, pytest 9.1.1, pytest-asyncio 1.4.0, rich 14.3.4,
asyncpg 0.32.0, web3 7.16.0, aiohttp 3.14.5, httpx 0.28.1, pydantic 2.14.1,
ccxt 4.5.87 (bumped from 4.5.85 on 2026-10-13 — multidict CVE remediation,
adapter module byte-identical; only ccxt + multidict moved in `uv.lock`).
(The root `CLAUDE.md` working copy was updated to 4.5.85 in the
Task 26 docs pass and to 4.5.87 with the CVE bump; the actual pin in
`pyproject.toml` is `ccxt==4.5.87`.)
