# Verification matrix — standalone refactor (Task 25)

Captured 2026-10-13 in the refactor worktree (`refactor/standalone-pmcopy`),
recorded through commits `ccbff5f` (shared integration Postgres) and `4aab97e`
(CI hardening + lint-debt clearance). Environment at that capture: Linux
6.12.111+deb13-amd64 x64, Python 3.14.6 (`.venv`, uv-managed), uv 0.11.31,
Docker with a local `postgres:16-alpine` image (digest
`sha256:721873c34ceb…`, matches the Task 25 brief). The interpreter rows below
are 3.14-era history: later on 2026-10-13 the project was retargeted to
Python 3.13 ONLY (`>=3.13,<3.14`, venv and Docker base 3.13.15) — the final
section records the re-runs on the current interpreter and is the authority
for today's toolchain. Every row quotes the exact command, its exit code,
counts, and duration; nothing is estimated. Rows that could not be executed
are marked UNVERIFIED with the reason — no check is recorded as clean without
a run. Rows (a)–(n) and the audit sections reflect their stated capture
dates; the 2026-10-13 follow-up-fix slice (F1–F6: ccxt 4.5.87 CVE
remediation, money-path/redaction/config/test-infra hardening batches, and
the docs close-out) re-verified them, and the 2026-10-13 Python 3.13-only
migration re-ran the gates on 3.13.15 — final section below records both.

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
`conftest.py` (fixture loader helpers) and six JSON scenario fixtures
(`delayed_buy_then_fill`, `duplicate_sell_delayed`, `partial_fill_then_cancel`,
`rejected_sell_after_reserved`, `restart_with_pending_orders`,
`venue_timeout_then_reconciled_fill`); there is no `test_*.py` module there, so
nothing is collected. No production or test code imports
`tests.replay.conftest.load_replay_fixture`; the replay-style scenario tests
(e.g. `tests/test_execution_fak_repost_defer.py::test_partial_fill_then_cancel_with_order_id_is_not_reposted`,
which embeds the partial-fill-then-cancel scenario inline) live in the default
tier and are covered by row (a). This is now explained at the source by
`tests/replay/README.md` (added 2026-10-13): the fixtures are replay inputs
for a future replay suite, deliberately without collected tests. Verdict:
replay directory = scaffolding; replay-equivalent coverage green via row (a).

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

Host-binary caveat (historical, 3.14-era): the workstation `mypy` (1.x on
Python 3.13.5) could not parse the four then-PEP-758 production files, so the
spine command was also confirmed under Python 3.14 via `uvx --python 3.14
mypy …` — identical clean result. **Superseded 2026-10-13**: the 3.13-only
migration parenthesized all 16 PEP 758 sites, so the spine command now runs
clean natively on the 3.13 host binary too, and CI installs mypy on 3.13
(see the final section).

## (l) bandit (CI command)

Command (CI): `bandit -r src/pmcopy/services src/pmcopy/orchestrators
src/pmcopy/clients src/pmcopy/config src/pmcopy/utils src/pmcopy/cache
src/pmcopy/storage src/pmcopy/interfaces src/pmcopy/domain
src/pmcopy/integrations src/pmcopy/bot_cli src/pmcopy/app.py
src/pmcopy/bootstrap.py -ll --skip B101,B311,B608,B108,B310`
Exit code: **0** — `No issues identified.` at medium+ severity.

Host-binary caveat (historical, 3.14-era; SUPERSEDED 2026-10-13): the same
binary on the local Python 3.13.5 host silently **skipped 4 production files**
with `syntax error while parsing AST` (PEP 758 syntax in
`runtime_bootstrap.py`, `trade_pipeline.py`, `multiUserConfig.py`,
`logger.py`); it was re-run under Python 3.14 via `uvx --python 3.14 bandit …`
— exit 0, `No issues identified.`, **0 files skipped**, ~88k LOC scanned. The
cause was removed by the 3.13-only migration (all 16 sites parenthesized), so
the 3.13 run scans the whole tree: recorded in the final section.

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
  soft step (`continue-on-error: true` **and** `|| true`) was first
  replaced by a blocking count-only ratchet (Task 25), then upgraded on
  2026-10-13 (follow-up slice, commit `5944e29`) to a **blocking per-file
  manifest ratchet**: the same single CI step now runs
  `python scripts/check_mypy_ratchet.py`, which requires EXACT per-file
  equality with `scripts/mypy_baseline.json` — no `continue-on-error`, no
  `|| true` anywhere in the step (pinned by
  `tests/release_gate/test_mypy_ratchet_manifest.py`).
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
expanded**; the type debt is ratcheted per-file instead. The earlier
count-only ratchet (total ≤ 34, retired 2026-10-13) could pass a lateral
move — errors deleted from one file while equal errors appeared in
another. It is now a committed PER-FILE manifest, `scripts/mypy_baseline.json`
(`{"files": {path → exact error count}, "mypy_version": "2.4.0"}` — **12
erroring files / 23 errors total (derived)**), enforced by
`scripts/check_mypy_ratchet.py` with the historical CI flags verbatim over
services/orchestrators/domain: growth, shrinkage, a new erroring file, and
a file cleaned to zero ALL fail, so the manifest only moves through the
deliberate `--update` path. Version-coherence caveat: the recorded
`mypy_version` is informational — CI installs the latest mypy (no pin), so
a future mypy release may shift counts; the recovery is an explicit
`python scripts/check_mypy_ratchet.py --update`, never a silent pass. The
manifest is the authoritative composition; the old inline 34-error list
(which mixed in storage/cache/integration files outside the three ratchet
targets) is superseded and retired.

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
| `requirements.txt` pins | 74 | 73 | −1 at the Task 25 capture (`coincurve` absent in the 3.14 era); the 2026-10-13 3.13 migration restored coincurve 21.0.0 to the lock and both exports (final section) |
| `requirements.runtime.txt` pins | 65 | 64 | same 3.14-era `coincurve` absence; restored on 3.13 (final section) |
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

Toolchain at the Task 25/26 capture: Python 3.14.6, pytest 9.1.1,
pytest-asyncio 1.4.0, rich 14.3.4,
asyncpg 0.32.0, web3 7.16.0, aiohttp 3.14.5, httpx 0.28.1, pydantic 2.14.1,
ccxt 4.5.87 (bumped from 4.5.85 on 2026-10-13 — multidict CVE remediation,
adapter module byte-identical; only ccxt + multidict moved in `uv.lock`).
(The root `CLAUDE.md` working copy was updated to 4.5.85 in the
Task 26 docs pass and to 4.5.87 with the CVE bump; the actual pin in
`pyproject.toml` is `ccxt==4.5.87`.) Superseded interpreter (2026-10-13):
the 3.13-only migration moved the venv and Docker base to Python 3.13.15
and restored `coincurve 21.0.0` in `uv.lock` + both exports — final section.

## 2026-10-13 follow-up-fix slice (F1–F6) — recorded runs (3.14-era interpreter)

Captured 2026-10-13 after the follow-up slice landed: F1 ccxt 4.5.87 +
multidict 6.9.1 (CVE-2026-104874 RESOLVED — audit re-runs recorded in the
Audits section above), F2 money-path hardening (fill-unit fallthrough,
shim environment parity, quarantine-count throttle, drift-service born-with
injection, retention purge below the lease), F3 redaction/logging (plural
credential keys, `redact_object` depth/cycle guard, `stack_info` redaction,
config-print list redaction), F4 config/loader strictness (chain_id bool
guard, key-compare normalization, `POSTGRES_PASSWORD` reject-with-hint,
version_guard cleanup + reason pinning, doctor installed-mode content
scan), F5 test-infra (container identity validation, Dockerfile/compose
guard extensions, per-file mypy ratchet manifest, PG temp-dir cleanup), and
the F6 docs/truth pass (this section; stale cap-inert wording corrected in
`docs/known_limitations.md` §6 + handoff §3; README wheel-install command;
`docs/rc1_validation.md` skip-caveat aligned with the script;
`tests/replay/README.md` added; Dependabot h2 PR #137 closed with a
supersession comment). Same environment as the header (3.14-era interpreter,
`Python 3.14.6`, superseded later the same day by the 3.13-only migration).
All commands executed from the worktree root with `.venv/bin/python`
(the then-current 3.14 venv; the 3.13.15 re-runs follow in the final section
below).

| Check | Command | Exit | Result | Duration |
|---|---|---|---|---|
| Default full suite | `.venv/bin/python -m pytest -q` | **0** | **5249 passed / 0 failed**, 710 deselected, 1 warning | 214.71s |
| Release-gate tier | `.venv/bin/python -m pytest -m release_gate -q` | **0** | **202 passed**, 5757 deselected | 43.56s |
| Integration tier | `.venv/bin/python -m pytest -m integration -q` | **0** | **553 passed**, 5406 deselected, 1 warning | 299.00s |
| Golden log fixtures | `.venv/bin/python tests/fixtures/golden_logs/regenerate.py --check` | **0** | `All 25 golden log fixtures match.` | ~5s |
| Requirements drift | `.venv/bin/python scripts/check_requirements_drift.py` | **0** | `OK: uv.lock, requirements.txt, requirements.runtime.txt, and pyproject.toml agree.` (uv 0.11.31) | <1s |

Tier deltas versus the Task 25/26 rows above (all additions are the
slice's new contract/regression tests; nothing was removed): default
5162 → 5249 (+87), release-gate 187 → 202 (+15), integration 539 → 553
(+14); deselected sets grew accordingly (681 → 710).

Integration churn note (zero-churn contract re-proven): `docker ps -aq |
sort | md5sum` was `6e3c3df9e9848837e75601345797d501` before AND after the
run (2 containers total, unchanged); the only running container is the ONE
shared `pmcopy-integration-pg` (`f8c51b7a105b`) — nothing was created,
removed, or re-pulled. Zero new `/tmp/multicopier-copiers-*` leaks: the
newest pre-existing leak's mtime (15:16:24) predates the run window, i.e.
F5's temp-dir registry drained everything this run created.

## 2026-10-13 Python 3.13-only migration (P1–P3) — final recorded runs

Captured 2026-10-13 after the 3.13-only migration landed on branch
`chore/python-313-only` (T1 `da9033a` PEP 758 parenthesization + parse guard;
T2 `86df07c` interpreter/lock/CI/Docker retarget to `>=3.13,<3.14`; T3 this
docs truth pass). Environment: same host (Linux 6.12.111+deb13-amd64 x64),
interpreter **CPython 3.13.15** (worktree `.venv`, pyenv-provisioned; the
lock itself resolved under a uv-managed 3.13.14 — patch-independent, see
handoff §10), uv 0.11.31, mypy 2.4.0 in-venv, Docker with the shared
`pmcopy-integration-pg` container. Lock delta: `coincurve 21.0.0` re-entered
(`uv.lock` 74 → 75 packages; each requirements export gained exactly the
coincurve line + `# via ccxt`); cp313 wheels, no resolver conflicts, no force
flags. Every command executed from the worktree root with `.venv/bin/python`.
All rows below are current (green) — the 3.14-era rows above remain as
dated captures.

| Check | Command | Exit | Result | Duration |
|---|---|---|---|---|
| Default full suite | `.venv/bin/python -m pytest -q` | **0** | **5249 passed / 0 failed**, 711 deselected, 1 warning | 151.72s (2m33s wall) |
| Adapter contract group (CLAUDE.md 10-file command) | see row (b) | **0** | **228 passed** | 2.89s |
| Release-gate tier | `.venv/bin/python -m pytest -m release_gate -q` | **0** | **203 passed**, 5757 deselected | 41.66s |
| Integration tier (shared container) | `.venv/bin/python -m pytest -m integration -q` | **0** | **553 passed**, 5407 deselected, 1 warning | 265.44s |
| Golden log fixtures | `.venv/bin/python tests/fixtures/golden_logs/regenerate.py --check` | **0** | `All 25 golden log fixtures match.` (rich 14.3.4 unchanged → byte-stable) | ~5s |
| Requirements drift | `.venv/bin/python scripts/check_requirements_drift.py` | **0** | `OK: uv.lock, requirements.txt, requirements.runtime.txt, and pyproject.toml agree.` (uv 0.11.31) | <1s |
| mypy ratchet (per-file manifest) | `.venv/bin/python scripts/check_mypy_ratchet.py` | **0** | `OK (exact per-file match)` — 12 files / 23 errors, mypy 2.4.0; zero drift, no `--update` | <1s |
| mypy money-spine (CI command) | see row (k) | **0** | `Success: no issues found in 4 source files` — clean natively on the 3.13 host binary (the 3.14-era host-parse caveat is gone) | <1s |
| bandit (CI scope) | see row (l) | **0** | 0 medium / 0 high (46 low below the `-ll` threshold); **`Files skipped (0)`** — full-tree coverage on the 3.13 host (the 3.14-era capture skipped 4 PEP 758 files) | — |
| ruff full scan | `ruff check --exclude=legacy,test.py,bootstrap.py .` | **0** | `All checks passed!` | ~2s |
| compileall | `.venv/bin/python -m compileall -q .` | **0** | quiet success (no output) | ~4s |

Integration churn note (zero-churn contract re-proven on 3.13): `docker ps -aq
| sort | md5sum` was `6e3c3df9e9848837e75601345797d501` before AND after the
run (2 containers total, unchanged); the only running container is the ONE
shared `pmcopy-integration-pg` (`f8c51b7a105b`, `Up 15 hours`, same creation
timestamp) — nothing created, removed, or re-pulled. Leaked test databases
after the run: `SELECT count(*) FROM pg_database WHERE datname LIKE
'pmcopy_it_%'` → **0**. `/tmp/multicopier-copiers-*` count identical before
and after (255; all pre-existing).

Native signing (re-confirmed on this run): `eth_keys` default backend =
**`CoinCurveECCBackend`** (native), `coincurve` importable, and a sign →
recover roundtrip returns the account address. Warm measurement (private key
constructed once, 50-iteration mean): raw `sign_msg` **0.12 ms/sign**, EIP-191
`sign_message` **0.24–0.26 ms/sign** — reproducing the migration's recorded
0.12 / 0.24 (20-iteration means). The raw figure is method-sensitive
(constructing the key inside the timed loop inflates it), so the recorded
numbers are the reference; no speed claims.

Docker build/image verification (the migration's build, re-verified in-place
today): image `polymarket-copy-trader:py313-task2`
(`sha256:d3b6a4014566…`, 479 MB) built from the flipped Dockerfile at
`86df07c` — `docker run --rm --network none --entrypoint python <image> -V`
→ **Python 3.13.15**; in-image `coincurve 21.0.0` and `ccxt 4.5.87`; offline
CLI smokes `--help` / `doctor --help` exit 0/0. Registry digests re-verified
against the live registry today (`docker manifest inspect` + `docker buildx
imagetools inspect python:3.13.15-slim-bookworm`): index
`sha256:2325bb286ec344af3e5898cc224b5844e2707ac6e26b1632516fd3edc84a5e26`,
linux/amd64 `sha256:3e2de9c40ca4e3d73240059f9d48baff27908f10293e985a2f382a0378e6df4a`,
linux/arm64/v8 `sha256:ad4c34ff79289506e235b40dce75d629e25f226b597a2455804220f037e07531`
— all matching `docs/docker.md` and handoff §4. (The base tag itself is no
longer retained locally, so the digests were re-read from the registry; the
built image under test still carries the base layers.)

Docs/truth pass (T3): README/AGENTS.md/handoff/verification-matrix/
`known_limitations.md` retargeted to 3.13-only — the coincurve limitation is
removed as resolved-with-provenance, and the PEP 758 syntax policy plus the
corrected site count (16, not 4) are documented; the three non-pinning
synthetic `3.14` literals were re-tagged to the 3.13 form
(`tests/release_gate/test_compose_secret_split.py`,
`tests/release_gate/test_dockerfile_safety_defaults.py`,
`tests/test_packaged_resource_resolution.py`). Focused re-runs: the three
fixture files + the parse guard → **26 passed**; the docs-pinning contract
tests (`test_rc1_release_artifacts.py`, `test_docker_ccxt_runtime.py`,
`test_packaged_resource_resolution.py`) → **39 passed**.

Tier deltas versus the F1–F6 capture above: default 5249 → 5249 (same count,
now measured on 3.13.15), release-gate 202 → **203** (+1: the PEP 758 parse
guard added in T1), integration 553 → 553; the default run's deselected set
grew 710 → 711 (the guard is release-gate-marked).
