# Handoff — standalone refactor of the Polymarket copy-trading bot

Date: 2026-10-13. Branch: `refactor/standalone-pmcopy` (base `7a4a68c`,
HEAD at authoring: `e2a53ba` plus this docs commit). Worktree:
`.worktrees/refactor-standalone-pmcopy`. This document satisfies the
final-handoff list of `MASTER_REFACTOR_PROMPT.md` §13; it summarizes and
points at the per-task evidence rather than duplicating it. Primary
companions: `baseline-metrics.md`, `verification-matrix.md`,
`deletion-manifest.md`, `ownership-map.md`, `migration-and-rollback.md`, and
the SDD ledger + per-task reports under
`.superpowers/sdd/2026-10-11-standalone-refactor/`.

## 1. Implemented requirements; intentionally removed surfaces

**Implemented** — the requested standalone bot exists as working, verified
code:

- CCXT-only venue transport (`ccxt.prediction.polymarket`, exact pin
  `4.5.87`) behind `ClobGateway`; live adapter `CcxtPredictionGateway`,
  paper adapter `PaperExecutionGateway` (CCXT public reads + local
  simulated writes). Paper can never invoke a CCXT private write.
- All confirmed audit defects A01–A14 fixed with failing-then-passing
  regression tests (§3).
- Single clean import namespace `src/pmcopy/` (16 packages + 3 modules
  moved from the repo root; 980-file mechanical migration verified
  byte-equivalent, Task 18).
- Real installation: wheel/sdist authoritative, packaged defaults + SQL
  migrations via `importlib.resources`, explicit deployment root (no parent
  walk), `scripts/verify_package.py` battery green from fresh venvs with no
  source tree (Task 19).
- Python 3.13-only (`>=3.13,<3.14`), canonical `uv.lock` with generated,
  drift-gated requirements exports (Tasks 16–17; retargeted from the earlier
  3.14-only era on 2026-10-13 — see §10 and the verification-matrix final
  section).
- Docker: multi-stage wheel-only image on `python:3.13.15-slim-bookworm`, no
  source tree, no `PYTHONPATH`; `db.env`/`secrets.env` split per service
  (Tasks 15, 20; base re-pinned from `python:3.14.6-slim-bookworm` on
  2026-10-13).
- Multi-copier addon against the canonical account runtime: schema-v2
  secret-free roster, per-account state + DB lease, routing isolation,
  acceptance matrix §9 all green at hermetic and real-Postgres levels
  (Tasks 21–24).
- Quality gates made real: blocking ruff full scan (66-finding backlog
  cleared, no noqa additions), blocking mypy money-spine + per-file
  type-debt ratchet manifest (`scripts/mypy_baseline.json`, exact per-file
  equality — replaced the earlier 34-error total-only ratchet on
  2026-10-13), actionlint-clean workflows, no-xfail enforcement,
  shared-Postgres integration tier with zero container churn (Task 25).

**Intentionally removed product surfaces** (full evidence per row in
`deletion-manifest.md`):

- The entire remote operator control plane: HTTP/stream transport
  (`integrations/control_plane/`), application services
  (`services/control_plane/`), 8 CP orchestrators, 5 CP repositories, CP
  config model/files/secrets, `docs/ui/` + `docs/frontend-control-plane.md`,
  the 4 api-contract scripts, and `run_ccxt.py`. Stale `control_plane`
  config fails closed with a removal error.
- The dead V2 `execute_intent` execution path and its exclusive
  models/adapters in `services/execution/coordinator.py` (production
  `submit_*` surface byte-compatible; Task 13).
- The `AutomaticDiscoveryRunner` adapter (only consumer was the CP
  discovery queue).
- The Dependabot `h2`-only branch is superseded by the full re-lock
  (closed without merging on 2026-10-13 — PR #137, with a supersession
  comment pointing at the canonical `uv.lock` re-lock).

The local `pmcopy` CLI is now the only control surface; the health/metrics
server (loopback 9102) remains for supervision. Schema migrations,
including the control-plane one, are never deleted (existing databases keep
applying cleanly).

## 2. Module map, manifest, remaining compatibility code

- **Old-to-new map**: every root-level package/module `X` → `src/pmcopy/X`
  (`adapters, application, bot_cli, cache, clients, config, domain,
  integrations, interfaces, models, orchestration, orchestrators, ports,
  services, storage, utils` + `app.py`, `bootstrap.py`, `bot.py`). Tests
  stay at `tests/` (`pytest.ini pythonpath = src`). The deterministic codemod
  is preserved at `scripts/migrate_namespace.py` for auditability; it is
  one-shot and refuses double-runs.
- **Deletion/consolidation manifest**: `deletion-manifest.md` (finalized
  Task 26) — one row per removed path/symbol with consumers-verified
  evidence, classification, and replacement/reason.
- **Ownership map**: `ownership-map.md` maps each lifecycle stage
  (detection → admission/sizing → reservation → submit → fills →
  reconciliation → persistence) to its single owning module under
  `src/pmcopy/`.
- **Remaining justified compatibility code**:
  - the deprecated `config/runtime.local.yml` loader shim (documented
    layer, still participating per `docs/02-configuration.md`);
  - the legacy pre-split env-name translation table
    (`PMCOPY__NETWORK__*`, `PMCOPY__ONCHAIN_DETECTION__*`);
  - the v1 JSON multi-user roster loader (mutually exclusive with schema
    v2; strict since Task 14);
  - the instance-local arbitrary-tick precision patch in the CCXT adapter
    (removal condition: upstream stops hard-coding the tick table in
    raw-amounts — 4.5.87 still does; polymarket module byte-identical
    to 4.5.85);
  - `requirements.ccxt.txt` as the standalone pinned constraint for
    targeted installs;
  - kept control-plane-era infrastructure that is control-plane-free:
    `services/healthServer.py`, liveness heartbeat, telemetry hub (bounded
    in-process sink), `services/trading_control.py` +
    `trading_control_repo`, `market_reentry_lock_repository`.

## 3. Confirmed bug fixes + regression tests; risks investigated, not reproduced

Audit numbering note: the master prompt defines findings A01–A14 with two
grouped pairs (A05/A06, A10/A11); there is no separate A15. All were
confirmed and fixed:

| Finding | Fix (task) | Regression tests |
|---|---|---|
| A01 account-scoped ambiguous-submission recovery | T6 | `tests/test_ambiguous_recovery_account_isolation.py`, `tests/test_ambiguous_recovery_account_isolation_postgres.py`, `tests/test_orders_repo_ambiguous_recovery.py` |
| A02 complete order identity + explicit sizing units | T7 (migration `20261011_12`) | `tests/test_ccxt_ambiguous_evidence.py`, `tests/test_ccxt_prediction_gateway_contract.py`, `tests/release_gate/test_postgres_migration.py` |
| A03 authoritative history completeness | T8 | `tests/test_ccxt_prediction_gateway_pagination.py`, `tests/test_ccxt_prediction_gateway_errors.py`, `tests/test_ccxt_ambiguous_evidence.py` |
| A04 one account-owned coordination state | T10 | `tests/test_account_owned_position_coordinator.py` |
| A05 explicit follow whitelists (+ locked legacy-single-account rule) | T10 | `tests/test_runtime_follows_whitelist.py`, `tests/test_dynamic_leader_reconcile_gate.py` |
| A06 strict multi-user configuration boundary | T14 | `tests/test_multi_user_config_validation.py` |
| A07 secret redaction across all logging sinks | T5 | `tests/test_logger_secret_redaction.py`, `tests/test_logger.py` |
| A08 container secret isolation | T15 | `tests/release_gate/test_compose_secret_split.py` |
| A09 CCXT version-gate parsing | T4 | `tests/test_ccxt_compat_version_and_health.py` |
| A10/A11 dependency graph resolved, not isolated lines | T16 | `tests/test_ccxt_dependency_sync.py`, `tests/test_check_requirements_drift.py` |
| A12 baseline repaired + gates made real | T3 (baseline policy tests), T25 (CI) | `tests/test_live_environment_overrides.py`, discovery-policy test; CI gates in §5 |
| A13 real installation, not source shadowing | T19 | `tests/release_gate/test_packaging_proofs.py`, `tests/test_packaged_resource_resolution.py`, `tests/test_deployment_root_resolution.py`, `tests/test_logger_default_paths.py` |
| A14 expiry never releases an unresolved no-ID submission | T9 | `tests/test_reservation_expiry_no_id_release.py` (+ `_postgres`), `tests/release_gate/test_cli_reservations.py` |
| (addon) idempotency key lacked mode/environment axis | T23 | `tests/test_execution_idempotency_environment.py` |
| (addon) readiness snapshot hid which account was degraded | T23 | `tests/test_readiness_account_truth.py` |
| (addon) copier exposure cap not atomic cross-scope | T24 (2 fix rounds) | `tests/multicopier/` (reservation-transaction cap enforcement, real cross-scope race test) |

**Risks investigated but not reproduced / adjudicated sound**:

- The two unported V2-only behaviors from Task 13's 24-test accounting were
  adjudicated sound (production submit surface byte-compatible).
- Task 23 verified 7 of 9 addon audit claims directly; the two real gaps
  (above) were fixed RED-first. The deliberately NOT-fixed item:
  `CopyTradeIdentityBuilder`'s trade key keeps its legacy derivation —
  rekeying would orphan pre-upgrade in-flight claims, and its cross-mode
  blocking direction is fail-closed under-copy.
- Task 24's copier exposure cap: the follow-level
  `max_market_exposure_usdc` (schema-v2 `limits.max_open_exposure_quote`)
  was initially characterized as inert, then made real in two fix rounds —
  the BUY-admission headroom gate (`62458e9`) and the authoritative
  reservation-transaction check that counts in-flight reserved quote and
  serializes cross-scope BUY races (`c497bf5`; addon row above). The
  residual documented limitations are the pre-lock confirmed-basis TOCTOU
  window and partial-fill remainder drift
  (`docs/known_limitations.md` §6).
- Mimosa's six insecure-randomness LOW findings were audited site-by-site:
  all are timing/test jitter, none security-sensitive (table in
  `verification-matrix.md`).
- One unrelated flake (`tests/test_aethertrade_evidence_composition.py`
  fake-clock) was observed exactly once during Task 9, order-dependent,
  passes isolated and in every subsequent full run.

## 4. Exact versions, lock/export consistency, advisories, Docker metadata

| Component | Version |
|---|---|
| Python | **3.13.15** (CPython; `>=3.13,<3.14` enforced — 3.11–3.12 and 3.14 rejected fail-closed; retargeted 2026-10-13) |
| ccxt | **4.5.87** (exact pin; minimum for the prediction namespace `>=4.5.73,<5`; bumped from 4.5.85 on 2026-10-13 for the multidict CVE — adapter module byte-identical) |
| uv | 0.11.31 (pinned generator/CI version) |
| pytest / pytest-asyncio | 9.1.1 / 1.4.0 |
| rich / asyncpg / web3 / eth-account | 14.3.4 / 0.32.0 / 7.16.0 / 0.13.7 |
| aiohttp / httpx / pydantic | 3.14.5 / 0.28.1 / 2.14.1 |

- **Lock/export consistency**: `requirements.txt` and
  `requirements.runtime.txt` are generated from `uv.lock`
  (`scripts/generate_requirements.py`); `scripts/check_requirements_drift.py`
  reports byte-exact agreement (exit 0) and is CI-gated. `h2==4.4.1` +
  `hpack==4.2.0` are resolved coherently everywhere (the Dependabot
  half-bump is superseded). Runtime `pip check`: all 71 packages compatible.
- **Advisory RESOLVED (2026-10-13)**: `multidict 6.7.1` —
  CVE-2026-104874 (memory-exhaustion class in items-view union/subtraction).
  Resolved by the ccxt bump `4.5.85 -> 4.5.87` (adapter module
  byte-identical, zero behavior change): ccxt 4.5.87's requires_dist
  re-pins `multidict==6.9.1`, above the affected range 6.7.0–6.9.0. The
  lock and both requirements exports now carry `multidict==6.9.1`;
  re-audits (`uv audit` and `pip-audit -r requirements.txt --no-deps`,
  advisory DBs reachable) report zero known vulnerabilities. Previously
  recorded OPEN while `ccxt==4.5.85` pinned `multidict==6.7.1` exactly.
- **Native signing (2026-10-13, RESOLVED)**: on the 3.13 target ccxt's own
  pin `coincurve==21.0.0` (`python_version < "3.14"` marker) is in the lock
  with cp313 wheels, so `eth_keys` signs through the native
  `CoinCurveECCBackend`: measured 0.12 ms/sign raw `sign_msg` and
  0.24 ms/sign EIP-191 `sign_message` (20-iteration means). History with
  provenance: in the 3.13-unsupported 3.14 era (before 2026-10-13) coincurve
  was absent (no cp314 wheels) and signing ran on `eth_keys`' pure-Python
  `NativeECCBackend` — measured 3.08 ms/sign raw, 5.32 ms/sign EIP-191.
  Those slower numbers are superseded, not an open limitation; the
  round-trip signature contract tests pass on both paths (same recovered
  address).
- **Deferred upgrades (documented in Task 16 report)**: web3 8.x (requires
  eth-account>=0.14 + breaking API), rich 15.x, websockets 16/17 (need
  web3 8), uvloop >0.22.1 (ccxt exact pin; still `==0.22.1` in 4.5.87) and
  orjson >3.11.9 (ccxt 4.5.87 loosened this to `>=3.11.9,<4`, so an
  orjson-only re-lock upgrade is now possible; not taken in the CVE bump).
- **Docker base metadata (registry-verified 2026-10-13 for the 3.13 target)**:
  `python:3.13.15-slim-bookworm` — linux/amd64
  `sha256:3e2de9c40ca4e3d73240059f9d48baff27908f10293e985a2f382a0378e6df4a`,
  linux/arm64v8
  `sha256:ad4c34ff79289506e235b40dce75d629e25f226b597a2455804220f037e07531`,
  index / manifest-list digest
  `sha256:2325bb286ec344af3e5898cc224b5844e2707ac6e26b1632516fd3edc84a5e26`.
  The same policy applies as in the earlier capture: tag pin (not digest) —
  keeps Debian security updates flowing; the only repo precedent
  (`postgres:16-alpine`) pins tags too. 3.13.15 was chosen as the newest
  3.13 patch that exists both locally (pyenv) and in the official registry,
  for exact interpreter parity with the `.venv` the suite runs on. History
  (superseded, same day): the pre-migration image was
  `python:3.14.6-slim-bookworm`, linux/amd64
  `sha256:ff83a535339812dd72e69c93b3c48ddf7c85a324d6330af5797c82a255dbeef4`,
  linux/arm64v8
  `sha256:1609c3e634a6ad5b4759f67e34c24640d41858fffde69876245fd26afd632b7`,
  index `sha256:4c92ffcde4dd6f1ff72a24518f49fd4990b27134987dfa31a733badde66df9f8`.

## 5. Commands executed, results, skips, platforms

The full evidence table with exact commands, exit codes, counts, and
durations is `verification-matrix.md` (Task 25, re-verified at Task 26
authoring, re-run after the 2026-10-13 follow-up-fix slice, and re-run again
after the 2026-10-13 Python 3.13-only migration — final section there; the
rows below are the F1–F6 capture and are historical, the migration re-runs
are recorded in that final section). Summary (tier rows re-verified
2026-10-13 after the follow-up slice F1–F6; the remaining rows are the
Task 25/26 records):

| Check | Result |
|---|---|
| Default suite `pytest -q` | **5249 passed / 0 failed**, 710 deselected, 215s |
| Adapter contract group (10-file command) | 228 passed |
| Release gate `-m release_gate` | 202 passed |
| Integration `-m integration` | 553 passed (2026-10-13 re-run; earlier ×2 runs at 539), ONE shared container, zero churn (container-id set byte-identical before/after) |
| Replay suite `pytest tests/replay` | **Skipped (recorded)**: directory contains conftest + 6 JSON scenarios but no `test_*.py`; nothing collected (explained at the source by `tests/replay/README.md`, added 2026-10-13). Replay-equivalent scenarios live inline in the default tier. |
| `scripts/verify_package.py` | ALL PACKAGING PROOFS GREEN (wheel + sdist, fresh venvs, empty CWD) |
| Docker build + offline CLI smokes + CI-faithful paper smoke | all exit 0; image 485MB; `container healthy` |
| `uv pip check` / CI `pip check` | compatible |
| `scripts/check_requirements_drift.py` | OK |
| Golden log fixtures `regenerate.py --check` | All 25 match |
| mypy money-spine | clean (Task 25/26 capture; host-binary caveat was the 3.14-era PEP 758 parse failure — the migration parenthesized all 16 sites, so the same command is clean on the 3.13 host binary too: matrix final section) |
| bandit (CI scope) | No issues at medium+ (3.14-era host skipped 4 PEP 758 files; after the migration's parenthesization the 3.13 host run scans the whole tree with **0 files skipped** — matrix final section) |
| ruff full scan | exit 0 after real cleanup of 66 findings |
| compileall | exit 0 |
| pip-audit | **RESOLVED 2026-10-13**: zero known vulnerabilities after the ccxt 4.5.87 / multidict 6.9.1 re-lock (§4; was 1 OPEN — multidict) |
| Secret scan (gitleaks unavailable → documented grep patterns) | clean |
| actionlint (both workflows) | clean |

**Skipped/unavailable, with reasons**: gitleaks not installed (grep
patterns documented instead); live/authenticated trading obviously never
executed; GitHub-hosted CI runners not executed during the refactor
(workflow definitions were hardened and actionlint-verified locally only);
arm64 never built or run (digests recorded from the registry only).

**Platforms tested**: linux/amd64 only (Debian 13 host, kernel
6.12.111+deb13-amd64, Docker with local `postgres:16-alpine`).

## 6. Database/config migration and rollback

See `migration-and-rollback.md` for the full procedure. Essence: two
additive migrations landed (`20261011_12_order_submission_identity`,
`20261011_13_account_execution_leases`); migrations are never deleted (the
control-plane one stays so old databases apply cleanly); schema rollback
does not exist — rolling the code back over an additive schema is the
supported direction (safe-forward-only). In-flight state survives upgrade
via the restart-reconciliation contract (lease acquire →
`startup_reconcile_unconfirmed` → no-ID evidence loop →
`startup_drift_check`); unresolved no-ID submissions stay
duplicate-blocking until `recovered_order_id` or `confirmed_absent`;
reservation expiry never frees them (A14).

## 7. Usage with secret-free examples

`README.md` is the canonical quickstart (single-copier paper via uv/pip,
`pmcopy config validate` / `pmcopy doctor` / `pmcopy run`; Docker with the
`db.env`/`secrets.env` split; full CLI overview). Multi-copier: point
`PMCOPY__APP__COPIERS_CONFIG_FILE` at a schema-v2 roster — the template is
`src/pmcopy/config/copiers.example.yml`; wallet fields are env-var
REFERENCES (never values), all example addresses are synthetic, sizing
money is exact decimal text, `pmcopy run --copier ID` selects one account.

## 8. Logging compatibility + deliberate output changes

- **Compatibility evidence**: the 25 golden fixtures (12 order-lifecycle
  moments × console + JSONL) match byte-exact after Tasks 5, 12, 18, 23,
  and 25 (`tests/fixtures/golden_logs/regenerate.py --check` — "All 25
  golden log fixtures match"). The account-scoped lifecycle field added in
  Task 5 is present-when-set only, so single-copier output is unchanged.
- **Deliberate security-related changes** (A07 + Task 19):
  - `JsonFormatter` and `safe_error_detail` now redact through the shared
    value/key-fragment registry — previously the JSONL sink had NO
    redaction (a real leak, fixed; this intentionally changes JSONL output
    that contained secrets).
  - Runtime-resolved private keys register with the value registry
    (`register_secret_values`); redaction covers nested values and
    exceptions in both sinks.
  - `pmcopy config print` uses the shared engine with VALUE patterns:
    provider keys embedded in URL values (`/v2/<key>`, `?api_key=…`) are
    redacted, and the previous over-redaction of public `token_id` market
    identity was fixed.
  - RPC/WSS URLs are deliberately NOT registered (public endpoints must
    stay readable for diagnosis); the key-fragment matcher can over-redact
    trailing words on a line containing a secret fragment (safe direction).
  - CP token registration and the `CONTROL_PLANE` telemetry producer were
    removed with the surface, so stale labels cannot appear.
  - The 2026-10-13 redaction follow-up (plural credential keys classified,
    `redact_object` depth/cycle guard, `stack_info` redaction, config-print
    list redaction) widened coverage without changing any golden fixture
    output — 25/25 still byte-exact (re-verified after the slice).

## 9. Before/after measurements

Exact tables: `baseline-metrics.md` (captured 2026-10-11 at `925ad0d`) vs
`verification-matrix.md` §"Metrics after" (post-Task-25). Headlines, same
measurement method both times:

| Metric | Before | After |
|---|---|---|
| Production LOC / files | 119,668 / 425 | 110,662 / 401 (−7.5% LOC; removals outpaced additions) |
| Test LOC (exact) / files | 117,103 / 549 | 123,288 / 559 (+5.3%) |
| Top-level importable packages | 16 (+4 root modules) | 1 (`src/pmcopy`) |
| Default suite | 2 failed / 4904 passed | 5162 passed / 0 failed |
| Release-gate / integration tiers | 171 / not run (per-test containers) | 187 / 539 (one shared container) |
| Collected tests | 4906 (636 deselected) | 5162 default-tier of 5843 (681 deselected) |

Performance: no invented improvements. Measured signing cost is recorded in
§4 — the current 3.13 target signs natively (coincurve, 0.12 / 0.24 ms per
sign); the 3.14-era pure-Python numbers (3.08 / 5.32 ms) are superseded
history, not an open limitation. Integration-tier runtime with the
shared fixture is ~266–300s for 539–553 tests (versus per-test container
startup churn before, which the fixture eliminates). Largest-module sizes
moved with feature work, not the refactor (table in
`verification-matrix.md`).

## 10. Honest limits — what was NOT verified

- **Live trading** was never executed (obviously): no authenticated venue
  write, no funded account, no live order. All execution evidence is
  hermetic, disposable-Postgres, or paper-mode.
- **GitHub-hosted CI runners**: `ci.yml`/`release-gate.yml` were hardened
  and pass actionlint locally, but were not run on GitHub during the
  refactor; first push may still surface runner-specific issues. The 3.13
  migration keeps the same caveat: the matrices are now `["3.13"]`
  (single-entry, kept as a matrix), still locally verified only.
- **3.13 patch-level provenance**: the suite and Docker base run on CPython
  **3.13.15** (pyenv local + registry tag), while `uv lock` resolved under a
  uv-managed 3.13.14. The lock is interpreter-patch-independent and no
  committed artifact is 3.13.14-specific; the difference is recorded here
  rather than silently unified.
- **arm64**: base-image digests recorded from the registry; no arm64 build
  or test executed. Platforms tested: linux/amd64 only.
- **Dependabot alert inventory**: only the h2 PR was adjudicated
  (superseded — closed without merging on 2026-10-13, PR #137); the
  repository's full Dependabot alert list was not inventoried.
- **multidict CVE-2026-104874 RESOLVED (2026-10-13)**: the ccxt bump to
  4.5.87 re-pins `multidict==6.9.1` (above the affected range); re-audits
  report zero known vulnerabilities (§4).
- **mypy beyond the money-spine is a ratchet, not zero**: since
  2026-10-13 the ratchet is a committed PER-FILE manifest
  (`scripts/mypy_baseline.json`, 12 erroring files / 23 errors, recorded
  with mypy 2.4.0) enforced by `scripts/check_mypy_ratchet.py` — exact
  per-file equality, so lateral error moves can no longer hide inside a
  total. Version-coherence caveat: CI installs the latest mypy (no pin),
  so a future mypy release can shift counts; the deliberate recovery is
  `python scripts/check_mypy_ratchet.py --update` (never a silent
  count-only pass). The spine itself stays blocking-clean.
- **Deferred minors**: the SDD ledger records every task's deferred minor.
  The recurring money-adjacent ones still open: exposure caps operate on a
  confirmed basis (TOCTOU window pre-lock) and partial-fill remainders can
  drift past a cap (pre-existing settlement semantics — the copier-level
  cap itself IS enforced at BUY admission and authoritatively in the
  reservation transaction since the Task 24 fix rounds, §3); a stale
  prev-CTE snapshot can mislog takeover-vs-handoff; the shared-fixture GC
  helper is /proc-dependent (Linux-only). Fixed in the 2026-10-13
  follow-up slice: `_fills_consistent_with_submission` unit fallthrough;
  plural credential keys bypassing the key classifier; `redact_object`
  depth/cycle guard; config-print list redaction; the retention purge
  writing to the shared DB before leases are held (now post-lease);
  `chain_id` JSON-boolean coercion; private-key duplicate-compare
  whitespace normalization; legacy `POSTGRES_PASSWORD` in secrets.env
  (now rejected with a migration hint); doctor's code-gate scan vacuous in
  installed mode (now a real installed-package content scan); version_guard
  dead branch removed with the prerelease reason pinned. Full list:
  `.superpowers/sdd/2026-10-11-standalone-refactor/progress.md` and
  `.superpowers/sdd/2026-10-13-followup-fixes/`.
- **Instructions don't travel with clones**: root `CLAUDE.md` is
  git-ignored by operator choice; the worktree working copy was updated to
  the final system, but a fresh clone will not carry it. (`AGENTS.md` is
  tracked and updated on the branch.)
- **Replay scaffolding**: `tests/replay/` contains fixtures without a test
  module (nothing collected — exit 5 is the recorded, expected result);
  its scenarios are covered inline elsewhere, and `tests/replay/README.md`
  (added 2026-10-13) documents the directory as deliberate scaffolding for
  a future replay suite.
- **Golden fixtures are environment-pinned**: byte-exact capture depends on
  the pinned rich version and width 100; a rich upgrade is a regeneration
  event, not a free pass.
