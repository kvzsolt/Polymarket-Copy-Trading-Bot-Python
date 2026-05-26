# full-rework progress

Per CLAUDE.md, every issue slice records its outcome here.

## Issue #75 — Final integration tests and release gate

**Status:** in-flight on branch `issue-75`.

**Summary.** Stood up a release-gate test suite, registered the `release_gate`
pytest marker, added a project-wide `tests/conftest.py` exposing the
`make_exec_config` / `make_clob_gateway` / `StubClobClient` / `ScriptedWss`
fixtures, added two GitHub Actions workflows (`ci.yml`, `release-gate.yml`),
and documented the gate in `docs/release-gate.md`.

**Files changed.**

- `pytest.ini` — registered `release_gate` marker.
- `tests/conftest.py` *(new)* — shared `make_exec_config`,
  `make_clob_gateway`, `StubClobClient`, `ScriptedWss`, plus the
  `exec_config`, `mock_clob_gateway`, `stub_clob_client`, `fresh_db_path`,
  `scripted_wss` fixtures.
- `tests/release_gate/` *(new package)*:
  - `__init__.py`, `conftest.py` (auto-tags items with `release_gate`).
  - `test_matrix_coverage.py` — meta-test that imports every gate module.
  - `test_position_key_uniqueness.py` — matrix item 1.
  - `test_coordinator_serialization.py` — matrix item 2.
  - `test_cache_ttl_invalidation.py` — matrix item 3.
  - `test_wss_heartbeat.py` — matrix item 4.
  - `test_wss_reconnect_resync.py` — matrix item 5.
  - `test_sizing_metadata_driven.py` — matrix item 6.
  - `test_effective_idempotency.py` — matrix item 7.
  - `test_reservation_lifecycle.py` — matrix item 8.
  - `test_reconciler_terminal_ownership.py` — matrix item 9.
  - `test_startup_recovery.py` — matrix item 10.
  - `test_manual_drift_reconciliation.py` — matrix item 11.
  - `test_typed_config_fail_closed.py` — matrix item 12.
  - `test_clob_v2_gateway_smoke.py` — matrix item 13.
  - `test_concurrency_stress.py` — matrix item 14.
  - `test_e2e_dry_run_buy.py` — matrix item 15.
  - `test_e2e_dry_run_sell.py` — matrix item 16.
  - `test_protective_exit_integration.py` — matrix item 17.
- `.github/workflows/ci.yml` *(new)* — `unit`, `release-gate`,
  `lint-type-import`, `docker-smoke` jobs.
- `.github/workflows/release-gate.yml` *(new)* — run gate on release events
  + manual dispatch.
- `docs/release-gate.md` *(new)* — operator-facing description of the gate.

**Tests run locally.**

- `pytest -m release_gate` (gate tier).
- `pytest` (default unit tier).
- `python -m compileall .`.

**Known remaining risks.**

- The Docker smoke step in CI is best-effort and only confirms the image
  builds and the entrypoint can boot in paper mode with synthetic env vars.
  A real readiness probe against `/health/ready` is left as a follow-up
  because that endpoint depends on external Polygon RPC and CLOB
  connectivity that CI does not provide without secrets.
- `mypy` is soft-failed in `lint-type-import`. Strict-typing hardening is
  scheduled separately.

**Next issue.** #76 — runtime ops, Docker readiness, and observability.

**Commit SHA.** _to be filled in after the slice is merged_.

## Issue #76 — Runtime ops, readiness, observability

**Status:** in-flight on branch `issue-76`. Docker scope explicitly excluded
per the originating ticket conversation; remaining checklist items shipped.

**Summary.** Added three operator CLI subcommands (`reservations list`,
`stale-locks list`, `drift check`), a strict `/health/ready` endpoint backed
by an extended `ReadinessTracker` that fails closed on every required
subsystem, named metric constants for ops-facing counters/gauges, emission
of `wss_reconnect_total`, `wss_heartbeat_stall_total`,
`cache_stale_evictions_total`, and `reconciliation_delta_applied_total` at
the natural call sites, and live-mode doctor probes for the CLOB SDK, WSS
endpoint, and reconciler config. Hardened `_config_to_dict` so secrets
inside the `raw` env dict are also redacted.

**Files changed.**

- `bot_cli/commands/reservations.py` *(new)* — `bot reservations list`.
- `bot_cli/commands/stale_locks.py` *(new)* — `bot stale-locks list`.
- `bot_cli/commands/drift.py` *(new)* — `bot drift check`.
- `bot_cli/commands/__init__.py`, `bot_cli/main.py` — register new commands.
- `bot_cli/commands/doctor.py` — gateway SDK + WSS endpoint + reconciler
  config checks for live mode.
- `bot_cli/commands/config.py` — secrets inside dict values are now redacted.
- `services/healthServer.py` — extended `ReadinessTracker` defaults,
  `is_ready(strict=...)` helper, new `/health/ready` route.
- `services/metrics.py` — `inc_sync` helper plus operator metric name
  constants (`METRIC_*` and `GAUGE_*`).
- `cache/market_state_store.py` — emits cache invalidation flags on fill /
  reconcile / reconnect paths; legacy `price_cache.py` removed.
- `services/execution/reconciliation_service.py` — emits
  `reconciliation_delta_applied_total` after each applied fill delta.
- `clients/user_ws.py`, `clients/market_ws.py` — emit `wss_reconnect_total`
  and `wss_heartbeat_stall_total`.
- `tests/release_gate/test_cli_reservations.py` *(new)*.
- `tests/release_gate/test_cli_stale_locks.py` *(new)*.
- `tests/release_gate/test_cli_drift.py` *(new)*.
- `tests/release_gate/test_cli_redacted_config.py` *(new)*.
- `tests/release_gate/test_readiness_aggregator.py` *(new)*.
- `tests/release_gate/test_metrics_counters.py` *(new)*.

**Tests run locally.**

- `pytest -m release_gate` — 93 passed (was 67 pre-#76).
- `pytest` — 1474 passed, 8 skipped, 7 xfailed.
- `python -m compileall .` — clean.
- `python bot.py reservations list`, `… stale-locks list`,
  `… config print` — JSON output, secrets redacted, exit 0.

**Known remaining risks.**

- Docker readiness was explicitly out of scope for this slice; the
  Dockerfile + runtime docs item from the original checklist remains open
  and should be picked up separately if/when live operations are planned.
- The doctor `gateway_sdk` check is an import probe; it does not yet
  perform a signed venue handshake. Live ops should still rely on the
  startup health report for full venue reachability.
- `is_ready(strict=True)` requires every subsystem flag to be wired by the
  application composition root; until app.py wires gateway/wss/reconciler
  flags into the tracker (out of scope here), `/health/ready` will return
  503 even on a healthy paper run — by design (fail-closed).

**Next issue.** #77 — legacy cleanup and architecture documentation.

**Commit SHA.** _to be filled in after the slice is merged_.

## Issue #77 — Legacy cleanup and architecture documentation

**Status:** in-flight on branch `issue-77`.

**Summary.** Closed the rewrite with low-risk maintainability work:
removed the eight root-level probe scripts, archived three superseded
plan/usage docs, wrote the canonical architecture overview and the six
operator runbooks the audit enumerates, added a release-gate import
smoke test that proves production code never imports from `legacy.*` or
the deleted probes, and refreshed the root README to link the new docs.

**Files changed.**

- Deleted: `test_probe.py`, `test_probe2.py` … `test_probe6.py`,
  `test_doctor_detect.py`, `test_doctor_gnosis.py` (8 root probe
  scripts; zero production references).
- Archived (`git mv` into `docs/archive/`): `simplification-plan.md`,
  `full-rework-review-fixes-progress.md`, `usage.md`.
- New: `docs/archive/README.md` *(index of archived docs)*.
- New: `docs/architecture.md` *(canonical architecture overview;
  layered view, composition root, components, boundaries to defend)*.
- New: `docs/migration.md` *(`main`/`develop` → `full-rework` migration
  notes)*.
- New: `docs/runbooks/00-index.md` plus six runbooks —
  `clob-outage.md`, `wss-outage.md`, `circuit-breaker-open.md`,
  `stale-locks.md`, `reconciliation-drift.md`,
  `manual-off-bot-changes.md`.
- New: `tests/release_gate/test_no_legacy_imports.py` *(static AST
  scan; asserts no production module imports `legacy.*`,
  `test_probe*`, `test_doctor_detect`, or `test_doctor_gnosis`, and
  asserts the deleted probe scripts no longer exist at repo root)*.
- Edited: `README.md` *(replaced "Stack & Architecture" stub with real
  component map; added "Operator Documentation" reading order linking
  the numbered guide, runbooks, ARCHITECTURE, and MIGRATION)*.
- Edited: `utils/__init__.py` *(removed stale
  `# postOrder — REMOVED` comment)*.
- Edited: `docs/full-rework-progress.md` *(this entry)*.

**Tests run locally.**

- `python -m pytest tests/release_gate/test_no_legacy_imports.py -v -m ""`
  — 18 passed (after PR #90 review fixes: parametrised wildcard +
  nested-segment cases added; was 2 passed pre-fix).
- `python -m pytest tests/release_gate/ -m release_gate` —
  113 passed (was 97 in the initial #77 commit; +16 from the new
  parametrised cases).
- `python -m pytest -q` — 1474 passed, 8 skipped, 159 deselected,
  7 xfailed.
- `python -m compileall -q .` — clean.
- Doc-link sanity script (walks every `.md` under `docs/`, resolves
  every relative `.md` link) — `broken links: none`.

**PR #90 review fixes applied.**

- Renamed `docs/ARCHITECTURE.md` → `docs/architecture.md` and
  `docs/MIGRATION.md` → `docs/migration.md` to match the existing
  lowercase / kebab-case `docs/` naming convention. Updated all
  references (`README.md`, `docs/archive/README.md`,
  `docs/runbooks/00-index.md`, `docs/migration.md` self-link,
  this progress entry).
- Replaced the two enumerated `test_doctor_*` entries in
  `FORBIDDEN_PREFIXES` with a single `"test_doctor"` prefix so the
  invariant matches the docstring's `test_doctor_*` wildcard.
- Strengthened `_is_forbidden()` to walk every dotted segment, not
  just the head. Catches nested cases like `scripts.test_probe2` or
  `pkg.subpkg.legacy.foo`.
- Added parametrised tests covering wildcard, nested-segment, and
  no-overmatch behaviour for `_is_forbidden()`.

**Known remaining risks.**

- `legacy/tradeExecutor.py` still imported by
  `tests/test_copy_strategy.py` and `tests/test_services.py`; the new
  release-gate test enforces the production-side invariant. Removing
  those tests is a separate decision (would lose parity coverage).
- `services/positionCoordinator.py`,
  `services/positionActionCoordinator.py`, and
  `services/position_action_coordinator.py` are not duplicates
  (composition wrapper / per-scope lock / snake_case re-export shim);
  consolidating naming was out of scope.
- Several root meta files (`AGENTS.md`, `GEMINI.md`, `KILOCODE.md`,
  `LEARNINGS.md`, `changes.md`) remain untouched; treated as
  tool-specific or historical.

**Next issue.** None — #77 closes the audit remediation backlog.

**Commit SHA.** `11dbb0f` (slice commit on `issue-77`).

## Issue #78 — NearOneExitService replaces on-chain redemption

**Status:** landed on `live-trading` branch (deviation from one-issue-per-branch
discipline recorded by explicit user direction).

**Summary.** Removed the entire on-chain `CTF.redeemPositions` pipeline
(`services/redemptionService.py`, `services/autoRedeem.py`,
`services/redemption/` package, `storage/sqlite/redemptions_repo.py`,
`AbstractRedemptionRepository`, doctor `redemption_wallet` check, runtime
summary entry, log renderers, observability schema entries, and three
test modules). Replaced with `services/near_one_exit.py::NearOneExitService`,
which fires a SELL for the full held size through the standard
`ExecutionService.execute()` path when a token quote sustains
`>= NEAR_ONE_EXIT_THRESHOLD` (default `0.99`) for at least
`NEAR_ONE_EXIT_DWELL_SECONDS` (default `60`). Reuses `size_sell_order`
(SELL min-size parity verified explicitly), reservation, idempotency, and
reconciliation surfaces. Drops the `redemptions` SQLite table via a new
destructive migration in `storage/sqlite/migrations.py`.

**Files changed.**

- Deleted: `services/redemptionService.py`, `services/autoRedeem.py`,
  `services/redemption/{__init__,redemption_service,redemption_reconciler,wallet_validator}.py`,
  `storage/sqlite/redemptions_repo.py`,
  `tests/test_redemption_service.py`,
  `tests/test_issue7_redemption_consolidation.py`,
  `tests/test_wallet_validator.py`.
- New: `services/near_one_exit.py` *(NearOneExitService + dwell-clock)*.
- New: `tests/release_gate/test_near_one_exit.py` *(8 release-gate cases:
  arm, clear, fire, no-fire-before-dwell, no-fire-below-threshold,
  reset-on-drop, skip-when-held-below-venue-min, disabled-short-circuit)*.
- Edited: `config/app_config.py` *(RedemptionConfig → NearOneExitConfig,
  PollingConfig.redeem_check_interval removed)*,
  `config/__init__.py`, `config/env.py`, `app.py`, `bot_cli/commands/doctor.py`
  *(redemption_wallet → near_one_exit check)*, `bot_cli/commands/config.py`
  *(Decimal JSON serialization fix)*, `orchestrators/runtime_summary.py`,
  `domain/order_lifecycle.py` *(REDEEM/AUTO_REDEEM → NEAR_ONE_EXIT)*,
  `domain/ports/{__init__,repositories}.py`, `services/accountingService.py`
  *(record_redemption_attempt removed)*,
  `services/observability/event_schema.py` *(redemption_\* events →
  near_one_exit_\* events)*, `services/observability/event_schema.json`
  *(intent side enum no longer has REDEEM)*, `utils/logger.py`
  *(redemption renderers removed)*, `storage/sqlite/migrations.py`
  *(_ensure_redemptions_table → _drop_redemptions_table)*,
  `tests/test_new_modules.py` *(AutoRedeem block removed)*,
  `tests/test_storage.py`, `tests/test_domain.py`,
  `tests/release_gate/test_no_legacy_imports.py` *(FORBIDDEN_PREFIXES
  gains services.redemption / services.redemptionService /
  services.autoRedeem)*,
  `tests/release_gate/test_sizing_metadata_driven.py` *(5 new SELL-parity
  cases: upsized-when-held-allows, skipped-when-held-short,
  exactly-at-min, clamp-then-min-check, full-position-at-near-one)*.
- Docs: `docs/architecture.md`, `docs/migration.md`,
  `docs/runbooks/00-index.md`, new `docs/runbooks/near-one-exit.md`.

**Tests run locally.**

- `pytest tests/release_gate/test_near_one_exit.py tests/release_gate/test_sizing_metadata_driven.py tests/release_gate/test_no_legacy_imports.py -m ""` — 38 passed.
- `pytest tests/release_gate -m ""` — 124 passed (was 113 pre-#78),
  2 pre-existing failures in `test_e2e_dry_run_buy.py` /
  `test_e2e_dry_run_sell.py` (caused by the uncommitted
  `executionService.py` change on `live-trading` that requires
  `market_data_service`, unrelated to NearOne).
- `python -m compileall -q .` — clean.

**Known remaining risks.**

- DB migration drops the legacy `redemptions` table — destructive,
  no in-tree rollback. JSONL audit logs preserve forensic trail.
- NearOne uses `MarketDataService.get_quote_or_none()` (WSS → REST
  fallback). If both fail, the dwell timer never advances, so the
  service is fail-closed on no-data. Operators should still monitor
  WSS uptime via the existing `wss_reconnect_total` /
  `wss_heartbeat_stall_total` counters.
- The two e2e dry-run release-gate tests need either a paired
  `market_data_service` fixture or an executionService rollback;
  that is pre-existing live-trading-branch debt, not new.

**Min-size handling verification (user item 3).** `services/order_sizing.py`
applies the same upsize-or-skip + tick / ceiling logic to **both** BUY and
SELL. Five explicit SELL parity cases now lock that behaviour into the
release gate. NearOne itself routes its full-position SELL through
`size_sell_order`, so a too-small SELL skips with
`sell_held_below_venue_min` instead of submitting a venue-rejected order.

**Branch.** `live-trading` (per user direction; logged here as a deliberate
deviation from `CLAUDE.md`'s one-issue-per-branch rule).



## Phase 5 — Proof & Hardening (commit `f237325`)

**Status:** complete on branch `feat/phase01-position-key-coordinator`.

**Summary.** Closed four small non-architectural reconciler findings flagged
in `reviews/phase04_*.md` and added the matrix-coverage tests reviewers
named. No architectural detours.

- `services/reconciliation.py` — H1 cancel-after-partial classify + partial
  refund via existing `record_partial_fill`; H2 closed-order probe cap
  (`RECONCILE_MAX_CLOSED_PROBES`, default 10); H3 interruptible loop sleep on
  `shutdown_event`; H4 rollback resolves `request.amount`/`request.size`
  first (canonical schema).
- `tests/test_reconcile_phase04.py` +5 — N6 terminal-fill-with-local-record,
  cancel-after-partial refund + SELL-ignored.
- `tests/test_phase05_reconcile_hardening.py` *(new)* — probe cap + loop
  shutdown.
- `tests/test_trade_pipeline_safety.py` +1 — N1 regression: two
  immediate-fill BUYs record once each (10 not 20).

**Tests run.** Full suite: 2007 passed, 5 skipped.

## Audit issues #92–#103 (13 commits, suite ends 2237 passed, 5 skipped)

**Status:** all 12 closed via code + tests; issues remain OPEN per CLAUDE.md
(operator-gated closure after acceptance review).

| # | Severity | Commit(s) | Outcome |
|---|---|---|---|
| 92 | CRITICAL | `cce5deb`, `995513f` | `SdkNumericAdapter` boundary + 12 adapter tests |
| 93 | HIGH | `f2a152a`, `c67fda3` | structured live-readiness probe; hard chain_id fail; +6 tests |
| 94 | HIGH | `f2a152a`, `c612102` | UUID intent_id fallback; +3 tests |
| 95 | CRITICAL | `cce5deb`, `032a66f` | fail-closed without atomic lock; +2 tests incl. real-SQLite stress |
| 96 | HIGH | `ac2b6f7` | chaos tests for unsupported / erroring probes |
| 97 | HIGH | `f2a152a`, `d342ba6` | fills missing order_id rejected; +3 tests |
| 98 | MED | `f2a152a` | reservation released on RetryableVenueError |
| 99 | MED | `d5e529f` | `OrderedDict` LRU cap on both caches; +5 tests |
| 100 | MED | `aa47930` | unknown CLOB errors → ambiguous, not terminal; +4 tests |
| 101 | MED | `32f965a` | `test_live_execution_invariants.py` real-SQLite e2e |
| 102 | MED | `762be93` | canonical Decimal normalize() in `compute_effective_key` (real bug fixed); +187 boundary cases |
| 103 | MED | `12fb9be` | `scripts/rollback.sh` + runbook + canary mode + CI check |

**Real bugs uncovered by the audit work (4):** #97 unattributable fills,
#99 unbounded caches, #100 terminal-misclassification, #102 trailing-zero
idempotency collision.

**Tests run.** Full suite: 2237 passed, 5 skipped. `compileall` clean.

**Pre-existing release-gate failures (7, unrelated):** sizing, cache TTL,
CLI redaction, near-one exit, readiness aggregator. None touched by any
of the 13 commits in this session. Tracked separately.

**Branch.** `feat/phase01-position-key-coordinator` — phase 5 work and
the audit-fix slices all landed on the same branch per user direction
(logged here as a deliberate deviation from CLAUDE.md's one-issue-per-
branch rule).

## PR #105 review remediation (10 commits, suite ends 2254 passed, 5 skipped)

**Status:** CI green; all P1 + P2 review findings from Greptile and Cubic
addressed on top of the audit-fix baseline.

**CI failure** — `tests/test_doctor_live.py::test_doctor_live_passes_when_all_subsystems_wired`
was blocked because `db_schema_ok` checked the live DB for required
tables but a fresh CI runner has no `data/bot_state.db`. Local passed
only because prior dev runs had created and migrated the file.
`_run_doctor` now builds a per-test migrated SQLite and points
`BOT_STATE_DB` at it (`b77d367`).

| Finding | Severity | Source | Commit | Outcome |
|---|---|---|---|---|
| CI fail | — | unit-tests | `b77d367` | migrated tmp DB in `_run_doctor`; 9/9 doctor tests pass on a no-DB host |
| 92 (tick math) | P1 | Cubic | `a1bf53d` | `utils/numeric` quantizes by tick directly; works for non-power-of-10 ticks; +1 parametrized test |
| 94 (UUID retry) | P1 | Greptile | `778b9a4` | deterministic intent_id derived from request shape replaces per-call `uuid4()`; retry-safe; existing `TestBlankSignalIdFallback` inverted to encode new policy |
| 97 (over-refund) | P1 | Greptile + Cubic | `f29fcc3` | partial-cancel rollback refunds only `requested - filled_notional` when `record_partial_fill` is absent; zero-price guard skips refund (manual reconcile owns it); +2 tests |
| 95 (atomic lock) | P1 | Cubic + Greptile | `b0ead48` | `_check_atomic_lock(None)` now returns `error`; `app.py` wires `orders_repo` through to `perform_startup_health_check`; +1 regression test |
| 99 (max_tokens) | P1 | Cubic | `19e7632` | `MarketStateStore.__init__` validates explicit `max_tokens` > 0; +1 parametrized test |
| 92 (size tick) | P1 | Cubic | `a5ad12f` | `clob_v2_gateway.submit_marketable_order` quantizes size with fixed `0.01` (`_SIZE_QUANTIZATION`), price with market tick; +1 regression test |
| 103 (canary sed) | P1 | Cubic | `7490cc6` | `set_or_append_cap` helper replaces or appends missing `BUY_MAX_AMOUNT` / `MAX_DAILY_SPEND_USDC`; +2 sandboxed tests |
| 103 (eval) | P2 | Cubic | `23d2d19` | `run()` uses `"$@"` instead of `eval "$*"`; call sites converted to positional args |
| 99 (typed config) | P2 | Cubic | `645300f` | `RiskConfig.risk_price_cache_max_size` exposes typed cap; both `RiskPriceCache()` call sites in `riskMonitor.py` read it; env fallback kept as legacy adapter inside `RiskPriceCache.__init__`; +3 tests |

**Real safety bugs caught by the review (5):**
- ephemeral UUID broke retry idempotency (#94)
- partial-cancel fallback over-refunded full notional including filled portion (#97 Greptile)
- partial-cancel with unresolved price over-refunded too (#97 Cubic)
- atomic-lock gate silently degraded to warning when `orders_repo` not wired (#95)
- canary mode silently failed to set caps when secrets.env lacked the keys (#103)

**Tests run.** Full suite: 2254 passed, 5 skipped. `compileall` clean.
CI-shape verification (with `data/bot_state.db` moved aside): all 119
tests across the 8 touched test files pass.

## Release-gate cleanup (5 commits, suite all green)

**Status:** PR #105 fully green on all 8 CI checks — unit, lint, docker
smoke, release-gate, Greptile, cubic. The release-gate failures
documented earlier as "pre-existing, unrelated" were unblocked once
the unit fix landed and CI ran release-gate for the first time on the
branch. All 7 failures were stale-test issues against stricter
production code; no production logic changed.

| Failure | Commit | Cause |
|---|---|---|
| `test_cache_ttl_invalidation::test_invalidate_clears_all_snapshots` | `2d5800b` | Test called `invalidate()` and expected tick cleared; `invalidate()` deliberately keeps tick (own-fill semantic), `evict()` is the full-clear contract |
| `test_cli_redacted_config` x2 | `bd7f192` | `load_config` rejected Python 3.13; patched `_runtime_python_supported` like other tests do |
| `test_near_one_exit::test_tick_fires_sell_after_dwell_elapsed` | `0df3bd3` | Live-mode coordinator gate (issue #70) requires a coordinator; test now marks config as paper to exercise the direct `ExecutionService.execute` path |
| `test_readiness_aggregator::test_strict_passes_when_all_subsystems_ok` | `c03717e` | `_STRICT_REQUIRED_KEYS` gained `drift_ok`; helper now sets it |
| `test_sizing_metadata_driven` x2 | `3aee7e7` | Tests assumed integer taker_min; `_taker_min_tokens` quantizes to 4 decimals (1.0102 at price 0.99) so inputs were tweaked to actually exercise upsize/below-min branches |

**Tests run.** `pytest -m release_gate`: 133 passed, 1 skipped. Full
suite: 2254 passed, 5 skipped. CI: all 8 checks green on push
`3aee7e7`.

## Postgres cutover Phase A + Bootup banner Phase B (PR #107, 12 commits)

**Status:** `feat/postgres-migration` opened as PR #107 against `develop-fix`. Phase A foundation (asyncpg pool + full schema + 12/12 repo ports) complete. Phase B (boot snapshot + banner + history persistence) complete. Phase A5 (call-site sweep across remaining ~70 files) deferred to follow-up — dual-stack `connect_db()` keeps SQLite live during transition.

### Phase A — Postgres cutover

| Slice | Commit | What |
|---|---|---|
| PG-A1 | `5e44153` | `docker-compose.yml` (postgres:16-alpine + bot service), `asyncpg>=0.30`, `testcontainers[postgres]>=4.0`, `DATABASE_URL` env + `DatabaseConfig.dsn/pool_min/pool_max/command_timeout` |
| PG-A2 | `140abdf` | `storage/postgres/connection.py` (singleton pool, idempotent init), dual-stack `connect_db()` (PG primary, SQLite legacy fallback) |
| PG-A3+A8 | `7a38be4` | `storage/postgres/migrations.py` (28 tables / 14 indexes / partial unique on `orders(order_id)`, 3-col PK on `positions`, native PG `boolean`, `jsonb`, `timestamptz DEFAULT now()`), `tests/release_gate/test_postgres_migration.py` (4 release-gate tests) |
| PG-A4 (cursors/fills/risk_blocks) | `89656d5` | Async ports with `SELECT FOR UPDATE` (cursors regress-guard), `ON CONFLICT DO NOTHING RETURNING id` (fills dedup), CASE-preserved operator clears (risk_blocks Batch 4 invariant) |
| PG-A4 (positions) | `0f093ef` | `async with conn.transaction()` + `SELECT FOR UPDATE` replaces SQLite `BEGIN IMMEDIATE` — verified 4×SELL-5 concurrent from size=20 → 0 (would be 15 without serialisation) |
| PG-A4 (risk_triggers/market_structure/leaders/reservations) | `8def69b` | Bool conversion for `discovered_leaders.active`, `market_structure_events.remapped`, `discovery_markets.{active,resolved,neg_risk}`; reservations concurrent capacity test: 2 concurrent \$150 vs \$200 cap → exactly 1 accepts |
| PG-A4 (orders, 780 LOC) | `4fd9755` | Atomic submission-lock (1/5 concurrent winners), lost-update guard on `update_submission_status` (2 concurrent patches both land in merged jsonb), terminal-state guard on `mark_processed_trade`, full `persist_execution_atomically` with rollback on duplicate fill |
| PG-A4 (discovery, 613 LOC) | `b9ac51b` | jsonb tolerance (dict or str), `IN(...)` → `= ANY($N::text[])`, dynamic WHERE with explicit `$N` index tracking, `MAX` → `GREATEST` for monotonic `last_seen_ts` guard |

### Phase B — Bootup banner + history

| Slice | Commit | What |
|---|---|---|
| PG-B1+B2 | `6b4c085` | `discovery_position_snapshots.wallet_role` ('leader' \| 'own') + idempotent `ADD COLUMN IF NOT EXISTS` migration + `(wallet_role, snapshot_ts DESC)` index. `services/discovery/position_snapshot_service.py` (fetch + persist + return in-memory `SnapshotResult`, fail-soft per wallet) |
| PG-B3 | `bb8d08e` | `print_traders_youre_copying` + `print_your_positions` panels in `runtime_summary.py`. Math helpers: `_safe_pct` (zero-denom guard), `_leader_aggregate_gain`. Rich-coloured PnL, cents formatting, top-N truncation |
| PG-B4 | `27a46c2` | Wired into `app.py` between health check + legacy banner. Reuses `_guarded_leader_fetch` for rate-limited boot fetch. Fail-soft fall-through to legacy panels on any pipeline failure |
| PG-B5 | `272e765` | 25 new tests across `tests/test_position_snapshot_service.py` (12) + `tests/test_runtime_summary_banner.py` (13). Math verified by hand: +26.67%, +40%, +67.59%, +62.50%, -14.29%, +133.33%, -25.00% |

### Verification

| Check | Result |
|---|---|
| `pytest -q` (unit) | 2343 passed (+25 from baseline), 5 skipped, 2 pre-existing errors unchanged |
| `pytest -m release_gate -o asyncio_mode=auto -q` | 160 passed, 1 skipped |
| `ruff check --select=F,E --ignore=E501,F401,F841,E402,E741,E731 --exclude=legacy,test.py,bootstrap.py .` | clean |
| `bandit -r services/discovery/position_snapshot_service.py orchestrators/runtime_summary.py` | 0 medium+ |
| End-to-end against `postgres:16-alpine` testcontainer | every repo verified, atomicity invariants explicitly exercised |
| Banner smoke test on canned data | math matches hand calc |

### Deferred to follow-up

- **PG-A5**: sweep remaining ~70 files that import `storage.sqlite.*` directly. Dual-stack `connect_db()` keeps SQLite live so existing call-sites continue working during transition.
- **PG-A6**: test-fixture cutover via `testcontainers` conftest — mechanical but voluminous.
- **PG-A7**: delete `storage/sqlite/` + remove `sqlite3` imports — gated on PG-A5 + PG-A6.

## Post-PR #107 PG-A5 partial sweep (3 commits)

After PR #107 opened, started incremental PG-A5 sweep on safe surface
to prove the pattern + de-risk future cutover passes.

| Slice | Commit | What |
|---|---|---|
| ci trigger | `2d5a6e9` | `.github/workflows/ci.yml` now triggers on `develop-fix` + `feat/*` so PR #107 actually runs CI |
| LeaderCursorStore | `fba01c1` | `services/leaderCursorStore.py` ported to async asyncpg pool + classmethod `create()` factory; sole production caller (`services.tradeMonitor.trade_monitor`) updated to `await`; test mocks switched to `AsyncMock(create=...)` pattern. 13/13 cursor tests pass |
| MarketStructureService | `c489f0b` | Full async rewrite incl. per-user `reconcile_positions` (single transaction wrap, `= ANY($N::text[])` for IN expansion, `now()` literal for timestamptz). 3 service tests migrated to live PG testcontainer via `_pg_pool()` async context manager — proves PG-A6 fixture pattern. 6/6 pass against real Postgres |

CI green (lint-type-import + unit py3.11/3.12 + release-gate py3.11 PASS; docker-smoke + release-gate py3.12 running at last check).

### Remaining PG-A5 surface (deferred to follow-up PR)

- `services/orderRepository.py` wrapper — 9 forwarding methods, exercised by 7 sites in `test_issue66_gateway_isolation.py` + `test_phase03_atomic_idempotency.py` + 2 production callers (`riskMonitor`, `runtime_bootstrap`).
- `utils/idempotency.py` — module-level helper used by ~10 sites for the legacy free-function `has_processed` / `mark_processed` API.
- `orchestrators/runtime_bootstrap.py` — central wire-up that instantiates 5 repos with `db_path`; rewriting it touches every caller via `await build_user_state(...)`.
- `services/discovery/ingestion_service.py`, `services/leaderRegistry`, `services/execution/*`, ~30 more files.

PG-A5 is mechanical but voluminous. Dual-stack `connect_db()` keeps SQLite live so this can ship in incremental PRs without breaking the green baseline.

## Post-PR #107 PG-A5 discovery slice (1 commit)

Finished the in-flight discovery migration that had landed half-converted
in the working tree before commit `c27a535`.

`services/discovery/scoring_input_service.py` had its import flipped to
`storage.postgres.discovery_repo.DiscoveryRepo` but the body still called
four PG async methods synchronously — the service was broken in that
state. Completed the conversion + ported the one production caller +
ported the three test files that exercise the discovery scoring /
reconstruction / raw-feature paths to a live `postgres:16-alpine`
testcontainer using the proven `_pg_pool()` async context-manager
pattern from `c489f0b`.

| Slice | What |
|---|---|
| `scoring_input_service.py` body | `assemble_for_trader` + `_build_last_trade_ts_index` made `async def`; `await` added to `reconstruct_for_trader`, `list_latest_position_snapshots`, `get_market` (per-key, single fetch via cache check), `list_trades` (single batch fetch) |
| `reconstructed_positions_service.py` | Committed the pre-existing fully-converted diff (sync→async + `storage.postgres.discovery_repo` import) |
| `raw_feature_joiner.py` | `join` + `_build_one` made `async def`; `await` added to `list_orderbook_snapshots` + `list_trades`. Removed `trader_wallet=` kwarg (PG `list_trades` takes it positionally) |
| `tests/test_repo_backed_reconstruction_service.py` | Rewritten: SQLite tmp_path → `_pg_pool()` testcontainer; all 4 tests `async def` with `pytestmark = pytest.mark.asyncio` for STRICT-mode compatibility |
| `tests/test_discovery_scoring_input_service.py` | Same rewrite pattern. 4 tests |
| `tests/test_raw_feature_joiner.py` | Same rewrite pattern. 17 tests across 5 classes |

### Verification

| Check | Result |
|---|---|
| `pytest tests/test_repo_backed_reconstruction_service.py tests/test_discovery_scoring_input_service.py tests/test_raw_feature_joiner.py -q` against live PG testcontainer | 25 passed in 73s |
| `pytest -q` (full unit) | 2342 passed, 6 skipped, 206 deselected, 2 pre-existing errors (unchanged) |
| `pytest -m release_gate -o asyncio_mode=auto -q` | 160 passed, 1 skipped |
| `ruff check --select=F,E --ignore=E501,F401,F841,E402,E741,E731` on touched files | clean |
| `compileall` on touched files | clean |
| `grep -rn "from storage.sqlite.discovery_repo" --include='*.py' .` excluding `storage/sqlite/` | `tests/test_storage.py:29` only — pan-repo ABC-conformance test, exercises every SQLite adapter against its sync `domain/ports` ABC; out of slice (deferred until ABCs flip async with the broader sweep) |

### Remaining PG-A5 surface (unchanged from prior section)

Production callers of `storage.sqlite.*` (≈26 files) and the ~50 SQLite-
backed test modules still rely on dual-stack `connect_db()`. Sweep
deferred to follow-up PRs — full 5-slice roadmap covers idempotency /
orderRepository / execution services / orchestrators / CLI / boot path /
final cleanup.

## Post-PR #107 PG-A5 CLI + housekeeping sweep (11 commits)

Follow-up to the discovery slice — drops every sqlite vestige from the
read-only CLI surface plus a handful of safe leaf-node cleanups. Each
commit keeps the green baseline (2342 unit + 160 release-gate).

| Slice | Commit | What |
|---|---|---|
| chore: orphan idempotency shim | `7348880` | Delete `services/execution/idempotency.py` — re-export shim with zero callers across the repo |
| tradeMonitor TradeCursor flip | `1a92935` | Flip import to `storage.postgres.cursors_repo.TradeCursor`; the frozen dataclass is structurally identical in both repos and the LeaderCursorStore runtime persistence already moved to async PG in `fba01c1` |
| app.py purge rewire | `c769107` | Drop the last production caller of `utils.idempotency`. Startup-time `purge_old_processed_trades(RETENTION_MAX_AGE_SECONDS)` now uses the async PG `OrdersRepo` directly with a defensive `RuntimeError` catch for the transition window before `connect_db()` |
| status CLI | `7ac3425` | `bot_cli/commands/status.py` async-rewritten. Opens own `init_pool`/`shutdown_pool` for the one-shot lifecycle, drops sqlite_master table-existence checks (long-running bot owns schema), `EXTRACT(EPOCH FROM ...)` replaces `strftime('%s', ...)` |
| positions CLI | `30ad37a` | `bot_cli/commands/positions.py` same pattern — single SELECT against `positions`, param markers `$N`, timestamptz isoformat guard |
| orders CLI pending | `1661329` | `bot orders pending` subcommand → async PG. The `reconcile` subcommand stays on the legacy SQLite `OrdersRepo` until `ReconciliationService` (1704 LOC) follows — inline comment documents the gap |
| reconcile CLI SELECT | `63444c7` | `bot reconcile`'s candidate-order SELECT now reads from canonical PG. Action path still routes through legacy SQLite `ReconciliationService`. Scoped import to keep blast radius bounded |
| asyncpg Pool DSN fix | `90aafd0` | `storage/postgres/connection.py` — asyncpg Pool uses `__slots__` in 0.30+, so `pool._polymarket_dsn = dsn` raised `AttributeError` once the new CLI tests started driving `init_pool`. Track DSN in a parallel module-level `_pool_dsn` instead; `shutdown_pool` + `_reset_for_tests` clear both |
| stale_locks CLI + tests | `55c3f8e` | `bot stale-locks list` async-rewritten. The jsonb `pending_since` extract + epoch arithmetic ported to `(response_json->>'pending_since')::float8::bigint` + `EXTRACT(EPOCH FROM …)`. All 4 release-gate tests ported to live PG testcontainer via `_pg_pool()`; end-to-end test drives `_run_stale_locks_async` directly to avoid clashing with the outer asyncio loop |
| drift CLI + tests | `af7a5fc` | `bot drift check` async-rewritten; sqlite_master check dropped, `_list_persisted_positions` takes asyncpg pool. 3 release-gate tests ported (2 pure-function tests stay sync) |
| reservations CLI + tests | `5443176` | `bot reservations list` async-rewritten; PG `order_reservations` schema already uses `bigint` epoch columns matching sqlite semantics, no cast changes. All 4 release-gate tests ported |

### Net effect

- Prod sqlite callers (excluding `storage/sqlite/` itself and tests):
  **26 → 14** files remaining.
- Tests now backed by live `postgres:16-alpine` testcontainer for
  every migrated CLI: stale_locks (4), drift (3), reservations (4),
  + the existing discovery slice (25). All counted under
  `pytest -m release_gate` (160 passed, 1 skipped — baseline unchanged
  because the ported tests replaced the same count of sqlite tests).

### Remaining prod sqlite surface

`bootstrap.py`, `app.py` (one `LeadersRepo` import for type hints),
`bot_cli/commands/doctor.py` (680 LOC, gates live trading per CLAUDE.md
— deserves its own session), `services/orderRepository.py` (114 LOC
wrapper + 9 forwarding methods, 7 test sites + ExecutionService
ripples), `services/execution/{reservation_service,reconciliation_service}.py`,
`services/riskMonitor.py`, `orchestrators/{trade_pipeline,
runtime_bootstrap,startup_health,leader_registry}.py`, `config/db.py`
(dual-stack rip is the last step), `utils/idempotency.py` (2 test
files still use the free-function API), `scripts/find_followable_traders.py`.

Plus ~47 sqlite-backed test modules to port.

## Post-PR #107 PG-A5 doctor slice (1 commit)

The last sqlite-bound CLI surface migrated to async PG. Critically:
``bot_cli/commands/doctor.py`` gates live trading per CLAUDE.md, so
every fail-closed semantic from PR #105 audit-fix #95 is preserved:
all three DB checks (``db_writable``, ``db_schema_ok``,
``stale_idempotency``) still emit ``blocked`` (not ``warning``) on
probe failure.

| Slice | Commit | What |
|---|---|---|
| doctor.py DB checks + test_doctor_live.py | `858c8c2` | ``_missing_db_tables`` async, queries ``information_schema.tables``. ``db_writable`` runs ``SELECT 1`` via ``init_pool``. ``stale_idempotency`` rewrites the epoch math to ``EXTRACT(EPOCH FROM now())::bigint - EXTRACT(EPOCH FROM created_at)::bigint``. Single pool shared across all three checks via try/finally + ``shutdown_pool``. ``tests/test_doctor_live.py`` ported to a per-test ``postgres:16-alpine`` testcontainer via ``_PgFixture`` async context manager — ``DATABASE_URL`` env override drives the doctor through the testcontainer. Each test's ``__aenter__``/``__aexit__`` clears the ``storage.postgres.connection`` singleton so containers don't leak across tests |

### Net effect after doctor slice

- Prod sqlite callers (excluding `storage/sqlite/` itself and tests):
  **14 → 13** files remaining.
- All `bot_cli/commands/*.py` now PG-native (status, positions, orders,
  reconcile, stale_locks, drift, reservations, doctor). The dual-stack
  bridge is no longer touched by any CLI command.
- 9 doctor tests pass against live PG testcontainer in ~24s.

### Remaining prod sqlite surface (post-doctor)

`bootstrap.py`, `app.py` (LeadersRepo type hint),
`services/orderRepository.py`, `services/riskMonitor.py`,
`services/execution/{reservation_service,reconciliation_service}.py`,
`orchestrators/{trade_pipeline,runtime_bootstrap,startup_health,leader_registry}.py`,
`config/db.py`, `utils/idempotency.py`, `scripts/find_followable_traders.py`.

These form a tighter dependency mesh — ExecutionService consumes the
order repo, ReservationService + ReconciliationService consume the
reservations/positions repos, the orchestrators consume the leaders
repo + wire everything. Incremental migration past this point requires
either a coordinated cross-file slice (e.g. "all execution-path
services in one pass") or a temporary async shim. Deferred to a
dedicated focused session.

## PG migration complete — final test-port slice (7 commits)

The post-doctor "tighter dependency mesh" called out above was already
silently resolved by commits ``b153be1`` (await get_position callbacks)
and ``a7b30d8`` (``feat(pg-final): kill storage/sqlite/ + flip entire
execution mesh to async PG``). All ``services/``, ``orchestrators/``,
``bootstrap.py``, ``app.py``, ``config/db.py``, ``utils/idempotency.py``,
and ``scripts/`` production code is now PG-native; remaining mentions
of ``storage.sqlite.*`` in the live tree are docstring references in
``storage/postgres/`` repos noting their historical translation source
(harmless, kept for provenance).

The only residual sqlite import surface lived in **5 module-skipped
test files** (4,575 LOC, ~98 tests) and one latent async-mock bug in
the release-gate suite. This slice closes them out.

| Slice | Commit | What |
|---|---|---|
| Issue #74 drift reconciliation tests | `ad748a2` | Drop module-level ``pytest.skip("sqlite-bound; pending PG port")``; swap 9 inline ``PositionRecord`` imports → PG; convert ``MockPositionsRepo`` / ``MockReservationService`` / inline mock helper classes (``list_active_for_scope``, ``mark_released``, ``release_terminal``) to async to match awaited surface at ``reconciliation_service.py:1621/1635/1643/1657``. 15 tests pass in 0.15s |
| Issue #70 reservation intent-lifecycle tests | `80d8755` | Finish the partial port from ``d932faf``. Drop module-level ``pytest.skip``; add canonical ``_pg_pool()`` testcontainer helper; port ``TestStaleReservationExpiry`` + ``TestReservationAuditLogging`` (4 ``@pytest.mark.integration`` tests) to async PG ``ReservationsRepo`` / ``PositionsRepo``; await ``svc.release_terminal``. 19 unit + 4 integration tests pass in 0.13s / 14.87s |
| Restart-rehydration risk tests | `7d7337b` | Drop module-level ``pytest.skip``; swap 6 top-level imports → PG; add ``_pg_pool()``; wrap every test body in ``async with _pg_pool() as pool:``; convert ``test_rehydrate_preserves_token_distinct_positions`` to ``async def``; await every previously-sync repo call (``append_fill``, ``rebuild_user_positions_from_fills``, ``get_position``, ``upsert_position``, ``save_submission``, ``get_by_order_id``, ``mark_processed_trade``, ``get_processed_trade``, ``get_active_risk_block``, ``list_fills_for_user_condition``); ``await rehydrate_position_manager(context)`` (``riskMonitor.py:702`` is now async). 11 tests pass in 39s |
| Trade-pipeline market-data authority tests | `a64a747` | Drop module-level ``pytest.skip``; swap 6 top-level imports → PG; drop ``_init_db(tmp_path)`` helper; change ``_make_context`` / ``_make_rest_context`` signature to ``(pool, tmp_path, ...)`` — pool drives repos, tmp_path stays only for the ``PnLTracker`` log file; wrap every test body in ``async with _pg_pool() as pool:``. 12 tests (WSS-primary + REST-snapshot) pass in 42s |
| Trade-pipeline safety tests (largest) | `e755aed` | The 1,511-LOC end-to-end safety net (duplicate prevention, market risk quarantine, retry budget, reconciliation, position drift sync, daily-spend ledger, leader-fraction sizing, token-id field priority). Same mechanical pattern as above plus: port the one raw-SQL block (``UPDATE processed_trades SET processed_at = ?``) to async asyncpg with ``$1/$2`` params + integer epoch; flip the two ``MagicMock(return_value=...)`` overrides of ``ctx.positions_repo.get_position`` / ``.apply_fill`` to ``AsyncMock``. 41 tests pass in 118s (35 async trade-pipeline + 6 sync ``TestTokenIdFieldPriority``) |
| Latent release-gate drift-mock fix | `35789ea` | ``tests/release_gate/test_manual_drift_reconciliation.py::_make_service`` still constructed ``positions_repo`` as a sync ``MagicMock`` with ``.get_position.return_value = SimpleNamespace(...)``. Service awaits ``get_position`` → ``TypeError: object types.SimpleNamespace can't be used in 'await' expression``. Pre-existing regression from ``a7b30d8`` that the release-gate gate's 68s smoke-run had been quietly failing on for the last 3 commits. Swap to ``AsyncMock`` for ``get_position`` / ``delete_position`` / ``upsert_position``. 4/4 pass in 0.11s |

### Final verification

| Check | Result |
|---|---|
| ``grep -rln "from storage.sqlite\|import storage.sqlite" --include='*.py' .`` excluding ``.venv``, ``build`` | **0 hits** — the only remaining ``storage.sqlite`` mentions in the tree are docstring comments inside ``storage/postgres/*.py`` noting "translated from ``storage/sqlite/<file>.py``" |
| ``storage/sqlite/`` directory | gone — was already removed from git in ``a7b30d8``; leftover ``__pycache__`` cleaned this session |
| ``pytest -q --ignore=tests/release_gate --ignore=tests/replay`` | **2,320 passed, 7 skipped, 46 deselected** in 846s (deselected = ``@pytest.mark.integration`` PG-container tests, run separately) |
| ``pytest -m release_gate -o asyncio_mode=auto -q`` | **153 passed, 6 skipped, 2,372 deselected** in 78s |
| ``python -m compileall -q`` on all ported files | clean |

### Net effect

- Production sqlite callers: **0**. Every ``services/``, ``orchestrators/``,
  ``bootstrap.py``, ``app.py``, ``config/db.py``, ``utils/idempotency.py``,
  ``scripts/``, and ``bot_cli/`` path is PG-native.
- Test sqlite callers: **0**. Every test under ``tests/`` either drives a
  live ``postgres:16-alpine`` testcontainer via the canonical
  ``_pg_pool()`` helper or uses ``AsyncMock`` against the async-repo
  surface.
- ``storage/sqlite/`` package: removed from disk.
- ``config/db.py`` is pure-PG (``connect_db()`` runs ``init_pool`` +
  ``apply_pg_migrations``; no sqlite fallback).
- CI gates green: 2,320 unit-suite passes + 153 release-gate passes.

The SQLite → Postgres migration is complete. The branch is ready for
review and merge into ``full-rework``.

## Nasty rework — legacy code kill (Phase A-E, 14 commits)

Follow-on slice on the same branch (`feat/postgres-migration`) — user
direction was "remove all legacy code and related path, migrate them
to actual stuff". Deletes every compat-only surface and migrates every
production ``ENV.get(...)`` callsite to typed ``AppConfig``.

### Phase A — dead code (3 commits)

| Slice | Commit | What |
|---|---|---|
| Drop `legacy/` package | `c75423c` | `rm -rf legacy/` (288-LOC `tradeExecutor.py` + empty `__init__.py`). Production never imported it (gate at `tests/release_gate/test_no_legacy_imports.py`). Delete sole consumers: `tests/test_services.py` (521 L) + `tests/test_copy_strategy.py` (143 L) |
| Drop MongoDB-era interfaces | `1aa0484` | `interfaces/__init__.py` exported `UserActivityInterface` + `UserPositionInterface` (depended on optional `bson.ObjectId`). No production references. Reduce file to doc-only stub |
| Tighten release-gate gate | `68d7e62` | Add `test_legacy_package_was_deleted` assertion that mirrors the existing `test_root_probe_scripts_were_deleted` — pins the directory absence, not just import absence |

### Phase B — `config/settings.py` + `_profit_lock_mode == "legacy"` (2 commits)

| Slice | Commit | What |
|---|---|---|
| Drop `_profit_lock_mode == "legacy"` branch | `4d780d7` | `services/positionManager.py` carried three init paths; only `config=AppConfig` was used in production. Drop the `risk_settings=` kwarg + the whole `elif risk_settings is not None:` block + the legacy `evaluate_exit` arm at lines 448-460. No-arg path defaults to `"disabled"`. Armed-then-retrace pattern is functionally subsumed by Priority 4 (trailing-stop) when configured via `AppConfig.features.enable_trailing_stop` |
| Delete `config/settings.py` + 7 legacy-shim tests | `7ffd63a` | `config/settings.py` was an env-name translation layer (`STOP_LOSS_PCT → stop_loss_ratio`, 18 mappings) used only by pre-rewrite tests; production reads original env names via `config/app_config.py` directly so no env-name compatibility is lost. Delete + 7 test files (`test_config_settings`, `test_legacy_env_migration`, `test_issue8_risk_cleanup`, `test_config_edge_cases`, `test_risk_behavior`, `test_new_modules`, `test_position_math`) |

### Phase C — `save_submission_legacy` + legacy idempotency probe (2 commits)

| Slice | Commit | What |
|---|---|---|
| Drop `save_submission_legacy` wrapper | `d8875fc` | `storage/postgres/orders_repo.py:289` was a thin dict→`OrderSubmissionRecord` converter that internally called `save_submission(record)`. Inline the conversion in `ExecutionService._save_submission`; delete `save_submission_legacy`; update 4 test callers |
| Drop pre-sizing idempotency probe | `5868dc2` | `services/executionService.py:663/714-715` looked up rows by the pre-sizing `_idempotency_key(req)` after the post-sizing `idem_key` missed — a transition shim from the post-sizing key migration. No real orders in prod DB (live blocked; per-test PG containers persist nothing) so no legacy keys exist on disk. Delete the probe + the `_idempotency_key(req)` helper itself; update three test patches from `_idempotency_key` → `_effective_idempotency_key` |

### Phase D — `ENV.get(...)` → `AppConfig` migration (7 commits, ~98 callsites)

| Slice | Commit | What |
|---|---|---|
| Add 6 missing AppConfig fields | `8784b40` | `ClobConfig.clob_v2_ready: bool` (fail-closed False), `NetworkConfig.{collateral,pusd,ctf_exchange_v2}_contract_address: str`, `ReconciliationConfig.max_closed_probes: int = 10`, `PollingConfig.retention_max_age_seconds: float = 30 * 86400`. All wired in `load_config` |
| `riskEngine` | `a81d45e` | 11 `ENV.get` reads (BALANCE_POLICY_* + MAX_*_EXPOSURE_USDC + MAX_DAILY_DRAWDOWN_PCT) gone. The legacy fallback for callers that bypass `init_risk_engine(config)` now returns a hardcoded fail-closed default `AccountCapPolicy`; `compute_market_headroom` no-config branch uses the same hardcoded defaults the env reads used (300/600/500/0.20). Drops `_env_bool` / `_env_float` / `_env_decimal` helpers + `from config.env import ENV` import |
| `traderDiscoveryService` | `b3b3dae` | 10 `ENV.get` reads (DISCOVERY_*, MIN_LEADER_*, AUTO_DISCOVER_*) gone. No-config init now sets `_enabled = False` + `_auto_discover = False` — service stays disabled on misconfigured bootstrap rather than running discovery on default values |
| `createClobClient` | `a18ba97` | 7 `ENV.get` reads gone (CLOB_HTTP_URL + META_CACHE_TTL_* at import time, CLOB_V2_READY + PUSD/COLLATERAL/CTF contract addrs in `create_clob_client`). Add `configure_from_appconfig(config)` module setter, called from `runtime_bootstrap.py:218` at startup. `create_clob_client` accepts `config=` kwarg; reads gate from `config.clob.clob_v2_ready` + contract addresses from `config.network.*` |
| `getMyBalance` | `5995e11` | 5 `ENV.get` reads gone. Same module-singleton + `configure_from_appconfig(config)` pattern as createClobClient; wired in `runtime_bootstrap.py:223`. Tests monkeypatch the new singletons (`_RPC_URL`, `_COLLATERAL_CONTRACT_ADDRESS`, …) instead of `ENV` |
| `reconciliation` | `03a824e` | 4 `ENV.get` reads gone (RECONCILE_STALE_SECONDS, RECONCILE_AUTO_CANCEL, RECONCILE_MAX_CLOSED_PROBES, RECONCILE_INTERVAL_SECONDS). All three constructor settings had an `AppConfig`-first branch already; drop the env fallback in favour of hardcoded defaults. `max_closed_probes` now reads from the new `ReconciliationConfig.max_closed_probes` field |
| Residual sweep (`app.py`, `tradeMonitor`, `doctor`, `idempotency`) | `e7ee18d` | Final 4 ENV.get callsites: `app.py:660` uses `application.config.polling.retention_max_age_seconds`; `services/tradeMonitor.py:67` hardcodes 0.25s default; `bot_cli/commands/doctor.py:269` reads `config.clob.clob_v2_ready`; `utils/idempotency.py:108` hardcodes 30-day retention. Drops 3 `from config.env import ENV` imports |

### Phase E — close-out + test fixups (2 commits)

| Slice | Commit | What |
|---|---|---|
| Fix 14 tests driving deleted ENV-fallback | `7abb80c` | `test_account_cap_policy.py` (10 fails) — patched `services.riskEngine.ENV`; replace with local `_policy(...)` builder that constructs `AccountCapPolicy` directly. `test_collateral_readiness.py` (1 fail) — replace `ENV` patch with `SimpleNamespace` AppConfig stub passed as `config=`. `test_decision_engine.py` (2 fails) — drop ENV patches; set discovery-gate thresholds via instance attrs. `test_issue69_order_sizing_idempotency.py` (1 fail) — rewrite `test_effective_key_different_from_legacy` against `_effective_idempotency_key` only |
| Close-out doc + push | _this commit_ | docs/full-rework-progress.md update, final verification, push |

### Final verification

| Check | Result |
|---|---|
| `grep -rn "ENV\.get\|from config.env import ENV" --include='*.py' .` excluding `tests/` and `config/env.py` | **0 hits** in production code (the only remaining `ENV.get` callsites live inside `config/env.py` itself for the `validate_runtime_env()` legacy adapter, plus test files — both allowed by CLAUDE.md) |
| `legacy/` package | gone |
| `config/settings.py` | gone |
| `save_submission_legacy` / `_idempotency_key` / `_profit_lock_mode == "legacy"` | gone |
| `pytest -q --ignore=tests/release_gate --ignore=tests/replay` | **1998 passed, 3 skipped, 46 deselected** in 787s |
| `pytest -m release_gate -o asyncio_mode=auto -q` | **154 passed, 6 skipped, 2046 deselected** in 62s |

### Net effect

- **Zero production `ENV.get(...)`** — every service / orchestrator /
  CLI command / util / app.py path reads from typed `AppConfig`.
- **Zero `legacy/` package** — production gate forbids re-introduction.
- **Zero `config/settings.py`** — env-name translation shim deleted;
  `config/app_config.py` reads original env names directly.
- **Zero `save_submission_legacy`** + **zero pre-sizing
  `_idempotency_key`** — effective post-sizing key is the only
  idempotency contract.
- **Zero `_profit_lock_mode == "legacy"` branch** — only `"advanced"`
  (typed `AppConfig`) and `"disabled"` modes remain.
- Six new typed `AppConfig` fields cover the previously-untyped env
  keys (`clob_v2_ready`, three `network.*_contract_address` fields,
  `reconciliation.max_closed_probes`, `polling.retention_max_age_seconds`).
- ~3,750 LOC removed from the codebase (legacy package, settings shim,
  7 legacy-shim test files, env-fallback branches across 9 production
  files).
- Test counts: 1998 unit + (release-gate, see push) passing.
- CLAUDE.md non-negotiables held: live trading gate still blocked,
  duplicate-order protection preserved, no float money types
  introduced, sizing remains metadata-driven with effective
  idempotency computed after sizing, every new AppConfig field has a
  fail-closed default.

The branch is now a clean handoff to `full-rework` with no compat
layers left to chase.
