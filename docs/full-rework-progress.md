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
