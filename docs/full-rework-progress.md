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
- `services/price_cache.py` — emits `cache_stale_evictions_total` on fill /
  reconcile / reconnect invalidation paths.
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
- New: `docs/ARCHITECTURE.md` *(canonical architecture overview;
  layered view, composition root, components, boundaries to defend)*.
- New: `docs/MIGRATION.md` *(`main`/`develop` → `full-rework` migration
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
  — 2 passed.
- `python -m pytest tests/release_gate/ -m release_gate` —
  97 passed (was 93 pre-#77; +2 from the new `test_no_legacy_imports`
  module, +2 net from gate housekeeping).
- `python -m pytest -q` — 1474 passed, 8 skipped, 143 deselected,
  7 xfailed (deltas vs #76 baseline: 0).
- `python -m compileall -q .` — clean.
- Doc-link sanity script (walks every `.md` under `docs/`, resolves
  every relative `.md` link) — `broken links: none`.

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
