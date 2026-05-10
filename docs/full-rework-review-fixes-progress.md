# `full-rework-review-fixes` — progress

Branch tracking the audit remediation defined in `audit.md` and `plan.md`. One issue per slice. Live trading remains disabled.

## Issue order

| Issue | Slice | Status |
|---|---|---|
| #66 | CLOB V2 gateway isolation | complete |
| #67 | Canonical PositionKey + action coordinator | complete (sub-branch `fix/issue-67-position-key-coordinator`) |
| #68 | Market-data cache + WSS lifecycle | pending |
| #69 | Metadata-driven sizing + effective idempotency | pending |
| #70 | Reservations + intent lifecycle ownership | pending |
| #71 | Reconciliation + startup recovery ownership | pending |
| #72 | Protective exits + re-entry blocking correctness | pending |
| #73 | Typed config fail-closed + stale tests | pending |
| #74 | Manual / off-bot position reconciliation | pending |
| #75 | Final integration tests + release gate | pending |
| #76 | Runtime ops, Docker readiness, observability | pending |
| #77 | Legacy cleanup + architecture documentation | pending |

## Pre-flight baseline

Captured before any code edits on `full-rework-review-fixes`.

- Base branch: `full-rework` at commit `fb9c142` (`docs: replace phased-rewrite docs with audit + remediation plan`).
- Working branch: `full-rework-review-fixes` (new, pushed `-u`).
- Baseline pytest collection: `1313 / 1317 tests collected (4 deselected)` — pytest 8.4.2.
- Baseline failures (pre-existing on `full-rework`):
  - `tests/test_execution_idempotency_extended.py::TestDuplicateSignalSuppression::test_same_signal_id_second_call_ignored`
  - `tests/test_execution_idempotency_extended.py::TestDuplicateSignalSuppression::test_different_signal_id_allowed`
  - `tests/test_execution_idempotency_extended.py::TestDuplicateSignalSuppression::test_same_idempotency_key_blocks`
  - `tests/test_execution_idempotency_extended.py::TestStaleLockExpiry::test_expired_submission_allows_new_execution`
  - `tests/test_execution_idempotency_extended.py::TestAmbiguousPostHandling::test_ambiguous_post_failure_returns_unconfirmed`
  - `tests/test_execution_idempotency_extended.py::TestAmbiguousPostHandling::test_post_exception_after_create_flagged_ambiguous`
  - `tests/test_execution_idempotency_extended.py::TestAmbiguousPostHandling::test_create_exception_is_normal_failure`
  - All 7 are the audit-flagged stale tests — they construct `ExecutionService(...)` configless and trigger the existing fail-closed `execution_rejected_config_missing` short-circuit (services/executionService.py:273) before any gateway path is reached. Resolution: `#73 typed-config fail-closed` owns rewriting the configless constructor + these tests. `#66` does NOT touch these tests because the gateway-shaped fixture would still hit the same fail-closed rejection; new gateway-acceptance coverage lives in `tests/test_issue66_gateway_isolation.py` with proper typed-config fixtures.
- Baseline full-suite run: `pytest -q --ignore=test_probe.py` exceeded 600s wall clock without completing. Symptom localized to non-`#66` paths (likely socket / asyncio waits in unrelated suites). Not blocking issue #66; verification will run targeted subsets.
- `test_probe.py` (repo-root probe script that calls `polygon-rpc.example.com` at import time) is excluded from runs via `--ignore=test_probe.py`. Quarantine/removal is `#77`'s scope.

Targeted-subset baseline run for issue #66 (passes):

```text
tests/test_clob_v2_gateway_isolation.py ............................. 3 passed
tests/test_execution_idempotency_extended.py::TestIdempotencyKey..... 3 passed
tests/test_execution_idempotency_extended.py::TestDuplicate*.......... 3 failed (pre-existing)
tests/test_execution_idempotency_extended.py::TestStaleLock*.......... 1 failed (pre-existing)
```

## Issue #66 — CLOB V2 gateway isolation (complete pending PR)

### Summary

CLOB V2 venue boundary is now isolated behind a single canonical Protocol.
`ExecutionService` no longer imports SDK order types directly; the two-step
`create_market_order → post_signed_order` is replaced by a single
`gateway.submit_marketable_order` call. Structured venue errors drive
retryable / non-retryable / ambiguous classification end-to-end.
`runtime_bootstrap` builds a single gateway instance per user runtime so
later issues (#71) can share it with the reconciler / coordinator.
`startup_health` now probes `gateway.check_compatibility()` and refuses live
mode when the SDK is unavailable.

### Files changed

- `interfaces/__init__.py` — tolerant `bson` import (legacy quarantine note,
  full removal deferred to #77).
- `interfaces/clob_gateway.py` — NEW. Canonical `ClobGateway(Protocol)`.
- `integrations/polymarket/clob_v2_gateway.py` — `_build_submit_result`
  helper preserves `ambiguous_post_failure` / `non_retryable` flags;
  `MarketableOrderRequest.tick_size` / `neg_risk` honored when caller
  pre-fetched metadata; new `check_compatibility()` probe; `_classify_error`
  now matches `timeout` / `connection` keywords on message in addition to
  exception type name.
- `integrations/polymarket/models.py` — `VenueSubmitResult` gains
  `ambiguous_post_failure: bool` and `non_retryable: bool` fields;
  `MarketableOrderRequest` gains optional `tick_size: Decimal | None` and
  `neg_risk: bool | None` fields.
- `services/execution/coordinator.py` — local duplicate `ClobGateway`
  Protocol renamed to `OrderSubmitter`, with backward-compat alias and
  cross-reference to `interfaces.clob_gateway.ClobGateway`.
- `services/execution/reconciliation_service.py` — local duplicate renamed
  to `OrderQueryGateway`, with backward-compat alias.
- `services/executionService.py` — drops `BUY` / `OrderType` / `SELL`
  imports; adds `clob_gateway` constructor kwarg with `ClobV2Gateway`
  default-construct fallback for legacy call sites; replaces the two-step
  submit with `gateway.submit_marketable_order(MarketableOrderRequest(...))`;
  classifies `VenueAuthError` / `VenueInvalidOrderError` as non-retryable,
  `VenueUnavailableError` as ambiguous-POST (`submitted_unconfirmed`),
  `VenueRateLimitError` as retryable.
- `orchestrators/runtime_bootstrap.py` — constructs a single
  `ClobV2Gateway` per user runtime and exposes it on `UserRuntimeState`.
- `orchestrators/startup_health.py` — adds `clob_gateway` health item with
  5s timeout; live-mode flag downgrades warnings to errors.
- `tests/test_clob_v2_gateway_isolation.py` — new gate
  `test_no_sdk_order_types_in_execution_paths` forbids `OrderType` /
  `OrderArgs` / `MarketOrderArgs` identifiers in execution / risk / pipeline
  source files.
- `tests/test_issue66_gateway_isolation.py` — NEW. 11 acceptance tests for
  BUY / SELL submission, neg-risk propagation, non-retryable classification,
  ambiguous-POST handling, compatibility probe, and `_build_submit_result`
  flag plumbing.

### Tests run (this slice)

```text
tests/test_clob_v2_gateway_isolation.py            4 passed
tests/test_issue66_gateway_isolation.py           11 passed
tests/test_clob_v2_market_meta.py                 21 passed
tests/test_execution_coordinator.py                4 passed
tests/test_reconciliation_service.py               6 passed
tests/test_production_services.py                 14 passed (TestExecutionService)
tests/test_runtime_bootstrap_config.py             8 passed
tests/test_startup_health.py                       8 passed (no regressions)
```

Aggregated regression sweep (424 passed / 7 skipped) over 30 impacted suites
without test-pollution-prone files (see Remaining risks).

### Remaining risks

- `tests/test_execution_idempotency_extended.py` baseline failures (7
  failures, all pre-existing) remain; root cause is configless
  `ExecutionService(...)` construction hitting the existing fail-closed
  short-circuit at `services/executionService.py:273`. Owned by `#73`.
- `tests/test_execution_atomic_lock.py` hangs at baseline (pre-existing,
  pre-#66). Excluded from the regression sweep. Investigate separately;
  likely a deadlock under the autouse circuit-breaker reset.
- `tests/test_account_cap_policy.py` passes when run alone (35 / 35) but
  fails 4 tests when run after a test-pollution-prone neighbour. Pre-existing
  global-state mutation between suites; not caused by #66.
- Configless construction is still tolerated for backward compatibility;
  `#73` will tighten this. The auto-construct fallback in
  `ExecutionService.__init__` builds a `ClobV2Gateway` over the legacy
  `clob_client` so existing call sites keep working.
- Legacy `clients/order_gateway.py:OrderGateway` and
  `bson`-dependent `interfaces/__init__.py` legacy dataclasses remain in
  tree; quarantine in `#77`.
- `Decimal → float` casts inside `ClobV2Gateway.submit_marketable_order`
  (size / price_limit) — acceptable for now because the gateway is the
  isolated SDK boundary; tighten under `#69` / `#73`.

### Commit SHAs

- `a744c57` chore(issue-66): scaffold full-rework-review-fixes progress doc
- `302fff5` feat(issue-66): canonical ClobGateway protocol in interfaces/
- `e582f87` refactor(issue-66): wire ExecutionService through ClobV2Gateway
- `5ed9ab3` test(issue-66): gateway acceptance + strengthened isolation gate

### Next issue

_#68 — market-data cache + WSS lifecycle_

## Issue #67 — Canonical PositionKey + action coordinator (complete pending PR)

Sub-branch: `fix/issue-67-position-key-coordinator` (off `full-rework-review-fixes`).

### Summary

Canonical position identity ``(user_id, condition_id, token_id)`` now lives
in `domain/position_key.py` as `PositionKey` — a frozen-slots dataclass
with hex-id normalization, optional `outcome` (display only — excluded from
identity / hashing), and five spec-required factory constructors
(`from_signal_event`, `from_wss_event`, `from_stored_position`,
`from_execution_request`, `from_reconciliation_record`). The legacy
`services.positionActionCoordinator.PositionScope` becomes a back-compat
alias so the 8+ existing call sites keep working unchanged. The plan-named
snake_case path `services/position_action_coordinator.py` is added as a
re-export shim.

`services/execution/reconciliation_service.py` now optionally accepts a
`position_coordinator` kwarg and acquires the per-scope lock around the
two mutation blocks (filled-path + terminal-path) when the order record
carries the canonical fields; legacy records without those fields degrade
gracefully (no lock attempted, no exception). Structured `position_key`
fields are now emitted via `extra=` at every coordinator-acquire boundary
in `intentRegistry`, `riskMonitor`, `trade_pipeline`, and
`reconciliation_service`. Trade pipeline and risk monitor sites swap
inline `PositionScope(...)` for the new factory constructors where the
source data shape matches a factory's contract.

### Files changed

- `domain/position_key.py` — NEW. Canonical `PositionKey` + 5 factories +
  `log_fields()` + identity tuple; hex normalization; whitespace strip;
  empty-field validation.
- `services/position_action_coordinator.py` — NEW. Snake_case shim
  re-exporting `PositionActionCoordinator`, `PositionScope`, `PositionKey`.
- `services/positionActionCoordinator.py` — replace local `PositionScope`
  dataclass with `PositionScope = PositionKey` alias; drop `dataclass` import.
- `services/intentRegistry.py` — replace tuple-format `intent.begin`,
  `intent.release`, `intent.reservation_negative_residual` log lines with
  `extra=scope.log_fields()` structured fields.
- `services/riskMonitor.py` — emit `extra=scope.log_fields()` at the two
  protective-exit acquire sites (`scope_lock_acquired_confirm_fill`,
  `scope_lock_acquired_protective_exit`); pass optional `outcome` into
  the protective-exit scope.
- `orchestrators/trade_pipeline.py` — import `PositionKey` from
  `domain.position_key`; swap inline `PositionScope(...)` for
  `PositionKey.from_reconciliation_record(record)` and
  `PositionKey.from_signal_event(signal, context.user_id)`; emit
  `trade_pipeline.reconcile_scope_acquired` and
  `trade_pipeline.signal_scope_acquired` with structured fields.
- `services/execution/reconciliation_service.py` — accept optional
  `position_coordinator` constructor kwarg; add `_try_build_key`
  helper + `_scope_lock` async context manager; acquire per-scope lock
  around filled-path + terminal-path mutation blocks; emit structured
  `position_key.*` fields in `reconciliation_resolved_filled` and
  `reconciliation_resolved_terminal` audit events.
- `tests/test_position_key.py` — NEW. 26 tests (identity, normalization,
  validation, factories, alias resolution, snake_case shim).
- `tests/test_reconciliation_service_coordinator_wiring.py` — NEW. 5
  tests covering filled/terminal acquire, legacy graceful degrade,
  no-coordinator legacy path, pending-status no-acquire.
- `tests/test_position_action_coordinator.py` — append 3 spec-gap tests
  (different users do not collide; YES/NO via domain import; PositionKey
  vs PositionScope share one lock).
- `docs/full-rework-review-fixes-progress.md` — flip status row to
  complete; append this entry.

### Tests run (this slice)

```text
tests/test_position_key.py                                   26 passed
tests/test_position_action_coordinator.py                    12 passed (was 9)
tests/test_position_action_coordinator_integration.py         4 passed
tests/test_intent_registry.py                                22 passed
tests/test_reconciliation_service_coordinator_wiring.py       5 passed
tests/test_reconciliation_service.py                          6 passed
```

Targeted slice total: **75 passed** (4 deselected, 0 failed).

Regression sweep (no behavior change expected outside #67 surface):

```text
tests/test_risk_behavior.py
tests/test_execution_coordinator.py
tests/test_runtime_bootstrap_config.py
tests/test_startup_health.py
tests/test_phase5_hardening.py
tests/test_clob_v2_gateway_isolation.py
tests/test_issue66_gateway_isolation.py
tests/test_trade_pipeline_safety.py
tests/test_trade_pipeline_tiers.py
tests/test_reconciliation.py
tests/test_restart_rehydration_risk.py
                                                            182 passed (1 warning)
```

`python -m compileall -q domain/ services/ orchestrators/ tests/` — clean.

### Acceptance criteria mapping

- [x] `domain/position_key.py` with immutable `PositionKey(user, condition_id, token_id, outcome=None)`.
- [x] Constructors from signal events / WSS events / stored positions / execution requests / reconciliation records.
- [x] Casing/format normalization (hex lowercased, whitespace stripped) where safe.
- [x] `services/position_action_coordinator.py` with per-key async lock (snake_case shim wrapping the existing implementation).
- [x] Intent states: `IDLE`, `INTENT_PENDING`, `SUBMITTED`, `ACK_LIVE`, `PARTIAL`, `FILLED`, `CONFIRMING`, `DONE`, `ERROR`, `CANCELLED` (already present in `services/intentRegistry.py:43`).
- [x] Coordinator wired into trade pipeline, risk monitor, and reconciliation entrypoints (last one is opt-in via constructor injection — runtime wiring lands with #71 when reconciliation_service replaces `_reconcile_one`).
- [x] Structured log fields for `PositionKey` at mutation boundaries.

Tests:

- [x] YES and NO under same condition do not collide (`tests/test_intent_registry.py::test_yes_no_token_scopes_do_not_collide` + `tests/test_position_action_coordinator.py::test_yes_no_under_same_condition_do_not_collide_via_domain_import`).
- [x] Different users do not collide (`tests/test_position_action_coordinator.py::test_different_users_do_not_collide` + `tests/test_position_key.py::test_different_users_distinct_under_same_token`).
- [x] Same key serializes concurrent operations (`test_same_scope_is_serialized` + `test_same_key_via_domain_and_alias_share_one_lock` + `test_concurrent_buy_sell_serialized_via_coordinator_lock`).
- [x] Different token ids under same condition can run concurrently (`test_same_condition_different_token_are_distinct_scopes`).
- [x] Illegal intent transitions are rejected (`test_finalize_requires_by_reconciler_flag`, `test_finalize_rejects_non_terminal_state`, `test_submitter_transition_cannot_write_terminal`).

### Remaining risks

- `services/execution/reconciliation_service.py` is not yet on the live
  reconciliation path — production reconciliation runs via
  `_reconcile_one` in `orchestrators/trade_pipeline.py` (which IS
  coordinator-wired). Full handoff to the new service is owned by #71.
- `runtime_bootstrap.py` does not yet pass `position_coordinator` into
  `ReconciliationService` — wiring is preparatory; live wiring lands
  with #71 once reconciliation_service is the live reconciler.
- `riskMonitor.py:441` `release_scope` site (post-close) still uses
  inline `PositionScope(...)` — could move to `from_stored_position`
  but the close-confirmation context already has the fields locally;
  no factory migration needed.

### Commit SHAs

- `df3732a` feat(issue-67): canonical PositionKey in domain/
- `190eb76` refactor(issue-67): wire PositionKey factories + structured logs at mutation boundaries
- `5ce7dc6` test(issue-67): PositionKey unit tests + spec-gap coordinator tests + recon wiring tests
- (this commit) docs(issue-67): record final SHAs and complete status

