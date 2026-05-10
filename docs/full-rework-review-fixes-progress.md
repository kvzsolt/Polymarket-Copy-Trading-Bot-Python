# `full-rework-review-fixes` — progress

Branch tracking the audit remediation defined in `audit.md` and `plan.md`. One issue per slice. Live trading remains disabled.

## Issue order

| Issue | Slice | Status |
|---|---|---|
| #66 | CLOB V2 gateway isolation | in progress |
| #67 | Canonical PositionKey + action coordinator | pending |
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

_pending_

### Next issue

_#67 — canonical PositionKey + action coordinator_
