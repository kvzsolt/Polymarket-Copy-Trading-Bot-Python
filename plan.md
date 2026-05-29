# Full-rework remediation implementation plan

Date: 2026-05-10  
Repository: `kvzsolt/Polymarket-Copy-Trading-Bot-Python`  
Branch: `full-rework`

## Goal

Implement every audit finding in small, reviewable PR-sized slices while keeping the bot runnable in dry-run after each slice. Live trading remains disabled until all release gates pass.

## Global rules for every PR

- One slice per branch and PR.
- Keep live trading disabled.
- Add tests in the same PR as code.
- Use `Decimal` for money, price, size, fee, tick, and notional logic.
- Do not read raw `ENV` in production trading paths after the typed-config slice.
- Every trading decision must emit a structured reason.
- Every state mutation must carry `PositionKey` or enough fields to build it.
- Prefer narrow adapters and incremental rewrites over broad destructive refactors.

## PR sequence

1. `audit/01-clob-v2-gateway`
2. `audit/02-position-key-coordinator`
3. `audit/03-marketdata-wss-cache`
4. `audit/04-sizing-effective-idempotency`
5. `audit/05-reservations-intent-lifecycle`
6. `audit/06-reconciliation-startup-recovery`
7. `audit/07-risk-protective-exit-correctness`
8. `audit/08-typed-config-fail-closed`
9. `audit/09-manual-position-reconciliation`
10. `audit/10-test-release-gate`
11. `audit/11-runtime-ops-docker-observability`
12. `audit/12-legacy-cleanup-docs`

## PR 1 — CLOB V2 gateway isolation

Branch: `audit/01-clob-v2-gateway`  
Severity: Critical / live-trading blocker.

### Purpose

Remove venue SDK/version details from execution logic and create a single fail-closed CLOB V2 boundary.

### Work

- Add `interfaces/clob_gateway.py` protocol.
- Add `integrations/polymarket/clob_v2_gateway.py`.
- Move signing, post, market metadata, fee lookup, book snapshot, balance/collateral checks, negative-risk flags, and error normalization behind the gateway.
- Update `ExecutionService` to depend on gateway interface only.
- Normalize errors into retryable/non-retryable/ambiguous categories.
- Add startup compatibility check that fails closed if venue/version semantics cannot be verified.

### Tests

- Gateway mock tests for BUY and SELL submission.
- Negative-risk path uses correct gateway behavior.
- Non-retryable venue errors are classified correctly.
- Timeout after POST returns ambiguous submitted-unconfirmed.
- `ExecutionService` no longer imports SDK order types directly.

### Acceptance

- No active execution code imports CLOB SDK order types.
- Live mode cannot start if gateway compatibility check fails.

## PR 2 — Canonical PositionKey and coordinator foundation

Branch: `audit/02-position-key-coordinator`  
Severity: Critical.

### Purpose

Identify positions as `(user, condition_id, token_id)` everywhere and serialize same-position mutation.

### Work

- Add `domain/position_key.py`.
- Add constructors from signals, WSS events, stored positions, execution requests, and reconciliation records.
- Add `services/position_action_coordinator.py`.
- Add intent state enum: `IDLE`, `INTENT_PENDING`, `SUBMITTED`, `ACK_LIVE`, `PARTIAL`, `FILLED`, `CONFIRMING`, `DONE`, `ERROR`, `CANCELLED`.
- Wire coordinator into trade pipeline, risk monitor, and reconciliation entrypoints.
- Add structured logging fields for position key.

### Tests

- YES/NO under same condition do not collide.
- Different users do not collide.
- Same key serializes concurrent operations.
- Different token ids can run concurrently.
- Illegal intent transitions are rejected.

### Acceptance

- All position-mutating entrypoints can construct a `PositionKey`.
- Coordinator exists in runtime context.

## PR 3 — Market data cache and WSS lifecycle

Branch: `audit/03-marketdata-wss-cache`  
Severity: High.

### Purpose

Stop trading from stale market data and establish WSS-first runtime behavior.

### Work

- Add token-scoped cache with TTL for quote, book, tick, and metadata.
- Add invalidation hooks for own fills, price changes, tick-size changes, and reconciliation.
- Add market WSS client and user WSS client.
- Send `PING` every 5 seconds.
- On reconnect, fetch REST snapshot before applying deltas.
- Parse `price_change` size `0` as level delete.
- Use `assets_ids` in market subscription payload.
- Keep REST fallback, but fail closed on stale data.

### Tests

- Cache TTL expiry.
- Cache invalidation on own fill.
- Cache invalidation on tick-size change.
- Subscription payload uses `assets_ids`.
- Heartbeat sends `PING` every 5 seconds.
- Reconnect snapshots before deltas.
- Size `0` deletes a book level.

### Acceptance

- Execution gets quote/book/tick/meta through one freshness-aware service.
- Stale/missing quote or book fails closed.

## PR 4 — Metadata-driven sizing and effective idempotency

Branch: `audit/04-sizing-effective-idempotency`  
Severity: High.

### Purpose

Fix min-order/tick-size correctness and make duplicate protection represent the final executable order.

### Work

- Add `services/order_sizing.py` with `SizingDecision`.
- Compute BUY effective size/notional from venue min size, current price, worst protected price, tick, balance, per-trade cap, market cap, and account cap.
- Compute SELL effective size from venue min size and held size.
- Remove hardcoded venue-min constants from active paths.
- Add `services/idempotency.py`.
- Compute key after sizing/rounding from user, condition id, token id, side, effective size, effective price, tick, intent/source id, and policy version.
- Persist effective sizing details in order request JSON.

### Tests

- BUY below venue min upsizes only when allowed.
- BUY below venue min skips when headroom is insufficient.
- SELL below venue min skips when held size is insufficient.
- SELL upsizes only when held shares support it.
- Worst-price notional is consistent.
- Idempotency key changes when effective size, price, tick, token, or condition changes.

### Acceptance

- Duplicate suppression keys represent executable orders.

## PR 5 — Reservations and intent lifecycle ownership

Branch: `audit/05-reservations-intent-lifecycle`  
Severity: Critical under concurrency.

### Purpose

Prevent overspend/oversell and make in-flight ownership explicit.

### Work

- Extend coordinator intent record with quote/share reservation.
- Reserve quote for BUY before submission.
- Reserve token shares for SELL before submission.
- Block duplicate unresolved intents for same `PositionKey`.
- Release reservations on rejection, cancellation, expiry, failure, and terminal reconciliation.
- Make reservations recoverable after restart.
- Add operator-visible acquire/release logs.

### Tests

- Two BUYs cannot collectively exceed free quote.
- Two SELLs cannot collectively exceed held shares.
- Reservations release on create failure and venue rejection.
- Ambiguous POST keeps reservation pending until reconciliation.
- Stale reservation expiry triggers reconciliation before release.

### Acceptance

- Balance/held-size checks account for reservations.

## PR 6 — Reconciliation and startup recovery ownership

Branch: `audit/06-reconciliation-startup-recovery`  
Severity: High.

### Purpose

Make reconciliation the sole owner of terminal settlement and close restart gaps.

### Work

- Ensure repository lookup by venue `order_id`.
- Prefer venue `order_id`, with idempotency fallback only for legacy records.
- Run startup reconciliation before risk/trade loops.
- Pre-reconcile submitted-unconfirmed orders before risk evaluation.
- Apply only incremental positive deltas when venue cumulative matched size increases.
- Persist matched progress.
- Reconciler finalizes terminal intent states.
- Move final accounting out of immediate execution path.

### Tests

- Startup reconciliation runs before risk monitor.
- Cumulative matched size applies only missing delta.
- Non-final partial remains non-terminal.
- Filled terminal order transitions once.
- Reconciler releases reservations after terminal state.

### Acceptance

- No double-apply after restart.
- Terminal settlement has one owner.

## PR 7 — Protective exits and re-entry correctness

Branch: `audit/07-risk-protective-exit-correctness`  
Severity: High.

### Purpose

Ensure protective exits are safe and re-entry blocks activate only after confirmed full close.

### Work

- Persist risk trigger metadata separately from active durable block.
- Activate re-entry block only after confirmed protective SELL fill fully closes the scoped `PositionKey`.
- Partial protective fill updates position but does not activate full block.
- Rejected protective SELL does not activate block.
- Restart recovery follows live-flow rules.
- Risk monitor uses coordinator and reservations.

### Tests

- Rejected protective SELL does not block re-entry.
- Partial protective SELL does not block re-entry.
- Full protective SELL closes position and activates block.
- Restart partial does not block re-entry.
- Restart full close activates block once.
- Re-entry block is token-scoped.

### Acceptance

- Risk block state cannot get ahead of confirmed position state.

## PR 8 — Typed config fail-closed cleanup

Branch: `audit/08-typed-config-fail-closed`  
Severity: High.

### Purpose

Remove raw environment fallback from production execution paths and fix stale tests.

### Work

- Make production constructors require `AppConfig`.
- Add explicit legacy/test config adapter for compatibility tests.
- Update `ExecutionService` to fail closed when production config is missing.
- Replace raw `ENV` reads in execution/trading/risk paths with typed config.
- Ensure paper/live mode, min sizing, retry budgets, tier gates, balance policy, slippage, stale-book policy, and spread gates are typed-config driven.
- Ensure `secrets.env` can override only allowlisted sensitive keys.
- Update `tests/test_execution_idempotency_extended.py` to use typed config fixtures or explicit legacy tests.

### Tests

- Production constructor without config fails closed.
- Runtime bootstrap injects config everywhere.
- Idempotency tests pass with typed config.
- Raw-env fallback tests are isolated.
- `secrets.env` cannot override public policy.

### Acceptance

- No production service silently reads raw env when typed config is required.

## PR 9 — Manual/off-bot position reconciliation

Branch: `audit/09-manual-position-reconciliation`  
Severity: High.

### Purpose

Prevent repeated SELL failures and stale local positions after manual Polymarket changes.

### Work

- Classify SELL-side insufficient balance/allowance errors as non-retryable state drift.
- Fetch live positions for user/token on drift.
- If live size is zero, close/delete local position.
- If live size is smaller, shrink local position.
- Release or adjust SELL reservations after reconciliation.
- Add `manual_position_drift_reconciled` log event.

### Tests

- Manual sell to zero deletes local position.
- Manual partial sell shrinks local position.
- SELL insufficient balance does not retry three times.
- Drift reconciliation does not activate protective block incorrectly.
- Subsequent SELL uses updated live size.

### Acceptance

- Manual/off-bot drift converges before next SELL decision.

## PR 10 — Final test and release gate

Branch: `audit/10-test-release-gate`  
Severity: Release blocker.

### Purpose

Prove the rewrite is safe before live mode.

### Work

- Add mock-CLOB integration suite.
- Add WSS reconnect/resync integration tests.
- Add restart recovery tests with persisted submitted-unconfirmed orders.
- Add end-to-end dry-run copy BUY and SELL tests.
- Add risk protective exit integration tests.
- Add concurrency stress tests.
- Add CI job groups for unit, integration, lint/type/import, and Docker smoke.

### Required matrix

- PositionKey uniqueness.
- Coordinator lock serialization.
- Cache TTL/invalidation.
- WSS heartbeat and reconnect.
- Metadata-driven sizing.
- Effective idempotency.
- Reservation lifecycle.
- Reconciler terminal ownership.
- Startup reconciliation before monitors.
- Manual drift reconciliation.
- Typed config fail-closed.
- CLOB V2 gateway compatibility smoke.

### Acceptance

- All tests pass in CI.
- Live mode remains blocked unless release gate passes.

## PR 11 — Runtime operations, Docker, observability

Branch: `audit/11-runtime-ops-docker-observability`  
Severity: Medium / operational release blocker.

### Purpose

Make the bot operable and diagnosable in production-like environments.

### Work

- Add health/readiness output for config, gateway, WSS, reconciler, DB, circuit breaker, and pending intents.
- Add metrics for submissions, rejects, ambiguous submissions, stale locks, WSS reconnects, cache staleness, reservation counts, reconciliation deltas, and risk exits.
- Add CLI/operator commands for pending orders, reservations, stale locks, position drift, one-shot reconciliation, and redacted config.
- Update Docker/runtime docs for Python version, dependency install, env files, volumes, and health check.
- Refuse live mode when readiness fails.

### Tests

- Docker build smoke.
- CLI redacts secrets.
- Health check fails when gateway/config/WSS/reconciler is not ready.
- Metrics expose required counters.

### Acceptance

- Operators can diagnose skips, blocks, stale state, and circuit-open conditions.

## PR 12 — Legacy cleanup and documentation

Branch: `audit/12-legacy-cleanup-docs`  
Severity: Low / maintainability.

### Purpose

Remove confusing old code and document the new architecture after safety gates are in place.

### Work

- Remove or quarantine root-level probe scripts and unused legacy files from production paths.
- Delete duplicate concepts once new components are live.
- Update README and usage docs.
- Add migration notes from `main` / `develop` to `full-rework`.
- Add runbooks for CLOB outage, WSS outage, circuit open, stale locks, reconciliation drift, and manual/off-bot changes.
- Archive old phased prompt docs if superseded.

### Tests

- Import smoke proves production modules do not import legacy/probe modules.
- Documentation links validated where practical.

### Acceptance

- Repository no longer has conflicting active implementations.

## Final live-trading gate

Live trading can be considered only when:

- PRs 1-10 are merged.
- PR 11 or equivalent runtime readiness is merged.
- All must-fix checklist items in `audit.md` are complete.
- CI is green.
- Mock CLOB integration tests pass.
- Paper trading soak completes without duplicate orders, stale locks, unreconciled submitted-unconfirmed orders, or unexplained skip bursts.
- Operator verifies collateral, negative-risk behavior, CLOB V2 gateway, health checks, config, Docker, DB backup, and rollback procedure.

## Implementation order rationale

1. Gateway first because execution tests must target current venue semantics.
2. Position identity/coordinator second because all state safety depends on correct scoping.
3. Market data third because sizing/slippage need fresh metadata.
4. Sizing/idempotency fourth because duplicate protection must use final effective order details.
5. Reservations fifth because concurrency safety requires in-flight ownership.
6. Reconciliation sixth because terminal settlement and restart behavior must converge.
7. Risk seventh because protective exits depend on reconciliation/reservations.
8. Typed config eighth because service boundaries can then be made fail-closed.
9. Manual drift ninth because it is simpler after reconciliation/reservations exist.
10. Final tests tenth because core architecture is complete.
11. Ops/Docker eleventh because readiness should reflect final architecture.
12. Cleanup last because deletion is safest after replacement coverage exists.
