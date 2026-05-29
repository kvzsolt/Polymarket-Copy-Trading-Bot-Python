# Full-rework technical audit

Date: 2026-05-10  
Repository: `kvzsolt/Polymarket-Copy-Trading-Bot-Python`  
Branch audited: `full-rework`  
Baseline considered: current `main` / `develop` line conceptually, plus prior phased-rewrite context.

## Executive verdict

`full-rework` is materially better than the old `main` / `develop` line as a continuation point. It has moved in the right direction: more explicit execution lifecycle handling, safer ambiguous POST handling, pending-submission TTLs, fill-confirmation safeguards, atomic submission-lock support where available, typed runtime composition in bootstrap, and a coherent phased rewrite strategy.

However, `full-rework` is not live-trading ready. The branch still has production blockers around CLOB V2 isolation, canonical position identity, market metadata freshness, effective idempotency, reservations, reconciliation ownership, protective-exit correctness, config fail-closed behavior, and release-gate coverage.

Recommended decision: continue development from `full-rework`, but keep live trading disabled until the must-fix checklist is complete and proven by integration tests.

## Why `full-rework` is better than `main` / `develop`

1. Execution lifecycle is more explicit.
2. Ambiguous submission paths are handled more safely than a naive retry loop.
3. Duplicate-signal/idempotency handling is stronger than the legacy baseline.
4. Fill confirmation avoids blindly using requested amount as filled amount.
5. Runtime bootstrap is closer to typed dependency injection.
6. The branch has an explicit hardening roadmap and separates many production concerns better than the old line.

## Why it is still unsafe for live trading

1. CLOB V2 gateway compatibility is not fully isolated or proven.
2. Position identity must be canonical as `(user, condition_id, token_id)` everywhere.
3. Same-position actions can still race unless a coordinator/reservation model owns them.
4. Min-size, tick-size, stale quote, spread, and book-quality handling need a single source of truth.
5. Idempotency must be based on the final effective executable order, not only raw request fields.
6. Reconciliation must be the sole terminal settlement owner.
7. Protective exit and re-entry-block timing must be proven under partial/rejected/restarted orders.
8. Typed config fail-closed behavior is incomplete because some services/tests still allow configless construction.
9. Manual/off-bot position changes need deterministic reconciliation before retries.
10. Release readiness needs a stronger integration matrix and runtime readiness gates.

## Top 10 highest-risk issues

### 1. CLOB V2 gateway and signing/collateral compatibility are not proven

Severity: Critical / live-trading blocker.

Active execution must not depend directly on SDK order types or old CLOB assumptions. The bot needs a single CLOB V2 gateway that owns signing, post, market metadata, book snapshots, negative-risk behavior, collateral/balance checks, and venue error normalization.

Risk scenarios:

- Orders fail signature verification after venue-side type/domain changes.
- Negative-risk orders are signed/submitted through the wrong path.
- Balance/allowance checks target old collateral semantics.
- Tests pass against mocks while live order creation fails.

Required fix:

- Add a CLOB V2 gateway interface.
- Move all SDK imports and venue semantics behind that gateway.
- Fail closed at startup if gateway compatibility cannot be verified.

### 2. Canonical position identity is not guaranteed everywhere

Severity: Critical.

The bot must identify positions as `(user, condition_id, token_id)`. Condition-only identity can collapse YES/NO tokens under the same condition.

Risk scenarios:

- BUY on YES later causes SELL on NO.
- Re-entry block for one outcome blocks the other outcome.
- Reconciliation updates the wrong position.
- Cache keyed only by condition reuses wrong token metadata.

Required fix:

- Add a canonical `PositionKey`.
- Enforce it across signal ingestion, position storage, risk monitor, execution request, reconciliation, market-data cache, reservations, and logs.

### 3. Same-position engine collisions can defeat order-level idempotency

Severity: Critical.

Order-level idempotency does not serialize every state mutation. Trade pipeline, risk monitor, reconciliation, delayed-fill recovery, and restart handling can observe stale state and issue contradictory actions.

Risk scenarios:

- Copy BUY and protective SELL race.
- Reconciler applies fill while another task computes SELL from stale local state.
- Two different signals pass duplicate checks but overbuy the same scoped position.
- Risk monitor evaluates stale local state before submitted-unconfirmed orders converge.

Required fix:

- Add per-position coordinator/lock.
- Add explicit intent state machine.
- Require all position-mutating actions to go through the coordinator.

### 4. No durable quote/share reservation model for in-flight intents

Severity: High / Critical under concurrency.

Balance checks before submission are time-of-check/time-of-use races. BUY needs quote reservation; SELL needs share reservation.

Risk scenarios:

- Two BUYs individually pass balance checks and collectively overspend.
- Two SELLs oversell the same shares.
- A timeout leaves a local lock with no deterministic reservation release path.
- Manual/off-bot state changes invalidate local sellable size.

Required fix:

- Add durable reservations tied to `PositionKey` and intent id.
- Release reservations on rejection, cancel, timeout expiry, reconciliation failure, and confirmed terminal state.

### 5. Minimum sizing and tick handling are not centralized around venue metadata

Severity: High.

The correct venue minimum and tick-size behavior must come from market metadata, not global constants or strategy guesses.

Risk scenarios:

- Valid small BUY is skipped incorrectly.
- SELL uses BUY-side copy floors and is rejected.
- Worst-price notional differs between decision and execution.
- Tick-size changes cause repeated venue rejections.

Required fix:

- Add a `SizingDecision` component.
- Feed it market metadata, tick size, worst protected price, held size, balance/headroom, and policy.
- Return either an effective executable order or a structured skip reason.

### 6. Idempotency is not based on the effective adjusted order

Severity: High.

Duplicate suppression should hash the final executable order after sizing and quantization, not only the raw request.

Risk scenarios:

- Raw small BUY is upsized but duplicate key represents only the raw request.
- Tick-size change changes executable price but not the idempotency key.
- Same signal across different condition context is not distinguished.
- Retry semantics diverge between decision and execution.

Required fix:

- Compute idempotency after sizing/rounding from `(user, condition_id, token_id, side, effective_size, effective_price, tick_size_at_decision, intent/source id, policy_version)`.

### 7. Market-data freshness and WSS lifecycle are not release-grade

Severity: High.

Trading requires fresh token-scoped quote/book/tick/metadata and robust WSS lifecycle handling.

Risk scenarios:

- Quote used for slippage is stale.
- Zero-size book level remains as phantom liquidity.
- Tick-size change is missed.
- WSS reconnect resumes deltas without snapshot resync.
- User order/fill event is missed and local state remains stale.

Required fix:

- Implement WSS-first market/user streams.
- Use heartbeat and reconnect with REST snapshot resync.
- Invalidate cache on own fills, price changes, tick-size changes, and reconciliation.
- Fail closed on stale/missing venue data.

### 8. Reconciliation and startup recovery are not proven as sole settlement path

Severity: High.

The reconciler should own terminal state transitions and apply only incremental fill deltas.

Risk scenarios:

- Cumulative venue matched size is applied twice after restart.
- Protective SELL recovery lacks metadata and closes the wrong scoped position.
- Startup risk monitor evaluates stale state before pending orders reconcile.
- Unknown order drift is matched by wrong key.

Required fix:

- Reconciler uses venue `order_id` first.
- Apply only positive fill deltas.
- Startup reconciliation runs before risk/trade loops.
- Reconciler owns terminal settlement.

### 9. Protective exits and re-entry blocks can become inconsistent

Severity: High.

Risk blocks must not activate until the protective SELL is confirmed and the scoped position is fully closed.

Risk scenarios:

- Protective SELL is rejected but re-entry is blocked.
- Partial protective fill activates permanent block.
- Restart recovery double-applies matched size.
- Re-entry block scopes by condition instead of token.

Required fix:

- Persist trigger metadata separately.
- Activate durable block only after confirmed full close.
- Cover live and restart paths.

### 10. Typed config fail-closed behavior is incomplete and tests are stale

Severity: High.

The known observation is valid: tests that instantiate `ExecutionService(...)` without config are not just stale setup; they indicate lower-level service boundaries still allow configless operation.

Risk scenarios:

- Tests pass while live fail-closed path is not exercised.
- Raw environment fallback changes policy silently.
- Future composition path bypasses typed config.
- Different services read different policy sources.

Required fix:

- Production constructors should require typed `AppConfig`.
- Raw-env fallback should exist only in explicit legacy/test adapters.
- Update stale tests to use minimal typed config fixtures.

## Concrete bug list

### `services/executionService.py`

- `ExecutionService.__init__(..., config: AppConfig | None = None)` permits configless construction.
- `execute()` can fall back to raw environment-derived config when typed config is missing.
- Raw request idempotency is computed before effective sizing/rounding.
- Idempotency lacks condition id, effective worst price, tick size, venue min size, negative-risk metadata, and policy version.
- Direct CLOB SDK imports leak venue details into execution logic.
- Decimal values are converted to float near order creation; conversion should be isolated and tested inside gateway serialization.
- Process-local submission semaphore is not a durable reservation model.
- Fill confirmation is safer than baseline, but settlement ownership remains split across services/repositories.

### `tests/test_execution_idempotency_extended.py`

- Instantiates `ExecutionService(...)` without typed config.
- Tests legacy configless path instead of intended fail-closed production path.
- Does not cover effective idempotency after min-size adjustment, tick rounding, condition-token scoping, or policy-version changes.

### `tests/test_production_services.py`

- Mixed typed-config and non-config construction patterns should be rationalized.
- Stale pending-submission expiry should be tested as part of startup reconciliation before monitors run.

### `orchestrators/runtime_bootstrap.py`

- Good: runtime bootstrap injects typed `AppConfig` into production services.
- Remaining issue: lower-level services still allow configless construction, so future paths can bypass bootstrap safety.

### Risk/reconciliation paths

- Verify durable risk blocks activate only after confirmed protective SELL full close.
- Verify reconciliation uses venue `order_id` as primary lookup.
- Verify startup reconciliation always completes before risk monitor and trade loops begin.
- Verify cumulative matched size is applied as incremental deltas only.

### Market-data/WSS paths

- Market WSS subscription must use `assets_ids`.
- Heartbeat should be 5 seconds.
- Reconnect must trigger REST snapshot before deltas.
- `price_change` with size `0` must delete the level.
- Tick-size changes and own fills must invalidate scoped cache.

### Config/runtime

- `secrets.env` must not override public trading policy.
- Docker/runtime entrypoint must refuse live mode with missing typed config or failed readiness.

## Missing tests list

### Trading safety

- BUY/SELL copy decision tests for YES and NO under same condition.
- BUY below venue minimum upsizes only when balance/caps allow.
- BUY below venue minimum skips when headroom is insufficient.
- SELL below venue minimum skips when held size is insufficient.
- SELL below venue minimum upsizes only when held shares support it.
- Worst-price notional is carried from decision into execution.
- Tick-size rounding tests for BUY ceil and SELL floor.
- Slippage rejection for missing, stale, wide, or thin books.

### Execution lifecycle

- Effective idempotency changes when adjusted size, price, tick, condition, or token changes.
- Same raw signal but different effective order does not incorrectly collide.
- Concurrent same effective order acquires one submission lock.
- Ambiguous POST writes submitted-unconfirmed and startup reconciliation resolves it before monitors run.
- Create-order failure is non-ambiguous.
- Pending lock TTL expiry triggers reconciliation before release.
- Non-retryable venue rejections do not consume retry budget like network errors.

### Reservations/collision handling

- Same-position BUY and SELL serialize.
- Different tokens under same condition run independently.
- BUY quote reservation prevents overbuy.
- SELL share reservation prevents oversell.
- Reservations release on rejection, cancel, timeout expiry, stale lock expiry, and terminal reconciliation.
- Reconciler is sole writer for terminal intent states.

### Reconciliation

- Startup reconciliation runs before risk monitor.
- Venue cumulative matched size applies only positive delta.
- Partial order remains non-terminal.
- Filled SELL closes local position exactly once.
- Unknown venue order uses `order_id` first.
- Manual/off-bot SELL shrinks/deletes local position after sync.

### Risk

- Protective trigger metadata can persist without active re-entry block.
- Re-entry block activates only after confirmed full close.
- Rejected protective SELL does not block re-entry.
- Partial protective SELL does not block re-entry.
- Restart recovery follows same rules as live recovery.
- Re-entry block is token-scoped.

### Market data / WSS

- Market WSS subscribe payload uses `assets_ids`.
- User WSS parses order/trade lifecycle events.
- Heartbeat sends `PING` every 5 seconds.
- Reconnect performs REST snapshot first.
- `price_change` with size `0` deletes level.
- `tick_size_change` invalidates cache.
- Own fill invalidates scoped cache.

### Config and release

- Production constructors fail closed without typed config.
- Runtime bootstrap injects config into all production services.
- `secrets.env` cannot override non-sensitive runtime policy.
- Paper/live gating reads typed config.
- Docker image starts health/readiness checks but refuses unsafe live mode.
- Dependency/import smoke test proves the intended CLOB V2 gateway is active.

## Architecture cleanup recommendations

1. Introduce `ClobGatewayV2`; execution should not import SDK order types directly.
2. Add canonical `PositionKey` and require it everywhere state mutates.
3. Add `PositionCoordinator` as the only route for same-position mutation.
4. Split decision, sizing, execution, settlement, and reconciliation into explicit phases.
5. Make market metadata/cache a first-class dependency.
6. Move idempotency into a dedicated effective-intent module.
7. Replace raw environment fallback with typed config in production paths.
8. Remove or quarantine legacy/probe files from production import paths.
9. Add health/readiness outputs for config, gateway, WSS, reconciler, circuit state, and pending intents.
10. Add structured audit logs for skip/reject/submit/reconcile decisions.
11. Make release gates explicit and automated.

## Must fix before live trading

- [ ] CLOB V2 gateway isolated and verified.
- [ ] Current collateral/balance/allowance path verified.
- [ ] Negative-risk order handling verified.
- [ ] Canonical `(user, condition_id, token_id)` enforced everywhere.
- [ ] Same-position coordinator and intent state machine implemented.
- [ ] BUY quote and SELL share reservations implemented.
- [ ] Venue metadata-driven min order sizing implemented.
- [ ] Tick-size-aware quantization implemented.
- [ ] Effective adjusted idempotency implemented.
- [ ] WSS market/user streams implemented with heartbeat/reconnect.
- [ ] Cache invalidation implemented for own fills, price changes, tick-size changes, and reconciliation.
- [ ] Reconciler owns terminal settlement and incremental fill deltas.
- [ ] Startup reconciliation runs before risk/trade loops.
- [ ] Protective blocks activate only after confirmed full close.
- [ ] Manual/off-bot position changes reconcile local state before retrying.
- [ ] Production constructors fail closed without typed config.
- [ ] Stale tests updated or explicitly marked legacy.
- [ ] Integration test matrix passes.
- [ ] Docker/runtime entrypoint refuses unsafe live mode.

## Nice to fix later

- [ ] Remove old probe scripts and legacy code after replacement coverage exists.
- [ ] Add metrics for submissions, rejects, stale locks, reconciliation deltas, WSS reconnects, and circuit state.
- [ ] Add operator CLI for pending orders, reservations, stale locks, and position drift.
- [ ] Add paper-trading soak report generator.
- [ ] Add property-based tests for Decimal/tick rounding.
- [ ] Add migration docs from old `main` / `develop`.
- [ ] Add outage and drift runbooks.
- [ ] Add typed event schemas for WSS events.
- [ ] Add redacted effective-config diff command.

## Final release-readiness assessment

Continue development from `full-rework`, but keep live trading disabled. Live trading can be considered only after the must-fix checklist is complete, CI passes, mock-CLOB integration tests pass, and paper trading soaks without duplicate orders, stale locks, unreconciled submitted-unconfirmed orders, or unexplained skip bursts.
