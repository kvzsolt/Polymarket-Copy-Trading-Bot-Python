# CLAUDE.md

This repository contains a production-intended Polymarket copy-trading bot rewrite. Treat this codebase as safety-critical trading software.

## Current task

Implement findings from the `full-rework` audit and remediation plan.

- Base branch: `full-rework`
- Audit document: `audit.md`
- Plan document: `plan.md`
- GitHub issues: #66 through #77

Read `audit.md` and `plan.md` before making code changes. They are the source of truth for the remediation scope.

## Non-negotiable safety constraints

- Never place real orders during development or tests.
- Never enable live trading as part of audit remediation.
- Keep the bot fail-closed when config, venue state, market metadata, price/book freshness, or account state is missing.
- Do not remove safety checks to make tests pass.
- Do not weaken duplicate-order, stale-lock, or reconciliation protections.
- Do not collapse position identity to condition-only identity.
- Do not use floats for money, size, price, tick, fee, or notional logic except in a tightly isolated external SDK serialization adapter.
- Do not silently retry non-retryable venue rejections.
- Do not trade from stale quotes/books/tick metadata.

## Branch and commit discipline

**Rule:** Every issue gets its own branch. No more `full-rework-review-fixes`.

Branch naming:
```text
issue-66
issue-67
...
issue-77
```

Workflow per issue:
```bash
# 1. Start from latest full-rework
git fetch origin
git checkout full-rework
git pull origin full-rework

# 2. Create issue branch
git checkout -b issue-NN
git push -u origin issue-NN

# 3. Implement, commit, push
git add ...
git commit -m "fix(issue-NN): description"
git push origin issue-NN

# 4. When issue is complete and tests pass, merge into full-rework
git checkout full-rework
git merge --no-ff issue-NN -m "Merge issue-NN: description"
git push origin full-rework

# 5. Delete the issue branch (optional, but clean)
git push origin --delete issue-NN
git branch -d issue-NN
```

Commit style:
```text
fix(issue-66): isolate CLOB V2 gateway interface
test(issue-69): cover effective idempotency after sizing
docs(issue-76): add operator readiness runbook
```

Every issue-completion commit should include the related issue reference:
```text
Refs #66
```

Use `Fixes #66` only when all acceptance criteria and tests for that issue are complete.

## Progress tracking

Maintain:
```text
docs/full-rework-progress.md
```

Update it after each issue slice with:
- issue number
- summary
- files changed
- tests run
- known remaining risks
- next issue
- commit SHA

Commit and push this progress file with the related changes.

## Issue implementation order

Follow this order:
1. #66 — CLOB V2 gateway isolation
2. #67 — Canonical PositionKey and action coordinator
3. #68 — Market-data cache and WSS lifecycle
4. #69 — Metadata-driven sizing and effective idempotency
5. #70 — Reservations and intent lifecycle ownership
6. #71 — Reconciliation and startup recovery ownership
7. #72 — Protective exits and re-entry blocking correctness
8. #73 — Typed config fail-closed and stale tests
9. #74 — Manual/off-bot position reconciliation
10. #75 — Final integration tests and release gate
11. #76 — Runtime ops, Docker readiness, and observability
12. #77 — Legacy cleanup and architecture documentation

Reordering is allowed only when technically necessary. Document the reason in `docs/full-rework-progress.md`.

## Core architecture targets

### CLOB V2 gateway

Execution code must not import CLOB SDK order types directly.

Add a gateway/protocol layer that owns:
- signing
- order creation/posting
- market metadata
- order book snapshots
- price lookup
- fee lookup
- collateral/balance/allowance checks
- negative-risk behavior
- venue error normalization

`ExecutionService` should depend on this gateway interface, not SDK-specific types.

### Position identity

Use canonical position identity everywhere:
```text
(user, condition_id, token_id)
```

Optional outcome may be carried for display/logging, but must not replace token identity.

Add or use a canonical `PositionKey`.

All state mutation paths must preserve `PositionKey`:
- signal ingestion
- copy-trade decisions
- execution requests
- position storage
- risk monitor
- reconciliation
- market-data cache
- reservations
- logs/audit events

### Coordinator and intent lifecycle

Same-position actions must be serialized by a coordinator.

Intent states should cover:
```text
IDLE
INTENT_PENDING
SUBMITTED
ACK_LIVE
PARTIAL
FILLED
CONFIRMING
DONE
ERROR
CANCELLED
```

No component should mutate position/order lifecycle state without going through the coordinator or reconciler-owned settlement path.

### Reservations

A pre-submit balance check is not enough.

Implement:
- BUY quote reservations
- SELL share reservations
- release on rejection
- release on cancel
- release on timeout/expiry after reconciliation
- release on terminal reconciliation
- restart-recoverable reservation state

### Market data and WSS

Trading decisions require fresh token-scoped data:
- quote
- book
- tick size
- market metadata

Implement or preserve:
- TTL-based freshness
- invalidation on own fills
- invalidation on price changes
- invalidation on tick-size changes
- invalidation after reconciliation
- WSS market stream
- WSS user stream
- `PING` heartbeat every 5 seconds
- REST snapshot on reconnect before applying deltas
- `price_change` with size `0` deletes a level
- market subscription payload uses `assets_ids`

Fail closed on stale or missing data.

### Sizing and idempotency

Sizing must be metadata-driven.

Effective idempotency must be computed after sizing and tick rounding, from at least:
- user
- condition id
- token id
- side
- effective size
- effective price
- tick size at decision
- intent/source event id
- policy version

Do not compute duplicate keys only from raw request amount/price.

### Reconciliation

Reconciliation owns terminal settlement.

Rules:
- Prefer venue `order_id` lookup.
- Use idempotency-key fallback only for legacy records.
- Startup reconciliation must run before risk/trade loops.
- Apply only incremental positive deltas from cumulative matched size.
- Do not double-apply fills after restart.
- Reconciler releases reservations on terminal state.
- Execution records pending/ack state but should not own final accounting.

### Protective exits and re-entry blocks

Risk trigger metadata is not the same as an active durable re-entry block.

Only activate durable re-entry block after confirmed protective SELL fully closes the scoped `PositionKey`.

Rejected or partial protective SELLs must not activate full re-entry block.

Restart recovery must follow the same logic as live execution.

### Typed config

Production services must require typed `AppConfig`.

Raw environment fallback belongs only in explicit legacy/test adapters.

Important:
- live/paper mode
- sizing policy
- retry budgets
- tier gates
- balance policy
- slippage
- stale-book policy
- spread gates

must come from typed config in production paths.

`secrets.env` may override only allowlisted sensitive keys, not public trading policy.

### Manual/off-bot drift

SELL-side insufficient balance/allowance errors should be classified as non-retryable state drift.

On drift:
- fetch live holdings for the user/token
- close/delete local position if live size is zero
- shrink local position if live size is smaller
- release or adjust reservations
- log `manual_position_drift_reconciled`

## Test requirements

Add tests with each issue. Do not defer all tests to the end.

Minimum final matrix:
- CLOB V2 gateway mock tests
- PositionKey uniqueness tests
- coordinator serialization tests
- market-data cache TTL/invalidation tests
- WSS heartbeat/reconnect tests
- metadata-driven sizing tests
- effective idempotency tests
- reservation lifecycle tests
- reconciliation restart tests
- protective exit partial/rejected/full-close tests
- typed config fail-closed tests
- manual/off-bot drift tests
- end-to-end dry-run BUY/SELL tests
- Docker/runtime readiness smoke tests

Before finishing, run:
```bash
pytest
python -m compileall .
```

Also run any lint/type/import/Docker commands discovered in project config.

## Existing known stale test concern

`tests/test_execution_idempotency_extended.py` previously constructed `ExecutionService(...)` without config.

Treat this as both:
1. stale test setup, and
2. evidence that service boundaries still allow configless behavior.

Fix by making production constructors fail closed without typed config and updating tests to use minimal typed config fixtures or explicit legacy adapters.

## Live-trading gate

Live trading remains blocked unless all of the following are true:
- issues #66 through #75 are complete
- #76 or equivalent readiness/observability exists
- all must-fix items in `audit.md` are complete
- CI is green
- mock-CLOB integration tests pass
- paper-trading soak passes
- operator manually verifies collateral, negative-risk behavior, CLOB gateway readiness, config, Docker/runtime, DB backup, and rollback plan

Do not include a change that enables live trading in this remediation branch.

## GitHub issue comments

After each issue slice, comment on the related issue with:
- implementation summary
- commits pushed
- tests run
- remaining risks
- whether acceptance criteria are complete

Do not close issues until acceptance criteria are satisfied.
