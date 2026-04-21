# Deep Runtime Analysis — Polymarket-Copy-Trading-Bot-Python (`develop`)

## Scope and method

This document is a runtime/architecture analysis of the repository as it exists on the `develop` branch, focused on **real execution flow**, **risk-control interaction**, **re-entry churn loops**, and **production readiness**.

It is **not** a generic style review.

The analysis was built from direct inspection of the active-path files that currently shape runtime behavior:

- `app.py`
- `orchestrators/runtime_monitor.py`
- `orchestrators/runtime_bootstrap.py`
- `orchestrators/trade_pipeline.py`
- `domain/trading_context.py`
- `services/copyDecisionService.py`
- `services/executionService.py`
- `services/positionManager.py`
- `services/riskMonitor.py`
- `services/riskQuarantine.py`
- `services/reconciliation.py`
- `storage/sqlite/orders_repo.py`
- `storage/sqlite/positions_repo.py`
- `services/orderRepository.py`

The analysis deliberately assumes the current design is unsafe until proven otherwise.

---

## Executive summary

The bot already contains the **shape** of an anti-reentry mechanism, but it is implemented through the wrong abstraction and at the wrong ownership boundary.

The core design problem is this:

- the system has **no single source of truth** for the question:
  **“Can I buy this exact position again right now?”**
- instead, that answer is split across:
  - `processed_trades`
  - a synthetic quarantine helper
  - copy-decision scoring
  - execution idempotency
  - persisted positions
  - in-memory `PositionManager`
  - reconciliation
  - runtime config / ENV fallbacks

That split is why the bot can fight itself.

### Main conclusion

The current anti-reentry behavior is **real but overloaded**:

- `services/riskQuarantine.py` persists quarantine by writing a **synthetic processed-trade row** keyed only by:
  `user_id + condition_id`
- but persisted positions in `storage/sqlite/positions_repo.py` are keyed by:
  `user_id + condition_id + token_id`
- and active in-memory runtime state in `services/positionManager.py` still collapses positions to:
  `condition_id`

That means the bot’s three layers disagree on what a “position” even is.

### Why the churn loop exists

The observed bug pattern — protective SELL then normal re-BUY of the same market/outcome — is not just “one missing if statement.”

It comes from four structural faults:

1. **Re-entry policy is not first-class state**
   It is piggybacked on `processed_trades`, which is also used for dedupe/retry/execution bookkeeping.

2. **Position identity is inconsistent across layers**
   Persistence is token-aware, in-memory risk state is condition-only, quarantine is condition-only.

3. **Concurrent tasks can act on the same position without a shared late-stage serialization boundary**
   Risk monitor, leader monitor, reconciliation, and runtime rehydration all operate concurrently.

4. **Protective exit lifecycle is semantically muddy**
   Risk quarantine is marked before the protective SELL is durably resolved.

### Production-readiness verdict

**Not production-ready for unattended capital** in its current develop-branch shape.

It is good enough to show intent and partial mitigation, but not good enough to claim deterministic behavior under:

- delayed fills
- `submitted_unconfirmed` windows
- restart/recovery
- partial protective exits
- multi-loop races
- token-vs-condition identity mismatches

### Correct long-term direction

The right long-term fix is:

- **persistent, position-scoped re-entry blocks**
- **single entry-permission service**
- **token-aware canonical position identity**
- **explicit protective-exit lifecycle**
- **per-position action serialization**
- **deterministic venue/DB/runtime convergence**
- **adversarial tests and observability**

This is exactly the roadmap captured in the GitHub issue set created during the audit:

- `#23` parent roadmap
- `#24` risk-block persistence
- `#25` entry-permission service
- `#26` token-aware runtime identity
- `#27` protective-exit lifecycle
- `#28` per-position action serialization
- `#29` reconciliation/restart hardening
- `#30` config fallback cleanup
- `#31` adversarial tests + production readiness observability

---

## Actual runtime flow map

## 1. Application startup and concurrent runtime model

`app.py` is the real composition root.

At startup it:

- loads runtime env/config
- builds the application container
- builds one `UserRuntimeState` per runtime user
- creates leader-monitor contexts
- starts concurrent background loops for:
  - leader monitoring
  - risk monitoring
  - optional auto redemption
  - optional negative-risk arb
  - reconciliation

This matters because the main architecture problem is **not** isolated local logic — it is that multiple loops can mutate or reason about the same position concurrently.

### Active loops

From `app.py`, the active runtime loops are:

- leader monitor(s)
- risk monitor
- reconciliation loop(s)
- optional redemption loop
- optional arbitrage loop

This is a live multi-loop runtime, not a linear pipeline.

---

## 2. Runtime bootstrap and shared user state

`orchestrators/runtime_bootstrap.py` builds the live user state.

For each runtime user it creates:

- `OrdersRepo`
- `FillsRepo`
- `PositionsRepo`
- `PnLTracker`
- `RiskEngine`
- `AccountingService`
- `PositionManager`
- `ExecutionService`

### Critical architecture detail

It builds **one shared `PositionManager` per user runtime state**, then injects that shared in-memory manager into multiple `TradingContext`s.

That means:

- multiple leader contexts can share the same in-memory position/risk view
- while persisted positions underneath are token-aware in SQLite
- while quarantine currently lives elsewhere in processed-trade state

This is already a sign that ownership is split across incompatible layers.

---

## 3. Leader activity ingestion → pipeline entry

`orchestrators/runtime_monitor.py` runs `run_leader_monitor(...)`.

That flow is:

1. `trade_monitor(...)` observes leader activity
2. trades are queued into a worker
3. for each context, the worker calls:
   `execute_trade_pipeline(context, leader_wallet, trades, now_ts)`

This is the actual copy-trading entry path.

---

## 4. Trade pipeline — where BUY and SELL decisions are really processed

`orchestrators/trade_pipeline.py` is the active trade-execution orchestrator.

High-level flow:

1. reconcile old `submitted_unconfirmed` orders
2. normalize raw leader trades into `TradeSignal`
3. dedupe using `processed_trades`
4. check market quarantine
5. apply tier gate (`watchlist`, `paper_follow`, `live_follow`)
6. apply BUY or SELL-specific decision path
7. build `ExecutionRequest`
8. call `ExecutionService.execute(...)`
9. persist fills and update positions
10. mark processed trades as executed / skipped / failed / unconfirmed

This file is the most important active chokepoint in the system.

### Good news

There *is* already a pre-BUY quarantine check in the active path.

### Bad news

That quarantine check reads from:

- `get_market_risk_quarantine(context.orders_repo, user_id, condition_id)`

That means the anti-reentry gate is:

- backed by processed-trade state
- scoped only to condition
- external to execution’s durable permission model
- not aligned with token-aware positions

So the anti-reentry logic exists, but its identity model is too coarse and its ownership model is wrong.

---

## 5. Copy decision service

`services/copyDecisionService.py` is a **scoring and sizing gate**.

It decides:

- whether a BUY looks acceptable
- how much notional to use
- whether minimum executable thresholds are satisfied
- whether a SELL meets executable minimums

What it does **not** own:

- durable re-entry permission after a protective exit
- persistent risk-block semantics
- protective lifecycle state

This is important because it means the system already has a “decision gate,” but it is **not the same thing** as an anti-reentry policy engine.

---

## 6. Execution service

`services/executionService.py` owns:

- order submission
- submission idempotency
- pending lock acquisition
- retry behavior
- `submitted_unconfirmed` handling
- fill confirmation
- order persistence

This service is strong on **order-submission safety**.

It includes:

- idempotency keys
- atomic submission lock acquisition
- duplicate suppression
- pending submission TTL handling
- `submitted_unconfirmed` state

### But it does not own

- durable anti-reentry policy
- scoped risk blocks
- a canonical “may I buy again?” answer

So it protects against duplicate order submission far better than it protects against contradictory higher-level runtime intent.

That means the system can still avoid double-submitting the *same* order while simultaneously failing to prevent a logically contradictory BUY after a protective SELL.

---

## 7. Persisted positions

`storage/sqlite/positions_repo.py` is significantly more mature than the in-memory position model.

It persists positions keyed by:

- `user_id`
- `condition_id`
- `token_id`

It supports:

- `list_open_positions`
- `get_position(..., token_id=...)`
- `list_positions_for_condition`
- `upsert_position`
- `delete_position`
- `apply_fill`
- rebuild from fills

This is the best current candidate for the durable position identity source of truth.

---

## 8. In-memory position manager and active risk logic

`services/positionManager.py` is where the architecture starts lying to itself.

It stores:

- `_positions: dict[str, TrackedPosition]`

Keyed by:

- `condition_id`

It drives advanced risk action evaluation:

- stop-loss
- near-max exit
- profit lock
- staged TP
- trailing stop
- leader divergence

### Critical contradiction

This manager still assumes one active position per `condition_id`.

That is incompatible with:

- token-aware persisted positions
- token-aware execution requests
- token-aware sell logic in the trade pipeline

This is one of the most serious confirmed design flaws in the repo.

---

## 9. Risk monitor

`services/riskMonitor.py` is the active protective-exit subsystem.

There are two paths in the file:

- legacy compatibility path
- restart-safe context-based active path

The active restart-safe path is:

- `risk_monitor_loop_for_contexts(...)`
- `run_risk_check_for_context(...)`

That path:

1. reads persisted open positions from `PositionsRepo`
2. fetches cached/current prices
3. asks `PositionManager.evaluate_risk_action(...)`
4. if a risk action fires, it immediately calls:
   `mark_market_risk_quarantined(...)`
5. then submits a protective SELL via `ExecutionService`
6. then appends fill / updates positions / updates PnL / updates in-memory tracker

### Critical lifecycle flaw

The quarantine mark is written **before** the protective SELL has reached a durable outcome.

That means the block lifecycle currently conflates:

- “I decided to try to exit protectively”
with
- “This position is durably risk-exited and should be blocked from re-entry”

That is semantically wrong for a long-term robust design.

---

## 10. Quarantine helper

`services/riskQuarantine.py` is tiny but strategically important.

It implements quarantine as:

- a helper key: `risk_quarantine|user|condition`
- stored via `orders_repo.mark_processed_trade(...)`
- with status like `risk_quarantined_<reason>`

This is not a first-class policy system.
It is a thin alias over `processed_trades`.

That is the core anti-reentry design smell.

---

## 11. Orders repo / processed_trades

`storage/sqlite/orders_repo.py` carries too much responsibility.

It currently handles:

- order submission records
- pending locks
- retries
- unconfirmed orders
- fills
- processed trade dedupe
- synthetic quarantine state
- retention / cleanup
- atomic fill/position/trade persistence

The problem is not that it is “large.”
The problem is that `processed_trades` is serving **both**:

- execution bookkeeping
- safety-policy persistence

Those are different responsibilities and should not share the same state model.

---

## 12. Reconciliation

There are really two reconciliation-like behaviors in the active runtime.

### A. `services/reconciliation.py`

This service is intentionally conservative.

It:

- fetches exchange open orders
- checks for stale orders
- can cancel stale ones
- explicitly says it does **not** invent fills or modify position state

That makes it low-risk, but also weak as a full convergence engine.

### B. `trade_pipeline.py::reconcile_submitted_unconfirmed(...)`

This path:

- polls unconfirmed BUY/SELL orders
- confirms fills
- applies fills to local state
- updates positions
- updates PnL
- changes processed-trade state

This is much more operationally meaningful.

### Architectural implication

The repo does not have one clean, authoritative state-convergence model.
It has multiple partial ones.

---

## Confirmed conflict / loop patterns

## 1. Processed-trade-backed quarantine is the wrong persistence layer

### Confirmed

Quarantine is stored as a synthetic processed trade instead of dedicated policy state.

### Why it matters

This mixes:

- dedupe
- retry state
- execution lifecycle
- re-entry safety

### Resulting risks

- cleanup can interact with safety state
- policy semantics are hidden in order bookkeeping
- no authoritative re-entry model exists

---

## 2. Position identity mismatch across runtime layers

### Confirmed

Different subsystems use different identity models:

- `PositionsRepo`: `user + condition + token`
- `riskQuarantine`: `user + condition`
- `PositionManager`: `condition`

### Why it matters

The system cannot make precise anti-reentry decisions if it disagrees about what the position actually is.

### Consequence

A block can be too wide, too narrow, or silently mapped onto the wrong runtime object.

---

## 3. In-memory risk state is flatter than persisted position state

### Confirmed

`PositionManager` stores positions by `condition_id` only.

### Why it matters

Risk logic can evaluate and mutate a condition-scoped abstraction while execution and persistence operate on token-scoped holdings.

### Consequence

Protective exit logic can act like there is one position when persistence knows there may be multiple distinct token positions.

---

## 4. Protective block lifecycle is semantically wrong

### Confirmed

The active risk-monitor path marks risk quarantine before durable SELL completion.

### Why it matters

The system does not distinguish:

- triggered risk exit
- submitted risk exit
- partial protective exit
- completed protective exit
- failed protective exit

### Consequence

The meaning of “blocked” is unstable and can become misleading under real-world failure conditions.

---

## 5. Execution protects submission better than the architecture protects intent

### Confirmed

`ExecutionService` strongly protects against duplicate order submission but does not own anti-reentry policy.

### Why it matters

The system can still produce contradictory BUY-versus-SELL intent on the same position while avoiding technical order duplication.

### Consequence

You can have a logically insane runtime that is operationally neat at the order layer.

---

## 6. Multiple concurrent loops can reason about the same position independently

### Confirmed

The runtime concurrently runs:

- leader monitor pipeline
- risk monitor
- reconciliation
- startup / rehydration logic

### Why it matters

Without a per-position serialization boundary, different loops can each pass local checks using stale shared state.

### Consequence

Protective SELL and re-entry BUY can race.

---

## 7. Restart-safe path still rehydrates into condition-scoped memory

### Confirmed

Persisted token-aware positions are rehydrated into a condition-scoped `PositionManager`.

### Why it matters

Restart recovery can silently lose precision in active runtime identity.

### Consequence

The system can come back up already semantically wrong.

---

## 8. Reconciliation ownership is split and weakly defined

### Confirmed

- one reconciler is conservative and does not mutate positions
- another pipeline-specific reconciliation path does mutate positions

### Why it matters

There is no single durable convergence story.

### Consequence

Delayed fills, restart recovery, and venue/local drift are harder to reason about and more likely to be mishandled.

---

## Probable conflict / loop patterns

These are strongly likely based on the current active-path structure, but would benefit from runtime verification or log replay.

## 1. Protective SELL followed by fresh BUY in a narrow race window

Likely shape:

1. leader BUY arrives / buy path is evaluating
2. risk monitor fires protective SELL on same position
3. BUY path passed local checks before protective state/fill is durably visible
4. BUY executes after or around protective SELL
5. churn loop begins

This is highly plausible because there is no confirmed per-position action serialization boundary.

---

## 2. Partial protective exit with coarse quarantine semantics

Likely shape:

1. risk engine triggers partial exit
2. condition-level quarantine is written
3. system now has ambiguous semantics:
   - is whole market blocked?
   - is only remaining position blocked?
   - is re-entry blocked even though some exposure remains?

Because the current block model is condition-scoped and not first-class, partial exits are especially dangerous.

---

## 3. Processed-trade retention/cleanup interacting with policy state

`processed_trades` has retention cleanup.

Even if today’s exact cleanup path does not remove quarantine rows immediately, the architecture is still dangerous because safety state is stored in a table that is conceptually owned by execution bookkeeping.

This is a future regression trap.

---

## 4. Submitted-unconfirmed protective exits leaving ambiguous local permission state

Protective SELL can become `submitted_unconfirmed`.

Likely problems:

- risk block may already exist
- actual venue fill may still be unknown
- re-entry decision may treat the position as exited or blocked without robust lifecycle semantics

This is an especially ugly live-trading edge case.

---

## 5. Multi-leader contention on one user runtime

Because one user runtime shares in-memory state across multiple leader contexts, there is strong potential for one leader’s action to affect another leader’s interpretation of a condition-scoped in-memory position.

Even if intended at portfolio level, the code should make that explicit, not accidental.

---

## Root causes

## 1. No single source of truth for re-entry permission

This is the number one root cause.

The repo does not currently have one authoritative object/table/service that answers:

**“May this scoped position be re-entered?”**

Instead the answer is distributed across unrelated subsystems.

---

## 2. Split identity model

The repo cannot be stable while these remain different:

- persisted identity
- in-memory risk identity
- quarantine identity
- execution identity

The bot is literally reasoning about different objects in different places.

---

## 3. Responsibility duplication and missing ownership boundaries

Examples:

- `processed_trades` acts like both execution state and policy state
- reconciliation is split across modules
- entry gating exists, but re-entry gating is not unified with it
- risk monitor triggers blocks but does not own the full block lifecycle

---

## 4. Lifecycle ambiguity

A protective trigger is not the same thing as a completed protective exit.

The current design blurs those states.

---

## 5. Runtime concurrency without position-scoped coordination

There is not yet a confirmed late-stage coordinator for contradictory actions on the same scoped position.

---

## 6. Config-path dishonesty

Some active services can still fall back to ENV/default behavior instead of consuming a single injected config contract.

That is dangerous because the operator may think the runtime is enforcing one policy while a fallback path quietly enforces another.

---

## Where the architecture is lying to itself

This is the blunt version.

## Lie 1: “Quarantine exists, therefore re-entry is solved.”

False.

What exists is a synthetic processed-trade row keyed at condition scope.
That is not a robust first-class re-entry model.

---

## Lie 2: “The position model is consistent.”

False.

Persistence, memory, and quarantine all use different granularities.

---

## Lie 3: “Execution safety implies strategy safety.”

False.

Idempotent order submission does not prevent contradictory higher-level runtime decisions.

---

## Lie 4: “Reconciliation exists, so drift is under control.”

False.

The repo has partial reconciliation behaviors, not a single coherent convergence architecture.

---

## Lie 5: “Config is centralized.”

Not really.

The active runtime still has fallback/legacy paths that can undermine deterministic behavior.

---

## Lie 6: “Restart-safe means semantically safe.”

False.

Rehydrating token-aware persistence into condition-scoped memory is operational restart support, not semantic correctness.

---

## Recommended anti-reentry design

## Core principle

Do **not** solve this with a global trader kill switch.

The correct policy is:

> If a protective risk action fires for a specific scoped position, the bot may continue trading everything else, but it must not re-enter that same scoped position until explicit clear conditions are met.

---

## Best-fit granularity for this repo

### Recommended scope

Use **position-side scope**:

- `user_id`
- `condition_id`
- `token_id`

Optional later extension:

- `leader_id` if the repo explicitly wants leader-isolated policy

### Why not condition-only?

Because the repo already persists positions token-aware.
Condition-only is too coarse and directly conflicts with active persistence.

### Why not global?

Because the design goal explicitly rejects global trader shutdown, and the repo can clearly support more localized controls.

---

## Recommended model: `RiskBlock` / `ReentryBlock`

Create a first-class persistent object/table.

Suggested fields:

- `user_id`
- `condition_id`
- `token_id`
- `leader_anchor` (nullable)
- `block_reason`
- `trigger_source`
- `triggered_at`
- `effective_at`
- `expires_at`
- `cleared_at`
- `clear_reason`
- `metadata_json`

Optional helpful flags:

- `requires_fresh_leader_buy`
- `requires_regime_reset`
- `cooldown_seconds`
- `protective_exit_order_id`
- `protective_exit_state`

---

## Recommended gate: single entry-permission service

All BUY paths should consult one service immediately before BUY execution request creation.

That service should answer with structured results such as:

- `allow`
- `deny_risk_block_active`
- `deny_waiting_for_fresh_leader_buy`
- `deny_cooldown_active`
- `deny_waiting_for_regime_reset`
- `deny_position_state_unknown`

This should become the only authoritative answer to:

**“May I buy this again?”**

---

## Recommended clear rules

Best-fit clear rules for this repo:

### 1. Base hard rule

After a protective risk exit, block re-entry for that exact scoped position.

### 2. Minimum clear condition

Require both:

- cooldown elapsed
- fresh leader BUY after the protective exit timestamp

### 3. Optional stronger rule

Also require the fresh leader buy to be **meaningful**, for example:

- new tx hash after exit
- not the same stale signal
- not just local replay / delayed duplicate

### 4. Optional regime reset rule

Add later if needed:

- price moved sufficiently away and back
- market regime reset condition satisfied

### 5. Manual/operator clear

Needed for operational recovery.

---

## Why this design fits the repo best

Because the repo already has:

- token-aware persisted positions
- a trade-pipeline BUY chokepoint
- risk monitor as protective-exit origin
- execution service as order-submission chokepoint

So this is a strong **architectural correction**, not a giant rewrite.

---

## Design options evaluated

## 1. Market-level quarantine (`condition_id` only)

### Pros

- simple
- already partially present
- easy to reason about at broad level

### Cons

- too coarse for current persistence model
- can over-block unrelated token-side exposure
- not aligned with actual held identity

### Failure modes

- blocks too much
- still ambiguous under partial exits
- can hide token-specific bugs instead of fixing them

### Verdict

Not the best long-term fit.

---

## 2. Position-side quarantine (`user + condition + token`)

### Pros

- matches persisted positions
- precise
- directly solves the observed bug at correct scope
- allows other markets/positions to continue trading

### Cons

- requires identity cleanup in runtime memory
- requires explicit block store

### Failure modes

- can still fail if in-memory position identity remains condition-only

### Verdict

**Best fit for this repo.**

---

## 3. Condition-level quarantine with leader-specific overlays

### Pros

- flexible for leader-specific strategy semantics

### Cons

- more complex than necessary for first long-term fix
- can distract from core identity mismatch

### Verdict

Possible later extension, not the first move.

---

## 4. Cooldown-only block

### Pros

- easy to implement
- cheap operationally

### Cons

- brittle
- cooldown expiry alone does not guarantee meaningful state change
- can re-enter too early after noisy churn

### Verdict

Useful as one component, not sufficient by itself.

---

## 5. “Do not re-enter until leader meaningfully changes position”

### Pros

- aligns local re-entry with external causal change
- directly addresses copy-trading re-entry loops

### Cons

- needs clear definition of “meaningfully changes”
- requires durable leader-anchor logic

### Verdict

Strong design component.

---

## 6. “Do not re-enter until fresh leader BUY after the protective exit”

### Pros

- clean and concrete
- robust against replay of stale leader signals
- fits copy-trading causality

### Cons

- still benefits from cooldown and durable block lifecycle

### Verdict

**Recommended as part of the long-term default policy.**

---

## 7. In-memory only block

### Pros

- simple
- fast

### Cons

- restart unsafe
- useless for robust unattended runtime

### Verdict

Not acceptable for production.

---

## 8. Persistent DB block

### Pros

- restart safe
- observable
- explicit
- testable

### Cons

- requires schema + repository + lifecycle decisions

### Verdict

**Mandatory** for production-grade solution.

---

## 9. Explicit `risk_exit_reason` attached to position/block

### Pros

- makes operator reasoning much better
- supports differentiated clear logic
- improves observability and auditability

### Cons

- small complexity increase

### Verdict

Strongly recommended.

---

## 10. Require recovery/reset signal before re-entry

### Pros

- robust against immediate churn
- aligns with real market recovery

### Cons

- can be over-complex if done too early

### Verdict

Useful as optional second-stage sophistication.

---

## Minimum safe patch vs medium-term vs ideal architecture

The user requested a long-term solution rather than a surgical patch, but it is still useful to separate the layers.

## A. Minimum safe patch

This is not the main recommendation, but for completeness:

- keep current pipeline structure
- replace condition-only quarantine with `user + condition + token`
- store it persistently outside `processed_trades`
- make BUY path consult that block
- do not clear it automatically except by explicit policy

This would materially reduce the churn loop risk without a major rewrite.

### Limitation

It still leaves the split identity model and runtime race issues partially unresolved.

---

## B. Better medium-term refactor

This is the recommended implementation path.

### Step 1 — dedicated persistent risk blocks

Move re-entry blocks out of `processed_trades`.

### Step 2 — single entry-permission service

Make BUY permission flow through one authoritative gate.

### Step 3 — token-aware runtime identity

Refactor `PositionManager` and rehydration to stop using condition-only active identity.

### Step 4 — protective-exit lifecycle cleanup

Only activate durable blocks from explicitly defined lifecycle states.

### Step 5 — per-position action serialization

Prevent contradictory BUY/SELL intent on the same scoped position.

### Step 6 — reconciliation / restart convergence hardening

Unify convergence ownership and protect state across delayed fills and restarts.

### Step 7 — config cleanup

Remove active fallback dishonesty.

### Step 8 — adversarial tests and observability

Prove the failure shape is actually fixed.

---

## C. Ideal architecture

### Canonical state owners

#### Position state owner

- persisted token-aware positions repo

#### Re-entry policy owner

- dedicated `RiskBlock` repository + `EntryPermissionService`

#### Protective-exit lifecycle owner

- risk monitor / risk-exit coordinator

#### Execution owner

- `ExecutionService`

#### Convergence owner

- one coherent reconciliation + restart convergence model

#### Coordination owner

- per-position action coordinator / lock

### Core rule

No BUY reaches execution unless the entry-permission service says yes for the exact scoped position.

### Core invariant

Every subsystem uses the same scoped identity.

---

## GitHub roadmap created from this analysis

The analysis was turned into a long-term issue plan on the repo.

## Parent roadmap

- `#23` Architecture roadmap: eliminate re-entry loops and unify runtime state ownership

## Child issues

1. `#24` Replace processed-trade-backed quarantine with dedicated persistent position risk blocks
2. `#25` Introduce a single entry-permission service with explicit anti-reentry clear rules
3. `#26` Refactor runtime position identity to token-aware ownership across PositionManager and active contexts
4. `#27` Refactor protective-exit lifecycle so risk blocks activate on durable exit state, not pre-emptively
5. `#28` Serialize per-position actions to prevent risk-exit and re-entry races across concurrent runtime tasks
6. `#29` Harden reconciliation and restart convergence so venue, DB, and runtime state agree deterministically
7. `#30` Remove active runtime config fallback dishonesty and force injected configuration through live trading paths
8. `#31` Add adversarial runtime tests and production-readiness observability for re-entry safety and state convergence

Because GitHub sub-issues were not enabled for the repo, the roadmap was linked through:

- ordered parent comment on `#23`
- back-link comments on `#24` through `#31`

---

## Concrete production-readiness assessment

## What is already decent

- execution-side idempotency is better than average
- persisted positions are token-aware
- there is already an active quarantine check in the BUY path
- there is already explicit protective-exit logic
- there is already `submitted_unconfirmed` reconciliation behavior

## What is still unacceptable for unattended capital

- split position identity model
- non-first-class re-entry policy state
- policy state hidden in execution bookkeeping
- pre-emptive quarantine semantics
- no per-position action serialization
- weakly unified convergence model
- active fallback config ambiguity
- insufficient adversarial verification

## Final verdict

### For hobby / supervised use
Potentially workable with caution.

### For live unattended capital
**Not yet production-ready.**

The architecture still contains enough contradictions that a real-world failure window can produce behavior the operator did not intend.

---

## Open questions / runtime checks still worth verifying

Some things are already confirmed in code. Others would still benefit from runtime log validation.

## 1. Exact observed churn-loop sequence

Need a real trace of:

- protective SELL trigger
- quarantine mark time
- SELL submission outcome
- BUY signal arrival time
- BUY skip/allow decision
- processed-trade state at each point

This would prove the exact race/lifecycle shape of the observed bug in production.

---

## 2. Partial protective exits in practice

Need runtime evidence showing how partial exits interact with:

- remaining persisted position
- in-memory `PositionManager`
- current condition-level quarantine semantics

---

## 3. Multi-token same-condition reality in live flow

The current repo architecture strongly suggests token-aware persistence is necessary, but production logs would confirm how often same-condition token ambiguity actually appears in practice.

---

## 4. Interaction between old `processed_trades` cleanup and synthetic quarantine rows

Even if the design is already wrong in principle, runtime validation would help identify whether this is merely a latent hazard or an already-active hazard.

---

## 5. Whether leader-level isolation is desired in re-entry policy

Long-term recommendation currently assumes portfolio-local protection is primary and therefore the first block scope should be:

- `user + condition + token`

But if the product intent is explicitly “one leader’s protective exit should not block another leader’s fresh thesis on the same token,” then `leader_anchor` should become part of the policy semantics.

That is a product/strategy choice, not just a coding decision.

---

## Bottom line

The repo is closest to safety when you describe it this way:

- execution is somewhat disciplined
- persistence is more precise than memory
- risk logic is more ambitious than its state model can safely support
- anti-reentry exists in spirit, but not yet in a trustworthy architecture

The durable fix is not a giant rewrite.
The durable fix is to stop lying about ownership:

- make re-entry permission first-class
- make position identity consistent
- make protective lifecycle explicit
- serialize contradictory actions
- harden convergence
- prove it under ugly tests

That is the path from “clever bot with safety intentions” to “runtime that can actually be trusted.”
