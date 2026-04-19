# Benchmark: Copy-Trader Execution Lifecycle Hardening

This benchmark must be executed independently by two models on separate branches.
The goal is to compare planning quality, implementation quality, regression discipline, and repair ability on the same real bug family.

## Purpose

We observed a real execution bug family in the copy trader:

- a SELL order can be accepted by CLOB with `success: true` and `status: delayed`
- before reconciliation confirms the venue-side result, the bot can still attempt additional SELLs for the same position
- this creates duplicate / zombie follow-up attempts, misleading local state, minimum-size rejects, and venue-side `insufficient_balance` rejects
- the same architectural weakness can also affect BUY flows through in-flight duplication, reserved-balance misuse, and ambiguous submit handling

This benchmark is meant to test whether a model can:

1. identify the real failure mode correctly
2. propose an elegant long-term architecture instead of a patchy local fix
3. implement the foundation cleanly
4. preserve behavior with tests
5. recover well from failing tests and runtime edge cases

---

## Branch Rules

Each model gets its own branch.

Suggested branch names:

- `benchmark/gpt54-execution-lifecycle`
- `benchmark/opus47-execution-lifecycle`

Rules:

- no shared branch
- no cherry-picking from the other branch during benchmark execution
- no looking at the other branch's changes before both runs are complete
- both branches must start from the same base commit

---

## Benchmark Scope

Repo:

- `Polymarket-Copy-Trading-Bot-Python`

Primary problem domain:

- copy-trader execution lifecycle
- duplicate in-flight order prevention
- pending confirmation handling
- BUY and SELL reservation logic
- reconciliation-driven truth

The benchmark is intentionally focused on architecture and execution correctness, not cosmetic refactors.

---

## Required Deliverables

Each model must produce all of the following.

### 1. Paper Design

Create a short design note in the branch, for example:

- `docs/benchmark-execution-lifecycle-plan.md`

It must include:

- root cause analysis of the observed bug
- explicit distinction between venue error vs local lifecycle bug
- proposed order lifecycle / state machine
- BUY-side strategy
- SELL-side strategy
- reserved balance / reserved size strategy
- reconciliation strategy
- retry vs non-retryable rejection policy
- phased implementation plan
- explicit invariants

This is required before or alongside implementation.

### 2. Minimal-but-real Foundation Implementation

The model must implement the smallest useful slice of the long-term architecture, not just patch one line.

The expected minimum foundation is:

- in-flight dedup / locking for relevant position-action paths
- explicit pending-confirmation state handling
- prevention of duplicate SELL attempts while an exit is already in flight
- prevention of duplicate BUY attempts where appropriate
- reserved-aware handling for available balance / size decisions, or a cleanly structured placeholder abstraction for it
- clear classification of retryable vs permanent execution failures
- reconciliation integration or preparation hooks for truth resolution

This does **not** require the full final architecture in one shot, but it **does** require a clean foundation that future phases can build on.

### 3. Tests

The branch must add or update tests that cover at least:

- duplicate SELL suppressed after accepted-but-unconfirmed submit
- duplicate BUY suppressed in equivalent in-flight state
- permanent rejections are not blindly retried
- pending-confirmation state eventually resolves through reconciliation or timeout handling
- reserved / available calculations do not allow obviously duplicated spending or selling

### 4. Benchmark Notes

Add a final short note file, for example:

- `docs/benchmark-execution-lifecycle-results.md`

It must summarize:

- what was implemented
- what was intentionally deferred
- what tests were run
- what still feels risky

---

## Execution Phases

Both models must follow the same phase order.

### Phase A: Analyze

Inputs:

- the relevant codebase
- the observed runtime logs
- the known duplicate SELL scenario
- the known `submitted_unconfirmed` / delayed-order problem

Expected output:

- written root cause analysis
- clean problem framing

### Phase B: Design

Expected output:

- the paper design note
- explicit state machine and invariants
- specific implementation boundary for the first slice

### Phase C: Implement

Expected output:

- code changes for the selected foundation slice
- tests for the new behavior

### Phase D: Repair

If tests fail or runtime issues appear:

- fix them in the same branch
- do not abandon the architecture and panic-patch random lines

### Phase E: Summarize

Expected output:

- benchmark results note
- concise explanation of what remains for future phases

---

## Suggested Real-World Scenario to Anchor Against

The benchmark should explicitly account for this class of event:

1. leader emits SELL
2. bot computes proportional SELL size
3. venue accepts order with success and delayed / unconfirmed semantics
4. local reconciliation has not yet resolved the order
5. bot receives another trigger path touching the same position
6. bot must **not** attempt another SELL for already-reserved size
7. reconciliation later confirms fill / open / stale / reject
8. local position and reserved state must resolve cleanly

The BUY-side equivalent should also be considered:

1. leader emits BUY
2. bot decides to enter
3. venue response is ambiguous or delayed
4. bot must not double-spend available budget on the same unresolved action

---

## Hard Constraints

Both models must respect these constraints.

- no fake “confirmed” status just because submit returned success
- no blocking the main trade pipeline for a long synchronous confirmation wait
- no duplicate SELL or BUY while equivalent action is already in flight for the same position / market intent
- no silent swallowing of ambiguous states
- no architecture that depends on vibes instead of explicit state
- no regression that re-couples unrelated exit engines under a misleading switch
- no giant speculative rewrite far beyond the scope of the benchmark

---

## Evaluation Criteria

Score each branch on a 1-10 scale per category.

### 1. Root Cause Accuracy

Did the model correctly identify:

- accepted-first-sell followed by duplicate local attempts
- venue reject as downstream symptom, not the only bug
- reconciliation gap / pending lifecycle gap
- BUY-side symmetry, not just SELL-side patching

### 2. Architecture Quality

Does the design show:

- clear state machine
- explicit ownership of truth
- reserved balance / size reasoning
- clean separation of decision vs execution vs reconciliation
- future-proofing without overengineering

### 3. Implementation Quality

Does the implementation:

- actually enforce the intended behavior
- avoid duplicate in-flight actions
- classify failures cleanly
- keep code understandable
- avoid random hidden coupling

### 4. Test Quality

Do tests:

- target the real bug family
- cover positive and negative cases
- verify state transitions and dedup behavior
- reduce the chance of regressions

### 5. Repair Ability

When failing tests or runtime contradictions appear, does the model:

- repair coherently
- keep the architecture intact
- avoid thrashing
- converge quickly

### 6. Scope Discipline

Did the model:

- solve the benchmark problem
- avoid unrelated side quests
- defer later phases cleanly instead of half-implementing everything

### 7. Production Readiness Signal

Does the branch feel like:

- a trustworthy base for the next phase
- or a demo patch that will rot instantly

---

## Suggested Scoring Sheet

Use a table like this after both runs are complete.

| Category | GPT-5.4 Branch | Opus 4.7 Branch | Notes |
|---|---:|---:|---|
| Root Cause Accuracy |  |  |  |
| Architecture Quality |  |  |  |
| Implementation Quality |  |  |  |
| Test Quality |  |  |  |
| Repair Ability |  |  |  |
| Scope Discipline |  |  |  |
| Production Readiness Signal |  |  |  |
| **Total** |  |  |  |

---

## Nice-to-Have Comparison Metrics

These are not mandatory, but helpful.

- number of files changed
- number of tests added
- number of failing-test repair cycles needed
- number of regressions introduced during implementation
- amount of architecture that remained after repairs
- subjective bullshit ratio

---

## Benchmark Prompt Skeleton

Use the same benchmark brief for both models, adjusted only for runtime/tool syntax.

Required briefing content:

- explain the observed duplicate-order bug family
- include the relevant runtime log excerpts
- require a paper design note first
- require a clean foundation implementation, not a one-line hack
- require targeted tests
- require a final result summary
- forbid looking at the competing branch

---

## Pass / Fail Gate

A branch should be considered a serious contender only if all of the following are true:

- design note exists and is coherent
- implementation enforces in-flight dedup for the targeted scenario
- tests cover the core duplicate-order bug family
- no obvious regression in existing execution behavior
- final note clearly explains what is done vs deferred

If any of those fail, the branch may still be informative, but it should not win the benchmark.

---

## Final Reminder

This benchmark is not about pretty prose.
It is about whether the model can turn messy real trading-bot behavior into:

- explicit state
- enforceable invariants
- safe execution behavior
- reconcilable truth
- test-backed progress

If a branch cannot do that, it should lose, even if the writeup sounds smart.
