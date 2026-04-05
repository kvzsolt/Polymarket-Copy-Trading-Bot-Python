# Implementation Plan for the Python Repo

Based on:
- the rewritten research direction
- what’s already landed
- and **not** rebuilding bullshit that’s already done 😈🔥

Repo:
`/home/anon/Bots/Polymarket-Copy-Trading-Bot-Python`

---

# Goal

Turn the Python copy-trading bot from:

- decent strategy logic
- partially hardened execution
- research ideas scattered in notes

into:

- **honest leader scoring**
- **copyability-aware follower decisions**
- **safe live/paper promotion pipeline**

Without fake bankroll cosplay, fake Sharpe theater, or midpoint hallucination.

---

# Current state summary

## Already done
- decision brain ported ✅
- leader scoring service wired per batch ✅
- TS sizing logic ported ✅
- idempotency / partial-fill handling fixed ✅
- structured logging restored ✅
- a bunch of headroom/risk work already exists in legacy + partial active pipeline ✅
- tests green ✅

## Still missing / weak
- **book quality gate**
- **exchange-side reconciliation**
- some **balance/headroom regime wiring** in the active path
- no proper **copyability-first research store**
- no honest **leader evaluation pipeline** based on delay/slippage/depth
- too much historical temptation to infer garbage state from incomplete data

---

# Guiding principles

## Build this
- ledger-first
- snapshot-reconciled
- context-specific leader scoring
- signal-quality-first
- copyability-gated
- confidence-shrunk
- watchlist → paper-follow → live-follow ladder

## Do NOT build
- inferred bankroll
- fake conviction from trade size
- midpoint-only execution assumptions
- guessed sell fractions / guessed inventory
- global “top trader” vanity leaderboard
- fake Sharpe stack from fantasy equity curves

---

# Phased implementation roadmap

---

## Phase 0 — Lock down scope and stop future bullshit
### Goal
Freeze the architecture direction so no one reintroduces fake-capital junk.

### Tasks
1. Add a short architecture note in repo docs:
   - no bankroll inference
   - no midpoint-as-fill
   - no guessed inventory
   - leader ranking must optimize **future follower alpha**, not leader vanity PnL

2. Define the three scoring layers explicitly:
   - **research score**
   - **operational trust**
   - **live permission**

3. Define context buckets:
   - category
   - liquidity bucket
   - spread bucket
   - time-to-resolution bucket
   - maybe trade-size bucket

### Deliverables
- `docs/copy_leader_scoring.md`
- clear invariants for research + production

### Success criteria
- repo has one canonical scoring philosophy
- no more accidental reversion to fake metrics

---

## Phase 1 — Build the honest research data spine
### Goal
Create the minimum durable data store needed to score traders without lying.

### Build first
### 1. Trade ledger ingestion
Store observed leader trades with:
- trader
- timestamp
- tx hash
- conditionId
- asset/token id
- outcome / side
- price
- shares
- usdc size
- slug/title helpers

### 2. Market metadata store
Store:
- conditionId
- yes/no token ids
- category
- endDate
- resolved/open flags
- negative risk flags
- event/market slugs

### 3. Position snapshot ingestion
From current + closed positions:
- current size
- avgPrice
- currentValue
- curPrice
- realizedPnl
- closed position summaries

### 4. Orderbook snapshot capture
This is one of the most important pieces.
Capture near leader trades:
- best bid
- best ask
- midpoint
- spread
- shallow depth ladder
- timestamp aligned to trade

### 5. Short price-history capture
Enough to compute drift windows after leader trades:
- 1m
- 5m
- 30m
- 4h
- maybe 24h / to-resolution later

### Suggested module layout
```text
ingestion/
  activity_ingestor.py
  positions_snapshot_ingestor.py
  closed_positions_ingestor.py
  market_catalog_ingestor.py
  orderbook_snapshot_worker.py
  price_history_worker.py
```

### Suggested tables / entities
Start with only:
- `traders`
- `markets`
- `trades`
- `position_snapshots`
- `closed_position_snapshots`
- `orderbook_snapshots`
- `price_history_points`

### Success criteria
- for any observed leader trade, we can recover:
  - who traded
  - what they traded
  - roughly what book looked like
  - what market context it lived in

### Why this phase matters
Without this, everything after it is just **expensive astrology**.

---

## Phase 2 — Inventory reconstruction + confidence system
### Goal
Reconstruct enough position state to score behavior honestly, while explicitly marking ambiguity.

### Tasks
### 1. Inventory rebuilder
Maintain per `(trader, asset)`:
- signed inventory
- average cost
- realized PnL
- open lots / episode links

Use:
- average-cost accounting in production
- optional FIFO/LIFO only for research comparisons

### 2. Episode builder
Cluster trades into episodes:
- initial entry
- adds
- trims
- close
- unresolved carry

### 3. Reconciliation layer
Use position snapshots to validate/reconcile:
- reconstructed size
- known size
- known current value
- closed position transitions

### 4. Confidence scoring
Per trader / asset / episode:
- full confidence
- partial confidence
- oversell_or_missing_history
- sparse snapshot confidence
- reconstruction confidence score

### Rules
- if history is incomplete, **downgrade confidence**
- do not invent inventory
- do not infer bankroll
- do not guess sell-side ownership from arbitrary fractions

### Suggested modules
```text
reconstruction/
  inventory_rebuilder.py
  episode_builder.py
  reconciliation.py
  confidence.py
```

### Success criteria
- we can tell which leader records are:
  - usable
  - partially usable
  - contaminated
- fragile metrics can be disabled automatically on low-confidence records

### Why this phase matters
This is where fake leader-scoring systems usually become total bullshit.

---

## Phase 3 — MVP score families that actually matter
### Goal
Build the **smallest score stack that predicts follower alpha**, not self-congratulatory PnL.

## Build these first
### 1. Signal quality
Metrics:
- `entry_alpha_5m`
- `entry_alpha_30m`
- `entry_alpha_4h`
- `earlyness`
- maybe simple `exit_quality` later

Question answered:
> did the leader enter before repricing?

### 2. Copyability
Metrics:
- `delay_survival_10s`
- `delay_survival_30s`
- expected follower slippage
- remaining depth / depth availability
- spread penalty
- leader-impact penalty if feasible

Question answered:
> can we still make money after realistic delay and fill cost?

### 3. Confidence / robustness
Metrics:
- episode count
- distinct markets
- distinct active days
- reconstruction confidence
- concentration penalty

Question answered:
> is this score trustworthy, or just one lucky event and a fever dream?

### Scoring formula: MVP
Use a brutally simple one first:

```text
final_score
= hard_gate
* confidence
* (
    0.45 * signal_score
  + 0.40 * copyability_score
  + 0.15 * robustness_score
)
```

Hard gate = 0 if:
- too few episodes
- confidence too low
- slippage too high
- delay survival too weak
- context disallowed

### Suggested modules
```text
features/
  entry_alpha.py
  earlyness.py
  delay_survival.py
  slippage.py
  robustness.py

scoring/
  family_scores.py
  context_scores.py
```

### Success criteria
- we can rank leaders **per context**
- high-ranked leaders are those whose edge survives follower reality
- no fake bankroll / fake Sharpe dependence anywhere

---

## Phase 4 — Context buckets + promotion ladder
### Goal
Stop using one dumb universal ranking and start using context-specific permissions.

### Tasks
### 1. Context bucketing
At minimum:
- category
- spread bucket
- liquidity bucket
- days-to-resolution bucket

### 2. Promotion ladder
States:
- `watchlist`
- `paper_follow`
- `live_follow`

### 3. Promotion rules
#### Watchlist
Allowed if:
- enough activity
- promising signal quality
- copyability not yet proven

#### Paper-follow
Allowed if:
- positive entry alpha
- acceptable delay survival
- enough confidence
- no severe crowding

#### Live-follow
Allowed if:
- positive **follower** alpha after slippage
- stable enough over time
- risk behavior acceptable
- context permissions defined

### 4. Demotion rules
Demote if:
- follower alpha decays
- slippage rises
- crowding spikes
- leader drifts outside validated contexts
- reconstruction confidence degrades

### Suggested modules
```text
scoring/
  promotion_rules.py
  demotion_rules.py

live/
  leader_registry.py
  eligibility_service.py
  copy_permission_service.py
```

### Success criteria
- no one gets copied live just because they looked rich in a spreadsheet
- permissions become **context-specific**, not universal hero worship

---

## Phase 5 — Wire scoring into live decision pipeline
### Goal
Use the research layer to control actual copy behavior.

### Tasks
When a leader trade arrives:

1. identify context bucket
2. fetch latest leader permission
3. fetch current book snapshot
4. recompute real-time copyability
5. block if:
   - slippage too high
   - delay survival too weak
   - context disallowed
   - confidence low
6. if allowed:
   - apply size multiplier
   - pass into existing risk/execution pipeline

### Size multiplier
Do **not** use inferred bankroll.
Use:
- base sizing from existing strategy
- multiply by:
  - signal score factor
  - copyability factor
  - confidence factor
  - context permission factor

### Suggested modules
```text
live/
  leader_size_multiplier.py
  real_time_copyability.py
```

### Success criteria
- live decisions are gated by actual research
- follower execution respects market reality
- “leader looked good” is no longer enough

---

## Phase 6 — Finish the existing production gaps in parallel
These are still important even if the research layer improves.

### 1. Book quality gate
This should be finished early because it protects both:
- current bot logic
- future copyability logic

Rules to include:
- skip wide/phantom books
- require minimum real depth
- reject clearly non-copyable thin levels

### 2. Exchange-side reconciliation
Absolutely needed.
Make sure local state matches venue state:
- open orders
- fills
- partial fills
- terminal transitions
- missing / ambiguous venue outcomes

### 3. Balance/headroom final wiring
Make sure the active path uses the real regime/headroom logic cleanly.

### Success criteria
- bot no longer acts on fantasy inventory / fantasy venue state
- execution path is robust enough for research-informed live following

---

# Concrete 6-week MVP plan

## Week 1
- architecture doc
- schema/entities
- trade ingestion
- market metadata ingestion

## Week 2
- current/closed position snapshot ingestion
- orderbook snapshot capture
- short price-history capture

## Week 3
- inventory rebuilder
- episode builder
- reconstruction confidence system

## Week 4
- entry alpha
- earlyness
- delay survival
- slippage metrics

## Week 5
- context scores
- watchlist/paper/live promotion rules
- leader registry + permissions

## Week 6
- wire into live copy-decision path
- add book quality gate where still missing
- run paper-follow validation

---

# Priority order inside the repo

## Highest priority
1. **orderbook snapshot capture**
2. **inventory reconstruction + confidence**
3. **delay survival + slippage**
4. **context-specific scoring**
5. **live permission gating**

## Medium priority
6. book quality gate completion
7. exchange-side reconciliation
8. exit-quality metrics
9. crowding penalty
10. richer regime robustness

## Defer for now
- ML ranking models
- fancy dashboards
- Sharpe / portfolio analytics theater
- bankroll inference of any kind
- overengineered category-specialist submodels

---

# Suggested acceptance criteria per phase

## Phase 1 accepted when:
- every observed leader trade can be linked to market + asset + timestamped book context

## Phase 2 accepted when:
- every reconstructed episode has a confidence score
- ambiguous sells are flagged, not guessed

## Phase 3 accepted when:
- leader ranking works off signal + copyability + confidence
- no score depends on inferred bankroll

## Phase 4 accepted when:
- leaders can be promoted/demoted by context
- watchlist/paper/live statuses are real and persisted

## Phase 5 accepted when:
- live copy decisions check permission + slippage + delay survival before firing

## Phase 6 accepted when:
- venue reconciliation and book-quality gates stop obvious garbage trades

---

# What this plan intentionally does NOT try to solve yet
- perfect historical truth
- exact full wallet state
- off-platform hedges
- multi-wallet identity clustering
- perfect causal attribution of news

That’s fine.
We do **honest partial truth**, not fake omniscience.

---

# Final recommendation
If we’re serious, I’d attack this in this order:

### Immediate build order
1. **orderbook snapshots around leader trades**
2. **inventory reconstruction + confidence**
3. **delay survival / slippage metrics**
4. **context-specific leader scoring**
5. **paper-follow ladder**
6. **wire into live permissions**
7. **finish book quality + reconciliation gaps**

That gets us closest to a copy bot that’s:
- not lying to itself
- not copying uncopiable leaders
- and not torching money on fake edge

If you want, I can do the next step too:
### turn this into a **repo-native task list**
with exact files/modules to create in `Polymarket-Copy-Trading-Bot-Python` and what each one should own.
