# Polymarket Copy Trading Bot — Agent Prompt Pack

Use these prompts **literally**.
Base branch is always `develop`.
Each phase must be done on its own branch, with its own PR, with **2 AI reviews**, then merged back into `develop` before the next phase begins.
Reviewer identity is not pinned: any 2 automatic AI agents are acceptable, as long as they use two different review prompts for the phase.

Global hard rules for every implementation agent:
- Do not change logging shape unless explicitly required.
- Do not mix later-phase scope into the current phase.
- Do not skip tests.
- Use `Decimal` for money/price/size logic.
- WSS heartbeat is **5 seconds**, not 10.
- If the phase cannot be completed safely, stop and report exact blocker.
- After the phase is complete and validated, create a git commit for that bounded slice.

---

# PHASE 1 IMPLEMENTATION PROMPT

## Branch
`phase/01-position-key-coordinator`

## Prompt
You are implementing **Phase 1 only** of the Polymarket Copy Trading Bot rewrite in `/home/anon/Bots/Polymarket-Copy-Trading-Bot-Python`.

Base branch: `develop`
Working branch: `phase/01-position-key-coordinator`

Your mission is to implement these exact items and nothing beyond them:

1. Introduce a canonical `PositionKey` identity for positions:
   - `user`
   - `condition_id`
   - `token_id`
   - optional `outcome`
2. Add a `PositionCoordinator` with:
   - per-position async lock
   - per-position intent record
   - intent lifecycle state machine
3. Route same-position mutation through the coordinator where practical in this phase.
4. Do **not** implement WSS, sizing changes, or reservations yet unless needed for tiny compile-safe wiring.

Required state machine:
- `IDLE`
- `INTENT_PENDING`
- `SUBMITTED`
- `ACK_LIVE`
- `PARTIAL`
- `FILLED`
- `CONFIRMING`
- `DONE`
- `ERROR`
- `CANCELLED`

Required code shape:

```python
@dataclass(frozen=True, slots=True)
class PositionKey:
    user: str
    condition_id: str
    token_id: str
    outcome: str | None = None
```

```python
class PositionCoordinator:
    def __init__(self):
        self._locks: dict[PositionKey, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._intents: dict[PositionKey, IntentRecord] = defaultdict(IntentRecord)

    def acquire(self, key: PositionKey):
        return self._locks[key]
```

Constraints:
- Preserve existing logging behavior.
- Keep edits minimal and phase-bounded.
- Prefer additive changes over destructive rewrites.

Required tests:
- YES and NO positions under same condition do not collide
- same-position lock serializes concurrent operations
- intent state transitions smoke test

Required output:
- code changes
- tests passing for this slice
- short implementation summary
- git commit for the phase

---

# PHASE 1 REVIEW PROMPT A — Architecture / Race Review

Review PR for `phase/01-position-key-coordinator`.
Focus only on:
- correctness of canonical identity
- whether YES/NO collision risk is truly removed
- whether coordinator truly serializes same-position work
- state machine gaps / illegal transitions
- hidden concurrency hazards
Do not waste time on style nitpicks.
Return:
1. critical issues
2. non-critical improvements
3. merge recommendation

---

# PHASE 1 REVIEW PROMPT B — Regression / Integration Review

Review PR for `phase/01-position-key-coordinator`.
Focus only on:
- whether existing runtime paths would still work
- whether new identity/coordinator wiring could break current execution flow
- missing tests
- unsafe assumptions
Do not waste time on style nitpicks.
Return:
1. correctness risks
2. test gaps
3. merge recommendation

---

# PHASE 2 IMPLEMENTATION PROMPT

## Branch
`phase/02-price-cache-wss-heartbeat`

## Prompt
You are implementing **Phase 2 only** of the Polymarket Copy Trading Bot rewrite in `/home/anon/Bots/Polymarket-Copy-Trading-Bot-Python`.

Base branch: latest `develop` after Phase 1 merge.
Working branch: `phase/02-price-cache-wss-heartbeat`

Implement these exact items:
1. Add a position/token-scoped market data cache for quote/tick/meta.
2. Add TTL + invalidation.
3. Add market websocket client.
4. Add user websocket client.
5. Implement **5-second heartbeat ping**.
6. Implement reconnect behavior with REST snapshot bootstrap followed by WSS deltas.

Required invalidation triggers:
- own fill
- price change
- tick size change
- reconcile

Required websocket rule:
- market subscribe uses `assets_ids`
- heartbeat is every **5 seconds**
- if heartbeat/reply fails, reconnect

Required code shape:

```python
async def _heartbeat(ws):
    while True:
        await asyncio.sleep(5)
        await ws.send("PING")
```

```python
await ws.send(json.dumps({
    "assets_ids": asset_ids,
    "type": "market",
    "custom_feature_enabled": True,
}))
```

Constraints:
- Do not do Phase 3 sizing work here.
- Preserve logging shape.
- Use additive changes.

Required tests:
- cache reuse
- cache invalidation on fill/tick change
- heartbeat interval behavior
- reconnect/resync behavior
- parsing basic market/user lifecycle events

Required output:
- code changes
- tests passing for this slice
- short implementation summary
- git commit for the phase

---

# PHASE 2 REVIEW PROMPT A — WSS / Runtime Review

Review PR for `phase/02-price-cache-wss-heartbeat`.
Focus only on:
- correctness of 5-second heartbeat behavior
- reconnect/resync correctness
- websocket payload correctness (`assets_ids`, user/market handling)
- stale cache risks
- event invalidation correctness
Return:
1. critical runtime issues
2. edge cases
3. merge recommendation

---

# PHASE 2 REVIEW PROMPT B — Regression / Safety Review

Review PR for `phase/02-price-cache-wss-heartbeat`.
Focus only on:
- whether REST fallback behavior remains safe
- whether WSS integration could break dry-run or current runtime
- missing tests around disconnects and stale state
Return:
1. safety risks
2. test gaps
3. merge recommendation

---

# PHASE 3 IMPLEMENTATION PROMPT

## Branch
`phase/03-issue-38-min-order-idempotency`

## Prompt
You are implementing **Phase 3 only** of the Polymarket Copy Trading Bot rewrite in `/home/anon/Bots/Polymarket-Copy-Trading-Bot-Python`.

Base branch: latest `develop` after Phase 2 merge.
Working branch: `phase/03-issue-38-min-order-idempotency`

Implement these exact items:
1. Fix issue #38 with metadata-driven minimum order sizing.
2. Remove hardcoded global `5` logic where it wrongly acts as venue truth.
3. Read venue minimums from live metadata (`min_order_size` / `minimum_order_size` / `mos`, and tick metadata).
4. BUY below min:
   - upsize only if balance/headroom/policy allow
   - otherwise explicit skip with clear reason
5. SELL below min:
   - only upsize if held size supports minimum
   - otherwise skip safely
6. Recompute idempotency based on the **effective adjusted request**.

Required code shape:

```python
@dataclass(frozen=True)
class SizingDecision:
    accept: bool
    effective_size: Decimal
    effective_price: Decimal
    venue_min_size: Decimal
    reason: str
    upsized: bool
```

```python
def effective_intent_key(...):
    payload = "|".join([
        position.user, position.condition_id, position.token_id,
        side.upper(),
        format(effective_size, "f"),
        format(effective_price, "f"),
        format(tick_size_at_decision, "f"),
        intent_id,
    ])
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
```

Constraints:
- Do not do reservation/reconciliation ownership here unless required for tiny compatibility glue.
- Preserve logging shape; only add minimal structured reason lines if needed.
- Use `Decimal`.

Required tests:
- BUY upsizes when allowed
- BUY skips when headroom/balance insufficient
- SELL skips when held below minimum
- SELL upsizes only when held supports it
- idempotency changes when effective request changes

Required output:
- code changes
- tests passing for this slice
- short implementation summary
- git commit for the phase

---

# PHASE 3 REVIEW PROMPT A — Sizing / Venue Semantics Review

Review PR for `phase/03-issue-38-min-order-idempotency`.
Focus only on:
- whether venue metadata drives min sizing correctly
- whether BUY/SELL behavior is safe and policy-gated
- whether any hardcoded `5` venue-truth assumptions remain in the active path
- tick/precision/Decimal correctness
Return:
1. critical sizing issues
2. venue-semantic concerns
3. merge recommendation

---

# PHASE 3 REVIEW PROMPT B — Idempotency / Regression Review

Review PR for `phase/03-issue-38-min-order-idempotency`.
Focus only on:
- whether idempotency is computed from effective adjusted request
- whether retries / duplicates / stale raw-signal logic can still break execution
- missing tests for below-min paths
Return:
1. duplication/regression risks
2. test gaps
3. merge recommendation

---

# PHASE 4 IMPLEMENTATION PROMPT

## Branch
`phase/04-collision-reservations-reconciliation`

## Prompt
You are implementing **Phase 4 only** of the Polymarket Copy Trading Bot rewrite in `/home/anon/Bots/Polymarket-Copy-Trading-Bot-Python`.

Base branch: latest `develop` after Phase 3 merge.
Working branch: `phase/04-collision-reservations-reconciliation`

Implement these exact items:
1. Add BUY-side quote reservation for in-flight intents.
2. Add SELL-side token/share reservation for in-flight intents.
3. Block duplicate unresolved intents for same position.
4. Make reconciler the sole final writer for terminal state transitions and fill settlement.
5. Ensure cancel/fail paths release reservations.

Required code shape:

```python
@dataclass
class Reservation:
    quote_usdc: Decimal = Decimal(0)
    token_shares: Decimal = Decimal(0)
```

```python
async with coordinator.acquire(position_key):
    await coordinator.begin_intent(...)
    # submit order
    # wait for order/trade events
    # reconciler finalizes state
```

Constraints:
- Do not launch a giant rewrite outside the reservation/reconciliation problem.
- Preserve logging shape.
- Keep the reconciler ownership explicit.

Required tests:
- reservation release on cancel/fail
- no duplicate same-position unresolved intent
- no concurrent BUY/SELL mutation on same position
- reconciler owns final transition to DONE/ERROR

Required output:
- code changes
- tests passing for this slice
- short implementation summary
- git commit for the phase

---

# PHASE 4 REVIEW PROMPT A — Concurrency / Lifecycle Review

Review PR for `phase/04-collision-reservations-reconciliation`.
Focus only on:
- unresolved intent blocking
- reservation correctness
- lifecycle ownership
- race conditions around submit/ack/fill/confirm/cancel/fail
Return:
1. critical race/lifecycle issues
2. non-critical improvements
3. merge recommendation

---

# PHASE 4 REVIEW PROMPT B — Trading Safety Review

Review PR for `phase/04-collision-reservations-reconciliation`.
Focus only on:
- overspend / over-sell prevention
- stale reservation risk
- fill settlement correctness
- missing failure-path tests
Return:
1. safety risks
2. test gaps
3. merge recommendation

---

# PHASE 5 IMPLEMENTATION PROMPT

## Branch
`phase/05-tests-hardening-release-gate`

## Prompt
You are implementing **Phase 5 only** of the Polymarket Copy Trading Bot rewrite in `/home/anon/Bots/Polymarket-Copy-Trading-Bot-Python`.

Base branch: latest `develop` after Phase 4 merge.
Working branch: `phase/05-tests-hardening-release-gate`

Implement these exact items:
1. Close remaining high-value test gaps for phases 1-4.
2. Harden any tiny non-architectural issues found in prior reviews.
3. Prepare PR for final review and merge.

Required test matrix:
- PositionKey uniqueness
- coordinator lock serialization
- cache TTL + invalidation
- WSS heartbeat at 5s
- reconnect/resync
- issue #38 min sizing behaviors
- effective idempotency
- reservation lifecycle
- reconciler terminal ownership

Constraints:
- No new architecture detours.
- No broad cleanup spree.
- This phase is about proof and hardening.

Required output:
- final test suite updates
- tests passing
- short implementation summary
- git commit for the phase

---

# PHASE 5 REVIEW PROMPT A — Final Architecture Gate

Review PR for `phase/05-tests-hardening-release-gate`.
Focus only on:
- whether phases 1-4 are now coherently protected by tests
- whether any architectural hole remains unguarded
- whether merge to develop is justified
Return:
1. blockers
2. nice-to-have follow-ups
3. merge recommendation

---

# PHASE 5 REVIEW PROMPT B — Final Correctness Gate

Review PR for `phase/05-tests-hardening-release-gate`.
Focus only on:
- whether critical runtime regressions are covered
- whether issue #38, WSS heartbeat, idempotency, reservations, and reconciliation are all defended by tests
- whether any must-fix bug remains before merge
Return:
1. blockers
2. test gaps
3. merge recommendation

---

# FINAL MERGE CHECKLIST

Before merging any phase PR:
- branch is based on latest `develop`
- phase scope only
- tests pass
- 2 AI reviews complete
- findings triaged/fixed
- bounded slice committed

Before starting next phase:
- merge prior phase to `develop`
- pull latest `develop`
- create next phase branch fresh from `develop`
