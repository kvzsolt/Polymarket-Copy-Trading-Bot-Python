# Polymarket Copy Trading Bot — Agent-Executable Phased Plan

> Base branch: `develop`
> Rule: every phase = one branch = one PR = two AI reviews = merge back to `develop`
> WSS heartbeat: **5 seconds**. If heartbeat slips, reconnect.
> Execution rule: agents must follow the phase instructions **literally**. No improvising, no scope drift.

---

## Global Execution Contract

### Branch flow
```bash
git checkout develop
git pull --ff-only

git checkout -b phase/01-position-key-coordinator
# implement only phase 1
# open PR
# get 2 AI reviews
# merge to develop

git checkout develop
git pull --ff-only
git checkout -b phase/02-price-cache-wss-heartbeat
# implement only phase 2
# open PR
# get 2 AI reviews
# merge to develop
```

### Forbidden
- Do **not** mix phase scope.
- Do **not** add “nice to have” refactors outside the phase.
- Do **not** change logging shape unless the phase explicitly says so.
- Do **not** skip tests.
- Do **not** merge without 2 AI approvals.

### Required output from every phase
1. Code changes only within that phase scope
2. Tests for the phase
3. Short implementation note
4. PR ready for 2 AI reviews

---

## Phase 1 — Canonical Position Identity + Coordinator

### Goal
Make the bot treat a Polymarket position as:
`(user, condition_id, token_id)`

### Exact work
1. Add a canonical `PositionKey` type.
2. Add a per-position lock/coordinator.
3. Add intent state transitions.
4. Route all position mutation through the coordinator.

### Exact snippets to implement

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

```python
class IntentState(Enum):
    IDLE = "idle"
    INTENT_PENDING = "intent_pending"
    SUBMITTED = "submitted"
    ACK_LIVE = "ack_live"
    PARTIAL = "partial"
    FILLED = "filled"
    CONFIRMING = "confirming"
    DONE = "done"
    ERROR = "error"
    CANCELLED = "cancelled"
```

### File touch expectations
- new position identity module
- new coordinator module
- small integration wiring from runtime entry points

### Acceptance
- YES and NO do not collide on same condition
- only one active intent per position
- coordinator controls state transitions

### Tests
- distinct YES/NO keys
- lock serialization on same position
- intent state transition smoke test

---

## Phase 2 — Position-Scoped Price Cache + WSS Runtime + 5s Heartbeat

### Goal
Stop polling the same data repeatedly and switch to stream-first data flow where available.

### Exact work
1. Add token-scoped cache for quote/tick/meta.
2. Invalidate on own fill, price change, tick size change, reconcile.
3. Add market and user WSS clients.
4. Use `PING` every **5 seconds**.
5. On reconnect: REST snapshot first, then WSS deltas.

### Exact snippets to implement

```python
async def _heartbeat(ws):
    while True:
        await asyncio.sleep(5)
        await ws.send("PING")
```

```python
def on_tick_size_change(self, token_id: str, new_tick_size: Decimal):
    self._ticks[token_id] = TickSnapshot(new_tick_size, time.monotonic())
    self._quotes.pop(token_id, None)
```

```python
await ws.send(json.dumps({
    "assets_ids": asset_ids,
    "type": "market",
    "custom_feature_enabled": True,
}))
```

### File touch expectations
- price cache module
- market websocket client
- user websocket client
- reconciler hook for WSS events

### Acceptance
- market stream subscribes with `assets_ids`
- user stream parses trade/order lifecycle
- 5s heartbeat keeps the socket alive
- reconnect resync works

### Tests
- heartbeat interval test
- reconnect/resync test
- cache invalidation test

---

## Phase 3 — Issue #38: Metadata-Driven Min Order Sizing + Effective Idempotency

### Goal
Remove hardcoded `5` and make sizing venue-driven.

### Exact work
1. Read min order metadata from venue info.
2. BUY: if below min, upsize only if headroom/balance allow.
3. SELL: never invent shares; only upsize if held supports minimum.
4. Idempotency key must use **effective adjusted** request.

### Exact snippets to implement

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

### File touch expectations
- sizing module
- order submission gate
- idempotency module

### Acceptance
- below-min BUY upsizes or skips with explicit reason
- SELL below minimum handled safely
- effective request drives idempotency

### Tests
- BUY upsizes when allowed
- BUY skips when headroom/balance insufficient
- SELL skips when held below minimum
- key changes when effective order changes

---

## Phase 4 — Engine Collision Prevention + Reservations + Reconciliation Ownership

### Goal
Make in-flight state explicit so engines stop fighting each other.

### Exact work
1. Add BUY quote reservation.
2. Add SELL share reservation.
3. Final state is written only by reconciler.
4. Block duplicate unresolved intents.

### Exact snippets to implement

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

### Acceptance
- no duplicate active intent for same position
- reservations release on cancel/fail
- reconciler owns final transition to DONE/ERROR

### Tests
- reservation release on cancel/fail
- no interleaved same-position BUY/SELL execution
- reconciler finalization test

---

## Phase 5 — Tests, Review Gates, Merge

### Goal
Prove the rewrite is safe before merge.

### Required test matrix
- PositionKey uniqueness
- cache reuse + invalidation
- WSS heartbeat 5s
- reconnect/resync
- issue #38 sizing behavior
- idempotency on effective request
- coordinator lock serialization
- reservation lifecycle

### Review gate
Every PR must have:
1. AI review A: architecture / race / state machine
2. AI review B: correctness / sizing / regressions / tests

Reviewer identity is not fixed; any 2 automatic AI agents may perform these reviews.

### Merge rule
- green tests
- 2 AI approvals
- merge to `develop`

---

## Phase Deliverables Summary

### Phase 1 delivers
- canonical position identity
- coordinator
- intent state machine

### Phase 2 delivers
- price cache
- WSS market/user streams
- 5s heartbeat

### Phase 3 delivers
- issue #38 fix
- metadata-driven min sizing
- effective idempotency

### Phase 4 delivers
- reservations
- collision prevention
- reconciler ownership

### Phase 5 delivers
- test gate
- 2-AI PR review
- merge back to develop
