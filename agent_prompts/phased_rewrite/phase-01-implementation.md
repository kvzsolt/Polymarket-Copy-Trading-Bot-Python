# Phase 1 Implementation Prompt

Repo: `/home/anon/Bots/Polymarket-Copy-Trading-Bot-Python`
Base branch: `develop`
Working branch: `phase/01-position-key-coordinator`

Implement **Phase 1 only**.

## Mission
1. Introduce canonical `PositionKey` identity:
   - `user`
   - `condition_id`
   - `token_id`
   - optional `outcome`
2. Add `PositionCoordinator` with:
   - per-position async lock
   - per-position intent record
   - intent lifecycle state machine
3. Route same-position mutation through the coordinator where practical in this phase.
4. Do **not** implement WSS, sizing changes, or reservations yet unless needed for tiny compile-safe wiring.

## Required state machine
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

## Required code shape
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

## Constraints
- Preserve existing logging behavior.
- Keep edits minimal and phase-bounded.
- Prefer additive changes over destructive rewrites.
- Use `Decimal` in any money/price/size touchpoints encountered.

## Required tests
- YES and NO positions under same condition do not collide
- same-position lock serializes concurrent operations
- intent state transitions smoke test

## Required output
- code changes
- tests passing for this slice
- short implementation summary
- git commit for the phase
