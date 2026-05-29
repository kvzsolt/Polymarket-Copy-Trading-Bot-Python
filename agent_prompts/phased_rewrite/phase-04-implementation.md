# Phase 4 Implementation Prompt

Repo: `/home/anon/Bots/Polymarket-Copy-Trading-Bot-Python`
Base branch: latest `develop` after Phase 3 merge
Working branch: `phase/04-collision-reservations-reconciliation`

Implement **Phase 4 only**.

## Mission
1. Add BUY-side quote reservation for in-flight intents.
2. Add SELL-side token/share reservation for in-flight intents.
3. Block duplicate unresolved intents for same position.
4. Make reconciler the sole final writer for terminal state transitions and fill settlement.
5. Ensure cancel/fail paths release reservations.

## Required code shape
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

## Constraints
- Do not launch a giant rewrite outside the reservation/reconciliation problem.
- Preserve logging shape.
- Keep the reconciler ownership explicit.

## Required tests
- reservation release on cancel/fail
- no duplicate same-position unresolved intent
- no concurrent BUY/SELL mutation on same position
- reconciler owns final transition to DONE/ERROR

## Required output
- code changes
- tests passing for this slice
- short implementation summary
- git commit for the phase
