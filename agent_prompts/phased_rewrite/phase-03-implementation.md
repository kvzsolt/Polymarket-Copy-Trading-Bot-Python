# Phase 3 Implementation Prompt

Repo: `/home/anon/Bots/Polymarket-Copy-Trading-Bot-Python`
Base branch: latest `develop` after Phase 2 merge
Working branch: `phase/03-issue-38-min-order-idempotency`

Implement **Phase 3 only**.

## Mission
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

## Required code shape
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

## Constraints
- Do not do reservation/reconciliation ownership here unless required for tiny compatibility glue.
- Preserve logging shape; only add minimal structured reason lines if needed.
- Use `Decimal`.

## Required tests
- BUY upsizes when allowed
- BUY skips when headroom/balance insufficient
- SELL skips when held below minimum
- SELL upsizes only when held supports it
- idempotency changes when effective request changes

## Required output
- code changes
- tests passing for this slice
- short implementation summary
- git commit for the phase
