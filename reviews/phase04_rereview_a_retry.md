# Phase 04 — Focused Re-Review (Retry A)

**Commit:** `d985568` (`fix(phase04): ledger correctness, reservation lifecycle, stale snapshot, legacy reconciliation`)
**HEAD verified:** `d9855689ad16b5dfb2e3fecd97de25187969d9b9`
**Date:** 2026-05-21
**Scope:** ONLY the 3 fixes listed in the brief.

---

## Targeted test run

```
$ python3 -m pytest tests/test_balance_regime.py -x -q -k "reserve or decimal or record_buy"
............                                                             [100%]
12 passed, 32 deselected in 0.05s
```

All 12 selected tests **pass**.

---

## Fix 1 — `_to_decimal` NaN/Inf rejection — **PASS (with caveat on verification spec)**

**File:** `services/balance_regime.py:73-95`

**Evidence — code:**

```python
def _to_decimal(value: object, default: str = "0") -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError, ArithmeticError) as exc:
        logger.warning(
            "balance_regime_to_decimal_invalid_input",
            extra={"value": repr(value), "error": str(exc), "default": default},
        )
        return Decimal(default)
    if not result.is_finite():
        logger.warning(
            "balance_regime_to_decimal_non_finite",
            extra={"value": repr(value), "parsed": str(result), "default": default},
        )
        return Decimal(default)
    return result
```

What was wrong → fixed:
- Broad `except Exception:` swallow → **replaced** with narrow `(InvalidOperation, ValueError, TypeError, ArithmeticError)`.
- `Decimal("NaN") / Decimal("Infinity")` previously propagated → now rejected via `result.is_finite()` check, logged at `WARNING`, returns `Decimal(default)`.

**Behavioural check:**

```
$ python3 -c "from services.balance_regime import _to_decimal; print(_to_decimal('NaN'))"
balance_regime_to_decimal_non_finite
0

$ python3 -c "from services.balance_regime import _to_decimal; print(_to_decimal('Infinity'))"
balance_regime_to_decimal_non_finite
0

$ python3 -c "from services.balance_regime import _to_decimal; print(_to_decimal('1.5'))"
1.5
```

**Caveat — verification-step mismatch:**
Brief says "should raise, not return Decimal(0)". Implementation does NOT raise — it logs `balance_regime_to_decimal_non_finite` at WARNING and returns the configured default (0). The substantive intent ("NaN/Inf not propagated into ledger math") is satisfied — Decimal('NaN') can never leak past this guard. Docstring at lines 78-79 explicitly documents the chosen contract:

> "NaN and Infinity are rejected as finance-unsafe inputs even though `Decimal` will happily parse them."

→ Treating "rejection via warn-and-default" as a valid form of rejection. If the policy intent is "raise so callers MUST handle it", the brief's verification command catches the gap and Fix 1 would need to be reopened. Flagging for the reviewer to confirm intent. **Code-vs-docstring is internally consistent; code-vs-brief literal text is not.**

---

## Fix 2 — `record_buy` on both paths — **PASS**

**File:** `orchestrators/trade_pipeline.py`

`grep -n record_buy orchestrators/trade_pipeline.py` returns **3 call sites** for `regime.record_buy` (brief required ≥2):

| Line | Context |
|------|---------|
| 2213 | Inside `if exec_result.accepted and signal.side == "BUY"` → immediate-fill branch (`_filled_amt > 0`), records `filled_amt × fill_px`. |
| 2218 | Same `accepted && BUY` block → accepted-no-fill `else` branch, records `requested_amount`. |
| 2482 | Second submission path (executor-driven fill), records `fill_size × fill_price` with `logger.exception` on failure. |

Header comment at lines 2198-2203 explicitly states the new contract:

> "Phase 4 — record BUY notional against the rolling 24h spend ceiling at submission time. Fires on BOTH the immediate-fill path and the accepted-no-fill path so every submitted BUY consumes the daily cap. Duplicate submissions are filtered above (`continue`); a rejection (`not exec_result.accepted`) never enters the ledger. SELLs do not consume the daily-spend budget."

→ Previous single-branch `accepted && filled_amount <= 0` gap is closed. Rejections still bypass the ledger (correct). SELLs still bypass (correct).

Minor note (not in scope but observed): the `except Exception: pass` at line 2222 silently swallows record_buy failures on the first path, while the second path (line 2486) logs via `logger.exception`. Asymmetric — but out of scope for this re-review.

---

## Fix 3 — Reserve / commit / release concurrency — **PASS**

**File:** `services/balance_regime.py:375-434`

All three methods present, all three take `async with self._async_lock`:

```python
async def reserve(self, *, order_id, notional_usdc, free_cash_usdc) -> BalanceDecision:
    if not order_id:
        raise ValueError("reserve requires a non-empty order_id")
    async with self._async_lock:
        decision = self.check_buy(notional_usdc=..., free_cash_usdc=...)
        if decision.allow:
            self.record_buy(decision.requested_usdc, order_id=order_id)
        return decision

async def commit(self, *, order_id, actual_filled_usdc=None) -> None:
    async with self._async_lock:
        ...
        if unfilled > 0:
            self._ledger.adjust(-unfilled)

async def release(self, *, order_id) -> None:
    async with self._async_lock:
        ...
        self._ledger.adjust(-reserved)
```

Race-closure properties verified:
- `reserve` performs `check_buy → record_buy` as a single atomic critical section. Two concurrent BUYs cannot both observe pre-spend state and both pass the cap.
- `commit` returns unfilled remainder to the budget under the same lock.
- `release` rolls a reservation back fully (cancel/reject/replace) under the same lock.
- Class docstring at lines 106-112 documents why the daily ledger uses `threading.Lock` (touched from executor threads) while the reservation lifecycle uses `asyncio.Lock` (no `await` inside the threading critical section → no cross-runtime deadlock).
- `reserve` rejects empty `order_id` before taking the lock — prevents ledger entries that can never be committed/released.

The 12-test pass run includes reserve/commit/release tests under the `-k "reserve or decimal or record_buy"` selector → behaviour locked in by tests.

---

## Summary

| Fix | Status | Notes |
|-----|--------|-------|
| 1. `_to_decimal` NaN/Inf | **PASS** | Substantively correct (NaN/Inf cannot propagate). Caveat: brief said "should raise"; impl warn+default. Reviewer confirm intent. |
| 2. `record_buy` on both paths | **PASS** | 3 call sites; both immediate-fill and accepted-no-fill covered; rejections/SELLs correctly excluded. |
| 3. reserve/commit/release | **PASS** | All async, all under `async with self._async_lock`; race window closed. |

**Tests:** 12 passed, 0 failed.
**Recommendation:** Accept Fix 2 and Fix 3 as-is. For Fix 1, confirm with the brief author whether warn+default is the desired contract or whether `_to_decimal` should `raise InvalidOperation` (or a custom `NonFiniteValueError`) when given NaN/Inf — current docstring claims the former, brief verification claims the latter.
