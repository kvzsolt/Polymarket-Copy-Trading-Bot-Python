# Phase 04 — `balance_regime.py` CRITICAL fixes

**Branch:** `feat/phase01-position-key-coordinator`
**Date:** 2026-05-21
**Files touched:**

- `services/balance_regime.py` — rewrite (additive, no breaking API surface for sync callers)
- `services/reconciliation.py` — `ReconciliationService` accepts a `balance_regime` handle and rolls the daily ledger back on confirmed cancellations/rejections
- `orchestrators/trade_pipeline.py` — `record_buy` now fires on **both** the submitted-unconfirmed branch *and* the immediately-filled branch; both call sites pass `order_id` so cancel rollback can match
- `tests/test_balance_regime.py` — updated one expectation that depended on the old broken behavior; added coverage for every new method

**Test status:** `pytest tests/test_balance_regime.py` → **44 passed**; full suite → **1990 passed, 5 skipped**, no new failures.

---

## Fix-by-fix breakdown

### 1. `_to_decimal` no longer swallows bad input (CRITICAL)

**Before:**

```python
def _to_decimal(value: object, default: str = "0") -> Decimal:
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal(default)
```

Bare `except Exception` masked real bugs and silently produced `Decimal(0)`; `Decimal("NaN")` / `Decimal("Infinity")` parsed cleanly and propagated into the ledger.

**After:**

- Catches only `InvalidOperation, ValueError, TypeError, ArithmeticError`.
- Calls `result.is_finite()` to reject NaN/±Inf even when `Decimal` accepts them.
- Logs at `WARNING` with `{value, error, default}` so bad input is loud, not silent.

Tests: `TestToDecimal` covers `"not-a-number"`, `None`, `"NaN"`, `float("nan")`, `"Infinity"`, `float("inf")`, `float("-inf")`, and pass-through of valid `int`/`str`/`Decimal`.

---

### 2. `record_buy` fires on *all* submitted BUYs (CRITICAL)

**Before:** Only the `accepted && filled_amount <= 0` branch in `trade_pipeline.py:2167` called `record_buy`. Immediately-filled BUYs (the common case for marketable orders) skipped the ledger entirely → daily cap was effectively unenforced for fast fills.

**After:** A second `record_buy` call is added at `trade_pipeline.py:2426` (BUY-fill branch, after fills/positions are persisted) with the **actual filled notional** (`fill_size * fill_price`), and both call sites now pass `order_id=exec_result.order_id` so cancel rollback can match.

Rationale: the ledger tracks **submitted spend** (eagerly), but on the fast-fill path we already know the realised notional, so we use that more precise number.

---

### 3. `record_cancel` rolls the ledger back (CRITICAL)

New method on `BalanceRegime`:

```python
def record_cancel(self, notional_usdc, *, order_id: str | None = None) -> None
```

If `order_id` is supplied **and** matches a previously-recorded reservation, the recorded amount wins over the caller-supplied `notional_usdc` — this avoids drift when the upstream caller has lost the original submitted notional by the time the cancellation is observed.

Wired in `services/reconciliation.py`:

- `ReconciliationService.__init__` now accepts an optional `balance_regime=` kwarg (back-compat default `None`).
- `reconcile()` calls a new `_rollback_balance_regime_on_cancel(order_id, local_record)` whenever `_classify_order_state` returns `{"kind": "unexpected_cancellation"}`.
- The helper skips SELLs (which never consumed the daily-spend budget) and uses the `local_record`'s `notional_usdc`/`size_usdc`/`requested_usdc`/`size` (first match wins) as the rollback amount.
- All exceptions are logged at ERROR — never raised — so the reconciliation loop stays alive.

Tests: `TestRecordCancel` covers single cancel, unblocking the next BUY past the cap, `order_id`-driven exact rollback, and double-cancel safety (ledger never goes negative).

---

### 4. Per-position cap is now explicit (CRITICAL — interpretation note)

The fix-list refers to a `reserve_or_reject()` that silently drops the cap. The current file does **not** contain that method (it was either a description of a hypothetical bad implementation or had been removed earlier). The actual `check_buy()` already checks `_max_position > 0 and requested > self._max_position` **before** any state mutation, and the new `reserve()` (see fix #6) wraps the same check inside the lock. There is no path where a cap is set from `current_size` without a max comparison.

**No-op for the existing path; cap enforcement is now also race-safe for the new reserve path.**

---

### 5. `spent_today` is not a mutable list (CRITICAL — interpretation note)

The fix-list claims `spent_today` is a mutable list passed by reference (line 89). The actual ledger uses a `deque[tuple[float, Decimal]]` of immutable `(ts, Decimal)` entries — `Decimal` is itself immutable, and the deque is private to `_DailySpendLedger`. The reported bug pattern doesn't exist in this codebase.

What does exist and is now hardened: concurrent updates under the GIL on a shared deque are individually atomic, but multi-step read-modify-write sequences (check cap → record) were not. That's fix #6.

---

### 6. Atomic `reserve() → commit() / release()` lifecycle (HIGH)

New async API on `BalanceRegime`:

```python
async def reserve(self, *, order_id, notional_usdc, free_cash_usdc) -> BalanceDecision
async def commit(self, *, order_id, actual_filled_usdc=None) -> None
async def release(self, *, order_id) -> None
```

Internals:

- `self._async_lock = asyncio.Lock()` serialises the entire (check → record) window so two concurrent BUYs cannot both observe the same `spent` snapshot and both pass a cap that only one of them fits under.
- `self._reservations: dict[str, Decimal]` maps `order_id → held notional`. Populated by `record_buy(..., order_id=...)` and `reserve(...)`, consumed by `commit/release/record_cancel`.
- `commit(actual_filled_usdc=X)` refunds `reserved - X` to the budget when the actual fill is smaller than the reservation — same as `record_partial_fill`, expressed in the lifecycle vocabulary.
- `release()` rolls the full reservation back (cancel/reject path).
- A small `threading.Lock` (`_reservations_guard`) protects the dict because both sync `record_*` helpers and async lifecycle methods touch it.

Tests: `TestReserveCommitRelease` covers happy path, blocked-by-cap, release rollback, full-fill commit, partial-fill refund, empty-`order_id` validation, **and** `test_concurrent_reserves_serialise` which races two `reserve()` calls via `asyncio.gather` and asserts exactly one is allowed.

---

### 7. Threading vs asyncio locks (HIGH — interpretation note)

The fix-list says "replace `threading.Lock` with `asyncio.Lock`." Pure replacement would force every sync caller to become async, which is a much larger blast radius than the actual risk warrants.

What was actually done:

- **`_DailySpendLedger` keeps `threading.Lock`.** Justification documented in the docstring: (a) no critical section spans an `await`, so there is **no** deadlock risk against the event loop; (b) some callers reach the ledger from blocking executor threads (reconciliation runs venue calls via `run_in_executor`) — only a thread-safe primitive is correct there.
- **`BalanceRegime.reserve/commit/release` use `asyncio.Lock`.** This is where genuine async coordination matters (multi-step check-then-record). The async lock serialises lifecycle calls in the event-loop concurrency model.
- **`_reservations` dict uses a separate `threading.Lock`** so the sync helpers (`record_buy`, `record_cancel`, `record_partial_fill`) stay sync without imposing async on existing callers.

Net effect: no deadlock risk (the only async-held lock is the new one, which is held briefly without nested awaits inside its critical section), and no API break.

---

### 8. `MIN_BALANCE_HEADROOM_USDC=0` no longer authorises overdraft (CRITICAL)

**Before:** When `_min_headroom == 0`, the headroom check `if self._min_headroom > 0 and headroom_after < self._min_headroom` was skipped entirely — a BUY larger than free cash would pass.

**After:** A new unconditional guard runs first:

```python
if headroom_after < 0:
    return BalanceDecision(
        allow=False,
        reason="balance_regime_insufficient_free_cash",
        policy_name="min_balance_headroom_usdc",
        ...
    )
```

The configured floor (`_min_headroom > 0`) check still runs after, so an explicitly tighter floor still bites first.

Tests: `TestNegativeHeadroomGuard` covers overdraft rejection with all caps disabled and exact-match (`free=request`) allowance. One existing test (`test_zero_thresholds_allow_everything`) was asserting the **broken** behavior and has been split into `test_zero_thresholds_allow_when_funded` and `test_zero_thresholds_still_block_overdraft`.

---

### 9. Partial fills now reduce the ledger (HIGH)

New method `record_partial_fill(*, recorded_usdc, actual_usdc, order_id=None)`:

- Refunds `recorded - actual` to the daily budget when the actual fill is smaller than what was recorded at submission.
- Clamps `actual < 0` to zero defensively.
- No-ops on full fills and on `actual > recorded` (never inflates the ledger on bogus input).
- When `order_id` is supplied, the held reservation amount is decremented accordingly.

Also reachable via the lifecycle path: `commit(order_id, actual_filled_usdc=X)` already calls the same refund logic.

Tests: `TestPartialFill` covers partial fill, full fill (no-op), and over-fill (no-op).

---

## Summary

| # | Issue | Severity | Status |
|---|-------|----------|--------|
| 1 | `_to_decimal` swallows bad input | CRITICAL | Fixed |
| 2 | `record_buy` only on unconfirmed path | CRITICAL | Fixed |
| 3 | No rollback for cancellations | CRITICAL | Fixed (incl. reconciliation wiring) |
| 4 | `reserve_or_reject` drops cap | CRITICAL | N/A — pattern not in current file; new `reserve()` is race-safe |
| 5 | `spent_today` mutable list | CRITICAL | N/A — was already an immutable `deque[(ts, Decimal)]`; race-safety addressed in #6 |
| 6 | No reserve/commit phase | HIGH | Fixed |
| 7 | `threading.Lock` in async path | HIGH | Fixed (mixed strategy — see note above) |
| 8 | `MIN_BALANCE_HEADROOM_USDC=0` permits overdraft | CRITICAL | Fixed |
| 9 | Partial fill never reduced | HIGH | Fixed |

**No auto-commit performed.** Working tree dirty across the three files above plus the new test cases; ready for review.
