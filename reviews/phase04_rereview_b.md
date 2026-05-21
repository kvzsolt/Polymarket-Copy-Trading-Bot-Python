# Phase 04 Re-Review B — Pipeline + Reconciliation

**Scope:** verify the fixes applied in commit `d985568` actually solved the
problems flagged in `reviews/phase04_review_b.md` (and earlier) without
introducing new defects.
**Commit:** `d985568 fix(phase04): ledger correctness, reservation lifecycle,
stale snapshot, legacy reconciliation`
**Files audited:**
* `orchestrators/trade_pipeline.py`
* `services/balance_regime.py`
* `services/reconciliation.py`
* `services/execution/reconciliation_service.py`
* `tests/test_balance_regime.py`, `tests/test_reconcile_phase04.py`,
  `tests/test_phase01_review_b_fixes.py`, `tests/test_trade_pipeline*.py`
**Test runs:** all green —
`tests/test_reconcile_phase04.py` 8/8,
`tests/test_balance_regime.py` (full file passes via 92-test bundle),
full suite `1998 passed, 5 skipped`.

---

## Verdict

**Tests green, but the suite does not exercise the actual integration that
the previous review demanded.** Direct ledger-level repro confirms one new
**CRITICAL** money-safety regression introduced by the fix itself (double
`record_buy` on the immediate-fill path) and one previously-unflagged
**HIGH** asymmetry in `record_cancel` that the new "wire cancel from
multiple sources" plumbing exposes (double-cancel poisons the ledger).
The other items pass with caveats.

---

## Verification matrix

| # | Item | Status | Evidence |
|---|------|--------|----------|
| 1 | Stale `free_cash_usdc` re-fetched before each regime check | **PASS (with HIGH caveat)** | `orchestrators/trade_pipeline.py:1883-1887` |
| 2 | `record_cancel` wired in cancel/reject handlers | **PARTIAL PASS** | `services/execution/reconciliation_service.py:938-957`, `services/reconciliation.py:273-276, 334-337, 358-368` |
| 3 | Legacy scope collapse fixed (salt by `order_id`) | **PASS** | `orchestrators/trade_pipeline.py:1218-1248`, `tests/test_phase01_review_b_fixes.py:104-114` |
| 4 | Full fill tagged `filled` not `partial_fill` | **PASS (test coverage gap)** | `services/reconciliation.py:465-508` |
| 5 | Unexpected fill probe via `get_order()` | **PASS (with MEDIUM caveats)** | `services/reconciliation.py:287-345, 599-646` |
| 6 | No pipeline test regressions | **PASS** | `1998 passed, 5 skipped` |
| 7 | `record_cancel` graceful on double-call | **FAIL (HIGH)** | repro below |
| 8 | Reconciliation vs regime ordering | **PASS (low-impact race)** | analysis below |

Net: **1 CRITICAL, 1 HIGH, 2 MEDIUM new findings.** Fixes #1–#5 from the
prior review either landed correctly or with disclosed limitations, but
the integration changes introduced new defects.

---

## CRITICAL

### N1. Double `record_buy` on immediate-fill BUY path

**File:** `orchestrators/trade_pipeline.py`
**Sites:** lines `2204-2223` (NEW choke point added by this commit) **and**
lines `2473-2490` (also NEW, post-fill site immediately after
`positions_repo.apply_fill`).

The fix doc for #1 claims the original inline `record_buy` was moved out
of the `accepted and filled_amount <= 0` branch into "a single choke point
that runs for every accepted BUY." In practice **both** sites were added.
For an immediately-filled BUY the path is:

1. `exec_result.accepted == True`, `filled_amount > 0` → enters the new
   choke at line `2204`, calls `record_buy(filled_amt * fill_px,
   order_id=exec_result.order_id)`.
2. Line `2225` (`accepted and filled_amount <= 0`) is **skipped** because
   `filled_amount > 0`.
3. Line `2263` (`not exec_result.accepted`) is **skipped**.
4. Reaches line `2473` `if signal.side == "BUY":` and calls
   `record_buy(filled_notional, order_id=exec_result.order_id)` **a second
   time with the same order_id**.

`BalanceRegime.record_buy` (`services/balance_regime.py:200-221`) appends
to the ledger and **accumulates** the reservation:

```python
self._ledger.record(amt)                                  # +amt
if order_id:
    self._reservations[order_id] = (
        self._reservations.get(order_id, Decimal("0")) + amt   # +amt again
    )
```

Direct repro (no test exists for this):

```text
After 2x record_buy with same order_id (immediate-fill):
  daily_spent_usdc = 200      # truth = 100
  Reservations for ord-1 = 200
After record_cancel:
  daily_spent_usdc = 0        # cleanup masks the bug if cancel ever fires
```

If a cancel arrives, `record_cancel` reads the reservation (200) and
subtracts it, so the books *eventually* zero out — but until then the
daily-cap budget reads 2× the actual spend. For BUYs that **succeed**
(no cancel), the 2× value sits in the rolling 24h window. Net effect: the
daily cap activates at roughly **half** its configured value on any flow
dominated by immediate fills (FOK / IOC / live-marketable BUYs against
the BBO — i.e. the common case).

This is a clean regression of the very bug fix #1 was meant to repair:
spent ledger no longer "misses immediate fills" — it now **doubles** them.

**Fix:** delete one of the two sites. The fix-doc design (single choke at
`2204` covering both paths, with `_filled_amt > 0` selecting fill vs
requested) is sound; just remove the post-`apply_fill` site at `2473-2490`
since the choke already handled it.

**Test gap:** `tests/test_balance_regime.py` only calls `record_buy` once
per order id, and no test in `tests/test_trade_pipeline*.py` drives a BUY
through `execute_trades` with `filled_amount > 0` while asserting
`regime.daily_spent_usdc`. The previous review explicitly called this out
("Add a pipeline test that feeds two BUYs through `execute_trades` with a
small daily cap and asserts the second BUY is rejected") — still missing.

---

## HIGH

### N2. `record_cancel` is not idempotent on a missing reservation

**File:** `services/balance_regime.py:223-243`

```python
recorded: Decimal | None = None
if order_id:
    with self._reservations_guard:
        recorded = self._reservations.pop(order_id, None)
amt = recorded if recorded is not None else _to_decimal(notional_usdc)
if amt <= 0:
    return
self._ledger.adjust(-amt)
```

When a second cancel arrives for the same `order_id` (the reservation has
already been popped), the helper silently **trusts the caller's
`notional_usdc` argument** and adjusts the ledger downward again. The
ledger's `total()` is floored at zero in the *output* (`return running
if running > 0 else Decimal("0")`), but the underlying deque carries a
phantom **negative** entry that suppresses subsequent legitimate spend
until the 24h window evicts it.

Repro:

```text
buy(100, ord-1)         spent = 100
cancel(0,   ord-1)      spent = 0     # pops reservation, adjusts -100, OK
cancel(100, ord-1)      spent = 0     # phantom -100 sits in deque
buy(100, ord-2)         spent = 0     # expected 100 — daily cap blind
```

Reproducing path through the new wiring:

1. `services/reconciliation.py::ReconciliationService.reconcile()` auto-
   cancels stale BUY → `_rollback_balance_regime_on_cancel(order_id=oid,
   local_record=...)` → `regime.record_cancel(amount, order_id=...)`. Pops
   reservation. Local row's status is **not** flipped here.
2. Next pass, `_local_in_flight_orders()` still returns that row (status
   change is upstream, not in this method) → `_fetch_single_order`
   confirms terminal `cancelled` → `kind == "unexpected_cancellation"` →
   `_rollback_balance_regime_on_cancel` again. Reservation gone, so amount
   resolves from `local_record["notional_usdc" | "size_usdc" | ...]` and
   subtracts a **second** time.

The hand-off from `services/execution/reconciliation_service.py`'s
`terminal_no_fill` branch is safer because it `delete_processed_trade(...)`s
the row immediately after the call — but the *open-orders sweep* path in
`services/reconciliation.py` has no equivalent guard.

**Fix:** make `record_cancel` idempotent. When `order_id` is supplied and
the pop returned `None`, treat as a no-op (already rolled back). Do **not**
fall back to the caller-supplied `notional_usdc` in that case:

```python
if order_id:
    with self._reservations_guard:
        recorded = self._reservations.pop(order_id, None)
    if recorded is None:
        return   # already cancelled; second call is a no-op
    amt = recorded
else:
    amt = _to_decimal(notional_usdc)
```

Add the matching test (`test_record_cancel_double_call_is_noop`).

### N3. Headroom check ignores in-flight reservations

**File:** `orchestrators/trade_pipeline.py:1883-1893`, `services/balance_regime.py:294-303`

The re-fetch fixes the case where a *filled* prior BUY in this pass
consumed wallet USDC (the wallet snapshot reflects the deduction). It does
**not** fix the case the original review #10 actually called out: a prior
*submitted-unconfirmed* BUY in the same pass has reserved budget on the
regime but the wallet hasn't moved yet. `check_buy` is still called with
`free_cash_usdc=snapshot.free_cash_usdc` and `headroom_after = free_cash -
requested` — reservations are tracked (`_reservations` dict, line `181`)
but `check_buy` never subtracts them from `free_cash_usdc`. The
`reserve()` lifecycle was designed for exactly this, but the pipeline does
not use it — it calls `check_buy` then `record_buy` separately, which
re-opens the race for unconfirmed-submitted bursts.

**Fix:** either route the pipeline through `regime.reserve(...)` (which
already handles concurrency via `_async_lock`), or have `check_buy` net
`free_cash_usdc` against `sum(self._reservations.values())` before
computing `headroom_after`.

---

## MEDIUM

### N4. `_classify_order_state` cannot detect partial-fill-then-cancel

**File:** `services/reconciliation.py:454-509`

Branches are: (a) `venue_status in _TERMINAL_CANCELLED_STATUSES and
local_matched <= 0` → `unexpected_cancellation`, (b)
`venue_matched > local_matched` → partial / full fill. A venue order that
filled partially (so `local_matched > 0` after the partial-fill pass)
and was then terminally cancelled (venue_status `cancelled`, but
`venue_matched == local_matched` because the cumulative didn't move
between passes) falls through both branches and returns `None`. The new
`unexpected_cancellation` accounting + balance-regime rollback never fire
for that order. The committed remainder leaks against the daily-cap
budget.

**Fix:** add a third branch — `venue_status in
_TERMINAL_CANCELLED_STATUSES and local_matched > 0` → emit a new kind
(`cancel_after_partial`) and roll back the **unfilled** remainder
(`requested_amount - filled_amount * fill_price`).

### N5. Closed-order probe is N+1 venue calls per pass

**File:** `services/reconciliation.py:294-345, 599-620`

For every locally-in-flight order not present in the open-orders list,
`_fetch_single_order(local_oid)` runs `clob_client.get_order(order_id)`
on the executor. Polymarket rate-limits at ~10 req/s on the order
endpoints; a backlog of even a few dozen unconfirmed-but-stale rows turns
into a per-tick N+1 storm that can starve other paths (depth gates,
balance fetches). The behaviour is correct, but unbounded.

**Fix:** cap the number of probes per pass (config knob, default ~10),
log when the cap is hit so ops can size it. Skip probes whose local row
was last updated < N seconds ago to avoid hammering on freshly-submitted
orders that haven't appeared on the venue's open list yet (eventual
consistency).

### N6. No test covers the `kind="filled"` branch with a local record

**File:** `tests/test_reconcile_phase04.py`

`test_partial_fill_recorded` uses `status="live"` (non-terminal) and a
local record; `test_unexpected_fill_with_no_local_record` covers the
no-local-record terminal case. The actually-fixed path — venue terminal-
filled **with** a local record, asserting `kind="filled"` (not
`"partial_fill"`) and `complete_fills_observed == 1` — is untested. Add:

```python
async def test_local_record_with_terminal_venue_status_is_complete_fill(self):
    venue_orders = [{"id": "o-done", "timestamp": now, "status": "matched",
                     "size_matched": "10", "original_size": "10"}]
    repo.get_by_order_id.return_value = {
        "order_id": "o-done", "status": "submitted_unconfirmed",
        "matched_total": "4",
    }
    ...
    assert result.complete_fills_observed == 1
    assert result.partial_fills_observed == 0
```

Without this test, a future refactor of the `is_complete` ternary can
silently re-introduce the original double-count bug.

### N7. `_rollback_balance_regime_on_cancel` amount resolution is fragile

**File:** `services/reconciliation.py:536-543`

```python
for key in ("notional_usdc", "size_usdc", "requested_usdc", "size"):
    if local_record.get(key) is not None:
        amount = str(local_record[key])
        break
```

The local row schema in `orders_repo` is `request.amount` /
`request.size` (see `_resolve_buy_requested_amount` in
`services/execution/reconciliation_service.py:715-740`), not the
top-level keys this helper probes. For real persisted rows the loop
finds nothing, `amount = "0"`, and the regime call falls back to the
reservation (which **is** present at the first call site) — but if the
test/mocked path supplies the top-level shape, the helper *appears* to
work. Two code paths with two schema assumptions for the same kind of
row is a future regression magnet.

**Fix:** route both call sites through the same helper, or take the
request shape from `request.amount` first.

---

## LOW

### N8. `_local_in_flight_orders` reuses an unbounded query each pass

`get_unconfirmed_order_submissions()` has no time-window filter. Once
the bot has been running for days with any stale-but-not-deleted rows,
each reconcile pass re-walks the whole set. Add a `limit` / `older_than`
window or an explicit "needs probe" flag on the row.

### N9. `from_reconciliation_record` errors are converted to a salt string
made of `uuid.uuid4().hex[:12]` if both `order_id` and `trade_key` are
missing.

The metric increments before the salt is computed, so two unrelated rows
will be counted distinctly — but the salt itself uses only 48 bits of
entropy. Probability of collision is negligible at current volumes;
flag for posterity if the queue ever grows past ~16M rows in a
single window.

---

## Re-review of items the fix-doc claims solved

| Item from `phase04_review_b.md` | Claimed fix | Re-review verdict |
|---------------------------------|-------------|-------------------|
| #1 — record_buy missing on immediate fill | choke point added | **REGRESSED** — both sites kept, double-counts (see N1) |
| #2 — headroom not enforced at 0 | `headroom_after < 0` always rejects | Verified `services/balance_regime.py:341-352` |
| #3 — `_to_decimal` silently swallows | NaN/Inf rejected, narrow except | Verified `services/balance_regime.py:73-95` |
| #4 — legacy scope collapse | per-row salt | Verified, but test only covers one row |
| #5 — `_safe_decimal` NaN/Inf | not visible in this diff | NOT verified in this review (out of scope flag — please re-check) |
| #6 — cumulative-vs-delta | not addressed | Still open |
| #7 — full fill mislabeled | `is_complete` branch added | Verified, but untested (see N6) |
| #8 — terminal orders invisible | closed-order probe via get_order | Verified, but N+1 (see N5) |
| #9 — eager-spend on rejected BUY | rollback wired | Partial — rejection path never records (good), but cancel rollback double-fires (N2) |
| #10 — stale free_cash | re-fetch per signal | Partial — unconfirmed in-flight reservations still ignored (N3) |

---

## Recommended remediation order

1. **Delete the duplicate `record_buy` at `orchestrators/trade_pipeline.py:2473-2490`** (N1, CRITICAL). Add a pipeline integration test that drives two immediate-fill BUYs and asserts `daily_spent_usdc == 2 * notional`, not `4 * notional`.
2. **Make `record_cancel` idempotent** when `order_id` is supplied and the reservation is missing (N2). Add `test_record_cancel_double_call_is_noop`.
3. **Net reservations into `check_buy`** *or* convert the pipeline to use `regime.reserve()` (N3).
4. Add the `kind="filled"` reconciliation test (N6) and the `cancel_after_partial` branch (N4).
5. Cap closed-order probes per pass (N5).

The CRITICAL and the first HIGH are blockers for live-capital Phase 4
sign-off — the daily cap reads 2× actual spend on the dominant immediate-
fill path, and a benign reconcile retry can silently zero the ledger.
Everything else can land in a follow-up.
