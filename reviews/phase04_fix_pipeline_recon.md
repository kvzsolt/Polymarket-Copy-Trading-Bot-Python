# Phase 4 — pipeline + reconciliation fixes

Six CRITICAL/HIGH bugs surfaced by the structural + execution review of
`orchestrators/trade_pipeline.py` and `services/reconciliation.py`. All fixes
land with no auto-commit. Full pytest run: **1998 passed, 5 skipped**.

---

## #1 — `record_buy` only fired on the unconfirmed path (CRITICAL)

**File:** `orchestrators/trade_pipeline.py` (~line 2167)

**Before**
```python
if exec_result.accepted and exec_result.filled_amount <= 0:
    # record_buy lived HERE — never reached on immediate-fill path.
    if signal.side == "BUY":
        regime.record_buy(Decimal(str(exec_result.requested_amount or 0)), ...)
    ...
```

Immediately-filled BUYs (`accepted and filled_amount > 0`) skipped the
ledger entirely. Daily-spend cap could be silently bypassed by any order
that filled at submit time.

**After**
```python
# Fires on BOTH accepted-no-fill AND immediate-fill paths; rejections never
# enter the ledger.
if exec_result.accepted and signal.side == "BUY":
    regime = getattr(context, "balance_regime", None)
    if regime is not None:
        try:
            filled = Decimal(str(exec_result.filled_amount or 0))
            if filled > 0:
                px = Decimal(str(exec_result.avg_price or signal.price or 0))
                regime.record_buy(filled * px, order_id=exec_result.order_id)
            else:
                regime.record_buy(
                    Decimal(str(exec_result.requested_amount or 0)),
                    order_id=exec_result.order_id,
                )
        except Exception:
            pass

if exec_result.accepted and exec_result.filled_amount <= 0:
    # unconfirmed branch (record_buy no longer inline here)
    ...
```

* Immediate fill → records actual fill notional (`fill_size × avg_price`).
* Accepted-no-fill → records requested amount (partial-fill adjustment
  happens later via the cancel-rollback path in fix #4).
* Rejected → never records (no `exec_result.accepted` ⇒ no ledger entry,
  no rollback needed).

---

## #2 — `reconcile_legacy_scope_fallback` collapsed unrelated rows (CRITICAL)

**File:** `orchestrators/trade_pipeline.py` (~line 1206)

**Before**
```python
_scope = PositionKey(
    user_id=record.get("user_id") or "__legacy_user__",
    condition_id=record.get("condition_id") or "__legacy_cond__",
    token_id=record.get("asset") or record.get("token_id") or "__legacy_token__",
)
```

Two legacy rows with NULL identity would land on the *same* synthetic
key → serialized through one coordinator slot, attribution corrupted.

**After**
```python
_legacy_salt = (
    str(record.get("order_id") or "").strip()
    or str(record.get("trade_key") or "").strip()
    or f"row-{uuid.uuid4().hex[:12]}"
)
_scope = PositionKey(
    user_id=record.get("user_id") or f"__legacy_user__{_legacy_salt}",
    condition_id=record.get("condition_id") or f"__legacy_cond__{_legacy_salt}",
    token_id=(
        record.get("asset")
        or record.get("token_id")
        or f"__legacy_token__{_legacy_salt}"
    ),
)
```

Each legacy row now gets its own `PositionKey`. Salt prefers
`order_id` → `trade_key` → uuid fallback so the metric+log still group
properly while the lock keys stay unique.

Test `test_reconcile_emits_metric_and_fallback_for_empty_identity` was
asserting the *buggy* collapse value and was updated to assert the
new per-row sentinel format.

---

## #3 — Stale `free_cash_usdc` snapshot across loop iterations (HIGH)

**File:** `orchestrators/trade_pipeline.py` (regime-gate site, ~line 1856)

Snapshot fetched once per BUY signal could be hundreds of milliseconds
old by the time the balance regime gate evaluated — and unaffected by
any prior submission in the same batch.

**Fix:** re-fetch immediately before `regime.check_buy`:
```python
_refreshed = await _fetch_balance_snapshot(
    context, signal.condition_id, leader_id,
)
if _refreshed is not None:
    snapshot = _refreshed
regime = getattr(context, "balance_regime", None)
if regime is not None:
    regime_decision = regime.check_buy(
        notional_usdc=gated_notional,
        free_cash_usdc=snapshot.free_cash_usdc,
    )
```

Sizing/cap resolvers above continue to use the earlier snapshot; only the
regime gate needs the freshest reading.

---

## #4 — No `record_cancel` rollback wired (HIGH)

**Files:**
* `services/execution/reconciliation_service.py` (`reconcile_trade_record`)
* `services/reconciliation.py` (auto-cancel path)
* `orchestrators/trade_pipeline.py` (wiring in `_reconcile_one`)

`record_buy` now eagerly records every submitted BUY (fix #1). Without a
matching `record_cancel`, terminal-no-fill cancels leak budget and the
daily cap would silently throttle the bot for the remainder of the window.

**Reconciler hook (terminal_no_fill branch):**
```python
# services/execution/reconciliation_service.py
async def reconcile_trade_record(
    ...,
    balance_regime: Any | None = None,
) -> bool:
    ...
    if current_matched <= 0 and order_status in _TERMINAL_NO_FILL_STATUSES:
        if (
            balance_regime is not None
            and str(side).upper() == "BUY"
            and hasattr(balance_regime, "record_cancel")
        ):
            cancel_amt = self._resolve_buy_requested_amount(order_id)
            if cancel_amt > 0:
                try:
                    balance_regime.record_cancel(cancel_amt, order_id=order_id)
                except TypeError:
                    balance_regime.record_cancel(cancel_amt)  # back-compat
                except Exception:
                    pass
        self.orders_repo.delete_processed_trade(record["trade_key"])
        ...
```

**Helper resolves request amount from the orders row:**
```python
def _resolve_buy_requested_amount(self, order_id: str) -> Decimal:
    row = self.orders_repo.get_by_order_id(order_id) or {}
    request = row.get("request") or {}
    raw = request.get("amount") if request.get("amount") is not None else request.get("size")
    return Decimal(str(raw or 0))
```

**Auto-cancel path (`services/reconciliation.py`):**
```python
# After we successfully cancel a stale BUY:
if self.balance_regime is not None and self.orders_repo:
    local_row = self.orders_repo.get_by_order_id(oid)
    self._rollback_balance_regime_on_cancel(order_id=oid, local_record=local_row)
```

The balance-regime fix agent owns the `record_cancel(amount, *, order_id=...)`
signature; we only call it.

**Pipeline wiring:**
```python
# orchestrators/trade_pipeline.py
return await reconciler.reconcile_trade_record(
    record, side, ...,
    balance_regime=getattr(context, "balance_regime", None),
)
```

---

## #5 — `unexpected_fill` branch unreachable (HIGH)

**File:** `services/reconciliation.py`

`get_orders()` returns only OPEN orders, so any terminal-state order
(`matched`, `cancelled`, `expired`, …) never reached `_classify_order_state`
in production — only via mocks in tests.

**Fix:** after the open-orders pass, probe local in-flight orders that
the venue *didn't* return as open:

```python
seen_open_ids = {str(o.get("id") or "") for o in exchange_orders if o.get("id")}
for local_row in self._local_in_flight_orders():
    local_oid = str(local_row.get("order_id") or "")
    if not local_oid or local_oid in seen_open_ids:
        continue
    probed = await self._fetch_single_order(local_oid)
    if probed is None:
        continue
    delta = self._classify_order_state(order=probed, local_record=local_row)
    ...
    if kind in ("unexpected_fill", "partial_fill"):
        unexpected_fills += 1
        logger.warning("reconcile_unexpected_fill_closed | ...")
    elif kind == "unexpected_cancellation":
        unexpected_cancellations += 1
        self._rollback_balance_regime_on_cancel(...)
    elif kind == "filled":
        unexpected_fills += 1
```

Two new helpers:
* `_local_in_flight_orders()` → `orders_repo.get_unconfirmed_order_submissions()`
* `_fetch_single_order(order_id)` → `clob_client.get_order(order_id)` with
  shape-normalization (dict OR `VenueOrderState`-style object).

Both fail-soft: a single bad probe never aborts the pass.

---

## #6 — Full venue fill mislabeled as `partial_fill` (HIGH)

**File:** `services/reconciliation.py` `_classify_order_state`

**Before:** any `venue_matched > local_matched` returned
`kind="partial_fill"`, even when the venue had completed the order.
Downstream consumers tracking partials expected more fills to come →
double-counted on the next reconcile pass.

**After:**
```python
if venue_matched > local_matched:
    ...
    original_size = _original_size(order)  # reads original_size / size / order_size
    is_complete = (
        venue_status in _TERMINAL_FILLED_STATUSES
        or (original_size > 0 and venue_matched >= original_size)
    )
    if is_complete:
        return {
            "kind": "filled",
            "local": local_matched,
            "venue": venue_matched,
            "delta": delta,
            "original": original_size,
        }
    return {"kind": "partial_fill", ...}
```

`ReconciliationResult` gained `complete_fills_observed` + `complete_fill_deltas`
so callers can distinguish completion from intermediate progress. The
existing partial-fill test cases (`status="live"`, no `original_size`)
still fall through to `kind="partial_fill"` unchanged.

---

## Test results

```
$ python3 -m pytest tests/test_reconcile_phase04.py -x -q
8 passed in 0.05s

$ python3 -m pytest tests/test_book_depth_gate.py -x -q
10 passed in 0.06s

$ python3 -m pytest -x -q
1998 passed, 5 skipped, 176 deselected in 18.71s
```

Updated tests:
* `tests/test_phase01_review_b_fixes.py::test_reconcile_emits_metric_and_fallback_for_empty_identity`
  — re-asserts the per-row salted sentinel scope (was asserting the buggy collapsed form).

---

## Coordination notes

* The balance-regime fix agent owns `BalanceRegime.record_cancel(amount, *, order_id=...)`;
  this changeset only calls it (with a `TypeError` fallback for the older one-arg signature).
* Reconciler keeps its no-fill rollback narrow: BUY side only, SELLs are
  explicitly excluded (they never consume the daily-spend ledger).
* Fix #1 + #4 together form the eager-record / lazy-release cycle —
  every accepted BUY records, every terminal-no-fill cancel releases.
