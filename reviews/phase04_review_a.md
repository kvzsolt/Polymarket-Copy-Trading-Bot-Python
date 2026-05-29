# Phase 04 Structural Review — `c0e0406`

**Scope:** balance regime, book depth gate, exchange-side reconciliation.
**Reviewer:** structural / architectural (race conditions, async safety, state-machine, integration coherence, test gaps).
**Verdict:** Conceptually sound, but the daily-spend ledger and reconciliation state-machine have several CRITICAL gaps that must be closed before Phase 05 ships. Book-depth gate is the cleanest of the three.

---

## CRITICAL — must fix before Phase 05

### 1. Daily-spend ledger does NOT record immediate fills
- **File:** `orchestrators/trade_pipeline.py:2167`
- **Issue:** `record_buy` is called only inside the branch `if exec_result.accepted and exec_result.filled_amount <= 0`. Any BUY that is accepted AND fills immediately (FOK-style, marketable IOC, partial-then-resting fill that lands in the same call) bypasses the ledger entirely. The rolling 24h ceiling silently drifts low — the operator can blow through `MAX_DAILY_SPEND_USDC` by a wide margin if execution lands in the `accepted && filled>0` branch.
- **Fix:** Move `record_buy` to fire on every accepted BUY (record `max(requested, filled)` or just `requested` — definition must be documented). Pull the call out of the unconfirmed branch and place it once, immediately after `exec_result.accepted` is true and before any branching.

### 2. Daily-spend ledger never compensates for cancellations / rejections
- **File:** `services/balance_regime.py:75-91`, `orchestrators/trade_pipeline.py:2167-2178`
- **Issue:** `record_buy(requested_amount)` runs at submission time before fill confirmation. If the order is later cancelled (FOK fails, venue rejects, reconciliation flags `unexpected_cancellation`), the ledger still carries the full notional for 24h. Combined with #1 the budget can be permanently exhausted by a few stuck cancellations; the only escape is a process restart (which itself "over-allows" per the module docstring).
- **Fix:** Track per-order ledger entries keyed by `order_id` (not just timestamp + amount). On the reconciliation cancel/reject path (`reconcile_submitted_unconfirmed`, `_classify_order_state` returning `unexpected_cancellation`), call `regime.unrecord(order_id)`. Alternatively shift the record to fire on confirmed fill only — but then enforce a *reservation* on submission (see #5) to prevent burst-overspend between submit and confirm.

### 3. No reservation / pre-commit between submit and record
- **File:** `services/balance_regime.py:143-218`, `orchestrators/trade_pipeline.py:1857`
- **Issue:** `check_buy → record_buy` is two phases. In `execute_trades`, the loop iterates signals sequentially per-leader (OK), but `reconcile_submitted_unconfirmed` runs at the top of every cycle and other concurrent `execute_trades` calls (different leaders) share the same `TradingContext.balance_regime`. Two concurrent BUYs both see `spent=S`, both pass the gate, both then call `record_buy` → ledger ends at `S + 2*notional`, possibly above `max_daily`. Same shape for `min_balance_headroom` because `free_cash_usdc` is from a single pre-loop snapshot and not decremented as the loop spends.
- **Fix:** Introduce a two-phase reserve/commit: `regime.reserve(order_id, amount)` adds to a pending bucket, `regime.commit(order_id)` moves it to the confirmed ledger, `regime.release(order_id)` rolls it back. Gate checks must read `confirmed + pending`. Bind to `order_id` so reconciliation drives commit/release deterministically.

### 4. `ReconciliationService` races with `reconcile_submitted_unconfirmed` on the same `orders_repo` row
- **File:** `services/reconciliation.py:377-404`, `orchestrators/trade_pipeline.py:1170-1253`
- **Issue:** Two independent code paths now mutate the same persisted order row:
  - `ReconciliationService._record_partial_fill` calls `orders_repo.upsert_matched_total`.
  - `reconcile_submitted_unconfirmed → _reconcile_one` calls the reconciler that confirms fills and updates positions.
  - The reconciliation loop runs on its own interval (`reconciliation_loop`) and `execute_trades` runs on the trade tick. They are not serialized by the `position_coordinator` because `ReconciliationService` does not acquire any scope lock. Result: `matched_total` can be stomped, and partial-fill deltas can be observed twice or lost.
- **Fix:** Make `ReconciliationService._record_partial_fill` acquire the same `PositionKey` lock via `context.position_coordinator.acquire(scope)` before writing — or, cleaner, move all partial-fill persistence into `reconcile_submitted_unconfirmed` and let `ReconciliationService` only emit observations (metrics + log). Today's split violates the module's own ownership docstring (the docstring says reconciliation "does not mutate persisted position state" but `upsert_matched_total` IS mutation of order state, which is in the same row family).

### 5. Partial fills detected by `ReconciliationService` never reach the position manager / PnL tracker
- **File:** `services/reconciliation.py:304-375`
- **Issue:** When `_classify_order_state` returns `{"kind": "partial_fill", "delta": d}`, the only side-effect is `upsert_matched_total`. The local `PositionManager`, `PnLTracker`, and `BalanceRegime` ledger are all uninformed. Positions on the in-memory shadow stay stale until the next `reconcile_submitted_unconfirmed` cycle picks the order up — but that path only handles terminal fills (`submitted_unconfirmed → confirmed`), not incremental partial deltas. State-machine gap: the bot can sit with venue fills the local equity model can't see.
- **Fix:** Either (a) forward partial-fill deltas to `context.position_coordinator` + `pnl_tracker.apply_partial_fill(...)`, or (b) leave `ReconciliationService` observation-only and add a partial-fill path to `reconcile_submitted_unconfirmed`. Pick one owner — current split is the worst of both.

---

## HIGH

### 6. Ledger `record_buy` exception is swallowed
- **File:** `orchestrators/trade_pipeline.py:2175-2178`
- **Issue:** `except Exception: pass # pragma: no cover — defensive`. Combined with #1/#2, this means a silent failure here lets the bot keep buying without spend accounting. Defensive `try/except` around a pure-memory `_DailySpendLedger.record` (which does nothing that can raise except `Decimal` conversion, already guarded by `_to_decimal`) is dead defensiveness that risks hiding real bugs.
- **Fix:** Remove the `try/except` entirely. If you need a safety net, log at WARNING and emit `metrics.inc("balance_regime_ledger_record_failed_total")` before re-raising or continuing.

### 7. Per-process in-memory ledger silently under-enforces in multi-worker deployments
- **File:** `services/balance_regime.py:16-21, 99-141`
- **Issue:** Docstring acknowledges single-process scope and says crash only "over-allows after a crash — never silently under-allows." That is true for a single process. If a future deployment splits the executor across processes (or threads with separate `TradingContext`s), each gets its own ledger and the global cap collapses to per-worker. The risk is invisible at runtime — there is no fail-fast.
- **Fix:** Either (a) persist the ledger via `orders_repo` (hydrate on bootstrap from confirmed BUY notional in last 24h) and back the in-memory deque with a thin write-through wrapper, or (b) add a startup assertion `assert _is_single_worker(), "BalanceRegime requires single-worker deployment"`.

### 8. `_classify_order_state` does not flag `venue=filled` against `local=submitted_unconfirmed`
- **File:** `services/reconciliation.py:341-375`
- **Issue:** `unexpected_fill` is only emitted when `local_record is None`. If the local row exists and is `submitted_unconfirmed`, and the venue says `filled` with `matched > local_matched`, the function correctly classifies as `partial_fill` (delta = filled - 0). But: if the venue says `filled` with `matched == local_matched > 0` (full fill already partially observed), nothing fires — no state transition is signaled even though local row is still in non-terminal state. Reconciliation hands the row to the next cycle in the same wrong state.
- **Fix:** Add an explicit terminal-fill transition: if `venue_status in _TERMINAL_FILLED_STATUSES` and `local_status not in _TERMINAL_FILLED_STATUSES`, emit a `terminal_fill` kind (or upgrade `partial_fill` with `final=True`) so downstream pipeline knows to close out the order.

### 9. Snapshot `free_cash_usdc` is not decremented across the per-leader loop
- **File:** `orchestrators/trade_pipeline.py:1828-1862` (and snapshot at ~`portfolio_snapshot(...)` earlier)
- **Issue:** The same `snapshot` (which holds `free_cash_usdc`) is read by every signal in the loop. Five small BUYs against `free_cash=100`, `MIN_BALANCE_HEADROOM=20`, `each notional=20`, all pass the `headroom_after = 100 - 20 = 80 >= 20` check independently — but cumulatively the headroom is breached.
- **Fix:** Maintain a running `committed_this_cycle_usdc` and subtract from `snapshot.free_cash_usdc` before the headroom check, or push the snapshot refresh into the loop body.

### 10. `_DailySpendLedger.threading.Lock` blocks the asyncio event loop
- **File:** `services/balance_regime.py:73-96`
- **Issue:** `threading.Lock` is a kernel-level mutex; while critical sections are short, holding it from a coroutine still blocks the loop thread. Worse, mixing `threading.Lock` with all-coroutine consumers is a smell — there are no real threads contending.
- **Fix:** Drop the lock if `BalanceRegime` is only used from the asyncio thread (asyncio is single-threaded). If thread-safety is genuinely needed (e.g. metrics scraper from a background thread), document the threads that touch it. Don't pay the lock without identifying the writers.

### 11. `reconciliation_loop` ignores `shutdown_event` during sleep
- **File:** `services/reconciliation.py:451-461`
- **Issue:** Shutdown can wait up to `interval` (default 300s) before the loop wakes and checks `is_set()`. For local dev that means Ctrl-C takes 5 minutes to respond.
- **Fix:**
  ```python
  try:
      await asyncio.wait_for(shutdown_event.wait(), timeout=interval)
      break
  except asyncio.TimeoutError:
      continue
  ```
  Or use `asyncio.Event.wait()` against a deadline.

### 12. Daily-spend gate has no metrics emission for ledger size / pending budget
- **File:** `services/balance_regime.py`
- **Issue:** Operators have no way to alarm on "approaching daily cap" — only on the binary deny event. No counter for `daily_spend_usdc_gauge`, no histogram for per-buy notional, no metric per skip reason aside from the generic `trades_skipped_total`.
- **Fix:** Emit gauges on every `check_buy`: `balance_regime_daily_spent_usdc`, `balance_regime_daily_remaining_usdc`, `balance_regime_free_cash_headroom_usdc`. Add counter `balance_regime_blocks_total{reason}`.

### 13. `_to_decimal` swallows parse failures silently
- **File:** `services/balance_regime.py:56-60`
- **Issue:** If a malformed config value reaches the regime, the limit silently becomes 0 — which *disables* the cap. Fail-open. Same pattern in `_safe_decimal` for reconciliation.
- **Fix:** Validate at config-load time (already partially done in `_validate_semantics`). Inside `BalanceRegime`, raise on bad input rather than silently disabling. The semantic "0 disables" should be only for the `0` literal, not "anything I couldn't parse".

---

## MEDIUM

### 14. `_normalize_levels` accepts `Mapping` but iterates `.items()` without unpacking
- **File:** `services/execution/book_spread.py:67-95`
- **Issue:** `if isinstance(levels, Mapping): items: Any = levels.items()` then `for entry in items: if isinstance(entry, tuple) and len(entry) == 2: raw_price, raw_size = entry`. This works for ordinary dicts (items returns `(k, v)` tuples) but breaks silently for any `Mapping` whose `items()` yields non-tuple pairs. Lists of dicts (e.g. raw venue payload `[{"price": ..., "size": ...}, ...]`) are silently dropped — the depth check then sees zero notional and rejects the BUY for "thin book."
- **Fix:** Branch explicitly on the input shape (`dict`, `list[dict]`, `list[tuple]`) and reject unknown shapes loudly. Today the canonical form is `BookSnapshot.bids/asks` (`dict[Decimal, Decimal]`), so probably fine — but if the BookSnapshot shape ever changes, the gate becomes a silent reject.

### 15. Book depth gate disables when `MIN_BOOK_DEPTH_USDC=0` but does not emit observability
- **File:** `services/executionService.py:485-491`
- **Issue:** When disabled, no log line indicates "depth check skipped due to config=0", so on-call cannot distinguish "we let it through because depth was OK" vs. "we let it through because depth wasn't checked."
- **Fix:** Emit a single startup log line stating the configured threshold (similar to the spread-mode log already present), so audit trails are explicit.

### 16. `_DailySpendLedger.hydrate` can break monotonic eviction
- **File:** `services/balance_regime.py:75-96`, `228-236`
- **Issue:** `_entries` is a `deque` evicted from the left under the assumption it's time-ordered. `record(now=ts)` accepts an arbitrary timestamp and appends to the right. If `hydrate(...)` is fed entries in a non-monotonic order (e.g. fetched from DB without `ORDER BY ts`), the deque ends up unsorted and `_evict` (popleft until front >= cutoff) stops too early, leaking expired entries forever.
- **Fix:** Sort hydrated entries before append, or use `SortedList` / bisect-based eviction. Defensive: assert monotonic in `record`.

### 17. `mark_processed_trade` happens before submit on rejection paths
- **File:** `orchestrators/trade_pipeline.py:1896-1909`
- **Issue:** Balance-regime rejection writes `skipped_<reason>` to `processed_trade`, which marks the `trade_key` as handled. If the daily cap is briefly exceeded (e.g. cycle boundary), the next cycle will *not* retry the trade because the row is already there. For `max_daily_spend_exceeded` skips this is probably wrong — the trade was perfectly valid, just rate-limited. The leader's signal is silently dropped.
- **Fix:** Distinguish "permanent skip" (`position_size_exceeded`) from "transient defer" (`daily_spend_exceeded`, `headroom_floor`). For transient cases, do not mark `processed_trade` — let the next cycle retry once budget refreshes. Or mark with a TTL state like `deferred_until=<24h ahead>`.

### 18. No back-pressure when `unexpected_fill` accumulates
- **File:** `services/reconciliation.py:120-299`
- **Issue:** If the venue reports five orphan fills (orders the bot has no local record of), the loop logs them and counts them — but does nothing else. No alert, no auto-disable, no escalation. Either the bot is being shadowed by another instance using the same keys, or there's a key compromise. Silent.
- **Fix:** Threshold: after N `unexpected_fill` events in a window, raise a circuit-breaker (`riskMonitor.pause("orphan_fills_detected")`).

### 19. `_orders_client` falls through to `self.clob_client` even when neither it nor `.client` supports order management
- **File:** `services/reconciliation.py:155-173`
- **Issue:** The fallthrough returns the original `clob_client` even if `_supports_order_management(clob_client)` was already False. Then `_fetch_open_orders` calls `.get_orders()` on it, which `AttributeError`s inside the executor. Caught at `_fetch_open_orders`'s caller wrapper, but the error message will be opaque ("'MockClob' object has no attribute 'get_orders'").
- **Fix:** Raise `RuntimeError("clob client lacks order management methods")` at startup of the service when neither candidate supports the API.

### 20. Test coverage gaps
- **File:** `tests/test_balance_regime.py`, `tests/test_reconcile_phase04.py`
- **Issues:**
  - No concurrency test for `BalanceRegime` — the `threading.Lock` is untested under contention.
  - No test for the trade_pipeline integration path (a real `execute_trades` call with `balance_regime` rejecting a BUY → verify `processed_trade` row, metrics, and result).
  - No test for partial fill against a venue order where `local_status` is `submitted_unconfirmed` AND `venue_status` is terminal `matched` (terminal-fill transition).
  - No test that `record_buy` is called on the immediate-fill path (regression for finding #1).
  - No test that ensures `record_buy` is *not* called on a rejection (would catch a future bug where reject paths leak into the record branch).
- **Fix:** Add an integration-level test that spins up a minimal `TradingContext`, fires `execute_trades` with a mocked `execution_service`, and asserts ledger state after a mix of accepted/rejected BUYs.

---

## LOW

### 21. `ReconciliationService` docstring/code mismatch
- **File:** `services/reconciliation.py:1-26`
- **Issue:** Module docstring says "It does **not** invent fills or mutate persisted position state." But `_record_partial_fill` mutates the order row's `matched_total`. Either expand the docstring to clarify "position state ≠ order state" or move the mutation out.

### 22. Dead defensive `getattr(context, "balance_regime", None)` after `__post_init__` always sets it
- **File:** `orchestrators/trade_pipeline.py:1857, 2173`, `domain/trading_context.py:100-101`
- **Issue:** `TradingContext.__post_init__` unconditionally creates a `BalanceRegime` when `config` is an `AppConfig`. The `None` check at the call sites is for the test path where a `SimpleNamespace` is passed. Fine, but leaves a "this is always set in prod" ambiguity. Either typed-narrow at call sites or strip the guard.

### 23. `BalanceDecision.skip_code` is a property aliasing `reason`
- **File:** `services/balance_regime.py:51-53`
- **Issue:** Adds an alias for code clarity but creates two ways to read the same field. Prefer renaming `reason` → `skip_code` everywhere and drop the alias, or vice versa. Two names for one value is a magnet for future drift.

### 24. `print_skip_reason` and `log_trade_skip` both fire for the same skip
- **File:** `orchestrators/trade_pipeline.py:1868-1891`
- **Issue:** Console print + structured log + metrics inc. Triple emission for one event. Probably intentional (CLI UX + ops telemetry) but worth checking it matches the convention used for the decision-gate skip path just above it.

### 25. `band_pct` field of `BookDepthMetrics` is stored as a percent value (e.g. `Decimal(5)`) — not a fraction
- **File:** `services/execution/book_spread.py:18-36`
- **Issue:** Internally `band` is `band_pct / 100` (a fraction); externally `BookDepthMetrics.band_pct` is reconverted back to percent. Mixing units in adjacent fields is bug-prone. Recommend storing `band_fraction: Decimal` and computing percent at the log site only.

---

## Recommended fix order (before Phase 05)

1. **#1 + #2 + #5** — daily-spend ledger correctness end-to-end. Without this, the cap is theatre.
2. **#3** — reservation pattern. Required to make #1/#2 robust under concurrency.
3. **#4** — owner one of the two reconciliation paths. Today they race.
4. **#9** — running-headroom across the leader loop.
5. **#8 + #18** — reconciliation state-machine completeness and orphan-fill escalation.
6. **#11** — shutdown responsiveness (cheap).
7. **#17** — distinguish transient vs. permanent skips so deferrable trades aren't silently dropped.
8. **#20** — test coverage to prevent regressions on the above.

The book-depth gate (`book_spread.py`, `executionService.py` changes) is the cleanest piece of this phase — only LOW/MEDIUM findings against it. The balance-regime module is well-designed in isolation but its integration with the trade pipeline and reconciliation has the most exposure.
