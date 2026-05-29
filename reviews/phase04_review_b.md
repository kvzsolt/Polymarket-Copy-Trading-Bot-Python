# Phase 04 Execution Correctness Review — `c0e0406`

**Scope:** numerical correctness, Decimal/float discipline, order of operations,
edge cases, partial-fill / cancel / terminal state handling, test quality,
data integrity and money-safety.
**Files audited:** `config/app_config.py`, `domain/trading_context.py`,
`orchestrators/trade_pipeline.py`, `services/balance_regime.py`,
`services/execution/book_spread.py`, `services/executionService.py`,
`services/reconciliation.py`, and the three new test modules.
**Verdict:** the balance-regime ledger and the partial-fill classifier are
math-correct in isolation, but the *integration* into the pipeline has two
money-safety defects and several edge cases that the unit tests do not catch.

---

## CRITICAL

### 1. Daily-spend ledger misses every immediately-filled BUY
- **File:** `orchestrators/trade_pipeline.py:2167-2178` (also unreachable
  branch starting `~:2348`).
- **Issue:** `regime.record_buy(...)` is only invoked inside the branch
  `if exec_result.accepted and exec_result.filled_amount <= 0:` (the
  "submitted unconfirmed" path). The very next branch handles
  `accepted and filled_amount > 0` (immediate fills, FOK/IOC, partial-on-submit
  with rest cancelled) and never touches the regime. Net effect: any BUY that
  the venue fills synchronously bypasses `MAX_DAILY_SPEND_USDC` accounting.
  Combined with a generous spread/depth gate this is a clean way to blow
  through the daily cap without any signal in metrics or logs.
- **Recommended fix:** record at a single choke point that runs for every
  accepted BUY (e.g. immediately after `exec_result.accepted` is true and
  before the unconfirmed / filled branches diverge). Document whether the
  ledger tracks `requested_amount` (submission-time intent) or
  `filled_notional = fill_size * fill_price` (post-trade actuals); they
  differ for partial-on-submit and for slippage.

### 2. `headroom_after` computed but only enforced when a non-zero floor is set
- **File:** `services/balance_regime.py:161-208`.
- **Issue:** with `MIN_BALANCE_HEADROOM_USDC=0` (the default), the regime
  happily allows a BUY whose `requested_usdc > free_cash_usdc`
  (`headroom_after < 0`). The module docstring sells this as "refuse to
  execute if running the BUY would bring free cash below the configured
  floor" — but a floor of zero is treated as "no floor", not as the obvious
  semantic "don't go negative". Combined with finding #1 this is the only
  thing the regime can use to stop you spending cash you do not have.
- **Recommended fix:** even when `_min_headroom == 0`, reject on
  `headroom_after < 0` (you cannot have negative free cash). Add the test
  cases. Optionally: make the default config value non-zero so operators have
  to consciously opt out, not opt in.

### 3. Daily ledger uses `decimal.sum` with a `Decimal("0")` seed but accepts
non-Decimal records via `_to_decimal(value, default="0")`
- **File:** `services/balance_regime.py:75-81, 136-138, 228-236`.
- **Issue:** `record_buy` and `hydrate` both run input through `_to_decimal`,
  which catches `Exception` and silently returns `Decimal("0")`. A malformed
  fill (e.g. `record_buy("NaN")`, `record_buy(float("inf"))`,
  `record_buy(None)` from a buggy caller) is **silently swallowed** — the
  spend is never registered. Worse, `Decimal("NaN")` itself parses without
  raising and would then poison `total()` (subsequent arithmetic raises
  `InvalidOperation`). Today the call site uses
  `Decimal(str(exec_result.requested_amount or 0))` so the failure mode is
  benign, but the helper is a foot-gun.
- **Recommended fix:** require non-negative, finite Decimal inputs and raise
  loudly on bad data (or at minimum `logger.error` and return). Money math
  must not fail silently to `0`.

### 4. `reconcile_legacy_scope_fallback` permanently merges rows from
different markets under one synthetic `PositionKey`
- **File:** `orchestrators/trade_pipeline.py:1203-1232`.
- **Issue:** when `PositionKey.from_reconciliation_record` raises, the new
  code falls back to a sentinel
  `PositionKey("__legacy_user__","__legacy_cond__","__legacy_token__")`.
  Multiple unrelated legacy rows will reconcile under the *same* sentinel
  scope, so `position_coordinator.acquire(_scope)` serialises them and any
  state computation derived from `_scope` (PnL, exposure, dedupe) will
  conflate trades that belong to different markets. Risk-wise this is worse
  than the previous skip — equity / position attribution becomes
  permanently wrong for legacy rows.
- **Recommended fix:** when `user_id` is missing, fall back to
  `context.user_id` (we know who the bot is). For `condition_id`/`asset`,
  derive a stable per-row sentinel
  (`f"__legacy_{record['order_id']}__"`) so each legacy row gets a unique
  scope. Or simply re-raise once the metric has incremented — the original
  `return False` behaviour was actually safer than the new fallback.

---

## HIGH

### 5. `_safe_decimal` accepts `"NaN"` / `"Infinity"`
- **File:** `services/reconciliation.py:69-81`.
- **Issue:** `Decimal(str(value))` will happily parse `"NaN"`, `"Infinity"`,
  `"-Infinity"`. `_matched_size` then returns one of those, and the very
  next comparison (`venue_matched > local_matched`,
  `venue_matched - local_matched`) raises `InvalidOperation` for NaN and
  silently flows infinity into `partial_fill_deltas` and
  `upsert_matched_total`. Bad venue payloads should be rejected, not
  promoted into the persistence layer.
- **Recommended fix:** post-parse, reject `is_nan()`, `is_infinite()`, and
  negative values; fall back to `Decimal("0")` and log.

### 6. `_matched_size` collapses cumulative vs. delta on schema drift
- **File:** `services/reconciliation.py:83-93`.
- **Issue:** the helper picks the first present key from
  `("size_matched", "filled", "matched_size", "filled_size")` and treats it
  as a **cumulative** matched size. On Polymarket today `size_matched` is
  cumulative; on some venues `filled` is the per-update delta. If the
  schema ever shifts to a delta field the reconciler will overwrite
  `matched_total` with a smaller-than-cumulative number and lose history.
- **Recommended fix:** make cumulative-vs-delta an explicit, named parameter
  or use only one key (`size_matched`) and fail loudly on its absence.

### 7. `_classify_order_state` mislabels a full venue fill as a `partial_fill`
- **File:** `services/reconciliation.py:317-374`.
- **Issue:** when `local_record` exists, the only branches are
  unexpected-cancellation and the catch-all `venue_matched > local_matched`
  partial-fill recorder. A venue order whose `status` is in
  `_TERMINAL_FILLED_STATUSES` and whose `venue_matched > local_matched`
  is classified, persisted, and logged as `partial_fill` even though it is
  the *final* fill. The metric `partial_fills_observed` therefore double-
  counts: once when the order partially fills, again when it terminally
  fills. Downstream alerting that assumes "partial fill = order still open"
  will misroute.
- **Recommended fix:** branch on `venue_status` first: terminal-filled →
  `kind="full_fill"`, otherwise `kind="partial_fill"`. Add the matching
  field to `ReconciliationResult` and to the tests.

### 8. Terminal-filled venue orders are likely invisible to the reconciler
- **File:** `services/reconciliation.py:406-418`.
- **Issue:** `get_orders()` on py-clob-client returns *open* orders. The
  classifier's `unexpected_fill` branch fires only when
  `local_record is None` and the venue status is in
  `_TERMINAL_FILLED_STATUSES`, but the venue will not include those orders
  in an open-orders listing in the first place. The new "unexpected fill"
  detection therefore covers a narrow window (the brief tick where the
  venue still reports the order as live with a terminal status flag in
  flight) and is effectively dead code for most CLOB implementations.
- **Recommended fix:** call out the gap in the docstring, or extend
  `_fetch_open_orders` to additionally fetch a recent-trades / order-history
  endpoint and reconcile against that.

### 9. `record_buy` runs after submission acceptance but before the BUY is
durable
- **File:** `orchestrators/trade_pipeline.py:2167-2178`.
- **Issue:** the eager-spend semantics mean the ledger inflates on every
  cancelled / rejected / FOK-failed BUY. There is no compensating
  `release()` path on reject/cancel; the silent "except Exception: pass"
  around `record_buy` also means a transient bug never surfaces. Worst case:
  a market that rejects every order for an hour will exhaust the 24h spend
  cap with zero actual notional spent.
- **Recommended fix:** see fix for #1 — pair record/commit with
  release on failure paths.

### 10. Pre-loop `snapshot.free_cash_usdc` is used as a per-BUY headroom
input
- **File:** `orchestrators/trade_pipeline.py:1828-1865`.
- **Issue:** `snapshot.free_cash_usdc` is computed once before the signal
  loop. Each subsequent BUY in the same pass evaluates
  `check_buy(free_cash_usdc=snapshot.free_cash_usdc)` with the **stale**
  pre-loop value, never decremented for in-flight BUYs already submitted in
  this pass. With `MIN_BALANCE_HEADROOM_USDC` set, a single BUY is gated
  correctly but a burst of N BUYs in one tick all see the same headroom
  budget. Combined with #2 above the floor is more nominal than enforced.
- **Recommended fix:** decrement `free_cash_usdc` locally inside the loop
  by each accepted BUY's notional before the next iteration's `check_buy`,
  or fold the headroom check into a stateful method on the regime that
  applies the same intra-loop reservation logic the daily ledger should.

---

## MEDIUM

### 11. Book-depth band rejects a tradeable BBO when the spread is wide
- **File:** `services/execution/book_spread.py:115-167`.
- **Issue:** the band is `mid * (1 ± band_pct/100)`. With
  `band_pct=5%` and a wide book (bid 0.40 / ask 0.60 → mid 0.50, band
  `[0.475, 0.525]`), both BBO levels fall **outside** the band and
  `side_notional_usdc` collapses to 0. The depth gate therefore rejects
  marketable orders whose BBO has plenty of size but is just wider than the
  band. Defensible as a "wide spread = thin" heuristic, but the spread
  gate is supposed to own that decision; this duplicates it implicitly.
- **Recommended fix:** clamp the band to *at least* `max(band_high, best_ask)`
  for BUY (`min(band_low, best_bid)` for SELL), or always include the BBO
  level regardless of band, so the depth metric reflects *what the order
  would actually consume*.

### 12. `BOOK_DEPTH_BAND_PCT` validation forbids `0` even when the depth gate
is disabled
- **File:** `config/app_config.py:651-665`.
- **Issue:** `if not (0 < e.book_depth_band_pct <= 100)` requires
  `band_pct > 0` unconditionally, even when `min_book_depth_usdc == 0`
  (gate disabled). Operators who wipe both knobs to "disabled" will hit a
  startup config error.
- **Recommended fix:** only validate the band when the gate is enabled
  (`min_book_depth_usdc > 0`), or accept `band_pct == 0` and treat the
  resulting empty band as a no-op.

### 13. `_normalize_levels` does not log on bad input
- **File:** `services/execution/book_spread.py:69-97`.
- **Issue:** any malformed level (`(price, size)` not a 2-tuple, `Decimal`
  conversion failure, negative size, zero price) is silently dropped. For
  a money-handling path this is the wrong default — a venue that starts
  emitting a new payload shape will silently zero out depth and reject
  every BUY with `book_thin_depth`.
- **Recommended fix:** count drops and emit a single
  `logger.warning("book_normalize_dropped_levels", count=N)` per call when
  `N>0`, and a metric for ops.

### 14. `ReconciliationResult.errors` is a mutable list on a frozen dataclass
- **File:** `services/reconciliation.py:40-51`.
- **Issue:** `@dataclass(frozen=True)` with `errors: list[str] =
  field(default_factory=list)` blocks reassignment but does not prevent
  callers from mutating the list post-construction. If the result is ever
  cached or compared across passes, surprises follow.
- **Recommended fix:** make it `tuple[str, ...]` (consistent with
  `partial_fill_deltas` which already is a tuple).

### 15. Partial-fill delta is stored as a *string*, not a Decimal
- **File:** `services/reconciliation.py:243, 295`.
- **Issue:** the public `partial_fill_deltas: tuple[tuple[str, str], ...]`
  loses precision metadata. Consumers must `Decimal(str(...))` again to do
  math, and `str(Decimal)` uses scientific notation for some values
  (`Decimal("1E+2")` from `("100" - "0")` simplifications) which downstream
  parsers may mishandle.
- **Recommended fix:** publish `tuple[tuple[str, Decimal], ...]` or a small
  NamedTuple. If the API must stay string-only, normalise via
  `format(delta, "f")`.

### 16. `_order_age_seconds` ms/s heuristic crosses over at a fixed boundary
- **File:** `services/reconciliation.py:100-113`.
- **Issue:** the heuristic `ts > 1e12 → divide by 1000` flips at
  `~33,658 AD` for seconds vs `2001-09-09 01:46:40` for ms. Any timestamp
  in *seconds* later than `1e12` (~31,688 years) is wrong-handled — not a
  real worry today, but combined with epoch-microseconds payloads from
  some upstreams (`1.7e15` for a microsecond timestamp lands as "ms",
  divided by 1000 → seconds in the year 55,807) the staleness check can
  silently behave as if every order is in the past or in the far future.
- **Recommended fix:** detect ms with a `> 1e11` and µs with `> 1e14`
  thresholds, with explicit clamping and a warning when an unknown unit
  shape lands.

### 17. `_TERMINAL_CANCELLED_STATUSES` includes `"rejected"`
- **File:** `services/reconciliation.py:66`.
- **Issue:** a rejection from the matching engine is not the same as a
  cancellation. Lumping them inflates `unexpected_cancellations` metrics
  for ops who care about manual cancels vs venue rejections.
- **Recommended fix:** separate the set, or surface `kind="rejected"`
  distinctly.

---

## LOW

### 18. `BalanceDecision` dataclass exposes `skip_code` as a property aliasing
`reason`
- **File:** `services/balance_regime.py:51-54`.
- **Issue:** the two attributes are always equal. One of them is dead
  surface area; consumers will inevitably reach for one then the other and
  diff. Keep `reason`, drop the property (or vice-versa).

### 19. `int(... .to_integral_value(rounding=ROUND_CEILING))` for bps math
- **File:** `services/execution/book_spread.py:48-65`.
- **Issue:** `ROUND_CEILING` rounds **toward +∞**, which on a negative
  spread (`best_ask < best_bid` — a crossed book) rounds *up* in the
  unsigned sense and underreports the absolute spread. Crossed books
  shouldn't happen but the API is exposed to them.
- **Recommended fix:** clamp `absolute = max(absolute, Decimal(0))` before
  the bps conversion, or use `ROUND_HALF_UP`.

### 20. Tests use `asyncio.new_event_loop().run_until_complete` instead of
`pytest-asyncio`
- **File:** `tests/test_book_depth_gate.py:206, 216, 227, 237, 247`.
- **Issue:** five test methods spin up a fresh event loop each call. This
  conflicts with any session-scoped pytest-asyncio plugin and leaks event
  loops (no `.close()`), which surfaces as `ResourceWarning` under
  `-W error`. The reconciliation tests in the same PR correctly use
  `@pytest.mark.asyncio`.
- **Recommended fix:** convert the depth-gate tests to
  `@pytest.mark.asyncio async def` and drop the manual loop creation.

### 21. `test_filled_field_supported` does not assert which field "won"
- **File:** `tests/test_reconcile_phase04.py:84-108`.
- **Issue:** the test confirms a partial-fill is observed when `filled=8`
  is the only matched-size field, but it does not pin down the precedence
  order. If a later change reorders the keys in `_matched_size`, the test
  still passes; if both `filled` and `size_matched` are present and differ,
  silent corruption is possible. Add a test that asserts `size_matched`
  beats `filled` on conflict.

### 22. `BalanceRegime.daily_spent_usdc` uses `time.time()` indirectly through
`_ledger.total()`
- **File:** `services/balance_regime.py:132-138`.
- **Issue:** the property reads wall-clock time. Tests that freeze the
  clock with `freezegun` will see eviction behaviour that differs from
  production. Inject a `now_provider` into the ledger to make eviction
  deterministic in tests.

### 23. `mid` falls back to `best_ask` (or `best_bid`) when only one side
exists, but the depth band straddles non-existent territory
- **File:** `services/execution/book_spread.py:119-141`.
- **Issue:** if only the ask side exists, `mid == best_ask` and the band
  is `[best_ask*0.95, best_ask*1.05]`. Half of that band is *above* the
  best ask and contains no asks; the depth is still summed in the upper
  half but the BUY side does not benefit from any of the lower half. The
  one-sided book metric is asymmetric in a non-obvious way.
- **Recommended fix:** when one side is missing, take that side's band as
  `[best_known, best_known * (1 + band)]` (asks) or `[best_known * (1 -
  band), best_known]` (bids).

### 24. `_normalize_levels` accumulates duplicate prices but original dict
shape cannot have duplicates
- **File:** `services/execution/book_spread.py:96`.
- **Issue:** `out[price] = out.get(price, Decimal("0")) + size` only
  matters for the iterable-of-tuples shape. For the canonical
  `{price: size}` shape the de-dup is a no-op but reads as defensive
  programming for something that can't happen. Cosmetic.
- **Recommended fix:** drop the `+ size` accumulation, or add a comment
  pinning the supported shapes so a reader doesn't think tuple-of-pairs
  is a first-class API.

---

## Test quality

- The `tests/test_balance_regime.py` suite tests the regime **in isolation**.
  No test exercises the pipeline → regime integration, so the two CRITICAL
  defects above (#1, #10) are invisible. Add a pipeline test that feeds two
  BUYs through `execute_trades` with a small daily cap and asserts the
  second BUY is rejected.
- `tests/test_book_depth_gate.py` constructs a full `ExecutionService`,
  which is the right depth of test, but never checks the *log payload* of
  the rejection. The structured-log fields (`band_low`, `band_high`,
  `side_notional_usdc`) are the only observability surface for ops; assert
  them with `caplog`.
- `tests/test_reconcile_phase04.py` has no test for the
  partial-fill-then-cancel path (local_matched > 0, venue terminal cancel),
  the full-fill case (#7 above), or the no-fields-present `_matched_size`
  edge.

---

## Summary

The new code is structurally clean but ships with two real money-safety
defects (#1, #2) and one data-integrity defect (#4) that the unit tests do
not catch. Fix those before allowing Phase 04 to gate live capital. The
remaining HIGH/MEDIUM items are robustness work that can land in a
follow-up but should not slip indefinitely — particularly the cumulative
vs delta ambiguity (#6) and the full-fill mislabel (#7), which will
distort the very metrics ops will use to validate Phase 04 in production.
