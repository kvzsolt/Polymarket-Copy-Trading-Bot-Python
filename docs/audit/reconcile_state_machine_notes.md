# Reconciliation / reservation state machine — verified invariants (PR1)

Step 7 was a **verification** pass: develop already implements the deep-research
reconciliation safety contract. No production change was required; a guard test
was added to keep the fail-safe config defaults from being weakened.

## Lifecycle states
`domain/order_lifecycle.py::OrderLifecycleStatus` —
`CREATED → RESERVED → SUBMITTING → SUBMITTED_UNCONFIRMED → {SUBMITTED_DELAYED |
ORDER_LIVE} → MATCHED_PENDING_CONFIRMATION → {PARTIALLY_FILLED | FILLED} →
TRADE_MINED → TRADE_CONFIRMED`, with failure/ambiguous arms
`REJECTED`, `TRADE_RETRYING`, `RECONCILIATION_REQUIRED`, `AMBIGUOUS_TIMEOUT`,
`AUTO_RELEASED_AMBIGUOUS`. Transitions guarded by
`services/execution/state_machine.py` (`ALLOWED_TRANSITIONS`).

## Ambiguous-release policy (the safety core)
`services/execution/reconciliation_service.py::_maybe_auto_release_ambiguous`:

1. `_is_past_ambiguous_timeout` + `_is_past_auto_release_grace` only **trigger**
   the probes — they never release on their own. **No TTL-only release.**
2. `_run_auto_release_probes` runs all four probes (get_order live?, fills
   present?, position size matches?, get_orders_for_market) and classifies each
   `clean / inconclusive / error`.
3. `probe["errors"] > auto_release_max_probe_errors` (default 0) → **hold**.
4. `auto_release_require_all_probes_clean` (default True) and
   `probe["inconclusive"] > 0` → **hold**.
5. Only when every probe is clean → release.

Pinned by `tests/test_ambiguous_timeout_auto_release.py`
(`..._fires_when_all_probes_clean`, `..._aborted_when_get_order_returns_live`,
`..._aborted_when_fills_present`, `..._aborted_on_probe_error`,
`..._aborts_when_fills_throw_delayed_api`, `..._disabled_via_config`) and the
new `tests/test_config.py::...test_order_confirmation_safe_release_defaults`.

## Duplicate-submit prevention
Atomic submission lock in the orders repo guarantees exactly-one submission per
logical fill; the non-acquirer fails closed before the gateway. Pinned by
`tests/test_execution_atomic_lock.py`
(`..._blocks_duplicate_concurrent_submission`,
`..._n_concurrent_identical_intents_yield_exactly_one_submission`,
`..._two_races_only_one_executes`) and `tests/test_execution_idempotency_extended.py`.

## Deferred to PR2 (onchain detection)
`(tx_hash, log_index)` mined-fill dedupe and pending→mined reconciliation belong
where `log_index` is produced — the `OrderFilled` log detector. `log_index` does
not yet exist anywhere in the codebase.
