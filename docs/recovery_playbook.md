# Recovery Playbook

When to use: bot crashed, container restarted, machine rebooted, or you
followed `emergency_shutdown.md` and now want to bring trading back.

## Boot-time convergence flow

`app.main()` runs these phases in fixed order before any monitor task is
scheduled:

1. **`startup_reconcile_unconfirmed`** — polls the venue for every order row
   in `submitted_unconfirmed`, `delayed`, `order_live`, `matched_pending_
   confirmation`. Applies incremental cumulative-matched deltas only:
   ```
   apply = max(venue_cumulative_matched - locally_applied_matched, 0)
   ```
   Idempotent. Cancelled/expired/rejected/invalid rows release their
   reservation through the reconciler. Confirmed rows consume the reservation
   once.
2. **`startup_drift_check`** — `PositionDriftService.scan_user` per user. For
   each `(user_id, condition_id, token_id)` scope it compares local position
   size against the venue's reported size, plus active reservations. Any
   non-clean record sets `readiness.drift_ok=False` and adds the scope to the
   paused set.
3. Monitors start (`risk_monitor_loop_for_contexts`, leader monitors).

Reordering is enforced by `tests/release_gate/test_startup_recovery.py`.

## Ambiguous-timeout handling

If an order sat in `submitted_unconfirmed` longer than
`ORDER_CONFIRMATION_MAX_WAIT_SECONDS` (default 90s), the reconciler stamps it
`ambiguous_timeout` and emits an audit event. Reservation stays **active**.

After an additional `ORDER_CONFIRMATION_AUTO_RELEASE_GRACE_SECONDS`
(default 300s), the strict probe path runs:

1. `clob_gateway.get_order(venue_order_id)` — must NOT report live/matched/delayed.
2. `clob_gateway.get_orders_for_market(market_id)` — must NOT list the order.
3. `clob_gateway.get_fills(market_id)` — must NOT include a fill for this order.
4. Local `positions_repo.get_position` — must show no size delta consistent with this order.

All four must be clean. Any error counts as a failure unless
`ORDER_CONFIRMATION_AUTO_RELEASE_MAX_PROBE_ERRORS > 0`. Any inconclusive result
also blocks if `ORDER_CONFIRMATION_AUTO_RELEASE_REQUIRE_ALL_PROBES_CLEAN=true`
(the default).

When every probe is clean → `auto_released_ambiguous` lifecycle log fires,
reservation released with `reason="auto_release_after_ambiguous_timeout"`,
metric `reconciliation_auto_release_ambiguous_total` incremented.

If the probe path declines to release, the row stays `ambiguous_timeout`
and the operator must reconcile manually:

```bash
python -m bot_cli.main orders --status ambiguous_timeout
sqlite3 data/bot_state.db "SELECT order_id, request_json, response_json, updated_at FROM orders WHERE status='ambiguous_timeout';"
```

Decide based on venue history whether the order ever filled. If it did,
update the row's matched_total + status manually via the CLI repair command.
If it never filled, release the linked reservation explicitly.

## Drift recovery

Drift scopes pause all coordinator submits with `skip_code="position_drift_
unresolved"`. To resolve:

1. Read the JSONL `position_drift` entries for the affected `(condition_id,
   token_id)`.
2. Verify local vs venue size via `python -m bot_cli.main positions`.
3. If local is stale → run `python -m bot_cli.main drift --resolve
   --condition <cond_id> --token <token_id>` after correcting the local row
   (or, when `POSITION_DRIFT_ALLOW_AUTO_PRUNE_ZERO=true` and venue==0 and no
   active reservation, the startup sweep already pruned automatically).
4. If venue position is unexpected → manual review on Polymarket. Do not
   restart live until you know who placed the trade.

## Operator inspection checklist after any restart

- [ ] `startup_reconcile_unconfirmed` summary: 0 orphan rows.
- [ ] `startup_drift_check` summary: `drift_ok=True`.
- [ ] No `ambiguous_timeout` rows in `orders`.
- [ ] No `active` reservation without a linked `order_id`.
- [ ] Strict `/health/ready` returns 200 with no failed keys.
- [ ] First `order_lifecycle` log after restart shows `submitted_unconfirmed`
      → `live` (or `delayed`) → `confirmed` for any in-flight order.

If any item is unchecked, stay in paper mode until it clears.
