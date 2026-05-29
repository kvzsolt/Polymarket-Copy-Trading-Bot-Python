# Runbook — Manual / off-bot position changes

If you (or anything other than the bot) trade against the same wallet
while the bot is running, the bot will see SELL-side
`insufficient_balance` / `insufficient_allowance` errors and classify
them as state drift. Handling is automated, but operators should still
verify.

## Symptoms

- Audit lines: `manual_position_drift_reconciled`, `sell_skipped_drift`,
  `holdings_resync_applied`.
- A SELL intent fails with `insufficient_balance` or
  `insufficient_allowance` even though the local position shows enough
  shares.
- `bot drift check` reports rows, often with persisted size > live size.

## Diagnosis

1. Confirm the SELL drift was classified as **non-retryable**:
   ```bash
   grep manual_position_drift_reconciled logs/audit.jsonl | tail
   ```
   The classifier should *not* retry these — retrying eats fee budget
   without changing the outcome.
2. Run `bot drift check` to see the post-classifier delta. If the delta
   is non-zero, the holdings-resync did not run yet (or failed).
3. Check whether the operator action was an outright SELL (live size
   smaller) or a full close (live size zero). Both are handled, but they
   produce different audit lines.

## Resolution

The system handles this automatically:

1. Classifier marks the SELL as non-retryable drift.
2. `services/execution/reconciliation_service.py` fetches live holdings
   for `(user, token)`.
3. If live size is zero → local position is deleted; reservations
   released.
4. If live size is smaller → local position is shrunk; reservations
   adjusted.
5. `manual_position_drift_reconciled` is logged with the before/after
   sizes.

Operator actions:

- **Do not** retry the SELL by hand. Let the reconciler-owned path run.
- **Do not** edit the SQLite `positions` table to "match" — that races
  the reconciler.
- If the bot is *not* running when you trade manually, that's fine;
  startup reconciliation will catch the drift before any new trading
  loop starts.
- If you need to permanently exclude a position from the bot's view,
  remove the leader/market from your follow config (see
  `../06-leader-selection.md`) — that's a config-level action, not a
  runtime action.

## Verification

```bash
.venv/bin/python bot.py drift check
.venv/bin/python bot.py reservations list
grep manual_position_drift_reconciled logs/audit.jsonl | tail -5
```

Drift check should be empty, reservation totals should match expected
exposure, and the audit log should show the resync events you expect.

## Post-mortem notes

- Track how often manual drift happens. Frequent drift is a signal that
  the operator is using the same wallet for both manual and automated
  trading — recommend a separation of wallets.
- Never weaken the SELL-side error classifier to "retry on
  `insufficient_balance`" — that is the rule that prevents the bot from
  spinning on a manually-closed position.
