# Runbook — Reconciliation drift

Reconciliation drift means the bot's persisted state and the venue's
state disagree about a `PositionKey`. The reconciler is authoritative; if
it cannot converge, that is the incident.

## Symptoms

- Audit lines: `reconciliation_drift`, `reconciliation_unresolved`,
  `fill_apply_skipped`.
- Metrics: `reconciliation_drift_total` rising,
  `reconciliation_unresolved_intents` non-zero.
- `bot drift check` reports rows.
- A position you expect to hold is missing, or shows a size different from
  the venue.

## Diagnosis

```bash
.venv/bin/python bot.py drift check
.venv/bin/python bot.py drift check --user-id <user>
.venv/bin/python bot.py drift check --epsilon 0.0001   # loosen tolerance
```

Each row reports `(user_id, condition_id, token_id)`, the persisted
size, and the live size. Possible causes:

1. **Off-bot trade** — operator placed a manual order. See
   [manual-off-bot-changes](manual-off-bot-changes.md).
2. **Missed fill** — a WSS gap and the resync REST snapshot did not
   converge.
3. **Double-apply** — a fill was applied twice (should be impossible
   given the reconciler's incremental cumulative-matched-size delta;
   evidence of a regression).
4. **Order id divergence** — a venue rewrote the `order_id`; reconciler
   should fall back to idempotency key only for legacy rows.

## Resolution

1. **Pause trading** (`KILL_SWITCH=true`).
2. Restart the bot. Startup reconciliation runs before the trade and
   risk loops; it fetches authoritative venue state and applies positive
   deltas only.
3. Re-run `bot drift check`. If the drift is gone, clear the kill
   switch.
4. If drift persists for the same `PositionKey` after restart, treat as
   a state-corruption incident:
   - Snapshot the relevant rows from `positions`, `fills`, `orders`,
     `intents`, `reservations`.
   - Compare against the venue API for the same `(user, condition,
     token)`.
   - If the venue is right (it usually is), shrink/delete the local
     position by hand only after capturing both states.

## Verification

```bash
.venv/bin/python bot.py drift check
grep reconciliation_unresolved logs/audit.jsonl | tail
.venv/bin/pytest tests/release_gate/test_cli_drift.py -v
```

`drift check` should return no rows; the audit log should show no new
`reconciliation_unresolved` lines after restart.

## Post-mortem notes

- Capture which reconciler path failed (`order_id` lookup vs idempotency
  fallback).
- If the cause was a WSS gap, follow up by validating the resync logic
  in `clients/user_ws.py`.
- Never weaken the "positive incremental delta only" rule to make a row
  reconcile.
