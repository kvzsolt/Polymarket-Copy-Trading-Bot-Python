# Runbook — Stale submission locks

Orders sit in a submission lock while the bot is calling the CLOB. If the
process crashes or the venue hangs, the lock can outlive the
`EXECUTION_PENDING_SUBMISSION_TTL_SECONDS` budget. The reconciler clears
these on startup, but operator inspection is sometimes faster.

## Symptoms

- `bot stale-locks list` returns rows.
- New intents for the same `PositionKey` rejected with
  `lock_held` / `pending_submission`.
- Audit log shows `submission_lock_acquired` without a matching
  `submission_lock_released` for the same `intent_id` past the TTL.

## Diagnosis

```bash
.venv/bin/python bot.py stale-locks list
.venv/bin/python bot.py stale-locks list --ttl-seconds 60   # tighter window
.venv/bin/python bot.py stale-locks list --user-id <user>   # one user only
```

For each row, check:

- `status` — usually one of `pending_lock`, `submitting`, `pending`,
  `submitted_unconfirmed`, `delayed`.
- `age_seconds` — how long the lock has been held.
- `order_id` — populated means the order *was* sent and the reconciler
  should resolve it; empty means the request never reached the venue.

## Resolution

1. **Prefer reconciler-driven recovery.** Restart the bot with
   `KILL_SWITCH=true` set; startup reconciliation fetches the live order
   state and either confirms, cancels, or releases each stuck lock.
2. **Manual release is a last resort.** Only use it when the reconciler
   has run and a row remains stuck because the venue has no record of
   the order. The CLI provides no force-release command by design — open
   a SQLite shell against `data/copy_bot.sqlite3` and update the row by
   hand only after capturing the row contents to the incident log.
3. After release, the reservation tied to the intent must also clear.
   Cross-check with `bot reservations list --user-id <user>`.

## Verification

```bash
.venv/bin/python bot.py stale-locks list
.venv/bin/python bot.py reservations list
.venv/bin/pytest tests/release_gate/test_cli_stale_locks.py -v
```

Stale lock list should be empty (or contain only fresh, in-flight rows).

## Post-mortem notes

- A stale lock that the reconciler could not resolve is a bug. File it
  with the row contents (redact wallet addresses) and the corresponding
  audit-log slice.
- Repeated stale locks after restart usually indicate clock skew between
  the bot host and the venue — verify NTP.
