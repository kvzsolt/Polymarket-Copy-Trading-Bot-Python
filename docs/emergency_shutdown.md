# Emergency Shutdown

The only runtime pause authority is the PostgreSQL trading control. Environment
files are startup configuration and are not watched.

## Immediate action

```bash
pmcopy trading pause --reason "incident: describe the trigger"
pmcopy trading status
```

Every live submission re-reads this control immediately before calling the
unified SDK. If PostgreSQL is unavailable, the permission check also fails
closed. Already-submitted or ambiguous orders are not cancelled or retried
automatically; reconciliation owns them until its `ambiguous_timeout` and
venue evidence permit a terminal decision.

If the CLI cannot reach PostgreSQL, terminate the bot process as the secondary
containment step:

```bash
docker compose stop bot
# or send SIGTERM to the native pmcopy process
```

## Evidence and recovery

1. Preserve audit logs, pending orders, reservations, fills, and the database.
2. Rotate any exposed CLOB/RPC/database credentials. Treat a leaked private key
   as a wallet incident.
3. Reconcile every `submitted_unconfirmed`, delayed, and live order.
4. Run `pmcopy doctor --live`, `pmcopy drift`, and the test/release gates.
5. Restart while still paused.
6. Resume only with an explicit operator decision:

   ```bash
   pmcopy trading resume --confirm LIVE --reason "incident resolved"
   ```

Never edit `LIVE_TRADING_ENABLED` or `KILL_SWITCH`; those names are not runtime
controls.
