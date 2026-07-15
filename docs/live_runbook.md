# Live Operations Runbook

## Start

1. Keep `execution.trading_mode: paper` for the paper soak.
2. Run `pmcopy doctor --paper`, the unit suite, and Docker-backed integration
   tests while the runtime remains in paper mode.
3. Stop the bot, set `execution.trading_mode: live` in
   `config/runtime.local.yml`, restart, and run `pmcopy doctor --live`;
   startup configuration is immutable.
4. Confirm reconciliation, wallet identity, database, RPC, market data, and
   user-stream readiness while the database control remains paused.
5. Run:

   ```bash
   pmcopy trading resume --confirm LIVE --reason "operator preflight passed"
   pmcopy trading status
   ```

## Observe

```bash
pmcopy status
pmcopy orders pending
pmcopy reservations list
pmcopy drift check
```

Investigate `submitted_unconfirmed`, stale market data, reconciliation blocks,
wallet drift, or an open circuit breaker before allowing another order.

## Pause

```bash
pmcopy trading pause --reason "maintenance or incident"
```

The control is checked immediately before every live SDK submission and is
shared by all bot processes using the database. A database outage also blocks
new live submissions. See `docs/emergency_shutdown.md` for containment and
recovery.
