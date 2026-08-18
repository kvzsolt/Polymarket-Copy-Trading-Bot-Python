# Live Operations Runbook

## Start

1. Keep `execution.trading_mode: paper` for the paper soak.
2. Run `pmcopy doctor --paper`, the unit suite, and Docker-backed integration
   tests while the runtime remains in paper mode.
3. Stop the bot, set `execution.trading_mode: live` in
   `config/environments/live/execution.yml` (with `PMCOPY_ENVIRONMENT=live`), restart, and run `pmcopy doctor --live`;
   startup configuration is immutable.
4. Confirm reconciliation, wallet identity, database, RPC, market data, and
   user-stream readiness while the database control remains paused.
5. Run:

   ```bash
   pmcopy trading resume --confirm LIVE --reason "operator preflight passed"
   pmcopy trading status
   ```

## Enable the operator API

`config/environments/live/control_plane.yml` sets `enabled: true`, so the live
environment starts the operator API on `127.0.0.1:9103`. It is authenticated
and fails closed: startup aborts with
`control_plane auth token is required ...` until the credential exists.

1. Generate two unrelated random values (for example
   `python -c "import secrets; print(secrets.token_urlsafe(32))"`, run twice)
   and put them in `secrets.env`, which stays mode `0600` and untracked:

   ```dotenv
   PMCOPY__CONTROL_PLANE__AUTH__TOKEN=<operator value>
   PMCOPY__CONTROL_PLANE__AUTH__VIEWER_TOKEN=<viewer value>
   ```

   The viewer entry is optional; when set it must differ from the operator
   entry, or startup fails closed. The viewer principal can read every resource
   and follow the live stream, and is rejected with `403` on every mutation —
   give a dashboard that one, never the operator value.

2. Restart the bot. Startup is the only time configuration is read.
3. Verify from the same host, keeping the real value in a shell variable so it
   never lands in a command line, a log, or shell history:

   ```bash
   read -rs TOKEN                 # paste the operator value, press enter
   curl -sS -H "Authorization: Bearer $TOKEN" \
     http://127.0.0.1:9103/api/v1/system/status
   curl -sS -o /dev/null -w '%{http_code}\n' \
     -H "Authorization: Bearer $TOKEN" \
     http://127.0.0.1:9103/api/v1/system/capabilities
   unset TOKEN
   ```

   A missing or wrong credential returns `401`; a credential without the route
   permission returns `403`. Never paste a real credential into a document, a
   ticket, or a chat.

The bind stays on loopback. Expose it only through a same-origin reverse proxy
that terminates TLS and authenticates separately; a non-loopback bind without
authentication is rejected at startup. See `docs/frontend-control-plane.md` for
the route, permission, and stream contract.

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

The control is checked immediately before every live CCXT submission and is
shared by all bot processes using the database. A database outage also blocks
new live submissions. See `docs/emergency_shutdown.md` for containment and
recovery.
