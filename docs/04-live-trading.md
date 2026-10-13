# Live Trading Cutover

Live execution has two independent, fail-closed gates:

1. `execution.trading_mode=live` (e.g. `PMCOPY__EXECUTION__TRADING_MODE=live`
   in `secrets.env`, or the `PMCOPY_ENVIRONMENT=live` overlay), read once at
   startup.
2. The PostgreSQL `trading_control` row, checked immediately before every venue submission.

There is no `--live` CLI override and environment files are not hot-reloaded.

## Prepare

```bash
cp secrets.env.example secrets.env
chmod 600 secrets.env
```

Populate the private key, RPC URL, and database URL in `secrets.env`
(`PMCOPY__WALLET__PRIVATE_KEY`, `PMCOPY__POLYMARKET__RPC_URL`,
`PMCOPY__DATABASE__DSN`). No CLOB apiKey/secret/passphrase belong there —
the CCXT adapter derives L2 credentials in memory from the private key, and
live signing is fixed to the Deposit Wallet (signature type 3). Set the
public funder address and leaders in `<config-dir>/traders/<name>.yml` (the
funder address is an optional live cross-check; the verified Deposit Wallet
funder is resolved at runtime).

Install and validate:

```bash
uv sync --frozen --extra dev
pmcopy doctor
uv run pytest -q
```

Keep the control paused while starting:

```bash
pmcopy trading pause --reason "preflight"
pmcopy run
```

After startup reconciliation, market/user streams, database health, wallet
identity, and the doctor output are all healthy, explicitly resume:

```bash
pmcopy trading resume --confirm LIVE --reason "preflight passed"
pmcopy trading status
```

The resume command is an operator action; deployments and tests must never run
it automatically.

## Docker

Set a non-default `POSTGRES_PASSWORD` in `db.env` (see `db.env.example`; the
postgres service reads nothing else), keep `secrets.env` and `db.env` mode
`0600`, and run:

```bash
docker compose up -d
docker compose exec bot pmcopy trading status
```

PostgreSQL is bound to `127.0.0.1` for development and remains reachable to the
bot over the internal Compose network.

## Stop or pause

Use `pmcopy trading pause --reason "..."` for an immediate submission stop.
Use SIGTERM, `Ctrl+C`, or `docker compose stop bot` for graceful process
shutdown. Reconciliation remains mandatory before the next resume.
