# Live Trading Cutover

Live execution has two independent, fail-closed gates:

1. `TRADING_MODE=live` in `config/runtime.local.env`, read once at startup.
2. The PostgreSQL `trading_control` row, checked immediately before every venue submission.

There is no `--live` CLI override and environment files are not hot-reloaded.

## Prepare

```bash
cp secrets.env.example secrets.env
chmod 600 secrets.env
```

Populate the private key, RPC URL, database URL, and optional CLOB credentials.
Set the public wallet address and leaders in `config/runtime.local.env`. The
unified SDK detects EOA/proxy/Safe/deposit-wallet signing mode; do not configure
a numeric signature type.

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
