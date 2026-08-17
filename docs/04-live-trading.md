# Live Trading Cutover

Live execution has two independent, fail-closed gates:

1. `trading_mode: live` in `config/environments/live/execution.yml`, selected with `PMCOPY_ENVIRONMENT=live`, read once.
2. The PostgreSQL `trading_control` row, checked immediately before each venue
   submission.

There is no `--live` CLI override and configuration is not hot-reloaded.

```bash
mkdir -p config/environments/live && cp config/environments/example/execution.yml config/environments/live/execution.yml
cp secrets.env.example secrets.env
chmod 600 secrets.env
```

Populate the private key, database DSN, and `POSTGRES_PASSWORD` in
`secrets.env`. The checked-in config supplies a public Polygon RPC; override
`PMCOPY__NETWORK__RPC_URL` only for a private provider. CCXT's
`load_api_credentials` uses the private key for L1 credential create-or-derive,
keeps L2 credentials in `exchange.options`, and signs with the configured
Deposit Wallet using POLY_1271/signature type 3.

Validate in paper mode:

```bash
uv sync --frozen --extra dev
pmcopy config validate
pmcopy doctor --paper
uv run pytest -q
pmcopy trading pause --reason "preflight"
pmcopy run
```

After reconciliation, data streams, database health, wallet identity, and the
doctor checks are healthy, stop the paper process, change `trading_mode` to
`live`, restart, then explicitly resume:

```bash
pmcopy doctor --live
pmcopy trading resume --confirm LIVE --reason "preflight passed"
pmcopy trading status
```

Deployments and tests must never resume trading automatically. Pause with
`pmcopy trading pause --reason "..."` before maintenance or investigation.
