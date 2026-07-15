# Live Trading Cutover

Live execution has two independent, fail-closed gates:

1. `execution.trading_mode: live` in `config/runtime.local.yml`, read once.
2. The PostgreSQL `trading_control` row, checked immediately before each venue
   submission.

There is no `--live` CLI override and configuration is not hot-reloaded.

```bash
cp config/runtime.local.example.yml config/runtime.local.yml
cp secrets.env.example secrets.env
chmod 600 secrets.env
```

Populate the private key, RPC URL, and database DSN in `secrets.env`. Set the
public funder address and manual leaders in `config/runtime.local.yml`.

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
