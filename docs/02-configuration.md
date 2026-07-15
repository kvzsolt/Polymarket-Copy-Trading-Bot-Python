# Configuration

`AppConfig` is immutable and loaded once. There is no flat environment parser
and no hot reload.

Precedence, highest first:

1. Process variables named `PMCOPY__SECTION__FIELD`
2. Mode-`0600` `secrets.env`
3. `config/trader_discovery.yaml`
4. Optional ignored `config/runtime.local.yml`
5. Checked-in `config/runtime.yml`

The checked-in runtime and discovery YAML files ship with the package. The
optional `config/runtime.local.yml` and `secrets.env` are resolved from the
process working directory, allowing a non-editable installation to use the
operator files beside its deployment without writing into `site-packages`.

Use `config/runtime.local.example.yml` as the local template. Public wallet
addresses, manual leaders, limits, and the paper/live selector belong there.
Credentials and private RPC URLs belong in `secrets.env`:

```dotenv
POSTGRES_PASSWORD=change-me
PMCOPY__DATABASE__DSN=postgresql://bot:change-me@localhost/polymarket_copytrading_python
PMCOPY__WALLET__PRIVATE_KEY=0x...
PMCOPY__WALLET__POLYGON_RPC_URL=https://...
PMCOPY__NETWORK__RPC_URL=https://...
PMCOPY__DISCOVERY__API__TOKEN=...
```

The plain `POSTGRES_PASSWORD` name is retained only because the official
PostgreSQL image consumes it. The application itself accepts nested
`PMCOPY__...` keys only.

`database.dsn`, `wallet.funder_address`, and the Polygon RPC URL are required
even in paper mode because idempotency, reconciliation, discovery history, and
the roster are PostgreSQL-backed. Unknown YAML, secret, or nested process keys
fail validation instead of being silently ignored.

Discovery thresholds, component weights, normalization ranges, decay,
shrinkage, scheduler cadence, backfill limits, hysteresis, and rank-driven
sizing multipliers all live in `config/trader_discovery.yaml`.

Useful checks:

```bash
pmcopy config validate
pmcopy config print
python scripts/run_discovery_only.py --list-only
```

`pmcopy config print` always redacts secrets; it has no unsafe unredacted mode.
