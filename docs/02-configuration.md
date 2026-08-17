# Configuration

`AppConfig` is immutable and loaded once. There is no flat environment parser
and no hot reload.

Precedence, highest first:

1. Process variables named `PMCOPY__SECTION__FIELD`
2. Mode-`0600` `secrets.env`
3. Programmatic overrides (tests/composition roots)
4. Per-leader overrides `config/leaders/<wallet>.yml`
5. Environment overrides `config/environments/<PMCOPY_ENVIRONMENT>/<service>.yml`
   (the environment is selected explicitly; default `paper`, never inferred
   from credential presence)
6. Deprecated legacy shim: an operator's untracked `config/runtime.local.yml`
   (legacy section names translated at the loader boundary; planned removal)
7. Canonical checked-in `config/<service>.yml` (one file per owning service)
8. Typed model defaults

Legacy `PMCOPY__NETWORK__*` / `PMCOPY__LEADER__*` / `PMCOPY__POLLING__*` /
`PMCOPY__ONCHAIN_DETECTION__*` names keep working through the same
translation table. `secrets.env` is resolved from the deployment root
(`PMCOPY_DEPLOYMENT_ROOT` or marker walk-up). See
`config/environments/example/execution.yml` for an environment template.
Public wallet addresses, manual leaders, limits, and the paper/live selector
belong in environment files. Credentials and private RPC URLs belong in
`secrets.env`:

```dotenv
POSTGRES_PASSWORD=change-me
PMCOPY__DATABASE__DSN=postgresql://bot:change-me@localhost/polymarket_copytrading_python
PMCOPY__WALLET__PRIVATE_KEY=0x...
# Optional private provider override:
# PMCOPY__NETWORK__RPC_URL=https://...
# Optional Falcon integration:
# PMCOPY__DISCOVERY__API__TOKEN=...
```

The plain `POSTGRES_PASSWORD` name is retained only because the official
PostgreSQL image consumes it. The application itself accepts nested
`PMCOPY__...` keys only.

`database.dsn` and the Polygon RPC URL are required even in paper mode because
idempotency, reconciliation, discovery history, and the roster are
PostgreSQL-backed.

`wallet.funder_address` is required in paper mode: paper never authenticates,
so the CCXT transport resolves no wallet and this public address is the
identity the balance and live-position health probes use. In live mode it is
the verified Deposit Wallet funder address CCXT signs against — it must be
set and must match the wallet derived from `PMCOPY__WALLET__PRIVATE_KEY` or
startup fails closed.

Unknown YAML, secret, or nested process keys fail validation instead of being
silently ignored.

Discovery thresholds, component weights, normalization ranges, decay,
shrinkage, scheduler cadence, backfill limits, hysteresis, and rank-driven
sizing multipliers all live in `config/discovery.yml`.

Useful checks:

```bash
pmcopy config validate
pmcopy config print
python scripts/run_discovery_only.py --list-only
```

`pmcopy config print` always redacts secrets; it has no unsafe unredacted mode.
