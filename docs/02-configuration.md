# 02 - Configuration Reference

The bot uses a strictly typed configuration system (`config.app_config.AppConfig`). It fails closed if required variables are missing.

## File Structure
- `secrets.env`: Holds sensitive keys (ignored by git).
- `db.env`: Holds only `POSTGRES_PASSWORD` for the Compose PostgreSQL service (ignored by git).
- `config/runtime.local.env`: Holds your operational overrides (leaders, sizing, limits).

## Core Variables

### Identity & Funding (`secrets.env` & `runtime.local.env`)
- `PRIVATE_KEY`: Your Polygon wallet private key. (Required for live trading).
- `FUNDER_ADDRESS`: The public address of the wallet funding the trades.
- `USER_ADDRESS`: Fallback leader wallet to follow (if explicit list is empty).
- `LEADER_WALLETS`: Comma-separated list of wallets to strictly follow.
- `FALCON_API_TOKEN`: Used by the Discovery engine to fetch wallet history.

### Risk & Sizing (`runtime.local.env`)
- `DAILY_DRAWDOWN_LIMIT_PCT`: Max percentage of portfolio you are willing to lose in 24h before the kill switch triggers.
- `MAX_POSITION_SIZE_USDC`: Hard cap on how much USDC can be allocated to a single position, regardless of the leader's sizing.
- `CIRCUIT_BREAKER_ENABLED`: Set to `true` to enable global trading halts on anomalous errors.

### Auto Discovery Engine
- `FALCON_MIN_WIN_RATE_15D`: Minimum win rate (e.g., 0.55) to consider a candidate.
- `FALCON_MIN_PNL_15D`: Minimum profit in the last 15 days.
- `DISCOVERY_DYNAMIC_MONITORING_ENABLED`: Set `true` to let the bot auto-rotate leaders.

## Multi-User / Allocator Mode
If running for multiple sub-accounts, you can specify `MULTI_USER_CONFIG_FILE`. Keep this blank for standard single-wallet operations.

The file is validated strictly (fail-closed): unknown keys are rejected at
every level, `enabled` accepts only JSON booleans or the exact strings
`"true"`/`"false"` (case-insensitive; absent means enabled), and money fields
(`allocation_pct`, `max_trade_usdc`, `max_market_exposure_usdc`) must be
decimal **text** (`"12.5"`, never a JSON number) with `allocation_pct` a
fraction of leader notional (`0 < x <= 1`, so `"0.07"` = 7%) and the USDC
caps strictly positive. Leader ids and `funder_address` must be valid
Ethereum addresses, and duplicate copier ids, duplicate wallet identities
(same funder or same private key), and duplicate leader ids inside one
user's follows are hard errors.

## Follow Whitelists (who gets copied)

Per-copier `follows` configuration is an **explicit whitelist** and the source
of truth. A leader wallet with no follow entry for a copier is **not** copied —
there is no implicit union of the global/discovered leader set into every
account. This applies at startup, to newly discovered leaders, and to the CLI.

**Legacy single-account migration.** When exactly ONE user account exists AND
its follows did not come from explicit configuration — i.e. no
`MULTI_USER_CONFIG_FILE` is set and the account is the config-less fallback —
the historical default still applies: that lone account auto-follows every
global leader (sized by the global `sizing`/`risk` configuration). The
default keys on the absence of explicit follows *configuration*, not on the
in-memory follows map being empty, so it applies at startup AND at every
leader refresh: the config-less lone account keeps auto-following newly
discovered/promoted leaders, exactly like the pre-whitelist behavior
(including `--auto-trader` startup). The moment the account is loaded from a
multi-user configuration — any follow entry (enabled or disabled), or even an
explicitly empty follows list — or a second account exists, the migration no
longer applies and absence of an entry means not-following.

An empty whitelist (or disabling all entries) stops **new** copy entries only.
Existing open positions keep their protective exits, reconciliation, and risk
monitoring: the runtime builds risk-only contexts for accounts with open
positions, and all of it runs through the same account-owned coordination
state (one position lock map and one intent registry per follower account,
shared by every leader binding and the execution coordinator).

## Cross-Process Account Lease (one active writer per account)

Every copier account is guarded by a database-backed lease
(`account_execution_leases`, one row per account) so two processes configured
for the same wallet + database can never both be active writers. The lease is
acquired at startup **before any venue runtime or user state is built**; a
second process that finds an active lease **fails closed** and exits with a
non-zero code instead of starting.

- **Identity**: the lease key is the persisted `user_id` string (typed as
  `CopierAccountId` at runtime). It is never the wallet, a display name, or
  any other renamable surface — renaming an account's configuration cannot
  mint a new ledger or a new lease.
- **Mode**: ONE lease per account regardless of `execution.trading_mode`.
  Paper and live processes contend for the same row, because both write
  money-shaped rows (reservations, orders, positions) to the shared
  database. The holder's mode is recorded on the row for observability only.
- **TTL / renewal**: a lease expires 60 seconds after its last heartbeat
  (`DEFAULT_LEASE_TTL_SECONDS`). The holder renews every 15 seconds
  (`DEFAULT_RENEWAL_INTERVAL_SECONDS`) via a supervised loop; the interval
  is deliberately far below the TTL so a handful of transient database
  errors never expire a live holder's lease (a transient renewal failure is
  logged and retried on the next tick).
- **Loss**: if a renewal confirms another process took the lease over, the
  manager raises `AccountLeaseLost` and the supervisor fail-closes the
  process — it never keeps trading un-leased.
- **Graceful shutdown**: SIGTERM/SIGINT releases every held lease before
  the database pool closes, so a restart acquires cleanly.
- **Crash / takeover**: a crashed holder (no release) keeps blocking new
  acquires until the TTL lapses; after expiry the next process takes over
  and logs an `account_lease_takeover` warning. Operators seeing that
  warning should confirm the previous process is really dead before
  assuming single-writer semantics were continuous.

Leases are per-account: two accounts in one process hold independent rows
and never conflict. The lease table is additive (migration
`20261011_13_account_execution_leases`) and applied automatically at
startup by the standard migration runner.

## Path Resolution & Packaged Resources (installed package)

The bot ships as the `pmcopy` package (`pip install` from the wheel/sdist).
Packaged defaults — the service YAML tree (`app.yml`, `execution.yml`, …,
`environments/<name>/`, `traders/*.yml`) and the SQL migrations under
`pmcopy/storage/postgres/migrations/` — are **readonly inputs** resolved
through `importlib.resources`, so a wheel install in a read-only
site-packages serves them unchanged and nothing is ever written into the
package tree. `python -m pmcopy`, the `pmcopy` console script, and
`pmcopy config print | validate` all work with the checkout absent from
`sys.path` (proven by `scripts/verify_package.py` and the
`tests/release_gate/test_packaging_proofs.py` gate).

### Deployment root (deployment-local files)

`secrets.env`, the deprecated `config/runtime.local.yml` shim, `log/`, and
`data/` are **deployment-local**, never package-relative. The deployment
root is resolved explicitly, in this order:

1. `PMCOPY_DEPLOYMENT_ROOT` environment variable — when it names an
   existing directory;
2. otherwise the process CWD.

**Parent directories are never walked.** A leftover `secrets.env` in an
ancestor directory is not discovered: run the bot from the deployment root
or export `PMCOPY_DEPLOYMENT_ROOT` explicitly. `secrets_path` /
`config_dir` arguments to the loader are the explicit override level above
both.

### Configuration layer precedence (lowest → highest)

1. typed defaults baked into the config models;
2. packaged service files (`pmcopy/config/<service>.yml` via
   `importlib.resources`, overridable wholesale by an explicit
   `config_dir` / `_CONFIG_ROOT` seam);
3. legacy `<deployment root>/config/runtime.local.yml` shim (translated,
   deprecated);
4. `<config_dir>/environments/<PMCOPY_ENVIRONMENT>/<service>.yml`;
5. `<config_dir>/traders/<name>.yml` (canonical followed-leader files);
6. legacy `<config_dir>/leaders/<wallet>.yml`;
7. program `overrides`;
8. `<deployment root>/secrets.env` (`PMCOPY__SECTION__FIELD`, mode 0600);
9. process `PMCOPY__SECTION__FIELD` variables.

`PMCOPY_ENVIRONMENT` (explicit argument → process env → `secrets.env` →
`paper`) selects the environment layer; it is never inferred from
credential presence.

### Logs and writable state

The default log directory is `<deployment root>/log` (never derived from
the installed module's location). The `logging.log_dir` /
`logging.app_log_jsonl_file` settings and explicit `log_dir` arguments to
`configure_logging()` override it. Relative `logging.*` paths resolve
against the CWD as before. In every case the JSONL sink and rotating files
land outside the installed package tree; the packaging battery asserts no
`*.jsonl` or `log*` directory appears under site-packages.

### Verifying an install

`scripts/verify_package.py` builds the wheel + sdist (`uv build`, or
`python -m build` when available) and proves both from fresh virtual
environments: entry points, `python -m pmcopy`, config validation against
an explicit external deployment root, `importlib.resources` lookups, a
deterministic paper-mode workflow smoke, deployment-root-honoring writable
paths, and a clean `pip check`. The sdist includes `tests/` (current
behavior); the wheel does not contain tests, secrets, or state.
