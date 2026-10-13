# Polymarket Copy Trading Bot (`pmcopy`)

![Python 3.14](https://img.shields.io/badge/python-3.14-blue.svg)
![License MIT](https://img.shields.io/badge/license-MIT-green.svg)
![Polymarket](https://img.shields.io/badge/exchange-Polymarket-blueviolet)

A standalone, single-process Polymarket copy-trading bot: it follows selected
leader wallets, sizes copies against your configured risk limits, executes
through `ccxt.prediction.polymarket` (the only authenticated trading
transport), and reconciles every submission against the venue with
PostgreSQL-backed idempotency.

This is a **local system**. There is no control plane, no operator HTTP API,
no dashboard, and no remote command surface — every operator action goes
through the `pmcopy` CLI on the host (or `docker compose exec`). The process
exposes only a loopback health/metrics endpoint for supervision.

---

## What it does

- **Copy decision engine** — reconstructs leader positions and computes
  percentage sell-offs, so partial exits and scaling are mirrored, not just
  "buy when the leader buys".
- **Risk & sizing** — portfolio-aware sizing from balance, exposure, daily
  drawdown limits, and per-position caps; the risk engine can shrink or
  reject a copy that does not fit your limits.
- **Execution & reconciliation** — asynchronous FAK/FOK submission through
  the CCXT adapter with explicit price limits, durable idempotency keys,
  atomic budget reservations, push-first fill evidence (user WSS) with REST
  fallback, and startup reconciliation of ambiguous submissions. An
  ambiguous POST is never blindly retried.
- **Trader discovery (optional)** — Falcon-based analytics rank copyable
  wallets against your bankroll; promotion/demotion runs locally with
  paper-evidence gates (`--auto-trader`).
- **Multi-copier (optional)** — one process can run several copier accounts,
  each with its own wallet, limits, and explicit leader whitelist, guarded by
  a database lease so two processes can never both write one account.

## Stack

- **Python** 3.14 only (`>=3.14,<3.15`); 3.11–3.13 are unsupported.
- **Venue transport** `ccxt==4.5.87` (`ccxt.prediction.polymarket`) behind a
  typed gateway; paper mode uses CCXT for public market reads and a local
  simulator for writes.
- **Persistence** PostgreSQL (orders, fills, positions, reservations,
  leases) with additive, schema-versioned migrations applied at startup.
- **Package layout** `src/pmcopy/` — install as a wheel; packaged defaults
  (service YAML, environments, traders, SQL migrations) resolve through
  `importlib.resources`, so an installed copy never depends on the checkout.
- **Operator CLI** `pmcopy` (or `python -m pmcopy`).

## Quickstart (single copier, paper mode)

Requires Python 3.14.x and either [uv](https://docs.astral.sh/uv/) or pip.

```bash
# 1. Install (uv, recommended)
uv venv && source .venv/bin/activate
uv sync --frozen --extra dev
#   -- or with plain pip --
# python3.14 -m venv .venv && source .venv/bin/activate
# pip install -r requirements.txt && pip install --no-deps -e .

# 2. Configure (from the deployment root — the directory you run from)
cp secrets.env.example secrets.env && chmod 600 secrets.env
cp db.env.example db.env          && chmod 600 db.env   # only if using Docker Compose
```

Edit `secrets.env` (only `PMCOPY__*` nested keys are accepted). For paper
mode the minimum is the database DSN, a Polygon RPC URL, and the public
funder address — the private key is **not** required in paper:

```env
PMCOPY__DATABASE__DSN=postgresql://bot:<password>@localhost:5432/polymarket_copytrading_python
PMCOPY__POLYMARKET__RPC_URL=https://polygon-mainnet.g.alchemy.com/v2/<your-key>
PMCOPY__WALLET__FUNDER_ADDRESS=0x<your-public-funder-address>
# PMCOPY__WALLET__PRIVATE_KEY=   # live only
```

Leaders are configured in `<config-dir>/traders/<name>.yml` (one file per
leader, the canonical roster surface) or discovered automatically. Create
the database (any PostgreSQL 16 instance; a Compose file is included) and
verify everything before starting:

```bash
# 3. Validate and diagnose
pmcopy config validate
pmcopy doctor

# 4. Run in paper mode
pmcopy run
```

`execution.trading_mode` (default `paper`) is the authoritative paper/live
control — it is never inferred from which credentials are present. Live mode
additionally requires the durable database trading control to permit
submissions (see `pmcopy trading`).

## Multi-copier usage

Enable the schema-v2 roster by pointing `app.copiers_config_file` at a copy
of `src/pmcopy/config/copiers.example.yml`, e.g. in `secrets.env`:

```env
PMCOPY__APP__COPIERS_CONFIG_FILE=/path/to/copiers.yml
PRIMARY_FUNDER_ADDRESS=0x1111111111111111111111111111111111111111
PRIMARY_PRIVATE_KEY=          # optional in paper
```

The roster is **secret-free by design**: wallet fields are environment
variable *names*, never values. All addresses below are synthetic.

```yaml
schema_version: 2
copiers:
  - id: primary                      # stable persisted ledger identity
    enabled: true
    mode: paper                      # must match execution.trading_mode
    wallet:
      funder_address_env: PRIMARY_FUNDER_ADDRESS
      private_key_env: PRIMARY_PRIVATE_KEY
    limits:
      max_order_quote: "5.00"        # exact decimal text, USDC
      max_open_exposure_quote: "50.00"
    follows:                         # explicit whitelist
      - leader_address: "0x1111111111111111111111111111111111111111"
        enabled: true
        sizing:
          mode: fixed                # mirror leader notional, capped per order
          quote_amount: "2.00"
```

Rules enforced fail-closed: strict booleans, exact decimal money, valid
addresses, no duplicate copier ids / wallets / leaders, one process-wide
mode, `follows` as a strict whitelist (empty = no new copies, protection
continues). Run one copier selectively with `pmcopy run --copier primary`;
without the flag every enabled copier starts, each holding its own
database lease.

## Docker

```bash
docker compose config --quiet
docker compose build --pull bot
docker compose up -d postgres
docker compose run --rm --no-deps bot doctor
docker compose up -d --no-build bot
docker compose logs -f bot
```

Secrets are split per service: `db.env` holds only `POSTGRES_PASSWORD` and
is read only by the postgres service; `secrets.env` holds the bot's
credentials and is read only by the bot. The image is a multi-stage
wheel-only build on `python:3.14.6-slim-bookworm` — the runtime stage never
contains the source tree. See [docs/docker.md](docs/docker.md).

## Operator CLI

```text
pmcopy run [--auto-trader|--auto-follow] [--copier ID]   main loop
pmcopy doctor [--live]                                  connectivity/diagnostic checks
pmcopy config print|validate|explain                    typed config inspection
pmcopy orders / positions / status                      read-only state views
pmcopy reconcile                                        trigger unconfirmed-order reconciliation
pmcopy reservations / stale-locks / drift               budget + drift inspection
pmcopy trading status|pause|resume|full-stop|clear-full-stop
pmcopy redeem [--execute]                               gasless redemption of resolved positions
pmcopy resolve                                          operator resolution of ambiguous orders
pmcopy rotate-traders / profile-leader / history-import  discovery tooling
```

Trading control notes: `trading full-stop` blocks new live submissions
durably and `trading resume` **refuses** to clear it — only the explicit
`pmcopy trading clear-full-stop --confirm LIVE --reason ...` action does.
See [docs/recovery_playbook.md](docs/recovery_playbook.md) and
[docs/live_runbook.md](docs/live_runbook.md).

## Operations pointers

- **Single active writer per account** — a database-backed lease
  (`account_execution_leases`) is acquired at startup before any runtime is
  built; a second process for the same account fails closed. After a crash,
  takeover happens when the 60s TTL lapses (logged as
  `account_lease_takeover`).
- **Logs** — structured JSONL + console under `<deployment root>/log/`;
  secrets are redacted across all sinks. Golden fixtures in
  `tests/fixtures/golden_logs/` pin the format.
- **Health** — `/health` and `/health/ready` on the metrics server
  (loopback 9102 by default); readiness reflects actual runtime state.
- **Runbooks** — [docs/runbooks/00-index.md](docs/runbooks/00-index.md),
  [docs/emergency_shutdown.md](docs/emergency_shutdown.md),
  [docs/redemption.md](docs/redemption.md).

## Development

```bash
uv sync --frozen --extra dev

.venv/bin/python -m pytest -q                      # default tier
.venv/bin/python -m pytest -m integration -q       # shared-Postgres tier (one container, no churn)
.venv/bin/python -m pytest -m release_gate -q      # release gates
```

Dependency changes go through the lock: edit `pyproject.toml`, run
`uv lock` (uv pinned to 0.11.31 in CI), then
`python scripts/generate_requirements.py` to regenerate
`requirements.txt` / `requirements.runtime.txt`; `python
scripts/check_requirements_drift.py` guards byte-exact consistency.

Further reading: [docs/00-read-this-first.md](docs/00-read-this-first.md),
[docs/02-configuration.md](docs/02-configuration.md),
[docs/03-paper-trading.md](docs/03-paper-trading.md) →
[docs/04-live-trading.md](docs/04-live-trading.md),
[docs/architecture.md](docs/architecture.md),
[docs/refactor/handoff.md](docs/refactor/handoff.md).

## Bug reports

Open a [GitHub Issue](https://github.com/kvzsolt/Polymarket-Copy-Trading-Bot-Python/issues/new)
with reproduction steps, expected vs actual behavior, redacted log snippets,
and your Python version and OS.

## Contributing

1. Fork, create a feature branch, commit, push, open a PR.
2. Every behavior change ships with a failing-then-passing regression test;
   run the focused test, the adapter contract group (for CCXT-touching
   changes), and the full default suite before submitting.

---

*Disclaimer: This software is for educational and experimental purposes only.
Trading involves risk, and this system does not guarantee profit. You can and
may lose money. Run at your own risk.*
