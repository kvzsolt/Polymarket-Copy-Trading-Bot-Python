# `pmcopy` — Polymarket Copy-Trading Engine (Standalone Lite Edition)

![Python 3.13](https://img.shields.io/badge/python-3.13-blue.svg)
![Venue](https://img.shields.io/badge/venue-Polymarket-6f42c1.svg)
![Transport](https://img.shields.io/badge/transport-ccxt%204.5.92-1f2937.svg)
![Persistence](https://img.shields.io/badge/state-PostgreSQL%2016-336791.svg)

**Copy smart money on Polymarket without donating your bankroll to float-rounding bugs, blind retries, or whale exit liquidity.**

`pmcopy` is a production-hardened, single-process Python 3.13 copy-trading engine for Polymarket. It watches target wallets on Polygon in real time, reconstructs what they are *actually* doing (including partial trims and full exits), sizes entries against your bankroll and risk caps using exact `Decimal` math, executes via `ccxt.prediction.polymarket`, and reconciles every order in PostgreSQL so a network hiccup never turns one `$10` trade into five. 🙂

---

## Wait—What Do You Mean "Lite (Lobotomized) Edition"?

Full transparency: this repository is the **standalone, self-hosted Lite engine** surgically detached—lovingly *lobotomized*, if we are being honest—from our full **Hosted Telegram Copy-Trading Bot**.

We took out the cloud control plane, the chat UI, the managed multi-tenant infrastructure, and the cross-venue router, and left the **entire institutional execution core** intact so engineers and self-custody purists can run it on their own metal.

| What You Get Here (`pmcopy` Open-Source Lite) | What Lives in the Hosted Telegram Bot |
|---|---|
| Pure local CLI (`pmcopy`) + Docker Compose—zero cloud middleman | Zero-DevOps cloud runtime controlled straight from Telegram 💬 |
| Self-hosted PostgreSQL 16 ledger, leases, & crash recovery | Instant tap-to-copy wallets, live PnL cards, & push alerts |
| Polymarket CLOB execution + Polygon WSS detection | **Polymarket live today + Kalshi support landing soon** 🚀 |
| Full risk engine, 7 sizing modes, multi-copier YAML, & Falcon discovery | Managed infra, zero RPC/Postgres babysitting, & hosted UX |

If you enjoy `chmod 600 secrets.env`, reading structured JSONL logs at 2 AM, and owning every packet between your box and Polygon: **stay right here, you are among friends.** If you would rather tap three buttons on your phone while waiting for coffee (and copy across **Kalshi** when it drops), that is what the hosted Telegram bot is for.

---

## Everything This Engine Actually Does (Zero Vaporware)

Every feature listed below is wired, tested, and shipping inside `src/pmcopy/`:

### 1. Real-Time On-Chain Leader Detection (3 Selectable Modes)
Powered by Alchemy Polygon WebSockets with automatic HTTPS RPC derivation and hourly bandwidth/CU guards (`PMCOPY__MAX_HOURLY_WS_MB`):
- **`on_chain_logs_only`** *(default)* — Server-side per-leader filtered `OrderFilled` event logs. Near-zero compute-unit burn and zero mempool noise.
- **`on_chain_pending`** — Exchange-locked Polygon mempool `matchOrders` stream with fast hex calldata decoding (`calldata_decoder`) for sub-block signal visibility.
- **`confirmed+mempool_reserved`** — Pre-reserves budget on mempool sight and promotes on confirmed `OrderFilled` receipts, backed by `mined_fill_deduplicator` and `source_transaction_gate` so the same fill is never copied twice.

### 2. True Position & Exit Reconstruction (Not Just "Buy When Whale Buys")
Most toy copy bots buy happily and leave you holding the bag when the leader scales out. `pmcopy` tracks leader inventory:
- **Proportional Sell-Off Mirroring** — `leader_sell_fraction_resolver` and `leader_sell_scope_guard` calculate the exact fraction of a position the leader exited and scale your `SELL` in shares against your confirmed holdings.
- **V1 Copy-Task Engine (Optional)** — Per-task USDC budgets (`budget.py`), lot-level accounting (`lots.py`), re-entry locks (`reentry_lock.py`), source freshness windows, and task-scoped protective exits.

### 3. Seven Polymorphic BUY Sizing Modes & Exact `Decimal` Math
Zero `float` drift anywhere on the money path—every price, share, tick, and USDC cap is `Decimal` end-to-end:
- **Sizing Strategies** — `fixed`, `exact_mirror`, `proportional`, `adaptive`, `tiered` (step-threshold multipliers), `balance_weighted`, and `portfolio_weighted` (`copy_sizing_v5`), plus classic range-tiered pipeline sizing (`copyStrategy`).
- **Arbitrary Tick Precision** — Native support for `0.01`, `0.005`, `0.0025`, and `0.001` Polymarket ticks (`domain/price_bound.py`) with conservative `BUY` ceiling / `SELL` floor snapping. Missing `tick_size`, `min_order_size`, or `neg_risk` fails closed—no silent `0.01` fallback exists anywhere.

### 4. Multi-Layer Risk & Market-Quality Firewall
Before a single order touches the venue, it has to survive:
- **Portfolio Caps** — Per-order USDC max, per-market open exposure cap (`max_market_exposure_usdc`), 24-hour wallet-percentage cap (`MAX_POSITION_PCT`), daily drawdown breaker, and balance-tiered risk regimes (`BalanceRegime`).
- **Orderbook Quality Gates** — Live spread (bps) and book depth verification (`book_spread.py`), price-band guards, caller-limit slippage clamping, geoblock checks on entries, and a shared venue `CircuitBreaker` for CLOB outages.
- **Automated Protective Risk Monitor** — Continuous stop-loss / take-profit / drawdown monitoring (`risk_monitor.py`) with bounded FAK exit reposting and retired-market (`404`) sentinel handling.

### 5. Crash-Safe CCXT Execution & Ambiguity-Preserving Reconciliation
- **Single Authenticated Transport** — All private order placement and cancellation runs exclusively through `ccxt.prediction.polymarket` (`ccxt==4.5.92`) using Polymarket Deposit Wallet signing (`POLY_1271` / signature type 3), with CLOB API credentials derived in memory from your EOA key.
- **Hard Paper / Live Airgap** — Paper mode reads live Polymarket books via CCXT and routes writes to `PaperOrderSimulator` (paper mode can never invoke a private write method). Live mode is controlled strictly by `execution.trading_mode` **plus** a durable database trading gate—never inferred just because a private key happens to sit in `secrets.env`.
- **Atomic Reservations & Zero Blind Retries** — `pg_advisory_xact_lock` SQL reservations prevent concurrent signals from overspending your wallet. Write-ahead pending state is persisted *before* network transmit; if a POST times out or disconnects (`submitted_unconfirmed`), `pmcopy` **never blindly retries**. Push-first User WSS + REST `ReconciliationService` proves what happened on the venue first—including mandatory startup reconciliation before live streams open.

### 6. Multi-Copier Fleet Support & Quantitative Discovery
- **Multi-Copier Isolation (Schema v2)** — Run several independent follower accounts in one process (`src/pmcopy/config/copiers.example.yml`). Each copier gets its own wallet env-var pointers (YAML stays 100% secret-free), independent limits, explicit leader whitelist, and a PostgreSQL single-writer lease (`account_execution_leases`, 60s TTL) so two processes can never trade the same account at once.
- **Falcon Wallet360 Trader Discovery (Optional)** — Score and rank candidate wallets (`rotate-traders`, `profile-leader`, `run --auto-trader`) by **bankroll-compatible copyability**: multi-window consistency, liquidity/capacity, drawdown, concentration, bot-signal detection, and paper-evaluation gates before promotion.
- **Gasless Relayer Redemptions & Drift Forensics** — Redeem resolved winning/losing positions gaslessly via Polymarket Relayer (`pmcopy redeem`), audit manual/off-bot wallet changes (`pmcopy drift check`), and resolve edge-case stuck orders with automatic JSON pre-write backups (`pmcopy resolve`).

---

## Requirements

- **Python `3.13.x` only** (`>=3.13,<3.14` — enforced at package install and at runtime so native `coincurve` EIP-712 signing stays active; Python 3.11, 3.12, and 3.14 are intentionally rejected).
- **PostgreSQL 16** (local service or the included `docker-compose.yml` container; schema migrations apply automatically on startup).
- **Alchemy Polygon WebSocket key** (`wss://polygon-mainnet.g.alchemy.com/v2/<your-key>`).

---

## Quickstart (Single Copier, Paper Mode)

Paper mode uses **real live Polymarket orderbooks and real on-chain leader trades**, while simulating fills locally. You do not even need a private key to test drive it.

### 1. Install

```bash
# Recommended: uv
uv venv --python 3.13
source .venv/bin/activate
uv sync --frozen --extra dev

# Alternative: standard pip
# python3.13 -m venv .venv && source .venv/bin/activate
# pip install -r requirements.txt && pip install --no-deps -e .
```

### 2. Configure `secrets.env`

```bash
cp secrets.env.example secrets.env && chmod 600 secrets.env
cp db.env.example db.env           && chmod 600 db.env   # only needed if running Postgres via Docker Compose
```

Edit `secrets.env` in your deployment directory (only `PMCOPY__*` nested keys are read):

```dotenv
PMCOPY__DATABASE__DSN=postgresql://bot:<password>@localhost:5432/polymarket_copytrading_python
PMCOPY__ALCHEMY_WSS_URL=wss://polygon-mainnet.g.alchemy.com/v2/<your-alchemy-key>

# Paper mode only needs a public funder address (0x...); private key stays empty:
PMCOPY__WALLET__FUNDER_ADDRESS=0xYourPublicDepositWalletAddress
# PMCOPY__WALLET__PRIVATE_KEY=

# Detection mode: on_chain_logs_only | on_chain_pending | confirmed+mempool_reserved
PMCOPY__DETECTION__MODE=on_chain_logs_only
```

### 3. Add Leaders to Follow

Configure one YAML file per target wallet in `config/traders/<name>.yml` inside your deployment directory (or inspect the packaged examples under `src/pmcopy/config/traders/`), or run with `--auto-trader` when Falcon discovery (`PMCOPY__DISCOVERY__API__TOKEN`) is configured.

### 4. Validate, Run Doctor, & Launch

```bash
# Check config types, bounds, and secret hygiene
pmcopy config validate

# Run connectivity & readiness checks for paper mode
pmcopy doctor --paper

# Start the copy-trading runtime (paper mode by default)
pmcopy run
```

---

## Going Live (Without Shooting Your Foot Off)

`pmcopy` refuses to trade live by accident. Entering live mode requires **all three** explicit steps:

1. **Supply your EOA signer key** in `secrets.env` (`PMCOPY__WALLET__PRIVATE_KEY=0x...`, signing for your Polymarket Deposit Wallet via `POLY_1271` / signature type 3—CLOB credentials are derived in memory, never stored on disk).
2. **Select the live environment** (`PMCOPY_ENVIRONMENT=live` or `PMCOPY__EXECUTION__TRADING_MODE=live`).
3. **Pass live diagnostics and arm the durable database trading control**:

```bash
PMCOPY_ENVIRONMENT=live pmcopy doctor --live
PMCOPY_ENVIRONMENT=live pmcopy trading status
PMCOPY_ENVIRONMENT=live pmcopy trading resume
PMCOPY_ENVIRONMENT=live pmcopy run
```

Need an emergency brake?
- `pmcopy trading pause` — Immediately blocks new live venue submissions.
- `pmcopy trading full-stop` — Durable emergency stop: blocks every live submission **and cancels all resting venue orders**. By design, `pmcopy trading resume` refuses to override a `full-stop`; clearing it requires an explicit operator confirmation (`pmcopy trading clear-full-stop`).

---

## Multi-Copier Fleet Usage (Schema v2)

Want one process to run multiple isolated wallets with different strategies and leader whitelists? Copy `src/pmcopy/config/copiers.example.yml` to your deployment config directory and point `PMCOPY__APP__COPIERS_CONFIG_FILE` at it:

```dotenv
PMCOPY__APP__COPIERS_CONFIG_FILE=/path/to/copiers.yml
PRIMARY_FUNDER_ADDRESS=0x1111111111111111111111111111111111111111
PRIMARY_PRIVATE_KEY=
```

The roster file itself is **100% secret-free**—wallet fields hold environment variable *names*, never raw keys:

```yaml
schema_version: 2
copiers:
  - id: primary
    enabled: true
    mode: paper                        # must match execution.trading_mode
    wallet:
      funder_address_env: PRIMARY_FUNDER_ADDRESS
      private_key_env: PRIMARY_PRIVATE_KEY
    limits:
      max_order_quote: "5.00"          # exact Decimal USDC string
      max_open_exposure_quote: "50.00"
    follows:
      - leader_address: "0x2222222222222222222222222222222222222222"
        enabled: true
        sizing:
          mode: fixed
          quote_amount: "2.00"
```

Run every enabled copier in one process, or isolate a single copier per container/process:

```bash
pmcopy run                    # runs all enabled copiers, each acquiring its own DB lease
pmcopy run --copier primary   # runs only 'primary'
```

---

## Docker Compose Deployment

The `Dockerfile` builds a clean multi-stage wheel image on `python:3.13.15-slim-bookworm` (the runtime container never carries the git checkout or test tree, and database credentials in `db.env` stay isolated from bot credentials in `secrets.env`):

```bash
docker compose config --quiet
docker compose build --pull bot
docker compose up -d postgres
docker compose run --rm --no-deps bot doctor --paper
docker compose up -d --no-build bot
docker compose logs -f bot
```

---

## Complete `pmcopy` CLI Reference

Every operator action happens locally via `pmcopy` (or `python -m pmcopy` / `docker compose exec bot pmcopy`):

| Command | Description |
|---|---|
| `pmcopy run [--auto-trader] [--copier ID]` | Start the copy-trading loop (`--auto-trader` / `--auto-follow` enables automated DB discovery roster; `--copier` scopes to one copier). |
| `pmcopy doctor (--paper \| --live)` | Verify DB migrations, RPC/WSS connectivity, CCXT venue compatibility, wallet readiness, and config invariants. |
| `pmcopy status` | Display current runtime, account lease, and ledger summary. |
| `pmcopy config print \| validate \| explain` | Print resolved config (secrets redacted), validate fail-closed rules, or explain any config key. |
| `pmcopy orders pending \| reconcile` | Inspect non-terminal orders or trigger reconciliation of unconfirmed orders. |
| `pmcopy positions [--user-id ID] [--json]` | List open follower positions and cost basis. |
| `pmcopy reconcile [--dry-run] [--user-id ID]` | Reconcile `submitted_unconfirmed` orders against venue order/fill history. |
| `pmcopy reservations list \| resolve` | Inspect active budget/share reservations and per-user totals, or safely release proof-verified expired holds. |
| `pmcopy stale-locks list \| resolve` | Inspect or expire submission locks stuck past TTL. |
| `pmcopy drift check` | Compare PostgreSQL position inventory against live on-venue holdings to flag manual/off-bot trades. |
| `pmcopy trading status \| pause \| resume \| full-stop \| clear-full-stop` | Inspect or toggle the durable fail-closed live trading permission gate. |
| `pmcopy redeem [--dry-run \| --execute] [--user-id ID] [--json]` | Preview (`--dry-run`, default) or submit (`--execute`) gasless redemptions for resolved positions via Polymarket Relayer. |
| `pmcopy resolve (--order-id ID \| --idempotency-key KEY) [...]` | Inspect evidence or record an audited manual terminal resolution (`filled`, `canceled`, `expired`, `confirmed_absent`) with automatic JSON backup. |
| `pmcopy rotate-traders [--rescore-only]` | Run Falcon Wallet360 discovery and print the hysteresis-qualified leader roster (`--rescore-only` uses cached snapshots). |
| `pmcopy profile-leader --wallet 0x... [--lookback-days N] [--output PATH]` | Profile a leader wallet's trading behavior and emit a structured JSON strategy profile. |
| `pmcopy history-import --plan PATH [--journal PATH] [--execute-read-only]` | Preview or collect reviewed historical protocol evidence. |

### Observability & Logs
- **Loopback Health & Metrics** — Bound locally (`127.0.0.1:9102` by default) exposing `/health`, `/health/ready`, and Prometheus `/metrics`.
- **Structured Redacted Logs** — Written to `<deployment-root>/log/PolymarketCopyTradingBot.jsonl`; private keys, DSN passwords, relayer secrets, and WSS tokens are automatically scrubbed across all log sinks.

---

## Development, Testing & Wheel Verification

```bash
# Default fast unit suite (~5,200+ tests)
.venv/bin/python -m pytest -q

# Core business-logic statement coverage gate (>= 60% floor)
.venv/bin/python -m coverage erase
.venv/bin/python -m coverage run -m pytest -q
.venv/bin/python -m coverage report --fail-under=60

# Release-gate invariants (AST, signing, startup ordering, packaging safety)
.venv/bin/python -m pytest -m release_gate -q

# Live PostgreSQL 16 integration suite (uses one shared container)
.venv/bin/python -m pytest -m integration -q

# Hermetic wheel + sdist build & fresh-venv smoke battery
.venv/bin/python scripts/verify_package.py
```

---

## License & Disclaimer

Released under the **MIT License**.

*This software is provided for educational and experimental purposes only. Prediction market trading carries substantial risk of loss—copying a profitable wallet in the past is not a free money printer in the future. Test thoroughly in `paper` mode first, size conservatively, and trade strictly at your own risk.*
