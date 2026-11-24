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

## All Known Bugs Fixed & Low-Latency Architecture Upgrade

**Every previously reported execution-blocking bug has been diagnosed, fixed, and regression-tested.** If you ever watched the bot log `detection_signal | source=on_chain_logs_only` and sit silently without submitting orders, those root causes are gone for good:

1. **Zero-Latency On-Chain Block Timestamps (`signal_age_unknown` fixed)** — `AlchemyDetectionRuntime` extracts the immutable chain `blockTimestamp` directly from Alchemy `OrderFilled` logs (**0 ms fast path**), falling back to a singleflight-coalesced, LRU-cached `RpcBlockTimestampReader` (`eth_getBlockByHash`) executed concurrently with `condition_id` lookup.
2. **Fail-Closed Leader Balance via Direct Polygon RPC (`leader_pusd_balance_required` fixed)** — `require_leader_balance: true` remains enforced. On each leader `BUY` fill, the leader's Polygon funder wallet is extracted directly from the `OrderFilled` log (`maker` / `token_holder_wallet`) and queried over a persistent async HTTP/2 pool (`ProviderBalanceReader` + bounded LRU/TTL/singleflight `CachedCollateralBalanceReader`) **only when the task uses portfolio/balance-based sizing** (`proportional`/`adaptive`/`tiered` with `proportional_balance_cap_enabled: true`, `balance_weighted`, or `portfolio_weighted`). For confirmed mined BUYs, pre-trade collateral is reconstructed as `rpc_balance + trade_notional`. Non-portfolio modes (`fixed`, `exact_mirror`, or uncapped modes) incur **zero** leader-balance RPC overhead.
3. **Visible Copy-Task Skip Diagnostics & Clean YAML-to-Postgres Sync** — `CopyTaskEventProcessor`, `worker.py`, and `account_dispatch.py` now log structured `copy_task_skipped` and `copy_task_dispatched` events straight to the console and `log/PolymarketCopyTradingBot.jsonl` (no more invisible skips). New `pmcopy tasks list` and `pmcopy tasks sync [--prune]` commands push YAML changes into PostgreSQL and soft-delete orphaned tasks whose YAML files were removed.
4. **Low-Latency Hot Path** — Cached `eth_getCode` verification (halving RPC round-trips per balance check), write-through TTL + singleflight `CachedCopyTaskReader`, concurrent `asyncio.gather` follower/leader snapshot fetches, speculative task-budget pre-fetch, and bounded startup leader-balance warm-up.
5. **Spotless OOP / SOLID Codebase Refactor** — Unified all orchestrators under `src/pmcopy/orchestrators/`, eliminated every legacy `camelCase` module filename (100% strict `snake_case`), dissolved duplicate package roots (`orchestration/`, `ports/`, `models/`), organized every loose module into cohesive subsystem packages, and split multi-thousand-line god modules into clean, single-responsibility collaborators pinned by AST architecture tests.

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
- **Sizing Strategies** — `fixed`, `exact_mirror`, `proportional`, `adaptive`, `tiered` (step-threshold multipliers), `balance_weighted`, and `portfolio_weighted` (`services/copy_sizing_v5/`), plus classic range-tiered pipeline sizing (`services/trading/copy_strategy.py`).
- **Scoped Leader-Balance RPC Lookup** — When portfolio/balance-based sizing is enabled (`proportional`/`adaptive`/`tiered` with `proportional_balance_cap_enabled: true`, `balance_weighted`, or `portfolio_weighted`), `pmcopy` extracts the leader's Polygon funder wallet directly from the on-chain `OrderFilled` log and fetches their `pUSD` collateral via cached, singleflight async HTTP/2 RPC—keeping `require_leader_balance: true` fail-closed while adding zero RPC overhead to `fixed` or `exact_mirror` tasks.
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

## How to Use `pmcopy`

### Quickstart (Single Copier, Paper Mode)

Paper mode uses **real live Polymarket orderbooks and real on-chain leader trades**, while simulating fills locally. You do not even need a private key to test drive it.

#### 1. Install

```bash
# Recommended: uv
uv venv --python 3.13
source .venv/bin/activate
uv sync --frozen --extra dev

# Alternative: standard pip
# python3.13 -m venv .venv && source .venv/bin/activate
# pip install -r requirements.txt && pip install --no-deps -e .
```

#### 2. Configure `secrets.env`

Never commit credentials into YAML files. Keep all secrets in a mode-`0600` `secrets.env` file in your deployment root (`PMCOPY_DEPLOYMENT_ROOT` if set, otherwise the current working directory):

```bash
cp secrets.env.example secrets.env && chmod 600 secrets.env
cp db.env.example db.env           && chmod 600 db.env   # only needed if running Postgres via Docker Compose
```

Edit `secrets.env` (only `PMCOPY__*` nested keys and `PMCOPY_ENVIRONMENT` are read):

```dotenv
# Optional: defaults to "paper". Set to "live" only after passing `pmcopy doctor --live`.
PMCOPY_ENVIRONMENT=paper

PMCOPY__DATABASE__DSN=postgresql://bot:<password>@localhost:5432/polymarket_copytrading_python
PMCOPY__ALCHEMY_WSS_URL=wss://polygon-mainnet.g.alchemy.com/v2/<your-alchemy-key>

# Paper mode only needs a public funder address (0x...); private key stays empty:
PMCOPY__WALLET__FUNDER_ADDRESS=0xYourPublicDepositWalletAddress
# PMCOPY__WALLET__PRIVATE_KEY=

# Detection mode: on_chain_logs_only | on_chain_pending | confirmed+mempool_reserved
PMCOPY__DETECTION__MODE=on_chain_logs_only
```

#### 3. Add One or More Leaders / Copy Tasks

Create one YAML file per followed leader under `src/pmcopy/config/traders/<name>.yml` (see [How to Add a Copy Task](#how-to-add-a-copy-task) below), or run with `--auto-trader` when Falcon discovery (`PMCOPY__DISCOVERY__API__TOKEN`) is configured.

#### 4. Validate, Sync Tasks, Run Doctor, & Launch

```bash
# 1. Validate every YAML file, trader copy-task schema, bounds, and secret hygiene
pmcopy config validate

# 2. Inspect the resolved effective config (secrets automatically redacted)
pmcopy config print

# 3. Sync trader YAML configs into PostgreSQL (and prune tasks whose YAMLs were removed)
pmcopy tasks sync --prune
pmcopy tasks list

# 4. Run connectivity & readiness checks for paper mode
pmcopy doctor --paper

# 5. Start the copy-trading runtime (paper mode by default)
pmcopy run
```

---

### Configuration Architecture & Layer Precedence

`pmcopy` splits configuration into **one strict YAML file per service** under `src/pmcopy/config/` (unknown keys, `NaN`/`Inf`, and invalid bounds fail closed at startup).

Layers merge from **lowest to highest precedence**:

1. **Typed Python defaults** (`src/pmcopy/config/app_config.py`)
2. **Base service YAML files** (`src/pmcopy/config/<service>.yml`)
3. **Environment overlay YAML files** (`src/pmcopy/config/environments/<PMCOPY_ENVIRONMENT>/<service>.yml`, default environment: `paper`)
4. **Canonical trader / copy-task files** (`src/pmcopy/config/traders/<name>.yml`)
5. **Local secrets file** (`<deployment_root>/secrets.env`, `PMCOPY__*` keys)
6. **Process environment variables** (`PMCOPY__*`, e.g. `PMCOPY__EXECUTION__SLIPPAGE_TOLERANCE=6.0`)

> **Environment variable naming rule:** Nested YAML keys map to `PMCOPY__<SECTION>__<KEY>` using double underscores (`__`). For example, `copy_trading.copy_tasks.enabled` becomes `PMCOPY__COPY_TRADING__COPY_TASKS__ENABLED=true`.

---

### How to Add a Copy Task

Every followed leader and its **V1 copy-task strategy** live in a single canonical file:

```text
src/pmcopy/config/traders/<fantasy-name>.yml
```

The filename stem (e.g., `alpha_trader` from `alpha_trader.yml`) must match `[a-zA-Z0-9][a-zA-Z0-9._-]{0,63}` and becomes the default `name` / `task_id` if `name:` is omitted.

#### Step 1 — Ensure the V1 Copy-Task Engine Is Enabled (if using per-task budgets/filters)

- `src/pmcopy/config/copy_trading.yml` ships with `copy_tasks.enabled: true`.
- **Important:** `src/pmcopy/config/environments/live/copy_trading.yml` overrides `copy_tasks.enabled: false` by default (so live runs on the account-level pipeline while still using `traders/*.yml` as the leader roster).
- To run per-task workers, budgets, lot accounting, and re-entry locks in `live`, set `copy_tasks.enabled: true` in `environments/live/copy_trading.yml` or pass `PMCOPY__COPY_TRADING__COPY_TASKS__ENABLED=true`.

#### Step 2 — Create `src/pmcopy/config/traders/<your-task-name>.yml`

Copy this complete template and customize it for your target wallet:

```yaml
# src/pmcopy/config/traders/alpha_trader.yml
# Single source of truth for following one leader wallet and its V1 copy task.

name: alpha_trader                                         # Short slug / task_id (defaults to filename stem)
wallet: "0x1111111111111111111111111111111111111111"        # Required: 42-char 0x Polymarket leader address
enabled: true                                              # false pauses the leader/task without deleting DB history

# Optional: alternate on-chain addresses if the leader trades via separate EOA/proxy
# eoa_address: "0x..."
# proxy_address: "0x..."

# Optional per-leader admission overrides (omit to inherit global copy_trading.yml / execution.yml values):
# copy_decision_min_score: 0.15            # [0, 1] minimum composite trade score
# copy_max_price_drift_pct: 15.0           # max % price drift from leader fill
# copy_max_edge_decay_pct: 90.0            # max % of upside-to-1.0 lost
# copy_max_signal_age_seconds: 100.0       # max signal age in seconds (0 disables)
# copy_liquidity_target_usdc: 50.0         # leader notional for full liquidity score
max_entry_price_drift_pct: 8.0             # one-shot copy-entry FAK price band (%) for this leader

# Complete V1 Copy-Task strategy (seeded into PostgreSQL `copy_tasks` table on startup):
task:
  monitoring_mode: normal                  # normal (confirmed fills only) | priority (allows provisional pending signals)
  sizing_mode: proportional                # must match sizing.mode below
  notifications_muted: false               # true mutes per-task alert notifications

  # 1. Source notional trigger gate (0 disables each bound)
  trigger:
    enabled: false                         # false bypasses min/max source-notional checks
    min_notional: 0                        # skip leader trades smaller than this USDC notional
    max_notional: 0                        # skip leader trades larger than this USDC notional (0 = no upper cap)

  # 2. Durable per-task spend & deduplication caps (0 disables each USDC cap)
  spend:
    enabled: true                          # false bypasses task-local spend caps (account-level risk still applies)
    max_total: 1000                        # lifetime cumulative USDC spend cap for this task (0 = unlimited)
    max_daily: 250                         # daily USDC spend cap for this task (0 = unlimited)
    max_per_market: 25                     # per-market USDC spend cap for this task (0 = unlimited)
    one_entry_per_market: false            # true = at most 1 entry per market for this task
    global_one_entry_per_market: false     # true = at most 1 entry per market across ALL tasks
    one_entry_per_sports_type: false       # true = at most 1 entry per sports market type for this task

  # 3. Source price eligibility & execution slippage/offset limits
  price:
    enabled: true
    min_price: 0.20                        # [0, 1] reject BUY signals below this leader fill price
    max_price: 0.90                        # [0, 1] reject BUY signals above this leader fill price
    offset_mode: percentage                # disabled | fixed | percentage (keep aligned with max_slippage_percent)
    offset_direction: above                # above | below (direction of limit offset from leader price)
    offset: 8                              # % when percentage (<= 1.0 when fixed); avoid fixed < slippage on high prices
    max_slippage_percent: 8                # max execution slippage (%) for FAK order limit

  # 4. Reaction rules (how leader events map to follower actions)
  reaction:
    enabled: true
    follow_buys: true                      # copy leader BUY fills
    copy_sells: true                       # proportionally mirror leader SELL fills
    buy_on_sells: false                    # HIGH-RISK: invert leader sells into follower buys
    counter_trade: false                   # HIGH-RISK: take the opposite outcome side
    copy_side: both                        # both | yes_only | no_only
    copy_merges: false                     # opt into CTF merge events
    copy_splits: false                     # opt into CTF split events
    copy_conversions: false                # opt into neg-risk conversion events

  # 5. Market metadata filters (empty list / 0 = filter disabled)
  market_filter:
    enabled: true
    include_keywords: []                   # e.g. ["election", "fed"] — title must contain at least one
    exclude_keywords: []                   # e.g. ["nba", "5m"] — reject if title contains any
    categories: []                         # allow-list of market categories
    tokens: []                             # allow-list of token symbols
    timeframes: []                         # allow-list of market timeframes
    min_liquidity_usdc: 0                  # minimum market liquidity in USDC (0 = disabled)
    min_volume_24h_usdc: 0                 # minimum 24h market volume in USDC (0 = disabled)
    max_days_until_close: 0                # maximum days until market close (0 = disabled)
    allow_expired_unresolved: false        # true permits entries on expired-but-unresolved markets

  # 6. Recurring active trading window (IANA timezone; disabled = 24/7)
  schedule:
    enabled: false
    timezone_name: UTC                     # any valid IANA timezone (e.g. "America/New_York", "Europe/Budapest")
    start: "00:00"                         # HH:MM (start > end defines an overnight window, e.g. "22:00" -> "06:00")
    end: "00:00"                           # HH:MM

  # 7. Per-task BUY sizing strategy
  sizing:
    enabled: true
    mode: proportional                     # fixed | exact_mirror | proportional | balance_weighted | portfolio_weighted | adaptive | tiered
    percentage: 5                          # % of leader notional (required > 0 for proportional / adaptive / tiered)
    fixed_amount: 3                        # USDC per copy (required > 0 for fixed mode)
    trade_multiplier: 1                    # > 0 global multiplier on calculated copy size
    weighted_multiplier: 1                 # > 0 multiplier for balance_weighted / portfolio_weighted modes
    buy_min_amount: 5                      # minimum BUY notional in USDC
    buy_max_amount: 12.5                   # maximum BUY notional in USDC (0 = no task cap)
    copyable_balance_percent: 25           # (0, 100] max % of follower balance this task may size against
    proportional_balance_cap_enabled: true # cap proportional % by follower/leader balance ratio
    require_leader_balance: true           # fail closed if proportional balance cap is on and leader balance is missing
    strict_fixed_amount: true              # fixed mode: true rejects if exact amount cannot fit; false clamps
    strict_exact_mirror: true              # exact_mirror mode: true rejects if full mirror cannot fit; false clamps
```

#### Step 3 — Validate, Sync to PostgreSQL, and Start

```bash
pmcopy config validate
pmcopy tasks sync --prune    # upsert active trader YAMLs into Postgres & retire deleted YAMLs
pmcopy tasks list            # verify active task versions, offsets, and sizing modes
pmcopy run
```

At startup you will see:

```text
v1_copy_task_seed | task_id=alpha_trader leader=0x1111...1111 follower=0xYourFunder
v1_copy_task_runtime | status=started active_tasks=1 ...
```

#### Important Copy-Task Lifecycle Notes

- **Startup seeding vs. explicit sync (`pmcopy tasks sync`):** On normal boot, `pmcopy run` inserts newly added trader YAMLs (`status=created`) and leaves already-persisted DB rows untouched (`status=already_exists`) so runtime edits are never silently clobbered. Whenever you edit an existing `src/pmcopy/config/traders/*.yml` or remove a trader YAML, run **`pmcopy tasks sync --prune`** to push the updated strategy into PostgreSQL (`version` increments atomically) and soft-delete orphaned DB tasks that no longer have a YAML file.
- **Aligning `offset_mode` with `max_slippage_percent`:** If `offset_mode: fixed` uses a small offset (e.g. `0.04`) while `max_slippage_percent: 8`, trades above `$0.50` will fail the offset ceiling (`price_limit_exceeds_offset`) because `8%` of `$0.70` is `$0.056 > $0.04`. Prefer `offset_mode: percentage` with `offset` matching `max_slippage_percent` (e.g. `8` and `8`) for consistent execution across all probabilities.
- **Portfolio-based sizing & leader RPC balances:** When `proportional_balance_cap_enabled: true` (for `proportional`/`adaptive`/`tiered`) or when using `balance_weighted`/`portfolio_weighted`, `pmcopy` automatically resolves the leader's Polygon `pUSD` balance straight from the `OrderFilled` log funder wallet via cached HTTP/2 RPC and reconstructs pre-trade collateral for confirmed BUYs. Keep `require_leader_balance: true` enabled—if RPC fails, entries fail closed instead of guessing. Non-portfolio modes (`fixed`, `exact_mirror`, or uncapped modes) skip leader balance RPC entirely for minimal latency.
- **Multi-copier task IDs:** When multiple follower accounts follow the same trader file, each follower automatically receives an isolated task ID of the form `<name>-<follower_account_lowercase>`.
- **Pausing a leader:** Set `enabled: false` in the trader file and run `pmcopy tasks sync` to pause new executions without erasing historical orders or PnL.
- **Sizing mode agreement:** `task.sizing_mode` and `task.sizing.mode` must match (or omit one so the loader derives it). Supported modes: `fixed`, `exact_mirror`, `proportional`, `balance_weighted`, `portfolio_weighted`, `adaptive`, `tiered`.

---

### Complete YAML Configuration Reference (All 17 Service Files + Overlays)

All base configuration templates live in `src/pmcopy/config/`. Every file is optional and strictly validated.

| File | What It Controls | Secrets in YAML? |
|---|---|---|
| `traders/<name>.yml` | Per-leader identity, enablement, admission overrides, and full **V1 copy-task** definition | No |
| `app.yml` | Global master `kill_switch`, DB history retention, Prometheus metrics listener, and multi-copier roster path | No |
| `database.yml` | PostgreSQL connection pool sizing and query timeout (`dsn` stays empty; set in `secrets.env`) | Never (`PMCOPY__DATABASE__DSN`) |
| `wallet.yml` | Signing wallet placeholder and Polygon `chain_id` (`137` mainnet / `80002` Amoy) | Never (`PMCOPY__WALLET__PRIVATE_KEY`) |
| `polymarket.yml` | Polygon RPC URL override (auto-derived from Alchemy WSS if blank), USDC/pUSD contract addresses, geoblock enforcement | Never |
| `detection.yml` | Alchemy WSS detection mode (`on_chain_logs_only`, `on_chain_pending`, `confirmed+mempool_reserved`), hourly WSS MB ceiling, pending-tx thresholds, native SQLite state | Never (`PMCOPY__ALCHEMY_WSS_URL`) |
| `copy_trading.yml` | Global copy-decision thresholds, signal age/drift caps, tier watchlist gate, and **V1 `copy_tasks` engine** switches (workers, shared price cache, budgets, re-entry locks) | No |
| `execution.yml` | Authoritative `trading_mode` (`paper` \| `live`), execution BUY `kill_switch`, slippage, share price band, entry price drift, reservations, spread/depth gates, circuit breaker, concurrency | No |
| `sizing.yml` | Global BUY sizing engine (`fixed`, `exact_mirror`, `proportional`, `balance_weighted`, `portfolio_weighted`, `adaptive`, `tiered`), min/max caps, trust multipliers, copyable balance % | No |
| `risk.yml` | Account exposure caps (per-market, per-leader, open positions, daily drawdown), balance-tiered headroom policy, WSS risk-mark spread filters, risk-monitor cadence | No |
| `protective_exits.yml` | Automated risk-reducing exits: stop loss, near-max trim/close, profit lock, trailing stop, staged take-profit, leader-divergence exit (requires `risk.enable_risk_mgmt: true`) | No |
| `market_data.yml` | Public orderbook/metadata freshness budgets, REST snapshot/bootstrap timeouts, and WSS silence/subscription timeouts | No |
| `reconciliation.yml` | Order confirmation polling delays, ambiguity auto-release safety probes, stale-order thresholds, and startup/runtime position-drift fail-closed policy | No |
| `discovery.yml` | Falcon Wallet360 automated trader discovery: shortlist filters, Wallet360 windows, market quality, scoring weights, longitudinal decay, hysteresis, scheduler, and bankroll-fit rules | Never (`PMCOPY__DISCOVERY__API__TOKEN`) |
| `logging.yml` | Log verbosity (`INFO`/`DEBUG`), log directory, rotating JSONL audit file path, byte limit, and backup count | No |
| `redemption.yml` | Gasless Polymarket Relayer redemption metadata (credentials live strictly in `secrets.env`) | Never (`PMCOPY__REDEMPTION__RELAYER_*`) |
| `provider_budget.yml` | Local SQLite Alchemy CU usage ledger path and billing-cycle reset day | No |
| `evidence.yml` | Optional local durable observation outbox and `pmrecon` evidence publisher settings | Never (`PMCOPY__EVIDENCE__TOKEN`) |
| `copiers.example.yml` | Schema v2 multi-copier fleet roster (env-var-only wallet references, per-copier limits, explicit follow whitelists) | Never (env-var *names* only) |
| `environments/<env>/<service>.yml` | Per-environment sparse overrides selected via `PMCOPY_ENVIRONMENT` (e.g. `environments/live/*.yml`, `environments/example/*.yml`) | No |

Expand any file below to view its full template and defaults:

<details>
<summary><strong><code>app.yml</code></strong> — Global kill switch, retention, Prometheus metrics listener, and fleet roster paths</summary>

Controls process-wide safety and operational listeners. Setting `kill_switch: true` keeps detection running while blocking every submission, including exits.

```yaml
# src/pmcopy/config/app.yml
kill_switch: false                     # Global master stop (blocks ALL submissions including exits)
retention_max_age_seconds: 2592000.0   # 30 days retention for processed-trade dedup/audit rows
enable_metrics_server: false           # true starts loopback /health, /health/ready, /metrics
metrics_port: 9102                     # TCP port (1..65535)
metrics_bind_host: 127.0.0.1           # Keep loopback unless protected by firewall/VPN
multi_user_config_file: ''             # Deprecated legacy multi-user path (leave empty)
copiers_config_file: ''                # Path to schema v2 copiers.yml (mutually exclusive with multi_user)
```
</details>

<details>
<summary><strong><code>database.yml</code></strong> — PostgreSQL 16 connection pool & command timeout</summary>

Configures the mandatory PostgreSQL persistence pool used for idempotency locks, orders, positions, leases, discovery history, and V1 copy-task state. Keep `dsn` blank in YAML and set `PMCOPY__DATABASE__DSN` in `secrets.env`.

```yaml
# src/pmcopy/config/database.yml
dsn: ''                        # Set PMCOPY__DATABASE__DSN in mode-0600 secrets.env
pool_min_size: 2               # Minimum warm connections kept open
pool_max_size: 20              # Maximum concurrent connections (>= pool_min_size)
command_timeout_seconds: 30.0  # Per-SQL-command timeout in seconds
```
</details>

<details>
<summary><strong><code>wallet.yml</code></strong> — Follower wallet placeholder & Polygon chain ID</summary>

Declares the signing chain ID (`137` Polygon mainnet or `80002` Amoy testnet). Never place a real private key here—supply `PMCOPY__WALLET__PRIVATE_KEY` and `PMCOPY__WALLET__FUNDER_ADDRESS` in `secrets.env`.

```yaml
# src/pmcopy/config/wallet.yml
private_key: ''   # Keep empty in YAML; set PMCOPY__WALLET__PRIVATE_KEY in secrets.env for live mode
chain_id: 137     # 137 = Polygon Mainnet | 80002 = Amoy Testnet
```
</details>

<details>
<summary><strong><code>polymarket.yml</code></strong> — Polygon RPC endpoints, collateral token addresses, and geoblock guard</summary>

Configures chain metadata, USDC/pUSD collateral contracts, and client-side geoblock checks. Leave `rpc_url` empty to auto-derive the HTTPS endpoint from `PMCOPY__ALCHEMY_WSS_URL`.

```yaml
# src/pmcopy/config/polymarket.yml
rpc_url: ''                                                          # Empty = auto-derived from PMCOPY__ALCHEMY_WSS_URL
fallback_rpc_url: ''                                                 # Optional secondary Alchemy HTTPS URL
chain_id: 137                                                        # 137 (Polygon) | 80002 (Amoy)
usdc_contract_address: '0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB'
collateral_contract_address: ''                                      # Optional explicit override
pusd_contract_address: '0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB'
enforce_geoblock: true                                               # Fail closed on startup/entries in restricted regions
```
</details>

<details>
<summary><strong><code>detection.yml</code></strong> — Alchemy WebSocket leader detection modes & bandwidth/CU circuit breaker</summary>

Selects how leader fills are observed on Polygon and bounds WebSocket bandwidth cost (`max_hourly_ws_mb`). If a pending/mempool mode exceeds the hourly MB cap, the runtime automatically degrades in-place to `on_chain_logs_only`.

```yaml
# src/pmcopy/config/detection.yml
mode: on_chain_logs_only               # on_chain_logs_only | on_chain_pending | confirmed+mempool_reserved
fetch_interval: 0.5                    # Leader polling/refresh cadence (seconds)
aggregate_signal_window_seconds: 1     # Batch window for aggregating rapid signals
leader_positions_min_fetch_spacing_ms: 250
leader_activity_min_fetch_spacing_ms: 250
trade_queue_max_batches: 64
onchain:
  alchemy_wss_url: ''                  # Set PMCOPY__ALCHEMY_WSS_URL in secrets.env
  include_legacy_v1_exchanges: false   # true also watches legacy V1 exchange contracts
  track_direct_user_txs: false         # true adds direct-user pending tx subscriptions
  max_hourly_ws_mb: 250                # Rolling 1h WSS bandwidth ceiling before auto-downgrade
  mempool_reservation_ttl_seconds: 15.0
  subscription_debounce_seconds: 2.0
  pending_tx_hashes_only: false
  pending_tx_max_signal_age_ms: 500
  pending_tx_detection_confidence_required: pending_probable   # pending_probable | mined_authoritative
  pending_copy_confirmation_enabled: false
  pending_copy_confirmation_deadline_seconds: 60.0
  pending_copy_confirmation_max_tracked: 2048
  pending_copy_auto_unwind_enabled: false
native:
  state_path: data/native_detection.sqlite3
  traders: []
  pending_enabled: false
  maximum_inclusions: 1000000
  recovery_maximum_gap: 5000
  recovery_overlap: 8
```
</details>

<details>
<summary><strong><code>copy_trading.yml</code></strong> — Global signal admission gates & V1 copy-task runtime engine</summary>

Sets roster-wide signal freshness, price-drift, and scoring thresholds, and toggles the V1 copy-task engine (per-leader FIFO workers, shared price cache, durable task budgets, and post-exit re-entry locks). Leader wallets themselves live exclusively in `traders/*.yml`.

```yaml
# src/pmcopy/config/copy_trading.yml
copy_decision_min_score: 0.15          # [0, 1] min composite score (used when enable_copy_decision_gate: true)
copy_max_price_drift_pct: 15.0         # Max % BUY price rise from leader fill when decision gate is on
copy_max_edge_decay_pct: 90.0          # Max % of upside-to-1.0 lost
copy_max_signal_age_seconds: 100.0     # Max source signal age in seconds (0 disables hard age fail)
copy_liquidity_target_usdc: 50.0       # Leader trade notional earning full liquidity score
enable_tier_gate_watchlist: false      # true restricts copies to watchlist-qualified tiers
enable_tier_gate_watchlist_log: false  # Verbose tier-gate diagnostics

copy_tasks:
  enabled: true                        # Master V1 copy-task engine switch (overridden to false in environments/live/)
  workers:
    enabled: true                      # Per-leader FIFO serial worker queues
    queue_size: 64                     # Max buffered batches per leader
    global_concurrency: 4              # Cross-leader concurrent workers
    max_leader_workers: 64             # Hard cap on instantiated leader workers
  shared_price_cache:
    enabled: true                      # Share fresh price reads across task workers
    max_entries: 5000
  task_budget:
    enabled: true                      # Enforce durable per-task spend & one-entry reservations
    reservation_ttl_seconds: 120
  reentry_lock:
    enabled: true                      # Block immediate re-entry after a protective exit
```
</details>

<details>
<summary><strong><code>execution.yml</code></strong> — Paper/live mode switch, price/book guards, reservations, and concurrency</summary>

The single authoritative `trading_mode` (`paper` vs `live`) selector plus every pre-submit order-quality guard, slippage ceiling, book-spread/depth check, circuit breaker, and execution concurrency limit.

```yaml
# src/pmcopy/config/execution.yml
trading_mode: paper                    # paper (live reads + local sim) | live (authenticated CCXT writes)
kill_switch: false                     # true refuses BUY entries while still allowing protective SELLs
sell_fraction_missing_policy: skip     # skip (fail closed) | full_close (liquidate entire position)
slippage_tolerance: 8.0                # Slippage bound (%)
min_share_price: 0.10                  # [0, 1] binary token entry floor
max_share_price: 0.90                  # [0, 1] binary token entry ceiling (0 disables)
max_entry_price_drift_pct: 5.0         # Default one-shot FAK price drift band (%) — overridable per trader
venue_metadata_ttl_seconds: 0.0        # 0 refetches tick_size/min_order_size/neg_risk before every signed order
non_terminal_failure_max_attempts: 3   # Retry cap for classified non-terminal errors (never ambiguous POSTs)
enable_copy_decision_gate: false       # true enforces copy_trading composite scoring thresholds
enable_near_resolution_gate: false     # true enforces tighter freshness near resolution
near_resolution_gate_price: 0.95
near_resolution_max_age_seconds: 15.0
pending_submission_ttl_seconds: 120
reservation_ttl_seconds: 120
reservations_enabled: false            # true enables pre-submit SQL BUY-quote/SELL-share reservations (true in live)
book_spread_mode: absolute_bps         # absolute_bps | relative_bps | ticks
max_book_spread_bps: 2500              # 0 disables bps spread gate
max_book_spread_ticks: 0               # Required > 0 when book_spread_mode is ticks
circuit_failure_threshold: 15          # Consecutive failures before venue circuit breaker opens
max_position_size_usdc: 20.0           # Per-copy USDC safety belt
max_daily_spend_usdc: 0.0              # Gross daily submitted BUY cap (0 disables)
min_balance_headroom_usdc: 0.0         # Free cash floor kept after BUY (0 disables)
min_book_depth_usdc: 0.0               # Required same-side depth within book_depth_band_pct (0 disables)
book_depth_band_pct: 5.0               # (0, 100] half-band around mid for depth checks
execution_submit_concurrency: 4
trade_analysis_concurrency: 6
reconcile_concurrency: 4
copy_fak_max_attempts: 8
exit_fak_max_attempts: 8
```
</details>

<details>
<summary><strong><code>sizing.yml</code></strong> — Global BUY sizing strategy (7 polymorphic modes) & per-copy bounds</summary>

Translates leader trade notional and account context into an exact `Decimal` USDC BUY amount before account risk caps and venue minimums are applied.

```yaml
# src/pmcopy/config/sizing.yml
mode: proportional                               # fixed | exact_mirror | proportional | balance_weighted | portfolio_weighted | adaptive | tiered
percentage: 5.0                                  # % of leader trade (proportional / adaptive / tiered)
fixed_amount: 3.0                                # Exact USDC amount (fixed mode)
trade_multiplier: 1.0                            # Global size multiplier
tiered_trade_multipliers: ''                     # CSV threshold:multiplier pairs for tiered mode (e.g. "50:1.0,250:0.5")
adaptive_basis: trade_size                       # trade_size | performance
adaptive_min_percent: 0.0
adaptive_max_percent: 0.0                        # 0 disables interpolation and falls back to percentage
adaptive_threshold_usdc: 500.0
buy_min_amount: 5.0                              # Minimum final BUY notional (USDC)
buy_max_amount: 17.5                             # Maximum final BUY notional (USDC)
position_threshold: 0                            # Open-position count that blocks new markets (0 disables)
max_position_pct: 0.07                           # (0, 1] per-market room as fraction of follower balance
enable_trust_size_multipliers: false             # true scales size by bounded leader trust score
trust_size_multiplier_min: 0.5
trust_size_multiplier_max: 1.25
copyable_balance_percent: 50.0                   # (0, 100] % of follower balance usable for copies
proportional_balance_cap_enabled: false          # true caps proportional % by follower/leader balance ratio
require_leader_balance_for_proportional: false   # true rejects if cap is enabled and leader balance is missing
portfolio_weight_multiplier: 1.0                 # Multiplier for balance_weighted / portfolio_weighted modes
strict_fixed_amount: true                        # true rejects if fixed_amount cannot fit; false clamps
strict_exact_mirror: true                        # true rejects if full mirror cannot fit; false clamps
```
</details>

<details>
<summary><strong><code>risk.yml</code></strong> — Exposure limits, balance-tiered headroom, risk-mark quality, and monitor cadence</summary>

Controls account-level exposure caps, the continuous risk monitor loop (`enable_risk_mgmt`), WSS mark-price sanity filters, and optional balance-tiered BUY headroom regimes.

```yaml
# src/pmcopy/config/risk.yml
enable_risk_mgmt: false                     # Master risk-monitor switch (overridden to true in environments/live/)
kill_switch: false                          # Risk-layer stop
risk_mark_max_spread_bps: 2000              # Max mid-relative spread bps for usable risk mark (0 disables)
risk_mark_max_abs_distance: 0.03            # Max absolute bid/ask gap for usable risk mark (0 disables)
max_market_exposure_usdc: 35.0              # Per-market exposure cap (active with tiered policy)
max_category_exposure_usdc: 210.0           # Category cap placeholder
max_leader_exposure_usdc: 140.0             # Per-leader exposure cap
max_open_positions: 25                      # Open-position cap when tiered policy is off
max_daily_drawdown_pct: 10.0                # Daily loss % BUY stop
enable_balance_tiered_policy: false         # Master switch for headroom_for_buy balance-tiered caps
balance_policy_reserve_pct: 0.40            # Cash reserve % held back from tradable headroom
balance_policy_reserve_floor: 35.0          # Minimum absolute USDC cash reserve
balance_policy_hard_cap_pct: 0.05           # Max % of equity per BUY
balance_policy_hard_cap_ceiling: 17.5       # Absolute USDC ceiling per BUY
balance_policy_max_open_positions: 12       # Open-position cap when tiered policy is on
balance_policy_allow_partial_exits: true
balance_policy_allow_pyramiding: false      # false blocks adding to an already-open position
balance_policy_max_adds_per_position: 1
balance_policy_base_min_score: 0.22
balance_policy_min_notional_usdc: 1.0e-05
balance_policy_dynamic_threshold_raise: false
day_start_equity_usdc: 0.0                  # 0 derives dynamically from runtime equity
risk_price_cache_max_size: 5000
risk_exit_reject_cooldown_seconds: 60.0
risk_pre_reconcile_timeout_seconds: 25.0
risk_pre_reconcile_backoff_seconds: 30.0
risk_check_interval: 6.0                    # Risk-monitor polling interval in seconds
```
</details>

<details>
<summary><strong><code>protective_exits.yml</code></strong> — Automated stop-loss, near-max salvage, profit-lock, trailing, and staged TP</summary>

Defines every automated risk-reducing exit threshold and feature switch. Requires `risk.enable_risk_mgmt: true` to run.

```yaml
# src/pmcopy/config/protective_exits.yml
enable_stop_loss: true                 # Salvage full exit when price falls below stop_loss_threshold
stop_loss_threshold: 0.08              # Absolute token price floor (not drawdown %)
stop_loss_grace_period_seconds: 300.0  # Hold grace period before stop-loss can trigger
enable_near_max_exit: true             # Partial trim at warn price + full close at exit price
near_max_price_warn: 0.95
near_max_price_warn_trim_ratio: 0.6    # Fraction [0, 1] sold on warning trim
near_max_price_exit: 0.99              # Immediate full close threshold (not delayed by grace period)
near_max_grace_period_seconds: 30.0
enable_profit_lock: true               # Realize partial gain at profit_lock_trigger_pct
profit_lock_trigger_pct: 70.0
profit_lock_sell_fraction: 0.5
enable_trailing_stop: false            # Trailing reversal exit
trailing_activation_pct: null          # null = automatic entry-price-aware activation
trailing_distance: 0.05
enable_staged_tp: false                # Multi-level staged take-profit
staged_tp_levels: 0.30,0.50            # CSV gain ratios paired with fractions below
staged_tp_fractions: 0.25,0.35
enable_leader_divergence_exit: false   # Exit when leader reduces/exits position
```
</details>

<details>
<summary><strong><code>market_data.yml</code></strong> — Public orderbook/metadata freshness, REST recovery, and WSS health timers</summary>

Bounds how fresh public market metadata, REST snapshots, and Market WSS quotes/books must be before entries or risk checks trust them.

```yaml
# src/pmcopy/config/market_data.yml
primary_source: rest_snapshot              # rest_snapshot | wss
rest_snapshot_max_age_ms: 1500
rest_snapshot_request_timeout_ms: 1250
rest_snapshot_total_budget_ms: 2000
rest_snapshot_max_spread_bps: 0            # 0 disables spread cap on non-copy REST snapshots
enable_rest_bootstrap: true                # Seed/recover WSS cache via REST
rest_bootstrap_max_age_ms: 500
rest_bootstrap_request_timeout_ms: 750
rest_bootstrap_total_budget_ms: 1000
rest_bootstrap_max_spread_bps: 300
rest_bootstrap_require_wss_connected: true
executable_quote_max_age_seconds: 2.0
executable_book_max_age_seconds: 2.0
executable_tick_max_age_seconds: 300.0
wss_connection_max_silence_seconds: 20.0
subscribe_timeout_seconds: 2.0
```
</details>

<details>
<summary><strong><code>reconciliation.yml</code></strong> — Order confirmation probing, ambiguity auto-release, and position-drift policy</summary>

Governs how `pmcopy` converges local PostgreSQL state with Polymarket venue truth—including startup reconciliation, confirmation probing of `submitted_unconfirmed` orders, and fail-closed drift detection.

```yaml
# src/pmcopy/config/reconciliation.yml
stale_threshold_seconds: 3600.0
auto_cancel_stale: false
max_closed_probes: 10
reconcile_interval_seconds: 300
order_confirmation:
  initial_delay_seconds: 2.0
  poll_interval_seconds: 3.0
  max_wait_seconds: 90.0
  delayed_extra_seconds: 2.0
  token_delay_overrides: {}
  condition_delay_overrides: {}
  auto_release_ambiguous: true
  auto_release_grace_seconds: 300.0
  auto_release_require_all_probes_clean: true
  auto_release_max_probe_errors: 0
  auto_release_retry_backoff_seconds: 30.0
  auto_release_retry_backoff_max_seconds: 900.0
  auto_release_baseline_missing_after_seconds: 86400.0
position_drift:
  check_on_startup: true
  fail_closed: true                         # Keep true in live mode to halt on unresolved drift
  allow_auto_prune_zero: true
  allow_external_position_tracking: false   # true imports manual/off-bot trades; false flags them as drift
  pause_scope_on_unresolved: true
  max_size_epsilon: '0.000001'
  prune_holdoff_seconds: 60.0
```
</details>

<details>
<summary><strong><code>discovery.yml</code></strong> — Falcon Wallet360 quantitative leader discovery, scoring, hysteresis, and bankroll fit</summary>

Drives `pmcopy rotate-traders`, `pmcopy profile-leader`, and `pmcopy run --auto-trader`. Set `PMCOPY__DISCOVERY__API__TOKEN` in `secrets.env` (never in YAML). Note: changing scoring or shortlist parameters invalidates `discovery_config_hash` and triggers a clean one-time roster rebuild.

```yaml
# src/pmcopy/config/discovery.yml (abridged structure — see full file for all 450+ lines of tuning knobs)
api:
  base_url: https://retriever.falconapi.net/api/v2/semantic/retrieve/parameterized
  token: ''                                 # Set PMCOPY__DISCOVERY__API__TOKEN in secrets.env
  timeout_seconds: 20.0
  max_retries: 6
  requests_per_second: 1.0
  burst: 1.0
shortlist:
  min_win_rate_15d: 0.45
  max_win_rate_15d: 0.92
  min_roi_15d: 0.0
  min_pnl_15d: 100.0
  min_total_trades_15d: 30
  max_total_trades_15d: 5000
  sort_modes: [h_score, roi, win_rate, pnl]
  page_size: 100
  max_pages: 5
  candidate_pool_size: 150
wallet360:
  windows: ['1', '3', '7', '15']
  max_drawdown_pct: 35.0
  min_market_diversity: 3
  reject_bot_flagged: true
  max_stake_cv: 3.2
  min_win_rate_consistency: 0.6
recency:
  max_hours_since_last_trade: 48
  min_trades_lookback: 3
  lookback_hours: 168
market:
  min_volume_24h: 5000.0
  min_liquidity_percentile: 40.0
  volume_trend: ALL                         # UP | DOWN | ALL
  max_top1_wallet_pct: 60.0
  min_unique_traders_7d: 25
  min_pass_ratio: 0.6
  max_markets_per_candidate: 8
social:
  enabled: false
  keywords: []
  hours_back: 24
  min_acceleration: 1.0
  min_author_diversity_pct: 20.0
scoring:
  weights:
    falcon_quality: 0.3
    copyability: 0.25
    recent_consistency: 0.15
    market_quality: 0.1
    volume_fit: 0.1
    rank_quality: 0.05
    risk_inverse: 0.05
  runtime_weights:
    roster_score: 0.7
    win_rate: 0.15
    pnl: 0.1
    activity: 0.05
longitudinal:
  enabled: true
  half_life_days: 7.0
  history_days: 90
  pnl_backfill_days: 180
  base_score_weight: 0.7
  longitudinal_score_weight: 0.3
hysteresis:
  promote_score: 0.55
  demote_score: 0.5
  min_tenure_cycles: 3
  min_dwell_seconds: 10800
scheduler:
  enabled: true
  rediscover_seconds: 1200
  rescore_seconds: 7200
  evaluation_concurrency: 2
  max_evaluation_error_ratio: 0.25
  max_active_leaders: 3
  start_from_roster: true
  startup_wait_timeout_seconds: 3900
falcon:
  enabled: false                            # Enabled automatically by --auto-trader
  candidate_limit: 50
  candidate_concurrency: 12
  min_h_score: '60'
  min_liquidity_percentile: '50'
  excluded_categories: [Crypto]
  bankroll:
    reserve_fraction: '0.20'
    max_positions: 12
    max_position_fraction: '0.10'
    target_trade_fraction: '0.08'
    min_order_usdc: '1'
    min_copyable_trade_share: '0.35'
    min_fit_score: '0.45'
```
</details>

<details>
<summary><strong><code>logging.yml</code></strong> — Structured console + rotating JSONL audit log</summary>

Sets log verbosity and rotating JSONL output location. Secret redaction is always active across all handlers.

```yaml
# src/pmcopy/config/logging.yml
level: INFO                                     # DEBUG | INFO | WARNING | ERROR | CRITICAL
log_dir: log
app_log_jsonl_file: log/PolymarketCopyTradingBot.jsonl
log_max_bytes: 5242880                          # 5 MiB per file before rotation
log_backup_count: 3                             # Number of rotated files kept
```
</details>

<details>
<summary><strong><code>redemption.yml</code></strong> — Optional gasless Polymarket Relayer redemption config</summary>

Empty placeholder in YAML by design—Relayer credentials for `pmcopy redeem --execute` must live in `secrets.env` (`PMCOPY__REDEMPTION__RELAYER_API_KEY` and `PMCOPY__REDEMPTION__RELAYER_API_KEY_ADDRESS`). Dry-run previews (`pmcopy redeem --dry-run`) work without credentials.

```yaml
# src/pmcopy/config/redemption.yml
{}
```
</details>

<details>
<summary><strong><code>provider_budget.yml</code></strong> — Local SQLite Alchemy CU usage ledger</summary>

Persists local informational Alchemy compute-unit estimates and billing-cycle day.

```yaml
# src/pmcopy/config/provider_budget.yml
ledger_path: data/provider-budget.sqlite3
limits:
  alchemy_billing_day: 1   # Day of month (1..28/31) when estimated CU cycle resets
```
</details>

<details>
<summary><strong><code>evidence.yml</code></strong> — Optional local durable observation outbox & <code>pmrecon</code> publisher</summary>

Controls whether raw on-chain observations are persisted to a local SQLite outbox and forwarded to an external `pmrecon` ingestion service. Off by default in standalone Lite deployments; set `PMCOPY__EVIDENCE__TOKEN` in `secrets.env` if enabling.

```yaml
# src/pmcopy/config/evidence.yml
enabled: false
outbox_path: data/evidence-outbox.sqlite3
max_events: 10000
max_bytes: 67108864
max_history: 1000000
origin: http://127.0.0.1:9104
timeout_seconds: 5
batch_size: 100
retry_delay_seconds: 5
history_journal_path: null
```
</details>

<details>
<summary><strong><code>environments/&lt;env&gt;/&lt;service&gt;.yml</code></strong> — Sparse environment overlays (<code>paper</code>, <code>live</code>, or custom)</summary>

Environment folders under `src/pmcopy/config/environments/<name>/` override **only** the keys that differ from the base files when `PMCOPY_ENVIRONMENT=<name>` is active. Shipped `environments/live/` overrides include:

- `environments/live/app.yml` — turns on loopback Prometheus metrics (`enable_metrics_server: true`).
- `environments/live/execution.yml` — sets `trading_mode: live`, `reservations_enabled: true`, and disables discretionary book-spread/depth/decision gates in favor of explicit leader-derived FAK bands.
- `environments/live/copy_trading.yml` — sets `copy_tasks.enabled: false` (keeps `traders/*.yml` as the roster while running the account-level pipeline unless flipped to `true`).
- `environments/live/detection.yml` — pins Alchemy live WSS settings (`mode: on_chain_logs_only`, `max_hourly_ws_mb: 250`, `pending_copy_confirmation_enabled: true`).
- `environments/live/risk.yml` — enables continuous risk monitoring (`enable_risk_mgmt: true`, `risk_mark_max_abs_distance: 0.05`).
- `environments/live/protective_exits.yml` — keeps only `enable_stop_loss: true` and `enable_near_max_exit: true` active.
- `environments/live/sizing.yml` — pins `enable_trust_size_multipliers: false`.
- `environments/live/market_data.yml` — tunes REST recovery timeouts (`rest_snapshot_max_age_ms: 1500`).
- `environments/live/discovery.yml` — keeps `falcon.enabled: false` unless `--auto-trader` is passed.

To create a custom environment (e.g. `staging`), copy `src/pmcopy/config/environments/example/` to `src/pmcopy/config/environments/staging/` and run with `PMCOPY_ENVIRONMENT=staging pmcopy run`.
</details>

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
| `pmcopy tasks list \| sync [--prune]` | Inspect persisted PostgreSQL `copy_tasks`, sync edits from `config/traders/*.yml` into DB, and optionally soft-delete orphaned tasks (`--prune`). |
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

### Observability, Logs & Troubleshooting
- **Loopback Health & Metrics** — Bound locally (`127.0.0.1:9102` by default) exposing `/health`, `/health/ready`, and Prometheus `/metrics`.
- **Structured Redacted Logs** — Written to `<deployment-root>/log/PolymarketCopyTradingBot.jsonl`; private keys, DSN passwords, relayer secrets, and WSS tokens are automatically scrubbed across all log sinks.
- **Inspecting Live Copy Decisions & Skips** — Every detected signal flows through structured log events you can grep immediately:
  ```bash
  # Watch real-time detections, copy-task skips (with exact reason codes), and order dispatches
  tail -f log/PolymarketCopyTradingBot.jsonl | grep -E "detection_signal|copy_task_skipped|copy_task_dispatched|order_submitted|fill_observed"
  ```
  Common intentional guard skip reasons: `price_out_of_bounds` (leader fill outside `[min_price, max_price]`), `below_buy_min_amount` (sized USDC below `buy_min_amount`), `spend_cap_exceeded` (task total/daily/market budget reached), `market_filter_rejected`, `reentry_locked`, or `price_limit_exceeds_offset`.

---

## Codebase Architecture (OOP / SOLID, 100% `snake_case`)

`src/pmcopy/` is organized into strict, acyclic subsystem packages (zero loose files, zero `camelCase` module names):

```text
src/pmcopy/
├── domain/          # Pure immutable domain models, exact Decimal math, price bounds, sizing rules (inward-only)
├── interfaces/      # Venue-neutral ports & repository Protocols
├── application/     # TradingContext, runtime state, copy-trade identity & confirmed-signal adapters
├── adapters/        # polygon/ (Alchemy WSS, block timestamps), persistence/, time/ (monotonic/receipt clocks)
├── clients/         # network/ (async HTTP/2, RPC fallback), ws/ (Market, User, Alchemy multiplex WebSockets)
├── integrations/    # polymarket/ (CCXT prediction client/gateway, Data API, relayer), falcon/ (Wallet360)
├── cache/           # Bounded MarketStateStore & lifecycle/staleness snapshots
├── storage/         # postgres/ (asyncpg repos, migrations, copy-task/budget/risk-lot repos), local/ (SQLite)
├── services/        # Cohesive domain services:
│   ├── accounts/    ├── accounting/  ├── compliance/     ├── copy_sizing_v5/  ├── copy_tasks/
│   ├── discovery/   ├── evidence/    ├── execution/      ├── market_data/     ├── observability/
│   ├── onchain_detection/  ├── positions/  ├── providers/  ├── risk/  ├── strategy_profile/
│   ├── telemetry/   └── trading/
├── orchestrators/   # Unified runtime orchestration:
│   ├── app/         ├── bootstrap/   ├── copy_tasks/     ├── detection/       ├── discovery/
│   ├── evidence/    ├── lifecycle/   ├── pipeline/       ├── settlement/      └── streaming/
├── config/          # Strict Pydantic YAML schemas, layered loader, validators, & packaged service YAMLs
├── bot_cli/         # Operator CLI (`pmcopy run | doctor | tasks | trading | reconcile | drift | redeem | ...`)
├── telemetry_ui/    # Local operator telemetry UI helpers
└── utils/           # Secret redaction, structured Rich/JSONL logging (`utils/logging/`), wallet & HTTP helpers
```

---

## Development, Testing & Wheel Verification

```bash
# Default fast unit suite (~5,400+ tests)
.venv/bin/python -m pytest -m "not integration and not release_gate" -q

# Core business-logic statement coverage gate (>= 60% floor)
.venv/bin/python -m coverage erase
.venv/bin/python -m coverage run -m pytest -q
.venv/bin/python -m coverage report --fail-under=60

# Release-gate invariants (AST, signing, startup ordering, packaging safety)
.venv/bin/python -m pytest -m release_gate -q

# Live PostgreSQL 16 integration suite (uses one shared container)
.venv/bin/python -m pytest -m integration -q

# Lint, security scan, type-debt ratchet, and hermetic wheel verification
ruff check .
bandit -r src/pmcopy -ll --skip B101,B311,B608,B108,B310
python scripts/check_mypy_ratchet.py
.venv/bin/python scripts/verify_package.py
```

---

## License & Disclaimer

Released under the **MIT License**.

*This software is provided for educational and experimental purposes only. Prediction market trading carries substantial risk of loss—copying a profitable wallet in the past is not a free money printer in the future. Test thoroughly in `paper` mode first, size conservatively, and trade strictly at your own risk.*
