# Polymarket Copy Trading Bot (Python)

A production-oriented Polymarket copy-trading engine built for operators who want more than a toy "mirror wallet" script.

This bot watches one or more leader wallets, scores their signals, applies portfolio-aware risk controls, executes copy trades through the Polymarket CLOB, manages exits, logs everything in structured form, and exposes health/metrics endpoints for real deployment.

In plain English: this repo is for running a serious copy-trading stack on Polymarket with guardrails.

---

## Why this exists

Most copy-trading bots are spiritually identical to a raccoon with root access:

- they blindly mirror leader trades,
- ignore live price drift,
- size positions with zero context,
- have weak idempotency,
- barely log anything,
- and become portfolio arsonists the second the market gets weird.

This project was clearly built in reaction to those exact pain points.

The architecture reflects hard-earned production lessons:

- **Don’t blindly copy. Score first.**
- **Don’t size from vibes. Size from balance, liquidity, and trust.**
- **Don’t assume exits are simple. Manage the full lifecycle.**
- **Don’t rely on memory. Persist state, fills, positions, and daily equity.**
- **Don’t run dark. Emit logs, metrics, health checks, and analytics.**
- **Don’t optimize for demo-day. Optimize for surviving live trading.**

---

## What the bot does

At a high level, the runtime loop is:

1. Load and validate environment + wallet configuration.
2. Resolve leader wallets to follow.
3. Optionally discover additional leaders from Polymarket analytics data.
4. Build one or more Polymarket CLOB clients.
5. Monitor leader wallet activity in near real time.
6. Aggregate bursty fills into usable signals.
7. Score the leader and the specific trade opportunity.
8. Apply risk headroom, capital caps, and execution constraints.
9. Execute copy trades with idempotency protection.
10. Track positions, P&L, equity, and potential exit triggers.
11. Auto-redeem eligible winning positions.
12. Optionally scan for negative-risk arbitrage opportunities.
13. Emit structured logs, console status, and health/metrics output.

This is not just a trade copier. It is a copy-trading **decision and execution framework**.

---

## Core feature set

### 1) Leader monitoring and copy execution

The bot monitors one or more leader wallets using Polymarket activity data and converts detected activity into copyable signals.

Implemented capabilities include:

- leader activity monitoring
- burst aggregation of same-market microfills
- multi-leader support via `LEADER_WALLETS`
- fallback single-leader mode via `USER_ADDRESS`
- per-leader follow configuration in multi-user mode
- buy/sell handling with copy-specific execution paths

**Why this matters:** raw activity feeds are noisy. Real copy-trading systems need signal normalization before they start touching capital.

---

### 2) Decision-first copy trading

One of the strongest things in this codebase is that it does **not** treat every leader trade as automatically valid.

Before copying a BUY, the bot evaluates:

- the leader’s trust/quality score
- live price drift vs. leader reference price
- edge decay since the original trade
- signal age
- notional/liquidity quality
- minimum executable size
- configured minimum decision score

Key modules:

- `services/copyDecisionService.py`
- `services/leaderScoringService.py`
- `services/tradeScoringService.py`
- `services/tradeAnalyticsService.py`

**Why this was implemented:** blind mirroring is how you end up buying stale entries, late pumps, or trash signals with zero edge left.

---

### 3) Production risk engine and capital policy

This bot has a real risk model instead of a single `max_trade_size` band-aid.

Implemented risk controls include:

- max position percentage caps
- balance-aware headroom checks
- market / category / leader exposure controls
- daily drawdown gating
- open-position count limits
- reserve-cash logic
- dynamic regime-based sizing policy

The repo includes a **balance-tiered regime system**:

- `micro`
- `small`
- `medium`
- `large`

Each regime changes:

- reserve cash
- hard cap ceiling
- max open positions
- whether partial exits are allowed
- whether pyramiding is allowed
- minimum score expectations
- minimum strategy notional

Key module:

- `services/riskEngine.py`

**Why this was implemented:** a $30 wallet and a $30,000 wallet should not behave the same. Treating them identically is how tiny accounts die and bigger accounts become sloppy.

---

### 4) Advanced exit logic

The bot doesn’t stop at entry. It actively manages position exits.

Implemented exit behaviors include:

- hard stop-loss
- take-profit
- near-max-price partial trim / full exit logic
- profit lock logic
- staged take-profit logic
- trailing stop logic
- leader-divergence exit support
- partial and full close tracking

Key module:

- `services/positionManager.py`
- `services/riskMonitor.py`

This is one of the more operator-friendly parts of the system because it acknowledges reality: profit-taking is not one-dimensional.

**Why this was implemented:** binary prediction markets can move fast near resolution and around late repricing. Exit quality matters almost as much as entry quality.

---

### 5) Execution safety and idempotency

Execution is treated as a first-class problem, not an afterthought.

Implemented protections include:

- idempotency keying for submissions
- duplicate submission detection
- market metadata normalization
- min-order-size handling
- tick-size-aware price quantization
- fee-aware execution
- fill confirmation polling
- slippage-constrained execution requests

Key modules:

- `services/executionService.py`
- `services/orderRepository.py`
- `services/marketMetaService.py`
- `services/feeService.py`

**Why this was implemented:** copy-trading bots die from bad execution plumbing long before they die from bad branding.

---

### 6) Multi-user / allocator-style operation

The project supports more than just one wallet copying one trader.

With `MULTI_USER_CONFIG_FILE`, the runtime can load multiple runtime users, each with:

- their own wallet config
- their own follow list
- custom allocation percentage
- custom max trade size
- custom market exposure caps

Key module:

- `services/multiUserConfig.py`

**Why this was implemented:** this moves the bot from "personal script" toward "copy-trading engine" territory.

---

### 7) Trader discovery and leader scoring

This repo does more than accept a hardcoded wallet list. It can enrich and expand its leader universe using external trader analytics.

Implemented discovery/scoring features:

- trader profile lookup
- leaderboard-style discovery
- cached trader profiles
- composite ranking
- win rate / PnL / activity blending
- discovery score integration into leader trust scoring

Key module:

- `services/traderDiscoveryService.py`
- `services/leaderScoringService.py`

**Why this was implemented:** not every high-activity trader is worth following, and not every profitable trader is equally copyable.

---

### 8) Auto-redeem for resolved markets

Winning markets don’t magically settle themselves into clean operator UX.

This bot includes redemption infrastructure for resolved positions:

- scan redeemable positions
- redemption attempt tracking
- EOA flow support
- relayer/proxy-safe abstraction hooks
- auto-redeem background loop

Key modules:

- `services/autoRedeem.py`
- `services/redemptionService.py`

**Why this was implemented:** copy trading without post-resolution redemption is unfinished business.

---

### 9) Negative-risk arbitrage scanning

The repo also contains a separate alpha path: **negative-risk arbitrage scanning**.

Implemented capabilities:

- detect markets where summed best asks across outcomes imply locked-in value
- account for fees in arb viability
- enforce minimum depth
- compute optimal arb size
- optional execution path integration

Key module:

- `services/negRiskArb.py`

**Why this was implemented:** if you already operate on Polymarket infrastructure, it makes sense to monetize structural mispricings too — not just leader flows.

---

### 10) Persistence, accounting, and analytics

This bot is designed to be inspectable after the fact.

Implemented persistence features:

- Mongo-backed accounting ledger (with no-DB fallback)
- orders persistence
- fills persistence
- positions ledger
- daily equity snapshots
- redemption persistence
- SQLite-backed runtime/idempotency state
- append-only P&L JSONL logging

Key modules:

- `services/accountingService.py`
- `services/orderRepository.py`
- `services/pnlTracker.py`
- `config/db.py`
- `utils/idempotency.py`

**Why this was implemented:** if a live trading system can’t explain what it did and why, it’s not a system.

---

### 11) Observability and deployment readiness

This repo is not pretending logs are optional.

Implemented operator tooling:

- Rich console output for live runs
- structured JSONL logs
- health server
- Prometheus-style metrics endpoint
- balance and startup status reporting
- trade analytics counters/gauges

Key modules:

- `utils/logger.py`
- `services/healthServer.py`
- `services/metrics.py`
- `services/tradeAnalyticsService.py`

**Why this was implemented:** because "it should be working" is not an observability strategy.

---

## Architecture overview

```text
Leader wallets / trader discovery
          │
          ▼
Trade monitor  ──► signal aggregation
          │
          ▼
Leader scoring + trade scoring + decision gate
          │
          ▼
Risk engine / balance regime / exposure checks
          │
          ▼
Execution service / fee service / market metadata
          │
          ▼
Position manager / P&L tracker / accounting ledger
          │
          ├──► risk monitor exits
          ├──► auto redeem
          ├──► neg-risk arb scanner
          └──► health + metrics + logs
```

---

## Repository structure

```text
config/
  db.py                  Mongo connection + DB selection
  env.py                 environment parsing and validation
  runtime.env            non-sensitive runtime configuration by subsystem

runtime_env.py           layered dotenv loader (runtime.env + .env)

services/
  tradeMonitor.py        leader wallet monitoring
  tradeExecutor.py       legacy copy-trade path (compat only)
  copyDecisionService.py decision-first copy gating
  tradeScoringService.py trade quality scoring
  leaderScoringService.py leader trust scoring
  traderDiscoveryService.py leader discovery / ranking
  riskEngine.py          portfolio and regime policy
  riskMonitor.py         background exit execution
  positionManager.py     open/closed position lifecycle
  executionService.py    safe execution layer
  feeService.py          fee discovery / caching
  marketMetaService.py   tick/min-order metadata
  pnlTracker.py          append-only P&L ledger
  accountingService.py   Mongo accounting ledger
  autoRedeem.py          background redemption loop
  redemptionService.py   redeemable position handling
  negRiskArb.py          negative-risk arb scanner
  healthServer.py        /health and /metrics endpoints
  multiUserConfig.py     multi-wallet runtime config

storage/sqlite/
  connection.py          SQLite connection factory + PRAGMAs
  migrations.py          centralized schema bootstrap/migrations
  orders_repo.py         idempotent order persistence and dedupe
  fills_repo.py          append-only fills persistence
  positions_repo.py      persisted position reconstruction/update
  cursors_repo.py        leader ingestion cursors
  leaders_repo.py        discovered leader persistence
  redemptions_repo.py    redemption attempt persistence

domain/
  trading_context.py     per-user injected runtime context
  models.py              normalized trading data models

network/
  http_client.py         shared async HTTP client (timeout/retry)
  web3_adapter.py        async wrapper for sync Web3 calls

orchestrators/
  trade_pipeline.py      active copy-trade execution orchestration
  leader_registry.py     configured/discovery leader state machine

legacy/
  README.md              deprecated path quarantine notes

utils/
  createClobClient.py    Polymarket CLOB wrapper
  fetchData.py           HTTP data access helpers
  getMyBalance.py        balance lookup
  idempotency.py         duplicate-signal protection
  logger.py              rich output + JSON logs

tests/
  unit and integration-style coverage for risk, execution, env security, etc.
```

---

## 2026 Safety Refactor Notes

The active runtime path is now orchestration-first and SQLite-backed:

- `app.py` is orchestration-only (context building, registry/monitor loops, service startup).
- Trade execution uses `TradingContext` (`domain/trading_context.py`) and `orchestrators/trade_pipeline.py`.
- Persistence is centralized under `storage/sqlite/*` (no scattered runtime table bootstrap).
- Risk monitoring has a restart-safe persisted path (`risk_monitor_loop_for_contexts`) that rebuilds state from stored positions/fills.

### Storage layer overview

`storage/sqlite/migrations.py` is the single schema bootstrap path and includes:

- `orders` (idempotency key, user/leader IDs, status, request/response payloads, attempts, errors, timestamps)
- `fills` (append-only execution records)
- `positions` (persisted open exposure by `user_id + condition_id`)
- `processed_trades` (cursor/idempotency dedupe support)
- `leader_cursors`
- `discovered_leaders`
- `redemptions`

### TradingContext model

`TradingContext` is a per-user injected runtime container:

- user identity (`user_id`, `wallet_address`)
- execution client
- repositories (`orders_repo`, `fills_repo`, `positions_repo`)
- per-user state/services (`pnl_tracker`, `position_manager`, `risk_engine`, `accounting_service`)
- execution path (`execution_service`)

This removes active-path dependence on module-level mutable singletons.

### Restart and rehydration behavior

- Fills are append-only.
- Open positions are stored in `positions` and can be rebuilt from `fills`.
- Risk checks run from persisted positions and then update persisted fills/positions on exits.
- On context startup, `rehydrate_position_manager(context)` loads persisted state into in-memory helpers.

### Discovery-only mode behavior

`LeaderRegistry` supports:

- `configured` mode (explicit leaders)
- `mixed` mode (configured + discovered candidates)
- `discovery_only` mode (no configured leaders)

When no active leaders are available, the bot stays alive, continues periodic discovery refresh, and emits explicit `no_leaders_monitored` telemetry.

### Migration and schema notes

Schema is auto-managed through `apply_migrations()` during `connect_db()`.

Order persistence now uses conflict-upsert semantics:

- `ON CONFLICT(idempotency_key) DO UPDATE`
- later successful submissions overwrite earlier failed rows
- retry attempts increment `attempt_count`

### Config/default changes

- `ENABLE_LEGACY_POSITION_SNAPSHOT=false` (default) disables legacy monitor snapshots into old model tables.
- `DISCOVERY_REFRESH_INTERVAL_SECONDS` controls periodic registry refresh.
- `DISCOVERY_PROMOTE_TO_ACTIVE` controls discovery candidate promotion into monitored set.

---

## What has been implemented — and why

This section is the "why this repo can sell itself" part.

### Decision-first copy engine
Instead of auto-copying every detected leader trade, the system evaluates whether the trade is still worth taking.

**Why:** leader fills age fast. In thin or fast-moving markets, late followers become exit liquidity.

### Trust-weighted leader selection
Leader quality is scored using wallet balance, activity, historical stats, and discovery enrichment.

**Why:** copying the wrong trader faster just loses money faster.

### Balance-tiered safety policy
Small accounts and larger accounts are treated differently.

**Why:** tiny accounts need stricter preservation logic; larger accounts need smarter scaling, not just bigger buttons.

### Advanced exits instead of dumb static TP/SL only
The system includes staged exits, near-max behavior, profit locks, and trailing logic.

**Why:** binary/prediction markets have unique payoff structures and late-stage repricing behavior. A one-rule exit system leaves money on the table or exits too late.

### Multi-user runtime support
The engine can operate as an allocator across multiple user wallets and follow maps.

**Why:** this evolves the bot from a solo hack into an operator platform.

### Auto-redeem and post-trade settlement tooling
The stack doesn’t stop once a market resolves.

**Why:** realized P&L is what matters, not just open-position screenshots.

### Negative-risk arbitrage path
The repo includes a distinct alpha engine for structural mispricing opportunities.

**Why:** if you already have execution, pricing, and monitoring infrastructure, it is rational to exploit more than one edge source.

### Structured logs, metrics, and accounting
The system is built to explain itself.

**Why:** production trading without auditability is just expensive superstition.

---

## Environment configuration

Environment settings are split by sensitivity:

- `config/runtime.env` → checked-in, non-sensitive runtime configuration grouped by subsystem.
- `config/runtime.local.env` → local non-sensitive overrides for public identities and machine-specific tweaks.
- `.env` → local sensitive values only (private keys, provider/API credentials, secret RPC URLs).
- `.env.example` → sensitive-only template to copy for local setup.
- `config/runtime.local.example` → template for local non-sensitive identity overrides.

Runtime entrypoints load config in this order:
1. `config/runtime.env`
2. `config/runtime.local.env`
3. `.env`

That keeps shared defaults, local public overrides, and secrets in separate lanes.

### Sensitive variables (`.env`)

```env
RPC_URL=
CLOB_API_KEY=
RELAYER_API_KEY=
PRIVATE_KEY=
PRIVATE_KEY_FILE=
```

### Local non-sensitive overrides (`config/runtime.local.env`)

```env
LEADER_WALLETS=
USER_ADDRESS=
FUNDER_ADDRESS=
PROXY_WALLET=
MULTI_USER_CONFIG_FILE=
```

### Non-sensitive variables (`config/runtime.env`)

`config/runtime.env` contains grouped sections with per-setting comments for:

- Polymarket / API
- Trading / Execution
- Risk
- Database / Storage
- Logging / Monitoring
- Feature Flags
- External Services

### Notes

- `config/runtime.local.env` is gitignored on purpose.
- Use `config/runtime.local.example` as the public/local identity template.

- `PRIVATE_KEY_FILE` is supported and validated with permission checks.
- `FUNDER_ADDRESS` is preferred; `PROXY_WALLET` exists as a backward-compatible fallback.
- The environment layer explicitly rejects inline comments in values (`KEY=value # comment` is invalid).
- Booleans are parsed as true only for `1`, `true`, or `yes` (case-insensitive).
- `LEADER_WALLETS` is a comma-separated list (`0x...,0x...`); whitespace around entries is trimmed.
- `LEADER_AUTO=true` is the one-switch auto mode: when `LEADER_WALLETS` and `USER_ADDRESS` are empty, the bot auto-discovers profitable leaders and promotes the top `LEADER_AUTO_COUNT` into live monitoring.
- `PERCENTAGE` still controls how much of each promoted leader trade you copy; `LEADER_AUTO_COUNT` only controls how many leaders are auto-followed.
- Numeric keys are strict base-10 `int`/`float` parses (non-numeric text fails validation).
- `ENV` is an import-time snapshot. Tests that patch `os.environ` after import should call `config.env.rehydrate_env()` (or patch module-level `ENV` directly) before asserting behavior.

---

## Multi-user mode

If you want the runtime to manage multiple funded wallets or multiple per-leader follow policies, set:

```env
MULTI_USER_CONFIG_FILE=path/to/runtime-users.json
```

The runtime loader supports per-user definitions including:

- wallet identity
- signature type
- funder address
- private key
- followed leaders
- allocation percent
- max trade size
- market exposure limits

This makes the repo suitable for desk-style operation, not just personal tinkering.

---

## Running the bot

### Local development

Install dependencies with your preferred toolchain.

Examples:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Or with editable install:

```bash
pip install -e .[dev]
```

Run the bot:

```bash
python app.py
```

---

## Health and metrics

If metrics are enabled:

```env
ENABLE_METRICS_SERVER=true
METRICS_PORT=9102
```

The bot exposes:

- `GET /health`
- `GET /metrics`

This is useful for:

- Docker/Kubernetes health checks
- VPS supervision
- Prometheus scraping
- uptime alerts

---

## Testing

The repository includes a substantial test suite covering:

- env security validation
- DB name extraction and DB behavior
- decision engine behavior
- execution safety
- risk regimes
- position manager behavior
- negative-risk arb logic
- logging behavior
- service-level integration scenarios

Typical command:

```bash
python -m pytest -q
```

If you keep a local venv in-repo:

```bash
.venv/bin/python -m pytest -q
```

---

## Audit snapshot of the current repo

This README was written after auditing the current codebase, not from wishful thinking.

### Strengths

- strong feature surface for a Python Polymarket bot
- real decision-first architecture instead of naive mirroring
- unusually thoughtful risk and exit logic
- idempotency and execution plumbing are present
- observability is much better than average
- multi-user support exists
- auto-redeem and neg-risk arb extend the system beyond basic copy trading

### Weak points / technical debt spotted during audit

- the previous README massively undersold the project
- runtime artifacts still live in the repo directory structure (`data/`, local venvs, logs present locally)
- there is a split persistence model (Mongo + local state files), which is powerful but increases operational complexity
- some integration points are intentionally partial/stubbed, especially around relayer-assisted redemption flows
- the installed global `pytest` command was not available in PATH during audit; local venv test execution worked instead
- the current test suite is large and valuable, but **not fully green** in the audited environment

### Test status observed during audit

Using the local venv runner:

- **268 passed**
- **66 failed**
- **1 skipped**
- **4 deselected**

That means this repo is clearly alive and heavily iterated, but not currently in a perfect green-state release condition.

If you want to sell this repo or present it publicly, that is not fatal — but for production trust, the failing test cluster should eventually be cleaned up.

---

## Who this is for

This repo is a strong fit for:

- Polymarket operators who want copy trading with actual controls
- quant-tinkerers who want a production-leaning foundation instead of starting from scratch
- desks or allocators experimenting with multi-wallet copy systems
- builders who want to combine leader following, risk management, redemption, and arb in one stack

This is **not** ideal if you want:

- a zero-config beginner bot
- a GUI-first consumer app
- a fully productized SaaS dashboard out of the box

This is infrastructure for serious builders.

---

## Positioning statement

If you want the short version:

> **Polymarket Copy Trading Bot (Python)** is a production-oriented copy-trading engine for Polymarket that combines leader discovery, signal scoring, balance-aware risk controls, execution safety, advanced exits, auto-redeem, observability, and optional negative-risk arbitrage in one operator-grade Python codebase.

That is the pitch.

And unlike the usual GitHub fantasy repo, this one actually has the modules to back it up.

---

## Next steps if you want to commercialize or open-source this harder

If you want this repo to sell itself even more effectively, the next leverage moves are:

1. get the test suite back to full green
2. clean runtime artifacts from the repo root
3. add a sample `MULTI_USER_CONFIG_FILE`
4. add one architecture diagram image
5. publish a strategy note comparing:
   - naive copy trading
   - decision-first copy trading
   - copy + risk-tiering
6. add deployment examples for Docker/systemd
7. add real performance screenshots or anonymized telemetry snapshots

Do those, and this repo stops looking like a strong internal engine and starts looking like a serious product foundation.

---

## Disclaimer

This software is for research, automation, and operational tooling purposes.

Prediction-market trading involves market risk, liquidity risk, execution risk, infrastructure risk, and regulatory/jurisdictional constraints. Use geoblocking, capital controls, and proper operational security. Nothing in this repository should be interpreted as financial advice.
