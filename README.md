# 🔮 Polymarket Copy Trading Bot

![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)
![License MIT](https://img.shields.io/badge/license-MIT-green.svg)
![Polymarket](https://img.shields.io/badge/exchange-Polymarket-blueviolet)

A production-grade Polymarket copy-trading ecosystem built for operators who want serious, hands-off automation. 

This is not a naive script that blindly mirrors a wallet. It is a robust algorithmic trading system that actively discovers profitable leaders, sizes trades based on dynamic risk policies, manages complex position lifecycle events, and executes reliably against the Polymarket CLOB.

---

## 💥 Why This Exists

Most copy-trading bots on the market are spiritually identical to a raccoon with root access. They blindly mirror leader trades, ignore live price drift, size positions with zero context, and become portfolio arsonists the second the market gets volatile.

This project was built to change that. I wanted a bot that I could trust to run while I sleep. The architecture reflects hard-earned production lessons:

- **🧠 Don’t blindly copy. Score and reconstruct first.**
- **⚖️ Don’t size from vibes. Size from balance, liquidity, and strict risk limits.**
- **🛡️ Don’t assume exits are simple. Manage the full position lifecycle.**
- **📊 Don’t run dark. Emit structured logs, health checks, and execute true reconciliation.**

---

## ⚙️ The Engines (Under the Hood)

The bot operates through specialized engines, each handling a distinct domain of the trading lifecycle. Here is what they do and why they matter:

### 🔍 Auto Trader Discovery Engine (NEW)
**What it is:** One automated funnel (`services/discovery/`) that shortlists wallets with H-Score, audits Wallet 360 across 1/3/7/15-day windows, validates recent trades and market quality, and builds a decay-weighted longitudinal rank.
**Why it's good:** Every threshold and coefficient lives in `config/discovery.yml`. A PostgreSQL roster applies consecutive-cycle hysteresis before promotion or demotion, and the same persisted rank feeds runtime trade decisions and bounded position sizing.

### 🧠 Copy Decision Engine
**What it is:** The brains behind signal translation (`CopyDecisionService`). It doesn't just buy when a leader buys. It perfectly reconstructs leader portfolios in real-time, calculates exact percentage sell-offs, and mimics partial exits or complex position scaling.
**Why it's good:** Leaders rarely enter and exit 100% of a position at once. A naive bot will buy the top and hold to zero. This engine ensures you scale out exactly when the leader trims their position, locking in profits flawlessly.

### 🛡️ Risk & Portfolio Engine
**What it is:** The financial firewall (`RiskEngine` & `balance_regime.py`). It enforces portfolio-aware sizing, daily maximum drawdown limits, position caps, and global kill switches.
**Why it's good:** It prevents you from getting wrecked. If a copied trader suddenly goes on tilt or makes a massive, illiquid bet, the Risk Engine overrides the signal, dynamically scales down your exposure, or rejects the trade entirely to protect your principal capital.

### ⚡ Execution & Reconciliation Engine
**What it is:** The execution layer connecting to the Polymarket CLOB V2 (`ExecutionService` & `ReconciliationService`). It handles asynchronous order submission, collateral readiness, and strict idempotency backed by PostgreSQL.
**Why it's good:** Network failures happen. WebSockets drop. This engine ensures institutional-grade reliability—meaning the bot will *never* double-copy a signal or lose track of a fill, even if the node crashes mid-execution. It reconciles expected states with on-chain reality at every startup.

---

## ⚡ Core Capabilities

* 🔍 **Trader Discovery & Ranking:** Automatically audits leaders across multiple windows, tracks longitudinal evidence, and stabilizes the active roster with hysteresis.
* 🎯 **Copy Decision Engine:** Reconstructs leader positions, calculates exact percentage sell-offs, and mimics partial exits flawlessly.
* 💰 **Portfolio-Aware Risk Sizing:** Adapts order sizes dynamically based on your available USDC, open exposure to the market/leader, daily drawdown limits, and max position caps.
* 🚧 **Orderbook Quality Gates:** Refuses to execute in thin, manipulated, or spread-gapped orderbooks. 
* 📈 **Profit Lock & Near-One Redemptions:** Dynamically protects unrealized gains and secures near-max resolution trims natively.
* 💾 **Idempotency & Reconciliation:** PostgreSQL-backed persistence ensures we never double-copy a signal, even if the node restarts mid-tick.
* 📝 **Structured Audit Logging:** Every decision, skip, fill, and rejection is cleanly logged in JSONL format for downstream analytics.

---

## 🛠️ Stack & Architecture

- **Language:** Python 3.11+ (3.13 unsupported).
- **Persistence:** PostgreSQL (positions, orders, fills, intents, reservations) — schema-versioned migrations.
- **Venue:** official unified `polymarket-client` SDK, pinned and isolated behind the typed gateway.
- **Composition root:** `bootstrap.py::build_app` validates config, initializes PostgreSQL, and wires the shared graph; `app.py::main` adds per-user runtimes.
- **Execution:** `interfaces.clob_gateway.ClobGateway` (port) → `integrations.polymarket.clob_v2_gateway` (adapter) → `services.executionService.ExecutionService` + `services/execution/` subsystem.
- **Coordination:** canonical `domain.position_key.PositionKey` everywhere; `services.positionActionCoordinator.PositionActionCoordinator` serialises per-position actions at runtime.
- **Reconciliation:** `ReconciliationService` owns terminal settlement; startup reconciliation runs before risk and trade loops.
- **Market data:** REST book snapshots are execution authority; market/user WebSockets maintain background state, subscriptions, and lifecycle events. Decisions fail closed on stale metadata.
- **Config:** immutable `config.app_config.AppConfig`, layered from YAML, mode-0600 secrets, and nested process overrides.
- **Observability:** `services.healthServer` exposes `/health`, `/ready`, `/metrics`; structured JSONL audit logs.
- **Operator CLI:** `bot.py` → `bot_cli.main` → `doctor`, `config`, `reservations`, `stale-locks`, `drift`.

---

## 📖 Operator Documentation

Read these in order:

1. [`docs/00-read-this-first.md`](docs/00-read-this-first.md) — what the bot is and is not.
2. [`docs/02-configuration.md`](docs/02-configuration.md) — full typed-config reference.
3. [`docs/03-paper-trading.md`](docs/03-paper-trading.md) → [`docs/04-live-trading.md`](docs/04-live-trading.md) — paper soak then live cutover.
4. [`docs/runbooks/00-index.md`](docs/runbooks/00-index.md) — incident runbooks (CLOB outage, WSS outage, circuit breaker open, stale locks, reconciliation drift, manual / off-bot drift).
5. [`docs/architecture.md`](docs/architecture.md) — canonical architecture overview.

---

## 🚀 Getting Started

### 1. Setup Environment
Ensure you have Python 3.11 or higher installed (3.13 is currently unsupported).

**Using `uv` (Recommended):**
```bash
uv venv
source .venv/bin/activate
uv sync --frozen --extra dev
```

**Using standard `pip`:**
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure
Configuration is service-owned: one canonical `config/<service>.yml` per
service (`app`, `database`, `wallet`, `polymarket`, `execution`, `detection`,
`discovery`, `copy_trading`, `sizing`, `risk`, `protective_exits`,
`market_data`, `reconciliation`, `logging`). Environment-specific overrides
live in `config/environments/<name>/<service>.yml` and are selected
explicitly with `PMCOPY_ENVIRONMENT=<name>` (default: `paper`; see
`config/environments/example/execution.yml` for a template). Per-leader
overrides live in `config/leaders/<wallet>.yml`.

```bash
cp secrets.env.example secrets.env
chmod 600 secrets.env
```
Open `secrets.env` and add only sensitive nested settings:
```env
POSTGRES_PASSWORD=choose-a-unique-database-password
PMCOPY__WALLET__PRIVATE_KEY=0xYourHexPrivateKey
PMCOPY__DATABASE__DSN=postgresql://bot:password@localhost/polymarket_copytrading_python
```
The secrets path is resolved from the deployment root (`PMCOPY_DEPLOYMENT_ROOT`
or marker walk-up), so launch `pmcopy` from this deployment directory. An
existing `config/runtime.local.yml` from an older deployment is still read
through a deprecated compatibility shim (legacy section names are translated);
migrate its values to `config/environments/<name>/<service>.yml`.

**Migrating an existing live deployment:** the previous `runtime.local.yml`
live values are now tracked in `config/environments/live/` (`trading_mode:
live`, the leader roster, the metrics endpoint). Export
`PMCOPY_ENVIRONMENT=live` before starting the bot, verify with
`pmcopy doctor --live`, then delete the old `config/runtime.local.yml` —
until it is deleted the deprecated shim keeps honouring it (environment
files take precedence).
Keep `execution.trading_mode: paper` through validation. Live startup requires
`execution.trading_mode: live` **and** a separate database-backed resume command.
The private key is the only Polymarket auth secret. The unified SDK uses it for
L1 credential create-or-derive, keeps the resulting L2 HMAC credentials in
memory, and derives the Deposit Wallet used with signature type 3. Do not add
CLOB API credentials or a signature type to `secrets.env`.

### 3. Run
Launch the bot using the Operator CLI.

**Native Execution:**
```bash
# Activate virtual environment if not already active
source .venv/bin/activate

# Run using the configured fail-closed mode
pmcopy run
```

**Docker Execution:**
```bash
docker compose up -d
```

### 4. CLI Tools & Testing
The bot includes an Operator CLI for manual interventions and checks:
```bash
source .venv/bin/activate
pmcopy doctor
pmcopy drift
pmcopy rotate-traders
pmcopy trading status
```
Run the test suite to ensure everything is functioning perfectly:
```bash
pytest -q
```

---

## 🐛 Bug Reports

If you encounter a bug, unexpected behavior, or a crash, please help improve the bot by opening a [GitHub Issue](https://github.com/kvzsolt/Polymarket-Copy-Trading-Bot-Python/issues/new). Include as much detail as possible:

- Steps to reproduce the issue
- Expected vs. actual behavior
- Relevant log snippets (redact any sensitive data like private keys or wallet addresses)
- Your Python version and OS

Bug reports are the fastest way to help this project improve — every report matters.

---

## 🤝 Contributing

Contributions are absolutely welcome! Whether it's a bug fix, a new feature, or documentation improvements, we'd love to have your input. Let's build the best copy-trading engine together.

1. **Fork the repository** 
2. **Create a feature branch** (`git checkout -b feature/amazing-new-feature`)
3. **Commit your changes** (`git commit -m 'feat: add amazing new feature'`)
4. **Push to the branch** (`git push origin feature/amazing-new-feature`)
5. **Open a Pull Request**

---

## 🗺️ Roadmap
**Trade detection via blockchain transactions**
**Improve execution flow**
**Fix known bugs, like risk management timeout**
**Longitudinal trader discovery and stable roster selection**
**Simplify codebase, refactor**

## Rust version
A version written in Rust, with all functions, logic, mechanism ported is coming soon, with much faster detection and execution time, make it able to copy nearly every type of traders.
---
*Disclaimer: This software is for educational and experimental purposes only. Trading involves risk, and this system does not guarantee profit. You can and may lose money. Run at your own risk.*
