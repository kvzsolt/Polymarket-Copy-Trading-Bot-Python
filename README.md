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
**What it is:** A fully automated subsystem (`services/discovery/`) that continuously ingests wallet data from Falcon and Polymarket history. It scores candidates based on PnL, win rates, and trading frequency. 
**Why it's good:** You no longer need to manually hunt for profitable wallets. The engine includes a **Copyability Replay Service** which simulates whether a trader's signals can actually be executed against your specific budget and risk parameters. The **Rotation Service** then automatically promotes hidden alphas to your active leader set and demotes failing wallets before they drain your portfolio.

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

* 🔍 **Trader Discovery & Ranking:** Automatically discovers profitable leaders, ranks them, and simulates their copyability against your actual budget.
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
- **Venue:** Polymarket CLOB V2 (`py_clob_client_v2`); requires `CLOB_V2_READY=true`.
- **Composition root:** `bootstrap.py::build_app` (pure, no I/O) wires the shared graph; `app.py::main` adds async / per-user wiring.
- **Execution:** `interfaces.clob_gateway.ClobGateway` (port) → `integrations.polymarket.clob_v2_gateway` (adapter) → `services.executionService.ExecutionService` + `services/execution/` subsystem.
- **Coordination:** canonical `domain.position_key.PositionKey` everywhere; `services.positionActionCoordinator.PositionActionCoordinator` serialises per-position actions at runtime.
- **Reconciliation:** `ReconciliationService` owns terminal settlement; startup reconciliation runs before risk and trade loops.
- **Market data:** `clients/market_ws.py` + `clients/user_ws.py` with 5s PING, REST snapshot on reconnect, `assets_ids` subscriptions; TTL-based price caches. Decisions fail closed on stale data.
- **Config:** typed `config.app_config.AppConfig`; production paths fail closed without it.
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
uv pip install -r requirements.txt
```

**Using standard `pip`:**
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure
Copy the configuration templates to set up your environment:

```bash
cp config/runtime.local.example config/runtime.local.env
```
*(Note: Create a `secrets.env` file to hold your credentials, as expected by docker-compose and the bot.)*

Open `secrets.env` and add your sensitive credentials:
```env
PRIVATE_KEY=0xYourHexPrivateKey
RPC_URL=https://polygon-mainnet.g.alchemy.com/v2/YourKey
```
Ensure your `config/runtime.local.env` has `LIVE_TRADING=enabled` and `PAPER_TRADE_ONLY=false` if you want to trade live.

### 3. Run
Launch the bot using the Operator CLI.

**Native Execution:**
```bash
# Activate virtual environment if not already active
source .venv/bin/activate

# Launch the bot in live mode
python bot.py run --live
```

**Docker Execution:**
```bash
docker compose up -d
```

### 4. CLI Tools & Testing
The bot includes an Operator CLI for manual interventions and checks:
```bash
source .venv/bin/activate
python bot.py doctor
python bot.py drift
python bot.py rotate-traders
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
**Trader discovery V3**
**Simplify codebase, refactor**

## Rust version
A version written in Rust, with all functions, logic, mechanism ported is coming soon, with much faster detection and execution time, make it able to copy nearly every type of traders.
---
*Disclaimer: This software is for educational and experimental purposes only. Trading involves risk, and this system does not guarantee profit. You can and may lose money. Run at your own risk.*
