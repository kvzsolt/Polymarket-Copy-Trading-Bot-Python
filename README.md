# 🔮 Polymarket Copy Trading Bot

![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)
![License MIT](https://img.shields.io/badge/license-MIT-green.svg)
![Polymarket](https://img.shields.io/badge/exchange-Polymarket-blueviolet)

A production-grade Polymarket copy-trading engine built for operators who want serious automation without the chaos.

This bot monitors leader wallets, scores their signals, applies portfolio-aware risk controls, executes copy trades through the Polymarket CLOB, manages exits, logs everything in structured form, and exposes health/metrics endpoints for real deployment.

---

## 💥 Why this exists

Most copy-trading bots on the market are spiritually identical to a raccoon with root access. They blindly mirror leader trades, ignore live price drift, size positions with zero context, and become portfolio arsonists the second the market gets volatile.

This project was built to change that. I wanted a bot that I could trust to run while I sleep. The architecture reflects hard-earned production lessons:

- **🧠 Don’t blindly copy. Score first.**
- **⚖️ Don’t size from vibes. Size from balance, liquidity, and trust.**
- **🛡️ Don’t assume exits are simple. Manage the full lifecycle.**
- **📊 Don’t run dark. Emit structured logs, metrics, health checks, and PnL analytics.**

---

## ⚡ Core Capabilities

* 🔍 **Trader Discovery & Ranking:** Automatically discovers profitable leaders, ranks them, and optionally promotes them to active trading based on rigorous PnL/win-rate criteria.
* 🎯 **Copy Decision Engine:** Reconstructs leader positions, calculates exact percentage sell-offs, and mimics partial exits flawlessly.
* 💰 **Portfolio-Aware Risk Sizing:** Adapts order sizes dynamically based on your available USDC, open exposure to the market/leader, daily drawdown limits, and max position caps.
* 🚧 **Orderbook Quality Gates:** Refuses to execute in thin, manipulated, or spread-gapped orderbooks. 
* 📈 **Profit Lock & Trailing Stops:** Dynamically protects unrealized gains and secures near-max resolution trims.
* 💾 **Idempotency & Reconciliation:** SQLite-backed persistence ensures we never double-copy a signal, even if the node restarts mid-tick.
* 📝 **Structured Audit Logging:** Every decision, skip, fill, and rejection is cleanly logged in JSONL format for analytics.

---

## 🛠️ Stack & Architecture

- **Language:** Python 3.11+
- **Persistence:** SQLite3 (State, fills, positions, orders)
- **Clients:** Polymarket CLOB API, Gamma API
- **Architecture:** Clean DI (Dependency Injection) via Composition Root. Everything is testable, decoupled, and explicit.

---

## 🚀 Getting Started

### 1. Setup Environment
Ensure you have Python 3.11 or higher installed.

```bash
# Create and activate a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configure
Copy the configuration templates to set up your environment:

```bash
cp config/runtime.local.example config/runtime.local.env
cp config/secrets.env.example secrets.env
```

Open `secrets.env` and add your sensitive credentials:
```env
PRIVATE_KEY=0xYourHexPrivateKey
RPC_URL=https://polygon-mainnet.infura.io/v3/YourKey
```

### 3. Run
Launch the bot:
```bash
python3 app.py
```

### 4. Test
Run the test suite to ensure everything is functioning perfectly:
```bash
pytest -q
```

---

## 🤝 Contributing

Contributions are absolutely welcome! Whether it's a bug fix, a new feature, or documentation improvements, we'd love to have your input. Let's build the best copy-trading engine together.

### How to Contribute:
1. **Fork the repository** to your own GitHub account.
2. **Create a feature branch** (`git checkout -b feature/amazing-new-feature`).
3. **Commit your changes** (`git commit -m 'feat: add amazing new feature'`).
4. **Push to the branch** (`git push origin feature/amazing-new-feature`).
5. **Open a Pull Request** against the main repository.

Please ensure your code passes the existing test suite (`pytest`) and adheres to the project's formatting guidelines.
